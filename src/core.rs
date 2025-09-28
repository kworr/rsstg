use crate::{
	command,
	sql::Db,
};

use std::{
	borrow::Cow,
	collections::{
		BTreeMap,
		HashSet,
	},
};

use async_std::{
	task,
	sync::{
		Arc,
		Mutex
	},
};
use chrono::{
	DateTime,
	Local,
};
use lazy_static::lazy_static;
use regex::Regex;
use reqwest::header::{
	CACHE_CONTROL,
	EXPIRES,
	LAST_MODIFIED
};
use tgbot::{
	api::Client,
	handler::UpdateHandler,
	types::{
		Bot,
		ChatPeerId,
		Command,
		GetBot,
		Message,
		ParseMode,
		SendMessage,
		Update,
		UpdateType,
		UserPeerId,
	},
};
use stacked_errors::{
	Result,
	StackableErr,
	anyhow,
	bail,
};

lazy_static!{
	pub static ref RE_SPECIAL: Regex = Regex::new(r"([\-_*\[\]()~`>#+|{}\.!])").unwrap();
}

/// Encodes special HTML entities to prevent them interfering with Telegram HTML
pub fn encode (text: &str) -> Cow<'_, str> {
	RE_SPECIAL.replace_all(text, "\\$1")
}

#[derive(Clone)]
pub struct Core {
	owner_chat: ChatPeerId,
	// max_delay: u16,
	pub tg: Client,
	pub me: Bot,
	pub db: Db,
	sources: Arc<Mutex<HashSet<Arc<i32>>>>,
	http_client: reqwest::Client,
}

impl Core {
	pub async fn new(settings: config::Config) -> Result<Core> {
		let owner_chat = ChatPeerId::from(settings.get_int("owner").stack()?);
		let api_key = settings.get_string("api_key").stack()?;
		let tg = Client::new(&api_key).stack()?;

		let mut client = reqwest::Client::builder();
		if let Ok(proxy) = settings.get_string("proxy") {
			let proxy = reqwest::Proxy::all(proxy).stack()?;
			client = client.proxy(proxy);
		}
		let http_client = client.build().stack()?;
		let me = tg.execute(GetBot).await.stack()?;
		let core = Core {
			tg,
			me,
			owner_chat,
			db: Db::new(&settings.get_string("pg").stack()?)?,
			sources: Arc::new(Mutex::new(HashSet::new())),
			http_client,
			// max_delay: 60,
		};
		let clone = core.clone();
		task::spawn(async move {
			loop {
				let delay = match &clone.autofetch().await {
					Err(err) => {
						if let Err(err) = clone.send(format!("🛑 {err}"), None, None).await {
							eprintln!("Autofetch error: {err:?}");
						};
						std::time::Duration::from_secs(60)
					},
					Ok(time) => *time,
				};
				task::sleep(delay).await;
			}
		});
		Ok(core)
	}

	pub async fn send <S>(&self, msg: S, target: Option<ChatPeerId>, mode: Option<ParseMode>) -> Result<Message>
	where S: Into<String> {
		let msg = msg.into();

		let mode = mode.unwrap_or(ParseMode::Html);
		let target = target.unwrap_or(self.owner_chat);
		self.tg.execute(
			SendMessage::new(target, msg)
				.with_parse_mode(mode)
		).await.stack()
	}

	pub async fn check (&self, id: i32, real: bool, last_scrape: Option<DateTime<Local>>) -> Result<String> {
		let mut posted: i32 = 0;
		let mut conn = self.db.begin().await.stack()?;

		let id = {
			let mut set = self.sources.lock_arc().await;
			match set.get(&id) {
				Some(id) => id.clone(),
				None => {
					let id = Arc::new(id);
					set.insert(id.clone());
					id.clone()
				},
			}
		};
		let count = Arc::strong_count(&id);
		if count == 2 {
			let source = conn.get_source(*id, self.owner_chat).await.stack()?;
			conn.set_scrape(*id).await.stack()?;
			let destination = ChatPeerId::from(match real {
				true => source.channel_id,
				false => source.owner,
			});
			let mut this_fetch: Option<DateTime<chrono::FixedOffset>> = None;
			let mut posts: BTreeMap<DateTime<chrono::FixedOffset>, String> = BTreeMap::new();

			let mut builder = self.http_client.get(&source.url);
			if let Some(last_scrape) = last_scrape {
				builder = builder.header(LAST_MODIFIED, last_scrape.to_rfc2822());
			};
			let response = builder.send().await.stack()?;
			{
				let headers = response.headers();
				let expires = headers.get(EXPIRES);
				let cache = headers.get(CACHE_CONTROL);
				if expires.is_some() || cache.is_some() {
					println!("{} {} {:?} {:?} {:?}", Local::now().to_rfc2822(), &source.url, last_scrape, expires, cache);
				}
			}
			let status = response.status();
			let content = response.bytes().await.stack()?;
			match rss::Channel::read_from(&content[..]) {
				Ok(feed) => {
					for item in feed.items() {
						if let Some(link) = item.link() {
							let date = match item.pub_date() {
								Some(feed_date) => DateTime::parse_from_rfc2822(feed_date),
								None => DateTime::parse_from_rfc3339(&item.dublin_core_ext().unwrap().dates()[0]),
							}.stack()?;
							let url = link;
							posts.insert(date, url.to_string());
						}
					};
				},
				Err(err) => match err {
					rss::Error::InvalidStartTag => {
						match atom_syndication::Feed::read_from(&content[..]) {
							Ok(feed) => {
								for item in feed.entries() {
									let date = item.published().unwrap();
									let url = item.links()[0].href();
									posts.insert(*date, url.to_string());
								};
							},
							Err(err) => {
								bail!("Unsupported or mangled content:\n{:?}\n{err}\n{status:#?}\n", &source.url)
							},
						}
					},
					rss::Error::Eof => (),
					_ => bail!("Unsupported or mangled content:\n{:?}\n{err}\n{status:#?}\n", &source.url)
				}
			};
			for (date, url) in posts.iter() {
				let post_url: Cow<str> = match source.url_re {
					Some(ref x) => sedregex::ReplaceCommand::new(x).stack()?.execute(url),
					None => url.into(),
				};
				if let Some(exists) = conn.exists(&post_url, *id).await.stack()? {
					if ! exists {
						if this_fetch.is_none() || *date > this_fetch.unwrap() {
							this_fetch = Some(*date);
						};
						self.send( match &source.iv_hash {
							Some(hash) => format!("<a href=\"https://t.me/iv?url={post_url}&rhash={hash}\"> </a>{post_url}"),
							None => format!("{post_url}"),
						}, Some(destination), Some(ParseMode::Html)).await.stack()?;
						conn.add_post(*id, date, &post_url).await.stack()?;
					};
				};
				posted += 1;
			};
			posts.clear();
		};
		Ok(format!("Posted: {posted}"))
	}

	async fn autofetch(&self) -> Result<std::time::Duration> {
		let mut delay = chrono::Duration::minutes(1);
		let now = chrono::Local::now();
		let queue = {
			let mut conn = self.db.begin().await.stack()?;
			conn.get_queue().await.stack()?
		};
		for row in queue {
			if let Some(next_fetch) = row.next_fetch {
				if next_fetch < now {
					if let (Some(owner), Some(source_id), last_scrape) = (row.owner, row.source_id, row.last_scrape) {
						let clone = Core {
							owner_chat: ChatPeerId::from(owner),
							..self.clone()
						};
						let source = {
							let mut conn = self.db.begin().await.stack()?;
							match conn.get_one(owner, source_id).await {
								Ok(Some(source)) => source.to_string(),
								Ok(None) => "Source not found in database.stack()?".to_string(),
								Err(err) => format!("Failed to fetch source data:\n{err}"),
							}
						};
						task::spawn(async move {
							if let Err(err) = clone.check(source_id, true, Some(last_scrape)).await {
								if let Err(err) = clone.send(&format!("{source}\n\n🛑 {}", encode(&err.to_string())), None, Some(ParseMode::MarkdownV2)).await {
									eprintln!("Check error: {err:?}");
									// clone.disable(&source_id, owner).await.unwrap();
								};
							};
						});
					}
				} else if next_fetch - now < delay {
					delay = next_fetch - now;
				}
			}
		};
		delay.to_std().stack()
	}

	pub async fn list (&self, owner: UserPeerId) -> Result<String> {
		let mut reply: Vec<String> = vec![];
		reply.push("Channels:".into());
		let mut conn = self.db.begin().await.stack()?;
		for row in conn.get_list(owner).await.stack()? {
			reply.push(row.to_string());
		};
		Ok(reply.join("\n\n"))
	}
}

impl UpdateHandler for Core {
	async fn handle (&self, update: Update) {
		if let UpdateType::Message(msg) = update.update_type {
			if let Ok(cmd) = Command::try_from(msg) {
				let msg = cmd.get_message();
				let words = cmd.get_args();
				let command = cmd.get_name();
				let res = match command {
					"/check" | "/clean" | "/enable" | "/delete" | "/disable" => command::command(self, command, msg, words).await,
					"/start" => command::start(self, msg).await,
					"/list" => command::list(self, msg).await,
					"/add" | "/update" => command::update(self, command, msg, words).await,
					any => Err(anyhow!("Unknown command: {any}")),
				};
				if let Err(err) = res {
					if let Err(err2) = self.send(format!("\\#error\n```\n{err}\n```"),
						Some(msg.chat.get_id()),
						Some(ParseMode::MarkdownV2)
					).await{
						dbg!(err2);
					};
				}
			};
		};
	}
}
