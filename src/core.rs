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
	num::TryFromIntError,
	sync::{
		Arc,
		Mutex
	},
};

use anyhow::{
	anyhow,
	bail,
	Result,
};
use async_std::task;
use chrono::DateTime;
use frankenstein::{
	AsyncTelegramApi,
	Error as FrankError,
	ParseMode,
	client_reqwest::Bot,
	methods::{
		GetUpdatesParams,
		SendMessageParams
	},
	types::{
		AllowedUpdate,
		MessageEntityType,
		User,
	},
	updates::UpdateContent,
};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum RssError {
	// #[error(transparent)]
	// Tg(#[from] TgError),
	#[error(transparent)]
	Int(#[from] TryFromIntError),
}

#[derive(Clone)]
pub struct Core {
	owner_chat: i64,
	max_delay: u16,
	pub tg: Bot,
	pub me: User,
	pub db: Db,
	sources: Arc<Mutex<HashSet<Arc<i32>>>>,
	http_client: reqwest::Client,
}

impl Core {
	pub async fn new(settings: config::Config) -> Result<Core> {
		let owner_chat = settings.get_int("owner")?;
		let api_key = settings.get_string("api_key")?;
		let tg = Bot::new(&api_key);

		let mut client = reqwest::Client::builder();
		if let Ok(proxy) = settings.get_string("proxy") {
			let proxy = reqwest::Proxy::all(proxy)?;
			client = client.proxy(proxy);
		}
		let http_client = client.build()?;
		let me = tg.get_me().await?;
		let me = me.result;
		let core = Core {
			tg,
			me,
			owner_chat,
			db: Db::new(&settings.get_string("pg")?)?,
			sources: Arc::new(Mutex::new(HashSet::new())),
			http_client,
			max_delay: 60,
		};
		let mut clone = core.clone();
		task::spawn(async move {
			loop {
				let delay = match &clone.autofetch().await {
					Err(err) => {
						if let Err(err) = clone.send(format!("🛑 {err:?}"), None, None).await {
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

	pub async fn stream(&mut self) -> Result<()> {
		let mut offset: i64 = 0;
		let mut params = GetUpdatesParams {
			offset: None,
			limit: Some(100),
			timeout: Some(300),
			allowed_updates: Some(vec![AllowedUpdate::Message]),
		};
		loop {
			let updates = self.tg.get_updates(&params).await?.result;
			if updates.is_empty() {
				offset = 0;
				params.offset = None;
				continue;
			}
			for update in updates {
				if i64::from(update.update_id) >= offset {
					offset = i64::from(update.update_id) + 1;
					params.offset = Some(offset);
				}
				if let UpdateContent::Message(msg) = update.content {
					if let Some(text) = msg.text {
						if let Some(entities) = msg.entities {
							let chars: Vec<u16> = text.encode_utf16().collect();
							for entity in entities {
								if entity.type_field == MessageEntityType::BotCommand && entity.offset != 0 {
									bail!("commands should be at message start");
								};
								let cmd = String::from_utf16_lossy(&chars[entity.offset as usize..entity.length as usize]);
								let words: Vec<&str> = text.split_whitespace().collect();
								let res = match cmd.as_ref() {
									"/check" | "/clean" | "/enable" | "/delete" | "/disable" => command::command(self, msg.chat.id, words).await,
									"/start" => command::start(self, msg.chat.id).await,
									"/list" => command::list(self, msg.chat.id).await,
									"/add" | "/update" => command::update(self, msg.chat.id, words).await,
									any => Err(anyhow!("Unknown command: {any}")),
								};
								if let Err(err) = res {
									if let Err(err2) = self.send(format!("\\#error\n```\n{err:?}\n```"),
										Some(msg.chat.id),
										Some(ParseMode::MarkdownV2)
									).await{
										dbg!(err2);
									};
								}
							};
						};
					};
				};
			}
		}
	}

	pub async fn send <S>(&self, msg: S, target: Option<i64>, mode: Option<ParseMode>) -> Result<()>
	where S: Into<String> {
		let msg = msg.into();

		let mode = mode.unwrap_or(ParseMode::Html);
		let target = target.unwrap_or(self.owner_chat);
		let send = SendMessageParams::builder()
			.chat_id(target)
			.text(msg)
			.parse_mode(mode)
			.build();
		loop {
			match self.tg.send_message(&send).await {
				Ok(_) => break,
				Err(err) => match err {
					FrankError::Api(ref resp) => {
						if resp.error_code == 429 {
							let mut my_delay = self.max_delay;
							if let Some(params) = resp.parameters {
								if let Some(delay) = params.retry_after {
									if delay < my_delay {
										my_delay = delay;
									}
								}
							}
							task::sleep(std::time::Duration::from_secs(my_delay.into())).await;
						} else {
							return Err(err.into());
						}
					},
					_ => return Err(err.into()),
				},
			}
		}
		Ok(())
	}

	pub async fn check (&mut self, id: &i32, owner: i64, real: bool) -> Result<String> {
		let mut posted: i32 = 0;
		let mut conn = self.db.begin().await?;

		let id = {
			let mut set = self.sources.lock().unwrap();
			match set.get(id) {
				Some(id) => id.clone(),
				None => {
					let id = Arc::new(*id);
					set.insert(id.clone());
					id.clone()
				},
			}
		};
		let count = Arc::strong_count(&id);
		if count == 2 {
			let source = conn.get_source(*id, owner).await?;
			conn.set_scrape(*id).await?;
			let destination = match real {
				true => source.channel_id,
				false => source.owner,
			};
			let mut this_fetch: Option<DateTime<chrono::FixedOffset>> = None;
			let mut posts: BTreeMap<DateTime<chrono::FixedOffset>, String> = BTreeMap::new();

			let response = self.http_client.get(&source.url).send().await?;
			let status = response.status();
			let content = response.bytes().await?;
			match rss::Channel::read_from(&content[..]) {
				Ok(feed) => {
					for item in feed.items() {
						if let Some(link) = item.link() {
							let date = match item.pub_date() {
								Some(feed_date) => DateTime::parse_from_rfc2822(feed_date),
								None => DateTime::parse_from_rfc3339(&item.dublin_core_ext().unwrap().dates()[0]),
							}?;
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
								bail!("Unsupported or mangled content:\n{:?}\n{err:#?}\n{status:#?}\n", &source.url)
							},
						}
					},
					rss::Error::Eof => (),
					_ => bail!("Unsupported or mangled content:\n{:?}\n{err:#?}\n{status:#?}\n", &source.url)
				}
			};
			for (date, url) in posts.iter() {
				let post_url: Cow<str> = match source.url_re {
					Some(ref x) => sedregex::ReplaceCommand::new(x)?.execute(url),
					None => url.into(),
				};
				if let Some(exists) = conn.exists(&post_url, *id).await? {
					if ! exists {
						if this_fetch.is_none() || *date > this_fetch.unwrap() {
							this_fetch = Some(*date);
						};
						self.send( match &source.iv_hash {
							Some(hash) => format!("<a href=\"https://t.me/iv?url={post_url}&rhash={hash}\"> </a>{post_url}"),
							None => format!("{post_url}"),
						}, Some(destination), Some(ParseMode::Html)).await?;
						conn.add_post(*id, date, &post_url).await?;
					};
				};
				posted += 1;
			};
			posts.clear();
		};
		Ok(format!("Posted: {posted}"))
	}

	async fn autofetch(&mut self) -> Result<std::time::Duration> {
		let mut delay = chrono::Duration::minutes(1);
		let now = chrono::Local::now();
		let mut conn = self.db.begin().await?;
		for row in conn.get_queue().await? {
			if let Some(next_fetch) = row.next_fetch {
				if next_fetch < now {
					if let (Some(owner), Some(source_id)) = (row.owner, row.source_id) {
						let mut clone = Core {
							owner_chat: owner,
							..self.clone()
						};
						task::spawn(async move {
							if let Err(err) = clone.check(&source_id, owner, true).await {
								if let Err(err) = clone.send(&format!("🛑 {err:?}"), None, None).await {
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
		Ok(delay.to_std()?)
	}

	pub async fn list (&mut self, owner: i64) -> Result<String> {
		let mut reply: Vec<Cow<str>> = vec![];
		reply.push("Channels:".into());
		let mut conn = self.db.begin().await?;
		for row in conn.get_list(owner).await? {
			reply.push(format!("\n\\#️⃣ {} \\*️⃣ `{}` {}\n🔗 `{}`", row.source_id, row.channel,
				match row.enabled {
					true  => "🔄 enabled",
					false => "⛔ disabled",
				}, row.url).into());
			if let Some(hash) = &row.iv_hash {
				reply.push(format!("IV: `{hash}`").into());
			}
			if let Some(re) = &row.url_re {
				reply.push(format!("RE: `{re}`").into());
			}
		};
		Ok(reply.join("\n"))
	}
}
