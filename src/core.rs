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
	sync::Arc,
};

use async_compat::Compat;
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
use smol::{
	Timer,
	lock::Mutex,
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

/// Escape characters that are special in Telegram MarkdownV2 by prefixing them with a backslash.
///
/// This ensures the returned string can be used as MarkdownV2-formatted Telegram message content
/// without special characters being interpreted as MarkdownV2 markup.
pub fn encode (text: &str) -> Cow<'_, str> {
	RE_SPECIAL.replace_all(text, "\\$1")
}

// This one does nothing except making sure only one token exists for each id
pub struct Token {
	running: Arc<Mutex<HashSet<i32>>>,
	my_id: i32,
}

impl Token {
	/// Attempts to acquire a per-id token by inserting `my_id` into the shared `running` set.
	///
	/// If the id was not already present, the function inserts it and returns `Some(Token)`.
	/// When the returned `Token` is dropped, the id will be removed from the `running` set,
	/// allowing subsequent acquisitions for the same id.
	///
	/// # Parameters
	///
	/// - `running`: Shared set tracking active ids.
	/// - `my_id`: Identifier to acquire a token for.
	///
	/// # Returns
	///
	/// `Ok(Token)` if the id was successfully acquired, `Error` if a token for the id is already active.
	async fn new (running: &Arc<Mutex<HashSet<i32>>>, my_id: i32) -> Result<Token> {
		let running = running.clone();
		let mut set = running.lock_arc().await;
		if set.contains(&my_id) {
			bail!("Token already taken");
		} else {
			set.insert(my_id);
			Ok(Token {
				running,
				my_id,
			})
		}
	}
}

impl Drop for Token {
	/// Releases this token's claim on the shared running-set when the token is dropped.
	///
	/// The token's identifier is removed from the shared `running` set so that future
	/// operations for the same id may proceed.
	///
	/// TODO: is using block_on inside block_on safe? Currently tested and working fine.
	fn drop (&mut self) {
		smol::block_on(async {
			let mut set = self.running.lock_arc().await;
			set.remove(&self.my_id);
		})
	}
}

#[derive(Clone)]
pub struct Core {
	owner_chat: ChatPeerId,
	// max_delay: u16,
	pub tg: Client,
	pub me: Bot,
	pub db: Db,
	running: Arc<Mutex<HashSet<i32>>>,
	http_client: reqwest::Client,
}

pub struct Post {
	uri: String,
	title: String,
	authors: String,
	summary: String,
}

impl Core {
	/// Create a Core instance from configuration and start its background autofetch loop.
	///
	/// The provided `settings` must include:
	/// - `owner` (integer): chat id to use as the default destination,
	/// - `api_key` (string): Telegram bot API key,
	/// - `api_gateway` (string): Telegram API gateway host,
	/// - `pg` (string): PostgreSQL connection string,
	/// - optional `proxy` (string): proxy URL for the HTTP client.
	///
	/// On success returns an initialized `Core` with Telegram and HTTP clients, database connection,
	/// an empty running set for per-id tokens, and a spawned background task that periodically runs
	/// `autofetch`. If any required setting is missing or initialization fails, an error is returned.
	pub async fn new(settings: config::Config) -> Result<Core> {
		let owner_chat = ChatPeerId::from(settings.get_int("owner").stack()?);
		let api_key = settings.get_string("api_key").stack()?;
		let tg = Client::new(&api_key).stack()?
			.with_host(settings.get_string("api_gateway").stack()?);

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
			running: Arc::new(Mutex::new(HashSet::new())),
			http_client,
			// max_delay: 60,
		};
		let clone = core.clone();
		smol::spawn(Compat::new(async move {
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
				Timer::after(delay).await;
			}
		})).detach();
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

	/// Fetches the feed for a source, sends any newly discovered posts to the appropriate chat, and records them in the database.
	///
	/// This acquires a per-source guard to prevent concurrent checks for the same `id`. If a check is already running for
	/// the given `id`, the function returns an error. If `last_scrape` is provided, it is sent as the `If-Modified-Since`
	/// header to the feed request. The function parses RSS or Atom feeds, sends unseen post URLs to either the source's
	/// channel (when `real` is true) or the source owner (when `real` is false), and persists posted entries so they are
	/// not reposted later.
	///
	/// Parameters:
	/// - `id`: Identifier of the source to check.
	/// - `real`: When `true`, send posts to the source's channel; when `false`, send to the source owner.
	/// - `last_scrape`: Optional timestamp used to set the `If-Modified-Since` header for the HTTP request.
	///
	/// # Returns
	///
	/// `Posted: N` where `N` is the number of posts processed and sent.
	pub async fn check (&self, id: i32, real: bool, last_scrape: Option<DateTime<Local>>) -> Result<String> {
		let mut posted: i32 = 0;
		let mut conn = self.db.begin().await.stack()?;

		let _token = Token::new(&self.running, id).await.stack()?;
		let source = conn.get_source(id, self.owner_chat).await.stack()?;
		conn.set_scrape(id).await.stack()?;
		let destination = ChatPeerId::from(match real {
			true => source.channel_id,
			false => source.owner,
		});
		let mut this_fetch: Option<DateTime<chrono::FixedOffset>> = None;
		let mut posts: BTreeMap<DateTime<chrono::FixedOffset>, Post> = BTreeMap::new();

		let mut builder = self.http_client.get(&source.url);
		if let Some(last_scrape) = last_scrape {
			builder = builder.header(LAST_MODIFIED, last_scrape.to_rfc2822());
		};
		let response = builder.send().await.stack()?;
		#[cfg(debug_assertions)]
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
							None => DateTime::parse_from_rfc3339(match item.dublin_core_ext() {
								Some(ext) => {
									let dates = ext.dates();
									if dates.is_empty() {
										bail!("Feed item has Dublin Core extension but no dates.")
									} else {
										&dates[0]
									}
								},
								None => bail!("Feed item misses posting date."),
							}),
						}.stack()?;
						let uri = link.to_string();
						let title = item.title().unwrap_or("").to_string();
						let authors = item.author().unwrap_or("").to_string();
						let summary = item.content().unwrap_or("").to_string();
						posts.insert(date, Post{
							uri,
							title,
							authors,
							summary,
						});
					}
				};
			},
			Err(err) => match err {
				rss::Error::InvalidStartTag => {
					match atom_syndication::Feed::read_from(&content[..]) {
						Ok(feed) => {
							for item in feed.entries() {
								let date = item.published()
									.stack_err("Feed item missing publishing date.")?;
								let uri = {
									let links = item.links();
									if links.is_empty() {
										bail!("Feed item missing post links.");
									} else {
										links[0].href().to_string()
									}
								};
								let title = item.title().to_string();
								let authors = item.authors().iter().map(|x| format!("{} <{:?}>", x.name(), x.email())).collect::<Vec<String>>().join(", ");
								let summary = if let Some(sum) = item.summary() { sum.value.clone() } else { String::new() };
								posts.insert(*date, Post{
									uri,
									title,
									authors,
									summary,
								});
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
		for (date, post) in posts.iter() {
			let post_url: Cow<str> = match source.url_re {
				Some(ref x) => sedregex::ReplaceCommand::new(x).stack()?.execute(&post.uri),
				None => post.uri.clone().into(),
			};
			if ! conn.exists(&post_url, id).await.stack()? {
				if this_fetch.is_none() || *date > this_fetch.unwrap() {
					this_fetch = Some(*date);
				};
				self.send( match &source.iv_hash {
					Some(hash) => format!("<a href=\"https://t.me/iv?url={post_url}&rhash={hash}\"> </a>{post_url}"),
					None => format!("{post_url}"),
				}, Some(destination), Some(ParseMode::Html)).await.stack()?;
				conn.add_post(id, date, &post_url).await.stack()?;
				posted += 1;
			};
		};
		posts.clear();
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
								Ok(None) => "Source not found in database?".to_string(),
								Err(err) => format!("Failed to fetch source data:\n{err}"),
							}
						};
						smol::spawn(Compat::new(async move {
							if let Err(err) = clone.check(source_id, true, Some(last_scrape)).await {
								if let Err(err) = clone.send(&format!("🛑 {source}\n{}", encode(&err.to_string())), None, Some(ParseMode::MarkdownV2)).await {
									eprintln!("Check error: {err}");
									// clone.disable(&source_id, owner).await.unwrap();
								};
							};
						})).detach();
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
