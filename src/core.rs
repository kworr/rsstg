use crate::{
	Arc,
	command,
	Mutex,
	sql::Db,
	tg_bot::{
		Callback,
		MyMessage,
		Tg,
		validate,
	},
};

use std::{
	borrow::Cow,
	collections::{
		BTreeMap,
		HashSet,
	},
	time::Duration,
};

use async_compat::Compat;
use chrono::{
	DateTime,
	Local,
};
use lazy_static::lazy_static;
use regex::Regex;
use reqwest::header::LAST_MODIFIED;
use smol::Timer;
use stacked_errors::{
	Result,
	StackableErr,
	anyhow,
	bail,
};
use tgbot::{
	handler::UpdateHandler,
	types::{
		CallbackQuery,
		ChatPeerId,
		Command,
		Update,
		UpdateType,
		UserPeerId,
	},
};
use ttl_cache::TtlCache;

lazy_static!{
	pub static ref RE_SPECIAL: Regex = Regex::new(r"([\-_*\[\]()~`>#+|{}\.!])").unwrap();
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

pub type FeedList = BTreeMap<i32, String>;
type UserCache = TtlCache<i64, Arc<Mutex<FeedList>>>;

#[derive(Clone)]
pub struct Core {
	pub tg: Tg,
	pub db: Db,
	pub feeds: Arc<Mutex<UserCache>>,
	running: Arc<Mutex<HashSet<i32>>>,
	http_client: reqwest::Client,
}

// XXX Right now that part is unfinished and I guess I need to finish menu first
#[allow(unused)]
pub struct Post {
	uri: String,
	_title: String,
	_authors: String,
	_summary: String,
}

impl Core {
	/// Create a Core instance from configuration and start its background autofetch loop.
	///
	/// The provided `settings` must include:
	/// - `owner` (integer): default chat id to use as the owner/destination,
	/// - `api_key` (string): Telegram bot API key,
	/// - `api_gateway` (string): Telegram API gateway host,
	/// - `pg` (string): PostgreSQL connection string,
	/// - optional `proxy` (string): proxy URL for the HTTP client.
	///
	/// On success returns an initialized `Core` with Telegram and HTTP clients, database connection,
	/// an empty running set for per-id tokens, and a spawned background task that periodically runs
	/// `autofetch`. If any required setting is missing or initialization fails, an error is returned.
	pub async fn new(settings: config::Config) -> Result<Core> {
		let mut client = reqwest::Client::builder();
		if let Ok(proxy) = settings.get_string("proxy") {
			let proxy = reqwest::Proxy::all(proxy).stack()?;
			client = client.proxy(proxy);
		}

		let core = Core {
			tg: Tg::new(&settings).await.stack()?,
			db: Db::new(&settings.get_string("pg").stack()?)?,
			feeds: Arc::new(Mutex::new(TtlCache::new(10000))),
			running: Arc::new(Mutex::new(HashSet::new())),
			http_client: client.build().stack()?,
		};

		let clone = core.clone();
		smol::spawn(Compat::new(async move {
			loop {
				let delay = match &clone.autofetch().await {
					Err(err) => {
						if let Err(err) = clone.tg.send(MyMessage::html(format!("🛑 {err}"))).await {
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
		let source = conn.get_source(id, self.tg.owner).await.stack()?;
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
			use reqwest::header::{
				CACHE_CONTROL,
				EXPIRES,
			};
			let headers = response.headers();
			let expires = headers.get(EXPIRES);
			let cache = headers.get(CACHE_CONTROL);
			if expires.is_some() || cache.is_some() {
				println!("{} {} {:?} {:?} {:?}", Local::now().to_rfc2822(), source.url, last_scrape, expires, cache);
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
						posts.insert(date, Post{
							uri: link.to_string(),
							_title: item.title().unwrap_or("").to_string(),
							_authors: item.author().unwrap_or("").to_string(),
							_summary: item.content().unwrap_or("").to_string(),
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
								let _authors = item.authors().iter().map(|x| format!("{} <{:?}>", x.name(), x.email())).collect::<Vec<String>>().join(", ");
								let _summary = if let Some(sum) = item.summary() { sum.value.clone() } else { String::new() };
								posts.insert(*date, Post{
									uri,
									_title: item.title().to_string(),
									_authors,
									_summary,
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
				self.tg.send(MyMessage::html_to(match &source.iv_hash {
					Some(hash) => format!("<a href=\"https://t.me/iv?url={post_url}&rhash={hash}\"> </a>{post_url}"),
					None => format!("{post_url}"),
				}, destination)).await.stack()?;
				conn.add_post(id, date, &post_url).await.stack()?;
				posted += 1;
			};
		};
		posts.clear();
		Ok(format!("Posted: {posted}"))
	}

	/// Determine the delay until the next scheduled fetch and spawn background checks for any overdue sources.
	///
	/// This scans the database queue, spawns background tasks to run checks for sources whose `next_fetch`
	/// is in the past (each task uses a Core clone with the appropriate owner), and computes the shortest
	/// duration until the next `next_fetch`.
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
							tg: self.tg.with_owner(owner),
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
							if let Err(err) = clone.check(source_id, true, Some(last_scrape)).await
								&& let Err(err) = clone.tg.send(MyMessage::html(format!("🛑 {source}\n<pre>{}</pre>", &err.to_string()))).await
							{
								eprintln!("Check error: {err}");
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

	/// Displays full list of managed channels for specified user
	pub async fn list (&self, owner: UserPeerId) -> Result<String> {
		let mut reply: Vec<String> = vec![];
		reply.push("Channels:".into());
		let mut conn = self.db.begin().await.stack()?;
		for row in conn.get_list(owner).await.stack()? {
			reply.push(row.to_string());
		};
		Ok(reply.join("\n\n"))
	}

	/// Returns current cached list of feed for requested user, or loads data from database
	pub async fn get_feeds (&self, owner: i64) -> Result<Arc<Mutex<FeedList>>> {
		let mut feeds = self.feeds.lock_arc().await;
		Ok(match feeds.get(&owner) {
			None => {
				let mut conn = self.db.begin().await.stack()?;
				let feed_list = conn.get_feeds(owner).await.stack()?;
				let mut map = BTreeMap::new();
				for feed in feed_list {
					map.insert(feed.source_id, feed.channel);
				};
				let res = Arc::new(Mutex::new(map));
				feeds.insert(owner, res.clone(), Duration::from_secs(60 * 60 * 3));
				res
			},
			Some(res) => res.clone(),
		})
	}

	/// Adds feed to cached list
	pub async fn add_feed (&self, owner: i64, source_id: i32, channel: String) -> Result<()> {
		let mut inserted = true;
		{
			let mut feeds = self.feeds.lock_arc().await;
			if let Some(feed) = feeds.get_mut(&owner) {
				let mut feed = feed.lock_arc().await;
				feed.insert(source_id, channel);
			} else {
				inserted = false;
			}
		}
		// in case insert failed - we miss the entry we needed to expand, reload everything from
		// database
		if !inserted {
			self.get_feeds(owner).await.stack()?;
		}
		Ok(())
	}

	/// Removes feed from cached list
	pub async fn rm_feed (&self, owner: i64, source_id: &i32) -> Result<()> {
		let mut dropped = false;
		{
			let mut feeds = self.feeds.lock_arc().await;
			if let Some(feed) = feeds.get_mut(&owner) {
				let mut feed = feed.lock_arc().await;
				feed.remove(source_id);
				dropped = true;
			}
		}
		// in case we failed to found feed we need to remove - just reload everything from database
		if !dropped {
			self.get_feeds(owner).await.stack()?;
		}
		Ok(())
	}

	pub async fn cb (&self, query: &CallbackQuery, cb: &str) -> Result<()> {
		let cb: Callback = toml::from_str(cb).stack()?;
		todo!();
		Ok(())
	}
}

impl UpdateHandler for Core {
	/// Dispatches an incoming Telegram update to a matching command handler and reports handler errors to the originating chat.
	///
	/// This method inspects the update; if it contains a message that can be parsed as a bot command,
	/// it executes the corresponding command handler. If the handler returns an error, the error text
	/// is sent back to the message's chat. Unknown commands produce an error which is also reported to the chat.
	async fn handle (&self, update: Update) -> () {
		match update.update_type {
			UpdateType::Message(msg) => {
				if let Ok(cmd) = Command::try_from(*msg) {
					let msg = cmd.get_message();
					let words = cmd.get_args();
					let command = cmd.get_name();
					let res = match command {
						"/check" | "/clean" | "/enable" | "/delete" | "/disable" => command::command(self, command, msg, words).await,
						"/start" => command::start(self, msg).await,
						"/list" => command::list(self, msg).await,
						"/test" => command::test(self, msg).await,
						"/add" | "/update" => command::update(self, command, msg, words).await,
						any => Err(anyhow!("Unknown command: {any}")),
					};
					if let Err(err) = res  {
						match validate(&err.to_string()) {
							Ok(text) => {
								if let Err(err2) = self.tg.send(MyMessage::html_to(
									format!("#error<pre>{}</pre>", text),
									msg.chat.get_id(),
								)).await {
									dbg!(err2);
								}
							},
							Err(err2) => {
								dbg!(err2);
							},
						}
					}
				} else {
					// not a command
				}
			},
			UpdateType::CallbackQuery(query) => {
				if let Some(ref cb) = query.data
					&& let Err(err) = self.cb(&query, cb).await
					&& let Err(err) = self.tg.answer_cb(query.id, err.to_string()).await
				{
					println!("{err:?}");
				}
			},
			_ => {
				println!("Unhandled UpdateKind:\n{update:?}")
			},
		}
	}
}
