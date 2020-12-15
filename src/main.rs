use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};

use config;

use tokio;

use rss;

use chrono::DateTime;

use regex::Regex;

use telegram_bot::*;
use tokio::stream::StreamExt;

use sqlx::postgres::PgPoolOptions;
use sqlx::Row;
use sqlx::Done; // .rows_affected()

#[macro_use]
extern crate lazy_static;

use anyhow::{anyhow, bail, Context, Result};

#[derive(Clone)]
struct Core {
	owner: i64,
	api_key: String,
	owner_chat: UserId,
	tg: telegram_bot::Api,
	my: User,
	pool: sqlx::Pool<sqlx::Postgres>,
	sources: Arc<Mutex<HashSet<Arc<i32>>>>,
}

impl Core {
	async fn new(settings: config::Config) -> Result<Core> {
		let owner = settings.get_int("owner")?;
		let api_key = settings.get_str("api_key")?;
		let tg = Api::new(&api_key);
		let core = Core {
			owner: owner,
			api_key: api_key.clone(),
			my: tg.send(telegram_bot::GetMe).await?,
			tg: tg,
			owner_chat: UserId::new(owner),
			pool: PgPoolOptions::new()
				.max_connections(5)
				.connect_timeout(std::time::Duration::new(300, 0))
				.idle_timeout(std::time::Duration::new(60, 0))
				.connect_lazy(&settings.get_str("pg")?)?,
			sources: Arc::new(Mutex::new(HashSet::new())),
		};
		let clone = core.clone();
		tokio::spawn(async move {
			if let Err(err) = &clone.autofetch().await {
				if let Err(err) = clone.debug(&format!("🛑 {:?}", err)) {
					eprintln!("Autofetch error: {}", err);
				};
			}
		});
		Ok(core)
	}

	fn stream(&self) -> telegram_bot::UpdatesStream {
		self.tg.stream()
	}

	fn debug(&self, msg: &str) -> Result<()> {
		self.tg.spawn(SendMessage::new(self.owner_chat, msg));
		Ok(())
	}

	async fn check<S>(&self, id: i32, owner: S, real: bool) -> Result<()>
	where S: Into<i64> {
		let owner: i64 = owner.into();
		let id = {
			let mut set = self.sources.lock().unwrap();
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
			let mut conn = self.pool.acquire().await
				.with_context(|| format!("Query queue fetch conn:\n{:?}", &self.pool))?;
			let row = sqlx::query("select source_id, channel_id, url, iv_hash, owner from rsstg_source where source_id = $1 and owner = $2")
				.bind(*id)
				.bind(owner)
				.fetch_one(&mut conn).await
				.with_context(|| format!("Query source:\n{:?}", &self.pool))?;
			drop(conn);
			let channel_id: i64 = row.try_get("channel_id")?;
			let destination = match real {
				true => UserId::new(channel_id),
				false => UserId::new(row.try_get("owner")?),
			};
			let url: &str = row.try_get("url")?;
			let mut this_fetch: Option<DateTime<chrono::FixedOffset>> = None;
			let iv_hash: Option<&str> = row.try_get("iv_hash")?;
			let mut posts: BTreeMap<DateTime<chrono::FixedOffset>, String> = BTreeMap::new();
			let feed = rss::Channel::from_url(url)
				.with_context(|| format!("Problem opening feed url:\n{}", &url))?;
			for item in feed.items() {
				let date = match item.pub_date() {
					Some(feed_date) => DateTime::parse_from_rfc2822(feed_date),
					None => DateTime::parse_from_rfc3339(&item.dublin_core_ext().unwrap().dates()[0]),
				}?;
				let url = item.link().unwrap().to_string();
				posts.insert(date.clone(), url.clone());
			};
			for (date, url) in posts.iter() {
				let mut conn = self.pool.acquire().await
					.with_context(|| format!("Check post fetch conn:\n{:?}", &self.pool))?;
				let row = sqlx::query("select exists(select true from rsstg_post where url = $1 and source_id = $2) as exists;")
					.bind(&url)
					.bind(*id)
					.fetch_one(&mut conn).await
					.with_context(|| format!("Check post:\n{:?}", &conn))?;
				let exists: bool = row.try_get("exists")?;
				if ! exists {
					if this_fetch == None || *date > this_fetch.unwrap() {
						this_fetch = Some(*date);
					};
					self.tg.send( match iv_hash {
							Some(x) => SendMessage::new(destination, format!("<a href=\"https://t.me/iv?url={}&rhash={}\"> </a>{0}", url, x)),
							None => SendMessage::new(destination, format!("{}", url)),
						}.parse_mode(types::ParseMode::Html)).await
						.context("Can't post message:")?;
					sqlx::query("insert into rsstg_post (source_id, posted, url) values ($1, $2, $3);")
						.bind(*id)
						.bind(date)
						.bind(url)
						.execute(&mut conn).await
						.with_context(|| format!("Record post:\n{:?}", &conn))?;
					drop(conn);
					tokio::time::delay_for(std::time::Duration::new(4, 0)).await;
				};
			};
			posts.clear();
		};
		let mut conn = self.pool.acquire().await
			.with_context(|| format!("Update scrape fetch conn:\n{:?}", &self.pool))?;
		sqlx::query("update rsstg_source set last_scrape = now() where source_id = $1;")
			.bind(*id)
			.execute(&mut conn).await
			.with_context(|| format!("Update scrape:\n{:?}", &conn))?;
		Ok(())
	}

	async fn delete<S>(&self, source_id: &i32, owner: S) -> Result<String>
	where S: Into<i64> {
		let owner: i64 = owner.into();
		let mut conn = self.pool.acquire().await
			.with_context(|| format!("Delete fetch conn:\n{:?}", &self.pool))?;
		match sqlx::query("delete from rsstg_source where source_id = $1 and owner = $2;")
			.bind(source_id)
			.bind(owner)
			.execute(&mut conn).await
			.with_context(|| format!("Delete source rule:\n{:?}", &self.pool))?
			.rows_affected() {
			0 => { Ok("No data found found\\.".to_string()) },
			x => { Ok(format!("{} sources removed\\.", x)) },
		}
	}

	async fn clean<S>(&self, source_id: &i32, owner: S) -> Result<String>
	where S: Into<i64> {
		let owner: i64 = owner.into();
		let mut conn = self.pool.acquire().await
			.with_context(|| format!("Clean fetch conn:\n{:?}", &self.pool))?;
		match sqlx::query("delete from rsstg_post p using rsstg_source s where p.source_id = $1 and owner = $2 and p.source_id = s.source_id;")
			.bind(source_id)
			.bind(owner)
			.execute(&mut conn).await
			.with_context(|| format!("Clean seen posts:\n{:?}", &self.pool))?
			.rows_affected() {
			0 => { Ok("No data found found\\.".to_string()) },
			x => { Ok(format!("{} posts purged\\.", x)) },
		}
	}

	async fn enable<S>(&self, source_id: &i32, owner: S) -> Result<&str>
	where S: Into<i64> {
		let owner: i64 = owner.into();
		let mut conn = self.pool.acquire().await
			.with_context(|| format!("Enable fetch conn:\n{:?}", &self.pool))?;
		match sqlx::query("update rsstg_source set enabled = true where source_id = $1 and owner = $2")
			.bind(source_id)
			.bind(owner)
			.execute(&mut conn).await
			.with_context(|| format!("Enable source:\n{:?}", &self.pool))?
			.rows_affected() {
			1 => { Ok("Source enabled\\.") },
			0 => { Ok("Source not found\\.") },
			_ => { Err(anyhow!("Database error.")) },
		}
	}

	async fn disable<S>(&self, source_id: &i32, owner: S) -> Result<&str>
	where S: Into<i64> {
		let owner: i64 = owner.into();
		let mut conn = self.pool.acquire().await
			.with_context(|| format!("Disable fetch conn:\n{:?}", &self.pool))?;
		match sqlx::query("update rsstg_source set enabled = false where source_id = $1 and owner = $2")
			.bind(source_id)
			.bind(owner)
			.execute(&mut conn).await
			.with_context(|| format!("Disable source:\n{:?}", &self.pool))?
			.rows_affected() {
			1 => { Ok("Source disabled\\.") },
			0 => { Ok("Source not found\\.") },
			_ => { Err(anyhow!("Database error.")) },
		}
	}

	async fn update<S>(&self, update: Option<i32>, channel: &str, channel_id: i64, url: &str, iv_hash: Option<&str>, owner: S) -> Result<String>
	where S: Into<i64> {
		let owner: i64 = owner.into();
		let mut conn = self.pool.acquire().await
			.with_context(|| format!("Update fetch conn:\n{:?}", &self.pool))?;

		match match update {
				Some(id) => {
					sqlx::query("update rsstg_source set channel_id = $2, url = $3, iv_hash = $4, owner = $5, channel = $6 where source_id = $1").bind(id)
				},
				None => {
					sqlx::query("insert into rsstg_source (channel_id, url, iv_hash, owner, channel) values ($1, $2, $3, $4, $5)")
				},
			}
			.bind(channel_id)
			.bind(url)
			.bind(iv_hash)
			.bind(owner)
			.bind(channel)
			.execute(&mut conn).await {
			Ok(_) => return Ok(String::from("Channel added\\.")),
			Err(sqlx::Error::Database(err)) => {
				match err.downcast::<sqlx::postgres::PgDatabaseError>().routine() {
					Some("_bt_check_unique", ) => {
						return Ok("Duplicate key\\.".to_string())
					},
					Some(_) => {
						return Ok("Database error\\.".to_string())
					},
					None => {
						return Ok("No database error extracted\\.".to_string())
					},
				};
			},
			Err(err) => {
				bail!("Sorry, unknown error:\n{:#?}\n", err);
			},
		};
	}

	async fn autofetch(&self) -> Result<()> {
		let mut delay = chrono::Duration::minutes(5);
		let mut now;
		loop {
			let mut conn = self.pool.acquire().await
				.with_context(|| format!("Autofetch fetch conn:\n{:?}", &self.pool))?;
			now = chrono::Local::now();
			let mut queue = sqlx::query("select source_id, next_fetch, owner from rsstg_order natural left join rsstg_source where next_fetch < now() + interval '5 minutes';")
				.fetch_all(&mut conn).await?;
			for row in queue.iter() {
				let source_id: i32 = row.try_get("source_id")?;
				let owner: i64 = row.try_get("owner")?;
				let next_fetch: DateTime<chrono::Local> = row.try_get("next_fetch")?;
				if next_fetch < now {
					//let clone = self.clone();
					//clone.owner_chat(UserId::new(owner));
					let clone = Core {
						owner_chat: UserId::new(owner),
						..self.clone()
					};
					tokio::spawn(async move {
						if let Err(err) = clone.check(source_id, owner, true).await {
							if let Err(err) = clone.debug(&format!("🛑 {:?}", err)) {
								eprintln!("Check error: {}", err);
							};
						};
					});
				} else {
					if next_fetch - now < delay {
						delay = next_fetch - now;
					}
				}
			};
			queue.clear();
			tokio::time::delay_for(delay.to_std()?).await;
			delay = chrono::Duration::minutes(5);
		}
	}

	async fn list<S>(&self, owner: S) -> Result<Vec<String>>
	where S: Into<i64> {
		let owner = owner.into();
		let mut reply = vec![];
		let mut conn = self.pool.acquire().await
			.with_context(|| format!("List fetch conn:\n{:?}", &self.pool))?;
		reply.push("Channels:".to_string());
		let rows = sqlx::query("select source_id, channel, enabled, url, iv_hash from rsstg_source where owner = $1 order by source_id")
			.bind(owner)
			.fetch_all(&mut conn).await?;
		for row in rows.iter() {
			let source_id: i32 = row.try_get("source_id")?;
			let username: &str = row.try_get("channel")?;
			let enabled: bool = row.try_get("enabled")?;
			let url: &str = row.try_get("url")?;
			let iv_hash: Option<&str> = row.try_get("iv_hash")?;
			reply.push(format!("\n\\#️⃣ {} \\*️⃣ `{}` {}\n🔗 `{}`", source_id, username,  
				match enabled {
					true  => "🔄 enabled",
					false => "⛔ disabled",
				}, url));
			if let Some(hash) = iv_hash {
				reply.push(format!("IV `{}`", hash));
			}
		};
		Ok(reply)
	}
}

#[tokio::main]
async fn main() -> Result<()> {
	let mut settings = config::Config::default();
	settings.merge(config::File::with_name("rsstg"))?;

	let core = Core::new(settings).await?;

	let mut stream = core.stream();

	loop {
		match stream.next().await {
			Some(update) => {
				if let Err(err) = handle(update?, &core).await {
					core.debug(&format!("🛑 {:?}", err))?;
				};
			},
			None => {
				core.debug(&format!("🛑 None error."))?;
			}
		};
	}

	//Ok(())
}

async fn handle(update: telegram_bot::Update, core: &Core) -> Result<()> {
	lazy_static! {
		static ref RE_USERNAME: Regex = Regex::new(r"^@[a-zA-Z][a-zA-Z0-9_]+$").unwrap();
		static ref RE_LINK: Regex = Regex::new(r"^https?://[a-zA-Z.0-9-]+/[-_a-zA-Z.0-9/?=]+$").unwrap();
		static ref RE_IV_HASH: Regex = Regex::new(r"^[a-f0-9]{14}$").unwrap();
	}

	match update.kind {
		UpdateKind::Message(message) => {
			let mut reply: Vec<String> = vec![];
			match message.kind {
				MessageKind::Text { ref data, .. } => {
					let mut words = data.split_whitespace();
					let cmd = words.next().unwrap();
					match cmd {

// start

						"/start" => {
							reply.push("We are open\\. Probably\\. Visit [channel](https://t.me/rsstg_bot_help/3) for details\\.".to_string());
						},

// list

						"/list" => {
							reply.append(&mut core.list(message.from.id).await?);
						},

// add

						"/add" | "/update" => {
							let mut source_id: Option<i32> = None;
							let at_least = "Requires at least 3 parameters.";
							if cmd == "/update" {
								let first_word = words.next()
									.context(at_least)?;
								source_id = Some(first_word.parse::<i32>()
									.with_context(|| format!("I need a number, but got {}.", first_word))?);
							}
							let (channel, url, iv_hash) = (
								words.next().context(at_least)?,
								words.next().context(at_least)?,
								words.next());
							if ! RE_USERNAME.is_match(&channel) {
								reply.push("Usernames should be something like \"@\\[a\\-zA\\-Z]\\[a\\-zA\\-Z0\\-9\\_]+\", aren't they?".to_string());
								bail!("Wrong username {:?}.", &channel);
							}
							if ! RE_LINK.is_match(&url) {
								reply.push("Link should be link to atom/rss feed, something like \"https://domain/path\"\\.".to_string());
								bail!("Url: {:?}", &url);
							}
							if let Some(hash) = iv_hash {
								if ! RE_IV_HASH.is_match(&hash) {
									reply.push("IV hash should be 14 hex digits.".to_string());
									bail!("IV: {:?}", &iv_hash);
								};
							};
							let channel_id = i64::from(core.tg.send(telegram_bot::GetChat::new(telegram_bot::types::ChatRef::ChannelUsername(channel.to_string()))).await?.id());
							let chan_adm = core.tg.send(telegram_bot::GetChatAdministrators::new(telegram_bot::types::ChatRef::ChannelUsername(channel.to_string()))).await
								.context("Sorry, I have no access to that chat\\.")?;
							let (mut me, mut user) = (false, false);
							for admin in chan_adm {
								if admin.user.id == core.my.id {
									me = true;
								};
								if admin.user.id == message.from.id {
									user = true;
								};
							};
							if ! me   { bail!("I need to be admin on that channel\\."); };
							if ! user { bail!("You should be admin on that channel\\."); };
							reply.push(core.update(source_id, channel, channel_id, url, iv_hash, message.from.id).await?);
						},

// check

						"/check" => {
							match &words.next().unwrap().parse::<i32>() {
								Err(err) => {
									reply.push(format!("I need a number\\.\n{}", &err));
								},
								Ok(number) => {
									core.check(*number, message.from.id, false).await
										.context("Channel check failed.")?;
								},
							};
						},

// clean

						"/clean" => {
							match &words.next().unwrap().parse::<i32>() {
								Err(err) => {
									reply.push(format!("I need a number\\.\n{}", &err));
								},
								Ok(number) => {
									let result = core.clean(&number, message.from.id).await?;
									reply.push(result.to_string());
								},
							};
						},

// enable

						"/enable" => {
							match &words.next().unwrap().parse::<i32>() {
								Err(err) => {
									reply.push(format!("I need a number\\.\n{}", &err));
								},
								Ok(number) => {
									let result = core.enable(&number, message.from.id).await?;
									reply.push(result.to_string());
								},
							};
						},

// delete

						"/delete" => {
							match &words.next().unwrap().parse::<i32>() {
								Err(err) => {
									reply.push(format!("I need a number\\.\n{}", &err));
								},
								Ok(number) => {
									let result = core.delete(&number, message.from.id).await?;
									reply.push(result.to_string());
								},
							};
						},

// disable

						"/disable" => {
							match &words.next().unwrap().parse::<i32>() {
								Err(err) => {
									reply.push(format!("I need a number\\.\n{}", &err));
								},
								Ok(number) => {
									let result = core.disable(&number, message.from.id).await?;
									reply.push(result.to_string());
								},
							};
						},

						_ => {
						},
					};
				},
				_ => {
				},
			};

			if reply.len() > 0 {
				if let Err(err) = core.tg.send(message.text_reply(reply.join("\n")).parse_mode(types::ParseMode::MarkdownV2)).await {
					dbg!(reply.join("\n"));
					println!("{}", err);
				};
			};
		},
		_ => {},
	};

	Ok(())
}
