use anyhow::{anyhow, bail, Context, Result};
use chrono::DateTime;
use sqlx::{
	postgres::PgPoolOptions,
	Row,
};
use std::{
	borrow::Cow,
	collections::{
		BTreeMap,
		HashSet,
	},
	sync::{Arc, Mutex},
};

#[derive(Clone)]
pub struct Core {
	//owner: i64,
	//api_key: String,
	owner_chat: telegram_bot::UserId,
	pub tg: telegram_bot::Api,
	pub my: telegram_bot::User,
	pool: sqlx::Pool<sqlx::Postgres>,
	sources: Arc<Mutex<HashSet<Arc<i32>>>>,
}

impl Core {
	pub async fn new(settings: config::Config) -> Result<Arc<Core>> {
		let owner = settings.get_int("owner")?;
		let api_key = settings.get_str("api_key")?;
		let tg = telegram_bot::Api::new(&api_key);
		let core = Arc::new(Core {
			//owner,
			//api_key: api_key.clone(),
			my: tg.send(telegram_bot::GetMe).await?,
			tg,
			owner_chat: telegram_bot::UserId::new(owner),
			pool: PgPoolOptions::new()
				.max_connections(5)
				.connect_timeout(std::time::Duration::new(300, 0))
				.idle_timeout(std::time::Duration::new(60, 0))
				.connect_lazy(&settings.get_str("pg")?)?,
			sources: Arc::new(Mutex::new(HashSet::new())),
		});
		let clone = core.clone();
		tokio::spawn(async move {
			loop {
				let delay = match &clone.autofetch().await {
					Err(err) => {
						if let Err(err) = clone.send(format!("🛑 {:?}", err), None, None).await {
							eprintln!("Autofetch error: {}", err);
						};
						tokio::time::Duration::from_secs(60)
					},
					Ok(time) => *time,
				};
				tokio::time::sleep(delay).await;
			}
		});
		Ok(core)
	}

	pub fn stream(&self) -> telegram_bot::UpdatesStream {
		self.tg.stream()
	}

	pub async fn send<'a, S>(&self, msg: S, target: Option<telegram_bot::UserId>, mode: Option<telegram_bot::types::ParseMode>) -> Result<()>
	where S: Into<Cow<'a, str>> {
		let mode = mode.unwrap_or(telegram_bot::types::ParseMode::Html);
		let target = target.unwrap_or(self.owner_chat);
		self.tg.send(telegram_bot::SendMessage::new(target, msg).parse_mode(mode)).await?;
		Ok(())
	}

	pub async fn check<S>(&self, id: &i32, owner: S, real: bool) -> Result<Cow<'_, str>>
	where S: Into<i64> {
		let owner = owner.into();

		let mut posted: i32 = 0;
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
			let mut conn = self.pool.acquire().await
				.with_context(|| format!("Query queue fetch conn:\n{:?}", &self.pool))?;
			let row = sqlx::query("select source_id, channel_id, url, iv_hash, owner, url_re from rsstg_source where source_id = $1 and owner = $2")
				.bind(*id)
				.bind(owner)
				.fetch_one(&mut conn).await
				.with_context(|| format!("Query source:\n{:?}", &self.pool))?;
			drop(conn);
			let channel_id: i64 = row.try_get("channel_id")?;
			let url: &str = row.try_get("url")?;
			let iv_hash: Option<&str> = row.try_get("iv_hash")?;
			let url_re = match row.try_get("url_re")? {
				Some(x) => Some(sedregex::ReplaceCommand::new(x)?),
				None => None,
			};
			let destination = match real {
				true => telegram_bot::UserId::new(channel_id),
				false => telegram_bot::UserId::new(row.try_get("owner")?),
			};
			let mut this_fetch: Option<DateTime<chrono::FixedOffset>> = None;
			let mut posts: BTreeMap<DateTime<chrono::FixedOffset>, String> = BTreeMap::new();
			let response = reqwest::get(url).await?;
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
						let feed = atom_syndication::Feed::read_from(&content[..])
							.with_context(|| format!("Problem opening feed url:\n{}\n{}", &url, status))?;
						for item in feed.entries() {
							let date = item.published().unwrap();
							let url = item.links()[0].href();
							posts.insert(*date, url.to_string());
						};
					},
					rss::Error::Eof => (),
					_ => bail!("Unsupported or mangled content:\n{:?}\n{:#?}\n{:#?}\n", &url, err, status)
				}
			};
			for (date, url) in posts.iter() {
				let mut conn = self.pool.acquire().await
					.with_context(|| format!("Check post fetch conn:\n{:?}", &self.pool))?;
				let post_url: Cow<str> = match url_re {
					Some(ref x) => x.execute(url),
					None => url.into(),
				};
				let row = sqlx::query("select exists(select true from rsstg_post where url = $1 and source_id = $2) as exists;")
					.bind(&*post_url)
					.bind(*id)
					.fetch_one(&mut conn).await
					.with_context(|| format!("Check post:\n{:?}", &conn))?;
				let exists: bool = row.try_get("exists")?;
				if ! exists {
					if this_fetch == None || *date > this_fetch.unwrap() {
						this_fetch = Some(*date);
					};
					self.tg.send( match iv_hash {
							Some(hash) => telegram_bot::SendMessage::new(destination, format!("<a href=\"https://t.me/iv?url={}&rhash={}\"> </a>{0}", &post_url, hash)),
							None => telegram_bot::SendMessage::new(destination, format!("{}", post_url)),
						}.parse_mode(telegram_bot::types::ParseMode::Html)).await
						.context("Can't post message:")?;
					sqlx::query("insert into rsstg_post (source_id, posted, url) values ($1, $2, $3);")
						.bind(*id)
						.bind(date)
						.bind(&*post_url)
						.execute(&mut conn).await
						.with_context(|| format!("Record post:\n{:?}", &conn))?;
					drop(conn);
					tokio::time::sleep(std::time::Duration::new(4, 0)).await;
				};
				posted += 1;
			};
			posts.clear();
		};
		let mut conn = self.pool.acquire().await
			.with_context(|| format!("Update scrape fetch conn:\n{:?}", &self.pool))?;
		sqlx::query("update rsstg_source set last_scrape = now() where source_id = $1;")
			.bind(*id)
			.execute(&mut conn).await
			.with_context(|| format!("Update scrape:\n{:?}", &conn))?;
		Ok(format!("Posted: {}", &posted).into())
	}

	pub async fn delete<S>(&self, source_id: &i32, owner: S) -> Result<Cow<'_, str>>
	where S: Into<i64> {
		let owner = owner.into();

		let mut conn = self.pool.acquire().await
			.with_context(|| format!("Delete fetch conn:\n{:?}", &self.pool))?;
		match sqlx::query("delete from rsstg_source where source_id = $1 and owner = $2;")
			.bind(source_id)
			.bind(owner)
			.execute(&mut conn).await
			.with_context(|| format!("Delete source rule:\n{:?}", &self.pool))?
			.rows_affected() {
			0 => { Ok("No data found found.".into()) },
			x => { Ok(format!("{} sources removed.", x).into()) },
		}
	}

	pub async fn clean<S>(&self, source_id: &i32, owner: S) -> Result<Cow<'_, str>>
	where S: Into<i64> {
		let owner = owner.into();

		let mut conn = self.pool.acquire().await
			.with_context(|| format!("Clean fetch conn:\n{:?}", &self.pool))?;
		match sqlx::query("delete from rsstg_post p using rsstg_source s where p.source_id = $1 and owner = $2 and p.source_id = s.source_id;")
			.bind(source_id)
			.bind(owner)
			.execute(&mut conn).await
			.with_context(|| format!("Clean seen posts:\n{:?}", &self.pool))?
			.rows_affected() {
			0 => { Ok("No data found found.".into()) },
			x => { Ok(format!("{} posts purged.", x).into()) },
		}
	}

	pub async fn enable<S>(&self, source_id: &i32, owner: S) -> Result<&str>
	where S: Into<i64> {
		let owner = owner.into();

		let mut conn = self.pool.acquire().await
			.with_context(|| format!("Enable fetch conn:\n{:?}", &self.pool))?;
		match sqlx::query("update rsstg_source set enabled = true where source_id = $1 and owner = $2")
			.bind(source_id)
			.bind(owner)
			.execute(&mut conn).await
			.with_context(|| format!("Enable source:\n{:?}", &self.pool))?
			.rows_affected() {
			1 => { Ok("Source enabled.") },
			0 => { Ok("Source not found.") },
			_ => { Err(anyhow!("Database error.")) },
		}
	}

	pub async fn disable<S>(&self, source_id: &i32, owner: S) -> Result<&str>
	where S: Into<i64> {
		let owner = owner.into();

		let mut conn = self.pool.acquire().await
			.with_context(|| format!("Disable fetch conn:\n{:?}", &self.pool))?;
		match sqlx::query("update rsstg_source set enabled = false where source_id = $1 and owner = $2")
			.bind(source_id)
			.bind(owner)
			.execute(&mut conn).await
			.with_context(|| format!("Disable source:\n{:?}", &self.pool))?
			.rows_affected() {
			1 => { Ok("Source disabled.") },
			0 => { Ok("Source not found.") },
			_ => { Err(anyhow!("Database error.")) },
		}
	}

	pub async fn update<S>(&self, update: Option<i32>, channel: &str, channel_id: i64, url: &str, iv_hash: Option<&str>, url_re: Option<&str>, owner: S) -> Result<&str>
	where S: Into<i64> {
		let owner = owner.into();

		let mut conn = self.pool.acquire().await
			.with_context(|| format!("Update fetch conn:\n{:?}", &self.pool))?;

		match match update {
				Some(id) => {
					sqlx::query("update rsstg_source set channel_id = $2, url = $3, iv_hash = $4, owner = $5, channel = $6, url_re = $7 where source_id = $1").bind(id)
				},
				None => {
					sqlx::query("insert into rsstg_source (channel_id, url, iv_hash, owner, channel, url_re) values ($1, $2, $3, $4, $5, $6)")
				},
			}
			.bind(channel_id)
			.bind(url)
			.bind(iv_hash)
			.bind(owner)
			.bind(channel)
			.bind(url_re)
			.execute(&mut conn).await {
			Ok(_) => Ok(match update {
				Some(_) => "Channel updated.",
				None => "Channel added.",
			}),
			Err(sqlx::Error::Database(err)) => {
				match err.downcast::<sqlx::postgres::PgDatabaseError>().routine() {
					Some("_bt_check_unique", ) => {
						Ok("Duplicate key.")
					},
					Some(_) => {
						Ok("Database error.")
					},
					None => {
						Ok("No database error extracted.")
					},
				}
			},
			Err(err) => {
				bail!("Sorry, unknown error:\n{:#?}\n", err);
			},
		}
	}

	async fn autofetch(&self) -> Result<std::time::Duration> {
		let mut delay = chrono::Duration::minutes(1);
		let mut conn = self.pool.acquire().await
			.with_context(|| format!("Autofetch fetch conn:\n{:?}", &self.pool))?;
		let now = chrono::Local::now();
		let mut queue = sqlx::query("select source_id, next_fetch, owner from rsstg_order natural left join rsstg_source where next_fetch < now() + interval '1 minute';")
			.fetch_all(&mut conn).await?;
		for row in queue.iter() {
			let source_id: i32 = row.try_get("source_id")?;
			let owner: i64 = row.try_get("owner")?;
			let next_fetch: DateTime<chrono::Local> = row.try_get("next_fetch")?;
			if next_fetch < now {
				let clone = Core {
					owner_chat: telegram_bot::UserId::new(owner),
					..self.clone()
				};
				tokio::spawn(async move {
					if let Err(err) = clone.check(&source_id, owner, true).await {
						if let Err(err) = clone.send(&format!("🛑 {:?}", err), None, None).await {
							eprintln!("Check error: {}", err);
						};
					};
				});
			} else if next_fetch - now < delay {
				delay = next_fetch - now;
			}
		};
		queue.clear();
		Ok(delay.to_std()?)
	}

	pub async fn list<S>(&self, owner: S) -> Result<String>
	where S: Into<i64> {
		let owner = owner.into();

		let mut reply: Vec<Cow<str>> = vec![];
		let mut conn = self.pool.acquire().await
			.with_context(|| format!("List fetch conn:\n{:?}", &self.pool))?;
		reply.push("Channels:".into());
		let rows = sqlx::query("select source_id, channel, enabled, url, iv_hash, url_re from rsstg_source where owner = $1 order by source_id")
			.bind(owner)
			.fetch_all(&mut conn).await?;
		for row in rows.iter() {
			let source_id: i32 = row.try_get("source_id")?;
			let username: &str = row.try_get("channel")?;
			let enabled: bool = row.try_get("enabled")?;
			let url: &str = row.try_get("url")?;
			let iv_hash: Option<&str> = row.try_get("iv_hash")?;
			let url_re: Option<&str> = row.try_get("url_re")?;
			reply.push(format!("\n\\#️⃣ {} \\*️⃣ `{}` {}\n🔗 `{}`", source_id, username,  
				match enabled {
					true  => "🔄 enabled",
					false => "⛔ disabled",
				}, url).into());
			if let Some(hash) = iv_hash {
				reply.push(format!("IV: `{}`", hash).into());
			}
			if let Some(re) = url_re {
				reply.push(format!("RE: `{}`", re).into());
			}
		};
		Ok(reply.join("\n"))
	}
}
