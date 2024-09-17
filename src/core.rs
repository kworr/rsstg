use anyhow::{anyhow, bail, Context, Result};
use async_std::task;
use chrono::DateTime;
use sqlx::postgres::PgPoolOptions;
use telegram_bot::{
	_base::Error as TgrError,
	Error as TgError,
};
use thiserror::Error;

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

#[derive(Error, Debug)]
pub enum RssError {
	#[error(transparent)]
	Tg(#[from] TgError),
	#[error(transparent)]
	Int(#[from] TryFromIntError),
}

#[derive(Clone)]
pub struct Core {
	owner_chat: telegram_bot::UserId,
	pub tg: telegram_bot::Api,
	pub my: telegram_bot::User,
	pool: sqlx::Pool<sqlx::Postgres>,
	sources: Arc<Mutex<HashSet<Arc<i32>>>>,
	http_client: reqwest::Client,
}

impl Core {
	pub fn new(settings: config::Config) -> Result<Arc<Core>> {
		let owner = settings.get_int("owner")?;
		let api_key = settings.get_string("api_key")?;
		let tg = telegram_bot::Api::new(api_key);
		let tg_cloned = tg.clone();

		let mut client = reqwest::Client::builder();
		if let Ok(proxy) = settings.get_string("proxy") {
			let proxy = reqwest::Proxy::all(proxy)?;
			client = client.proxy(proxy);
		}
		let http_client = client.build()?;
		let core = Arc::new(Core {
			tg,
			my: task::block_on(async {
				tg_cloned.send(telegram_bot::GetMe).await
			})?,
			owner_chat: telegram_bot::UserId::new(owner),
			pool: PgPoolOptions::new()
				.max_connections(5)
				.acquire_timeout(std::time::Duration::new(300, 0))
				.idle_timeout(std::time::Duration::new(60, 0))
				.connect_lazy(&settings.get_string("pg")?)?,
			sources: Arc::new(Mutex::new(HashSet::new())),
			http_client,
		});
		let clone = core.clone();
		task::spawn(async move {
			loop {
				let delay = match &clone.autofetch().await {
					Err(err) => {
						if let Err(err) = clone.send(format!("🛑 {:?}", err), None, None).await {
							eprintln!("Autofetch error: {}", err);
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

	pub fn stream(&self) -> telegram_bot::UpdatesStream {
		self.tg.stream()
	}

	pub async fn send<'a, S>(&self, msg: S, target: Option<telegram_bot::UserId>, mode: Option<telegram_bot::types::ParseMode>) -> Result<()>
	where S: Into<Cow<'a, str>> {
		let mode = mode.unwrap_or(telegram_bot::types::ParseMode::Html);
		let target = target.unwrap_or(self.owner_chat);
		self.request(telegram_bot::SendMessage::new(target, msg).parse_mode(mode)).await?;
		Ok(())
	}

	pub async fn request<Req: telegram_bot::Request> (&self, req: Req) -> Result<<Req::Response as telegram_bot::ResponseType>::Type, RssError> {
		loop {
			let res = self.tg.send(&req).await;
			match res {
				Ok(_) => return Ok(res?),
				Err(err) => {
					match &err {
						TgError::Raw(TgrError::TelegramError { description: _, parameters: Some(params) }) => {
							if let Some(delay) = params.retry_after {
								println!("Throttled, waiting {} senconds.", delay);
								task::sleep(std::time::Duration::from_secs(delay.try_into()?)).await;
							} else {
								return Err(err.into());
							}
						},
						_ => return Err(err.into()),
					}
				},
			};
		}
	}

	pub async fn check<S>(&self, id: &i32, owner: S, real: bool) -> Result<Cow<'_, str>>
	where S: Into<i64> {
		let owner = owner.into();
		let mut posted: i32 = 0;
		let mut conn = self.pool.acquire().await?;

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
			let source = sqlx::query!("select source_id, channel_id, url, iv_hash, owner, url_re from rsstg_source where source_id = $1 and owner = $2",
				*id, owner).fetch_one(&mut *conn).await?;
			let destination = match real {
				true => telegram_bot::UserId::new(source.channel_id),
				false => telegram_bot::UserId::new(source.owner),
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
						let feed = atom_syndication::Feed::read_from(&content[..])
							.with_context(|| format!("Problem opening feed url:\n{}\n{}", &source.url, status))?;
						for item in feed.entries() {
							let date = item.published().unwrap();
							let url = item.links()[0].href();
							posts.insert(*date, url.to_string());
						};
					},
					rss::Error::Eof => (),
					_ => bail!("Unsupported or mangled content:\n{:?}\n{:#?}\n{:#?}\n", &source.url, err, status)
				}
			};
			for (date, url) in posts.iter() {
				let post_url: Cow<str> = match source.url_re {
					Some(ref x) => sedregex::ReplaceCommand::new(x)?.execute(url),
					None => url.into(),
				};
				if let Some(exists) = sqlx::query!("select exists(select true from rsstg_post where url = $1 and source_id = $2) as exists;",
					&post_url, *id).fetch_one(&mut *conn).await?.exists {
					if ! exists {
						if this_fetch.is_none() || *date > this_fetch.unwrap() {
							this_fetch = Some(*date);
						};
						self.request( match &source.iv_hash {
								Some(hash) => telegram_bot::SendMessage::new(destination, format!("<a href=\"https://t.me/iv?url={}&rhash={}\"> </a>{0}", &post_url, hash)),
								None => telegram_bot::SendMessage::new(destination, format!("{}", post_url)),
							}.parse_mode(telegram_bot::types::ParseMode::Html)).await
							.context("Can't post message:")?;
						sqlx::query!("insert into rsstg_post (source_id, posted, url) values ($1, $2, $3);",
							*id, date, &post_url).execute(&mut *conn).await?;
					};
				};
				posted += 1;
			};
			posts.clear();
		};
		sqlx::query!("update rsstg_source set last_scrape = now() where source_id = $1;",
			*id).execute(&mut *conn).await?;
		Ok(format!("Posted: {}", &posted).into())
	}

	pub async fn delete<S>(&self, source_id: &i32, owner: S) -> Result<Cow<'_, str>>
	where S: Into<i64> {
		let owner = owner.into();

		match sqlx::query!("delete from rsstg_source where source_id = $1 and owner = $2;",
			source_id, owner).execute(&mut *self.pool.acquire().await?).await?.rows_affected() {
			0 => { Ok("No data found found.".into()) },
			x => { Ok(format!("{} sources removed.", x).into()) },
		}
	}

	pub async fn clean<S>(&self, source_id: &i32, owner: S) -> Result<Cow<'_, str>>
	where S: Into<i64> {
		let owner = owner.into();

		match sqlx::query!("delete from rsstg_post p using rsstg_source s where p.source_id = $1 and owner = $2 and p.source_id = s.source_id;",
			source_id, owner).execute(&mut *self.pool.acquire().await?).await?.rows_affected() {
			0 => { Ok("No data found found.".into()) },
			x => { Ok(format!("{} posts purged.", x).into()) },
		}
	}

	pub async fn enable<S>(&self, source_id: &i32, owner: S) -> Result<&str>
	where S: Into<i64> {
		let owner = owner.into();

		match sqlx::query!("update rsstg_source set enabled = true where source_id = $1 and owner = $2",
			source_id, owner).execute(&mut *self.pool.acquire().await?).await?.rows_affected() {
			1 => { Ok("Source enabled.") },
			0 => { Ok("Source not found.") },
			_ => { Err(anyhow!("Database error.")) },
		}
	}

	pub async fn disable<S>(&self, source_id: &i32, owner: S) -> Result<&str>
	where S: Into<i64> {
		let owner = owner.into();

		match sqlx::query!("update rsstg_source set enabled = false where source_id = $1 and owner = $2",
			source_id, owner).execute(&mut *self.pool.acquire().await?).await?.rows_affected() {
			1 => { Ok("Source disabled.") },
			0 => { Ok("Source not found.") },
			_ => { Err(anyhow!("Database error.")) },
		}
	}

	pub async fn update<S>(&self, update: Option<i32>, channel: &str, channel_id: i64, url: &str, iv_hash: Option<&str>, url_re: Option<&str>, owner: S) -> Result<&str>
	where S: Into<i64> {
		let owner = owner.into();
		let mut conn = self.pool.acquire().await?;

		match match update {
				Some(id) => {
					sqlx::query!("update rsstg_source set channel_id = $2, url = $3, iv_hash = $4, owner = $5, channel = $6, url_re = $7 where source_id = $1",
						id, channel_id, url, iv_hash, owner, channel, url_re).execute(&mut *conn).await
				},
				None => {
					sqlx::query!("insert into rsstg_source (channel_id, url, iv_hash, owner, channel, url_re) values ($1, $2, $3, $4, $5, $6)",
						channel_id, url, iv_hash, owner, channel, url_re).execute(&mut *conn).await
				},
			} {
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
		let now = chrono::Local::now();
		let mut queue = sqlx::query!(r#"select source_id, next_fetch as "next_fetch: DateTime<chrono::Local>", owner from rsstg_order natural left join rsstg_source where next_fetch < now() + interval '1 minute';"#)
			.fetch_all(&mut *self.pool.acquire().await?).await?;
		for row in queue.iter() {
			if let Some(next_fetch) = row.next_fetch {
				if next_fetch < now {
					if let (Some(owner), Some(source_id)) = (row.owner, row.source_id) {
						let clone = Core {
							owner_chat: telegram_bot::UserId::new(owner),
							..self.clone()
						};
						task::spawn(async move {
							if let Err(err) = clone.check(&source_id, owner, true).await {
								if let Err(err) = clone.send(&format!("🛑 {:?}", err), None, None).await {
									dbg!("Check error: {}", err);
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
		queue.clear();
		Ok(delay.to_std()?)
	}

	pub async fn list<S>(&self, owner: S) -> Result<String>
	where S: Into<i64> {
		let owner = owner.into();

		let mut reply: Vec<Cow<str>> = vec![];
		reply.push("Channels:".into());
		let rows = sqlx::query!("select source_id, channel, enabled, url, iv_hash, url_re from rsstg_source where owner = $1 order by source_id",
			owner).fetch_all(&mut *self.pool.acquire().await?).await?;
		for row in rows.iter() {
			reply.push(format!("\n\\#️⃣ {} \\*️⃣ `{}` {}\n🔗 `{}`", row.source_id, row.channel,
				match row.enabled {
					true  => "🔄 enabled",
					false => "⛔ disabled",
				}, row.url).into());
			if let Some(hash) = &row.iv_hash {
				reply.push(format!("IV: `{}`", hash).into());
			}
			if let Some(re) = &row.url_re {
				reply.push(format!("RE: `{}`", re).into());
			}
		};
		Ok(reply.join("\n"))
	}
}
