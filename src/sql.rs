use std::{
	borrow::Cow,
	fmt,
};

use async_std::sync::{
	Arc,
	Mutex,
};
use chrono::{
	DateTime,
	FixedOffset,
	Local,
};
use sqlx::{
	Postgres,
	Row,
	postgres::PgPoolOptions,
	pool::PoolConnection,
};
use stacked_errors::{
	Result,
	StackableErr,
	bail,
};

#[derive(sqlx::FromRow, Debug)]
pub struct List {
	pub source_id: i32,
	pub channel: String,
	pub enabled: bool,
	pub url: String,
	pub iv_hash: Option<String>,
	pub url_re: Option<String>,
}

impl fmt::Display for List {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::result::Result<(), fmt::Error> {
		write!(f, "#{} \\*️⃣ `{}` {}\n🔗 `{}`", self.source_id, self.channel,
			match self.enabled {
				true  => "🔄 enabled",
				false => "⛔ disabled",
			}, self.url)?;
		if let Some(iv_hash) = &self.iv_hash {
			write!(f, "\nIV: `{iv_hash}`")?;
		}
		if let Some(url_re) = &self.url_re {
			write!(f, "\nRE: `{url_re}`")?;
		}
		Ok(())
	}
}

#[derive(sqlx::FromRow, Debug)]
pub struct Source {
	pub channel_id: i64,
	pub url: String,
	pub iv_hash: Option<String>,
	pub owner: i64,
	pub url_re: Option<String>,
}

#[derive(sqlx::FromRow)]
pub struct Queue {
	pub source_id: Option<i32>,
	pub next_fetch: Option<DateTime<Local>>,
	pub owner: Option<i64>,
	pub last_scrape: DateTime<Local>,
}

#[derive(Clone)]
pub struct Db (
	Arc<Mutex<sqlx::Pool<sqlx::Postgres>>>,
);

impl Db {
	pub fn new (pguri: &str) -> Result<Db> {
		Ok(Db (
			Arc::new(Mutex::new(PgPoolOptions::new()
				.max_connections(5)
				.acquire_timeout(std::time::Duration::new(300, 0))
				.idle_timeout(std::time::Duration::new(60, 0))
				.connect_lazy(pguri)
				.stack()?)),
		))
	}

	pub async fn begin(&self) -> Result<Conn> {
		let pool = self.0.lock_arc().await;
		let conn = Conn ( pool.acquire().await.stack()? );
		Ok(conn)
	}
}

pub struct Conn (
	PoolConnection<Postgres>,
);

impl Conn {
	pub async fn add_post (&mut self, source_id: i32, date: &DateTime<FixedOffset>, post_url: &str) -> Result<()> {
		sqlx::query("insert into rsstg_post (source_id, posted, url) values ($1, $2, $3);")
			.bind(source_id)
			.bind(date)
			.bind(post_url)
			.execute(&mut *self.0).await.stack()?;
		Ok(())
	}

	pub async fn clean <I> (&mut self, source_id: i32, owner: I) -> Result<Cow<'_, str>>
	where I: Into<i64> {
		match sqlx::query("delete from rsstg_post p using rsstg_source s where p.source_id = $1 and owner = $2 and p.source_id = s.source_id;")
			.bind(source_id)
			.bind(owner.into())
			.execute(&mut *self.0).await.stack()?.rows_affected() {
			0 => { Ok("No data found found.".into()) },
			x => { Ok(format!("{x} posts purged.").into()) },
		}
	}

	pub async fn delete <I> (&mut self, source_id: i32, owner: I) -> Result<Cow<'_, str>>
	where I: Into<i64> {
		match sqlx::query("delete from rsstg_source where source_id = $1 and owner = $2;")
			.bind(source_id)
			.bind(owner.into())
			.execute(&mut *self.0).await.stack()?.rows_affected() {
			0 => { Ok("No data found found.".into()) },
			x => { Ok(format!("{x} sources removed.").into()) },
		}
	}

	pub async fn disable <I> (&mut self, source_id: i32, owner: I) -> Result<&str>
	where I: Into<i64> {
		match sqlx::query("update rsstg_source set enabled = false where source_id = $1 and owner = $2")
			.bind(source_id)
			.bind(owner.into())
			.execute(&mut *self.0).await.stack()?.rows_affected() {
			1 => { Ok("Source disabled.") },
			0 => { Ok("Source not found.") },
			_ => { bail!("Database error.") },
		}
	}

	pub async fn enable <I> (&mut self, source_id: i32, owner: I) -> Result<&str>
	where I: Into<i64> {
		match sqlx::query("update rsstg_source set enabled = true where source_id = $1 and owner = $2")
			.bind(source_id)
			.bind(owner.into())
			.execute(&mut *self.0).await.stack()?.rows_affected() {
			1 => { Ok("Source enabled.") },
			0 => { Ok("Source not found.") },
			_ => { bail!("Database error.") },
		}
	}

	pub async fn exists <I> (&mut self, post_url: &str, id: I) -> Result<Option<bool>>
	where I: Into<i64> {
		let row = sqlx::query("select exists(select true from rsstg_post where url = $1 and source_id = $2) as exists;")
			.bind(post_url)
			.bind(id.into())
			.fetch_one(&mut *self.0).await.stack()?;
		let exists: Option<bool> = row.try_get("exists").stack()?;
		Ok(exists)
	}

	pub async fn get_queue (&mut self) -> Result<Vec<Queue>> {
		let block: Vec<Queue> = sqlx::query_as("select source_id, next_fetch, owner, last_scrape from rsstg_order natural left join rsstg_source where next_fetch < now() + interval '1 minute';")
			.fetch_all(&mut *self.0).await.stack()?;
		Ok(block)
	}

	pub async fn get_list <I> (&mut self, owner: I) -> Result<Vec<List>>
	where I: Into<i64> {
		let source: Vec<List> = sqlx::query_as("select source_id, channel, enabled, url, iv_hash, url_re from rsstg_source where owner = $1 order by source_id")
			.bind(owner.into())
			.fetch_all(&mut *self.0).await.stack()?;
		Ok(source)
	}

	pub async fn get_one <I> (&mut self, owner: I, id: i32) -> Result<Option<List>>
	where I: Into<i64> {
		let source: Option<List> = sqlx::query_as("select source_id, channel, enabled, url, iv_hash, url_re from rsstg_source where owner = $1 and source_id = $2")
			.bind(owner.into())
			.bind(id)
			.fetch_optional(&mut *self.0).await.stack()?;
		Ok(source)
	}

	pub async fn get_source <I> (&mut self, id: i32, owner: I) -> Result<Source>
	where I: Into<i64> {
		let source: Source = sqlx::query_as("select channel_id, url, iv_hash, owner, url_re from rsstg_source where source_id = $1 and owner = $2")
			.bind(id)
			.bind(owner.into())
			.fetch_one(&mut *self.0).await.stack()?;
		Ok(source)
	}

	pub async fn set_scrape <I> (&mut self, id: I) -> Result<()>
	where I: Into<i64> {
		sqlx::query("update rsstg_source set last_scrape = now() where source_id = $1;")
			.bind(id.into())
			.execute(&mut *self.0).await.stack()?;
		Ok(())
	}

	pub async fn update <I> (&mut self, update: Option<i32>, channel: &str, channel_id: i64, url: &str, iv_hash: Option<&str>, url_re: Option<&str>, owner: I) -> Result<&str>
	where I: Into<i64> {
		match match update {
				Some(id) => {
					sqlx::query("update rsstg_source set channel_id = $2, url = $3, iv_hash = $4, owner = $5, channel = $6, url_re = $7 where source_id = $1")
						.bind(id)
				},
				None => {
					sqlx::query("insert into rsstg_source (channel_id, url, iv_hash, owner, channel, url_re) values ($1, $2, $3, $4, $5, $6)")
				},
			}
				.bind(channel_id)
				.bind(url)
				.bind(iv_hash)
				.bind(owner.into())
				.bind(channel)
				.bind(url_re)
				.execute(&mut *self.0).await
			{
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
				bail!("Sorry, unknown error:\n{err:#?}\n");
			},
		}
	}
}
