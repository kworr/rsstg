use std::borrow::Cow;

use anyhow::{
	Result,
	bail,
};
use chrono::{
	DateTime,
	FixedOffset,
	Local,
};
use sqlx::{
	Pool,
	Postgres,
	Row,
	postgres::PgPoolOptions,
	pool::PoolConnection,
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
}

#[derive(Clone)]
pub struct Db {
	pool: sqlx::Pool<sqlx::Postgres>,
}

pub struct Conn{
	conn: PoolConnection<Postgres>,
}

impl Db {
	pub fn new (pguri: &str) -> Result<Db> {
		Ok(Db{
			pool: PgPoolOptions::new()
				.max_connections(5)
				.acquire_timeout(std::time::Duration::new(300, 0))
				.idle_timeout(std::time::Duration::new(60, 0))
				.connect_lazy(pguri)?,
		})
	}

	pub async fn begin(&mut self) -> Result<Conn> {
		Conn::new(&mut self.pool).await
	}
}

impl Conn {
	pub async fn new (pool: &mut Pool<Postgres>) -> Result<Conn> {
		let conn = pool.acquire().await?;
		Ok(Conn{
			conn,
		})
	}

	pub async fn add_post (&mut self, id: i32, date: &DateTime<FixedOffset>, post_url: &str) -> Result<()> {
		sqlx::query("insert into rsstg_post (source_id, posted, url) values ($1, $2, $3);")
			.bind(id)
			.bind(date)
			.bind(post_url)
			.execute(&mut *self.conn).await?;
		Ok(())
	}

	pub async fn clean (&mut self, source_id: i32, owner: i64) -> Result<Cow<'_, str>> {
		match sqlx::query("delete from rsstg_post p using rsstg_source s where p.source_id = $1 and owner = $2 and p.source_id = s.source_id;")
			.bind(source_id)
			.bind(owner)
			.execute(&mut *self.conn).await?.rows_affected() {
			0 => { Ok("No data found found.".into()) },
			x => { Ok(format!("{x} posts purged.").into()) },
		}
	}

	pub async fn delete (&mut self, source_id: i32, owner: i64) -> Result<Cow<'_, str>> {
		match sqlx::query("delete from rsstg_source where source_id = $1 and owner = $2;")
			.bind(source_id)
			.bind(owner)
			.execute(&mut *self.conn).await?.rows_affected() {
			0 => { Ok("No data found found.".into()) },
			x => { Ok(format!("{} sources removed.", x).into()) },
		}
	}

	pub async fn disable (&mut self, source_id: i32, owner: i64) -> Result<&str> {
		match sqlx::query("update rsstg_source set enabled = false where source_id = $1 and owner = $2")
			.bind(source_id)
			.bind(owner)
			.execute(&mut *self.conn).await?.rows_affected() {
			1 => { Ok("Source disabled.") },
			0 => { Ok("Source not found.") },
			_ => { bail!("Database error.") },
		}
	}

	pub async fn enable (&mut self, source_id: i32, owner: i64) -> Result<&str> {
		match sqlx::query("update rsstg_source set enabled = true where source_id = $1 and owner = $2")
			.bind(source_id)
			.bind(owner)
			.execute(&mut *self.conn).await?.rows_affected() {
			1 => { Ok("Source enabled.") },
			0 => { Ok("Source not found.") },
			_ => { bail!("Database error.") },
		}
	}

	pub async fn exists (&mut self, post_url: &str, id: i32) -> Result<Option<bool>> {
		let row = sqlx::query("select exists(select true from rsstg_post where url = $1 and source_id = $2) as exists;")
			.bind(post_url)
			.bind(id)
			.fetch_one(&mut *self.conn).await?;
		let exists: Option<bool> = row.try_get("exists")?;
		Ok(exists)
	}

	pub async fn get_queue (&mut self) -> Result<Vec<Queue>> {
		let block: Vec<Queue> = sqlx::query_as("select source_id, next_fetch, owner from rsstg_order natural left join rsstg_source where next_fetch < now() + interval '1 minute';")
			.fetch_all(&mut *self.conn).await?;
		Ok(block)
	}

	pub async fn get_list (&mut self, owner: i64) -> Result<Vec<List>> {
		let source: Vec<List> = sqlx::query_as("select source_id, channel, enabled, url, iv_hash, url_re from rsstg_source where owner = $1 order by source_id")
			.bind(owner)
			.fetch_all(&mut *self.conn).await?;
		Ok(source)
	}

	pub async fn get_source (&mut self, id: i32, owner: i64) -> Result<Source> {
		let source: Source = sqlx::query_as("select channel_id, url, iv_hash, owner, url_re from rsstg_source where source_id = $1 and owner = $2")
			.bind(id)
			.bind(owner)
			.fetch_one(&mut *self.conn).await?;
		Ok(source)
	}

	pub async fn set_scrape (&mut self, id: i32) -> Result<()> {
		sqlx::query("update rsstg_source set last_scrape = now() where source_id = $1;")
			.bind(id)
			.execute(&mut *self.conn).await?;
		Ok(())
	}

	pub async fn update (&mut self, update: Option<i32>, channel: &str, channel_id: i64, url: &str, iv_hash: Option<&str>, url_re: Option<&str>, owner: i64) -> Result<&str> {
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
				.bind(owner)
				.bind(channel)
				.bind(url_re)
				.execute(&mut *self.conn).await
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
