use std::collections::BTreeMap;

use config;

use tokio;
use rss;
use chrono::DateTime;

use regex::Regex;

use tokio::stream::StreamExt;
use telegram_bot::*;

use sqlx::postgres::PgPoolOptions;
use sqlx::Row;
use sqlx::Done;

#[macro_use]
extern crate lazy_static;

use anyhow::{anyhow, Context, Result};

//type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Clone)]
struct Core {
	owner: i64,
	api_key: String,
	owner_chat: UserId,
	tg: telegram_bot::Api,
	my: User,
	pool: sqlx::Pool<sqlx::Postgres>,
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
				//.connect(&settings.get_str("pg")?).await?,
		};
		let clone = core.clone();
		tokio::spawn(async move {
			if let Err(err) = clone.autofetch().await {
				eprintln!("connection error: {}", err);
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

	async fn check(&self, id: &i32, real: bool) -> Result<()> {
		match self.pool.acquire().await {
			Err(err) => {
				self.debug(&format!("🛑 Query queue fetch conn:\n{}\n{:?}", &err, &self.pool))?;
			},
			Ok(mut conn) => {
				match sqlx::query("select source_id, channel_id, url, iv_hash, owner from rsstg_source where source_id = $1")
					.bind(id)
					.fetch_one(&mut conn).await {
					Err(err) => {
						self.debug(&format!("🛑 Query queue:\n{}\n{:?}", &err, &conn))?;
					},
					Ok(row) => {
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
						match rss::Channel::from_url(url) {
							Err(err) => {
								self.debug(&format!("🛑 Problem opening feed url:\n{}\n{}", &url, &err))?;
							},
							Ok(feed) => {
								for item in feed.items() {
									let date = match item.pub_date() {
										Some(feed_date) => DateTime::parse_from_rfc2822(feed_date),
										None => DateTime::parse_from_rfc3339(&item.dublin_core_ext().unwrap().dates()[0]),
									}?;
									let url = item.link().unwrap().to_string();
									posts.insert(date.clone(), url.clone());
								};
								for (date, url) in posts.iter() {
									match self.pool.acquire().await {
										Err(err) => {
											self.debug(&format!("🛑 Check post fetch conn:\n{}\n{:?}", &err, &self.pool))?;
										},
										Ok(mut conn) => {
											match sqlx::query("select exists(select true from rsstg_post where url = $1 and source_id = $2) as exists;")
												.bind(&url)
												.bind(id)
												.fetch_one(&mut conn).await {
												Err(err) => {
													self.debug(&format!("🛑 Check post:\n{}\n{:?}", &err, &conn))?;
												},
												Ok(row) => {
													let exists: bool = row.try_get("exists")?;
													if ! exists {
														if this_fetch == None || *date > this_fetch.unwrap() {
															this_fetch = Some(*date);
														}
														match self.tg.send( match iv_hash {
																Some(x) => SendMessage::new(destination, format!("<a href=\"https://t.me/iv?url={}&rhash={}\"> </a>{0}", url, x)),
																None => SendMessage::new(destination, format!("{}", url)),
															}.parse_mode(types::ParseMode::Html)).await {
															Err(err) => {
																self.debug(&format!("🛑 Can't post message:\n{}", &err))?;
															},
															Ok(_) => {
																match sqlx::query("insert into rsstg_post (source_id, posted, url) values ($1, $2, $3);")
																	.bind(id)
																	.bind(date)
																	.bind(url)
																	.execute(&mut conn).await {
																		Ok(_) => {},
																		Err(err) => {
																			self.debug(&format!("🛑Rrecord post:\n{}\n{:?}", &err, &conn))?;
																		},
																};
															},
														};
														drop(conn);
														tokio::time::delay_for(std::time::Duration::new(4, 0)).await;
													}
												},
											};
										}
									};
								};
								posts.clear();
							},
						};
						match self.pool.acquire().await {
							Err(err) => {
								self.debug(&format!("🛑 Update scrape fetch conn:\n{}\n{:?}", &err, &self.pool))?;
							},
							Ok(mut conn) => {
								match sqlx::query("update rsstg_source set last_scrape = now() where source_id = $1;")
									.bind(id)
									.execute(&mut conn).await {
									Err(err) => {
										self.debug(&format!("🛑 Update scrape:\n{}\n{:?}", &err, &conn))?;
									},
									Ok(_) => {},
								};
							},
						};
					},
				};
			},
		};
		Ok(())
	}

	async fn clean(&self, source_id: i32) -> Result<()> {
		match self.pool.acquire().await {
			Err(err) => {
				self.debug(&format!("🛑 Clean fetch conn:\n{}\n{:?}", &err, &self.pool))?;
			},
			Ok(mut conn) => {
				match sqlx::query("delete from rsstg_post where source_id = $1;")
					.bind(source_id)
					.execute(&mut conn).await {
					Err(err) => {
						self.debug(&format!("🛑 Clean seen posts:\n{}\n{:?}", &err, &self.pool))?;
					},
					Ok(_) => {},
				};
			},
		};
		Ok(())
	}

	async fn enable(&self, source_id: &i32, id: telegram_bot::UserId) -> Result<&str> {
		let mut conn = self.pool.acquire().await
			.with_context(|| format!("🛑 Enable fetch conn:\n{:?}", &self.pool))?;
		match sqlx::query("update rsstg_source set enabled = true where source_id = $1 and owner = $2")
			.bind(source_id)
			.bind(i64::from(id))
			.execute(&mut conn).await
			.with_context(|| format!("🛑 Enable source:\n\n{:?}", &self.pool))?
			.rows_affected() {
			1 => { Ok("Source disabled\\.") },
			0 => { Ok("Source not found\\.") },
			_ => { Err(anyhow!("Database error.")) },
		}
	}

	async fn disable(&self, source_id: &i32, id: telegram_bot::UserId) -> Result<&str> {
		let mut conn = self.pool.acquire().await
			.with_context(|| format!("🛑 Disable fetch conn:\n{:?}", &self.pool))?;
		match sqlx::query("update rsstg_source set enabled = false where source_id = $1 and owner = $2")
			.bind(source_id)
			.bind(i64::from(id))
			.execute(&mut conn).await
			.with_context(|| format!("🛑 Disable source:\n\n{:?}", &self.pool))?
			.rows_affected() {
			1 => { Ok("Source disabled\\.") },
			0 => { Ok("Source not found\\.") },
			_ => { Err(anyhow!("Database error.")) },
		}
	}

	async fn autofetch(&self) -> Result<()> {
		let mut delay = chrono::Duration::minutes(5);
		let mut now;
		loop {
			match self.pool.acquire().await {
				Err(err) => {
					self.debug(&format!("🛑 Autofetch fetch conn:\n{}\n{:?}", &err, &self.pool))?;
				},
				Ok(mut conn) => {
					now = chrono::Local::now();
					let mut queue = sqlx::query("select source_id, next_fetch from rsstg_order natural left join rsstg_source natural left join rsstg_channel where next_fetch < now();")
						.fetch_all(&mut conn).await?;
					for row in queue.iter() {
						let source_id: i32 = row.try_get("source_id")?;
						let next_fetch: DateTime<chrono::Local> = row.try_get("next_fetch")?;
						if next_fetch < now {
							match sqlx::query("update rsstg_source set last_scrape = now() + interval '1 hour' where source_id = $1;")
								.bind(source_id)
								.execute(&mut conn).await {
								Ok(_) => {},
								Err(err) => {
									self.debug(&err.to_string())?;
								},
							};
							let clone = self.clone();
							tokio::spawn(async move {
								if let Err(err) = clone.check(&source_id.clone(), true).await {
									eprintln!("connection error: {}", err);
								}
							});
						} else {
							if next_fetch - now < delay {
								delay = next_fetch - now;
							}
						}
					};
					queue.clear();
				},
			};
			tokio::time::delay_for(delay.to_std()?).await;
			delay = chrono::Duration::minutes(5);
		}
	}

}

#[tokio::main]
async fn main() -> Result<()> {
	let mut settings = config::Config::default();
	settings.merge(config::File::with_name("rsstg"))?;

	let core = Core::new(settings).await?;

	let mut stream = core.stream();

	while let Some(update) = stream.next().await {
		match handle(update?, &core).await {
			Ok(_) => {},
			Err(err) => {
				core.debug(&err.to_string())?;
			}
		};
	}

	Ok(())
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
							match core.pool.acquire().await {
								Err(err) => {
									core.debug(&format!("🛑 Disable fetch conn:\n{}\n{:?}", &err, &core.pool))?;
								},
								Ok(mut conn) => {
									reply.push("Channels:".to_string());
									let rows = sqlx::query("select source_id, username, enabled, url, iv_hash from rsstg_source left join rsstg_channel using (channel_id) where owner = $1 order by source_id")
										.bind(i64::from(message.from.id))
										.fetch_all(&mut conn).await?;
									for row in rows.iter() {
									//while let Some(row) = rows.try_next().await? {
										let source_id: i32 = row.try_get("source_id")?;
										let username: &str = row.try_get("username")?;
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
									}
								},
							};
						},

// add

						"/add" | "/update" => {
							let mut source_id: i32 = 0;
							if cmd == "/update" {
								source_id = words.next().unwrap().parse::<i32>()?;
							}
							let (channel, url, iv_hash) = (words.next().unwrap(), words.next().unwrap(), words.next());
							let ok_link = RE_LINK.is_match(&url);
							let ok_hash = match iv_hash {
								Some(hash) => RE_IV_HASH.is_match(&hash),
								None => true,
							};
							if ! ok_link {
								reply.push("Link should be link to atom/rss feed, something like \"https://domain/path\"\\.".to_string());
								core.debug(&format!("Url: {:?}", &url))?;
							}
							if ! ok_hash {
								reply.push("IV hash should be 14 hex digits.".to_string());
								core.debug(&format!("IV: {:?}", &iv_hash))?;
							}
							if ok_link && ok_hash {
								let chan: Option<i64> = match sqlx::query("select channel_id from rsstg_channel where username = $1")
									.bind(channel)
									.fetch_one(&core.pool).await {
										Ok(chan) => Some(chan.try_get("channel_id")?),
										Err(sqlx::Error::RowNotFound) => {
											let chan_id = i64::from(core.tg.send(telegram_bot::GetChat::new(telegram_bot::types::ChatRef::ChannelUsername(channel.to_string()))).await?.id());
											sqlx::query("insert into rsstg_channel (channel_id, username) values ($1, $2);")
												.bind(chan_id)
												.bind(channel)
												.execute(&core.pool).await?;
											Some(chan_id)
										},
										Err(err) => {
											reply.push("Sorry, unknown error\\.".to_string());
											core.debug(&format!("Sorry, unknown error:\n{:#?}\n", err))?;
											None
										},
								};
								if let Some(chan) = chan {
									match if cmd == "/update" {
											sqlx::query("update rsstg_source set channel_id = $2, url = $3, iv_hash = $4, owner = $4 where source_id = $1").bind(source_id)
										} else {
											sqlx::query("insert into rsstg_source (channel_id, url, iv_hash, owner) values ($1, $2, $3, $4)")
										}
										.bind(chan)
										.bind(url)
										.bind(iv_hash)
										.bind(i64::from(message.from.id))
										.execute(&core.pool).await {
										Ok(_) => reply.push("Channel added\\.".to_string()),
										Err(sqlx::Error::Database(err)) => {
											match err.downcast::<sqlx::postgres::PgDatabaseError>().routine() {
												Some("_bt_check_unique", ) => {
													reply.push("Duplicate key\\.".to_string());
												},
												Some(_) => {
													reply.push("Database error\\.".to_string());
												},
												None => {
													reply.push("No database error extracted\\.".to_string());
												},
											};
										},
										Err(err) => {
											reply.push("Sorry, unknown error\\.".to_string());
											core.debug(&format!("Sorry, unknown error:\n{:#?}\n", err))?;
										},
									};
								};
							};
						},

// addchan

						"/addchan" => {
							let channel = words.next().unwrap();
							if ! RE_USERNAME.is_match(&channel) {
								reply.push("Usernames should be something like \"@\\[a\\-zA\\-Z]\\[a\\-zA\\-Z0\\-9\\_]+\", aren't they?".to_string());
							} else {
								let chan: Option<i64> = match sqlx::query("select channel_id from rsstg_channel where username = $1")
									.bind(channel)
									.fetch_one(&core.pool).await {
										Ok(chan) => Some(chan.try_get("channel_id")?),
										Err(sqlx::Error::RowNotFound) => None,
										Err(err) => {
											reply.push("Sorry, unknown error\\.".to_string());
											core.debug(&format!("Sorry, unknown error:\n{:#?}", err))?;
											None
										},
								};
								match chan {
									Some(chan) => {
										let new_chan = core.tg.send(telegram_bot::GetChat::new(telegram_bot::types::ChatId::new(chan))).await?;
										if i64::from(new_chan.id()) == chan {
											reply.push("I already know that channel\\.".to_string());
										} else {
											reply.push("Hmm, channel has changed… I'll fix it later\\.".to_string());
										};
									},
									None => {
										match core.tg.send(telegram_bot::GetChatAdministrators::new(telegram_bot::types::ChatRef::ChannelUsername(channel.to_string()))).await {
											Ok(chan_adm) => {
												let (mut me, mut user) = (false, false);
												for admin in &chan_adm {
													if admin.user.id == core.my.id {
														me = true;
													};
													if admin.user.id == message.from.id {
														user = true;
													};
												};
												if ! me   { reply.push("I need to be admin on that channel\\.".to_string()); };
												if ! user { reply.push("You should be admin on that channel\\.".to_string()); };
												if me && user {
													let chan_id = core.tg.send(telegram_bot::GetChat::new(telegram_bot::types::ChatRef::ChannelUsername(channel.to_string()))).await?;
													sqlx::query("insert into rsstg_channel (channel_id, username) values ($1, $2);")
														.bind(i64::from(chan_id.id()))
														.bind(channel)
														.execute(&core.pool).await?;
													reply.push("Good, I know that channel now\\.\n".to_string());
												};
											},
											Err(_) => {
												reply.push("Sorry, I have no access to that chat\\.".to_string());
											},
										};
									},
								};
							};
						},

// check

						"/check" => {
							match &words.next().unwrap().parse::<i32>() {
								Err(err) => {
									reply.push(format!("I need a number\\.\n{}", &err));
								},
								Ok(number) => {
									match &core.check(number, false).await {
										Ok(_) => {
											reply.push("Channel enabled\\.".to_string());
										}
										Err(err) => {
											core.debug(&format!("🛑 Channel check failed:\n{}", &err))?;
										},
									};
								},
							};
						},

// clean

						"/clean" => {
							if core.owner != i64::from(message.from.id) {
								reply.push("Reserved for testing\\.".to_string());
							} else {
								let source_id = words.next().unwrap().parse::<i32>().unwrap_or(0);
								&core.clean(source_id).await?;
							}
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
				match core.tg.send(message.text_reply(reply.join("\n")).parse_mode(types::ParseMode::MarkdownV2)).await {
					Ok(_) => {},
					Err(err) => {
						dbg!(reply.join("\n"));
						println!("{}", err);
					},
				}
			}
		},
		_ => {},
	};

	Ok(())
}
