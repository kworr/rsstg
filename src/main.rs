use config;

use tokio;
use rss;
use chrono::DateTime;

use regex::Regex;

//use tbot;
//use tbot::prelude::*;

use futures::StreamExt;
use futures::TryStreamExt;
use telegram_bot::*;

use sqlx::postgres::PgPoolOptions;
use sqlx::Row;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

struct Core {
	owner: i64,
	owner_chat: UserId,
	tg: telegram_bot::Api,
	my: User,
	pool: sqlx::Pool<sqlx::Postgres>,
}

impl Core {
	async fn new(settings: config::Config) -> Result<Core> {
		let owner = settings.get_int("owner")?;
		let tg = Api::new(settings.get_str("api_key")?);
		let core = Core {
			owner: owner,
			my: tg.send(telegram_bot::GetMe).await?,
			tg: tg,
			owner_chat: UserId::new(owner),
			pool: PgPoolOptions::new().max_connections(5).connect(&settings.get_str("pg")?).await?,
		};
		tokio::spawn(async move {
			if let Err(err) = &core.autofetch().await {
				eprintln!("connection error: {}", err);
			}
		});

		let tg = Api::new(settings.get_str("api_key")?);
		Ok(Core {
			owner: owner,
			my: tg.send(telegram_bot::GetMe).await?,
			tg: tg,
			owner_chat: UserId::new(owner),
			pool: PgPoolOptions::new().max_connections(5).connect(&settings.get_str("pg")?).await?,
		})
	}

	fn stream(&self) -> telegram_bot::UpdatesStream {
		self.tg.stream()
	}

	fn debug(&self, msg: &str) -> Result<()> {
		self.tg.spawn(SendMessage::new(self.owner_chat, msg));
		Ok(())
	}

	async fn check(&self, id: i32, real: Option<bool>) -> Result<()> {
		match sqlx::query("select channel_id, url, last_fetch, iv_hash from rsstg_source where source_id = $1")
			.bind(id)
			.fetch_one(&self.pool).await {
			Ok(row) => {
				let channel_id: i64 = row.try_get("channel_id")?;
				let destination = match real {
					Some(true) => UserId::new(channel_id),
					Some(false) | None => self.owner_chat,
				};
				let url: &str = row.try_get("url")?;
				let last_fetch: Option<DateTime<chrono::FixedOffset>> = row.try_get("last_fetch")?;
				let mut this_fetch: Option<DateTime<chrono::FixedOffset>> = None;
				let iv_hash: Option<&str> = row.try_get("iv_hash")?;
				match rss::Channel::from_url(url) {
					Ok(feed) => {
						self.debug(&format!("# title:{:?} ttl:{:?} hours:{:?} days:{:?}", feed.title(), feed.ttl(), feed.skip_hours(), feed.skip_days()))?;
						for item in feed.items() {
							let date = DateTime::parse_from_rfc2822(item.pub_date().unwrap()).unwrap();
							let url = item.link().unwrap().to_string();
							if last_fetch == None || date > last_fetch.unwrap() {
								if this_fetch == None || date > this_fetch.unwrap() {
									this_fetch = Some(date);
								}
								match self.tg.send( match iv_hash {
										Some(x) => SendMessage::new(destination, format!("<a href=\"https://t.me/iv?url={}&rhash={}\"> </a>{0}", url, x)),
										None => SendMessage::new(destination, format!("{}", url)),
									}.parse_mode(types::ParseMode::Html)).await {
									Ok(_) => {
										match sqlx::query("insert into rsstg_post (source_id, posted, url) values ($1, $2, $3);")
											.bind(id)
											.bind(date)
											.bind(url)
											.execute(&self.pool).await {
												Ok(_) => {},
												Err(err) => {
													self.debug(&err.to_string())?;
												},
										};
									},
									Err(err) => {
										self.debug(&err.to_string())?;
									},
								}
							};
							tokio::time::delay_for(std::time::Duration::new(4, 0)).await;
						};
						// update last_fetch
						if this_fetch != None && (last_fetch == None || this_fetch.unwrap() > last_fetch.unwrap()) {
							match sqlx::query("update rsstg_source set last_fetch = $1 where source_id = $2;")
								.bind(this_fetch.unwrap())
								.bind(id)
								.execute(&self.pool).await {
								Ok(_) => {},
								Err(err) => {
									self.debug(&err.to_string())?;
								},
							};
						}
					},
					Err(err) => {
						self.debug(&err.to_string())?;
					},
				};
				match sqlx::query("update rsstg_source set last_scrape = now() where source_id = $1;")
					.bind(id)
					.execute(&self.pool).await {
					Ok(_) => {},
					Err(err) => {
						self.debug(&err.to_string())?;
					},
				};
			},
			Err(err) => {
				self.debug(&err.to_string())?;
			},
		};
		Ok(())
	}

	async fn clean(&self, source_id: i32) -> Result<()> {
		for query in vec!["delete from rsstg_post where source_id = $1;", "update rsstg_source set last_fetch = NULL where source_id = $1;"] {
			match sqlx::query(query)
				.bind(source_id)
				.execute(&self.pool).await {
				Ok(_) => {},
				Err(err) => {
					self.debug(&err.to_string())?;
				},
			}
		}
		Ok(())
	}

	async fn enable(&self, user: UserId, channel: &str) -> Result<()> {
		match sqlx::query("update rsstg_source set enabled = true from rsstg_channel where rsstg_channel.channel_id = rsstg_source.channel_id and rsstg_channel.username = $1 and owner = $2")
			.bind(channel)
			.bind(i64::from(user))
			.execute(&self.pool).await {
			Ok(_) => {},
			Err(err) => {
				self.debug(&err.to_string())?;
			},
		}
		Ok(())
	}

	async fn disable(&self, user: UserId, channel: &str) -> Result<()> {
		match sqlx::query("update rsstg_source set enabled = false from rsstg_channel where rsstg_channel.channel_id = rsstg_source.channel_id and rsstg_channel.username = $1 and owner = $2")
			.bind(channel)
			.bind(i64::from(user))
			.execute(&self.pool).await {
			Ok(_) => {},
			Err(err) => {
				self.debug(&err.to_string())?;
			},
		}
		Ok(())
	}

	async fn autofetch(&self) -> Result<()> {
		let mut delay = chrono::Duration::minutes(5);
		let mut source_id;
		let mut next_fetch: DateTime<chrono::Local>;
		let mut now;
		loop {
			let mut rows = sqlx::query("select source_id, next_fetch from rsstg_order limit 1;")
				.fetch(&self.pool);
			while let Some(row) = rows.try_next().await.unwrap() {
				now = chrono::Local::now();
				source_id = row.try_get("source_id")?;
				next_fetch = row.try_get("next_fetch")?;
				if next_fetch < now {
					&self.check(source_id, Some(true)).await?;
				} else {
					delay = next_fetch - now;
					if delay > chrono::Duration::minutes(5) {
						delay = chrono::Duration::minutes(5);
					}
				}
			};
			tokio::time::delay_for(delay.to_std()?).await;
		}
		//Ok(())
	}

}

#[tokio::main]
async fn main() -> Result<()> {
	let mut settings = config::Config::default();
	settings.merge(config::File::with_name("rsstg"))?;

	let re_username = Regex::new(r"^@[a-z][a-z0-9_]+$")?;
	let re_link = Regex::new(r"^https?://[a-z.0-9]+/[-_a-z.0-9/]+$")?;
	let re_iv_hash = Regex::new(r"^[a-f0-9]{14}$")?;

	/*
	tokio::spawn(async move {
		if let Err(e) = connection.await {
			eprintln!("connection error: {}", e);
		}
	}); */

	let core = Core::new(settings).await?;

	/*
	let mut bot = tbot::Bot::new(settings.get_str("api_key")?).event_loop();

	bot.command("start", //"Start working.",
		|context| async move {
			context.send_message_in_reply("Not in service yet. Try later.").call().await.unwrap();
		},
	);

	bot.command("list", //"List channels.",
		|context| async move {
			dbg!(&context.chat);
								let mut res = "Channels:\n".to_owned();
								let mut rows = sqlx::query("select username, channel_id, url, iv_hash from rsstg_source left join rsstg_channel using (channel_id) where owner = $1")
									.bind(context.chat.id.0)
									.fetch(&pool);
								while let Some(row) = rows.try_next().await.unwrap() {
									let username: &str = row.try_get("username").unwrap();
									let channel_id: &str = row.try_get("channel_id").unwrap();
									let url: &str = row.try_get("url").unwrap();
									let iv_hash: &str = row.try_get("iv_hash").unwrap();
									res.push_str(&format!("`{}`: `{}` iv:`{}`\n", username, url, iv_hash));
									//match row.get(3) as str {
									//Some(x) => x,
									//_ => "None"
									//}));
								}
								context.send_message_in_reply(&res).call().await.unwrap();
		},
	);
	*/

	let mut stream = core.stream();

	while let Some(update) = stream.next().await {
		let update = update?;
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
								reply.push("Not in service yet. Try later.".to_string());
							},

// list

							"/list" => {
								reply.push("Channels:".to_string());
								let mut rows = sqlx::query("select username, enabled, url, iv_hash from rsstg_source left join rsstg_channel using (channel_id) where owner = $1")
									.bind(i64::from(message.from.id))
									.fetch(&core.pool);
								while let Some(row) = rows.try_next().await? {
									let username: &str = row.try_get("username")?;
									let enabled: bool = row.try_get("enabled")?;
									let url: &str = row.try_get("url")?;
									let iv_hash: &str = row.try_get("iv_hash")?;
									reply.push(format!("\n\\*️⃣ `{}` {}\n🔗 `{}`\nIV `{}`", username,  
										match enabled {
											true  => "🔄 enabled",
											false => "⛔ disabled",
										}, url, iv_hash));
									//match row.get(3) as str {
									//Some(x) => x,
									//_ => "None"
									//}));
								}
							},

// add

							"/add" => {
								let (channel, url, iv_hash) = (words.next().unwrap(), words.next().unwrap(), words.next());
								let ok_link = re_link.is_match(&url);
								let ok_hash = match iv_hash {
									Some(hash) => re_iv_hash.is_match(&hash),
									None => true,
								};
								if ! ok_link {
									reply.push("Link should be link to atom/rss feed, something like \"https://domain/path\".".to_string());
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
												reply.push("Sorry, I don't know about that channel. Please, add a channel with /addchan.".to_string());
												None
											},
											Err(err) => {
												reply.push("Sorry, unknown error\\.".to_string());
												core.debug(&format!("Sorry, unknown error: {:#?}\n", err))?;
												None
											},
									};
									match chan {
										Some(chan) => {
											match sqlx::query("insert into rsstg_source (channel_id, url, iv_hash, owner) values ($1, $2, $3, $4) on conflict (channel_id, owner) do update set url = excluded.url, iv_hash = excluded.iv_hash;")
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
													core.debug(&format!("Sorry, unknown error: {:#?}\n", err))?;
												},
											};
										},
										None => {},
									};
								};
							},

// addchan

							"/addchan" => {
								let channel = words.next().unwrap();
								if ! re_username.is_match(&channel) {
									reply.push("Usernames should be something like \"@\\[a-z]\\[a-z0-9_]+\", aren't they?".to_string());
								} else {
									let chan: Option<i64> = match sqlx::query("select channel_id from rsstg_channel where username = $1")
										.bind(channel)
										.fetch_one(&core.pool).await {
											Ok(chan) => Some(chan.try_get("channel_id")?),
											Err(sqlx::Error::RowNotFound) => None,
											Err(err) => {
												reply.push("Sorry, unknown error\\.".to_string());
												core.debug(&format!("Sorry, unknown error: {:#?}\n", err))?;
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
								if core.owner != i64::from(message.from.id) {
									reply.push("Reserved for testing\\.".to_string());
								} else {
									let source_id = words.next().unwrap().parse::<i32>().unwrap_or(0);
									&core.check(source_id, None).await?;
								}
							},

// clear

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
								let channel = words.next().unwrap();
								if ! re_username.is_match(&channel) {
									reply.push("Usernames should be something like \"@\\[a-z]\\[a-z0-9_]+\", aren't they?".to_string());
								} else {
									match core.enable(message.from.id, channel).await {
										Ok(_) => {
											reply.push("Channel enabled\\.".to_string());
										}
										Err(err) => {
											core.debug(&err.to_string())?;
										},
									}
								}
							},

// disable

							"/disable" => {
								let channel = words.next().unwrap();
								if ! re_username.is_match(&channel) {
									reply.push("Usernames should be something like \"@\\[a-z]\\[a-z0-9_]+\", aren't they?".to_string());
								} else {
									match core.disable(message.from.id, channel).await {
										Ok(_) => {
											reply.push("Channel disabled\\.".to_string());
										}
										Err(err) => {
											core.debug(&err.to_string())?;
										},
									}
								}
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
	}
	/*
	loop {
		println!("cycle");
		for _ in botdb.query("select owner from rsstg_updates where owner is NULL limit 1;", &[])? {
			for row in botdb.query("update rsstg_updates set owner = $1 where update->>'update_id' = ( select update->>'update_id' from rsstg_updates where owner is NULL limit 1 for update skip locked ) returning update;", &[owner])? {
				let u :types::Update = serde_json::from_value(row.get(0))?;
				println!("update: {:?}", &u);
				/*
				if let Some(message) = &u.message {
					//if u["message"] != None {
					if let (Some(entities), Some(text)) = (&message.entities, &message.text) {
					//if u["message"]["entities"] {
						for entry in entities {
							if &entry.type_ == "bot_command" {
								println!("command: {:?}", &text.chars().skip(entry.offset as usize).take(entry.length as usize).collect::<String>());
							}
							println!("entity: {:?}", &entry);
						}
					}
				}
				*/
			}
		}
		std::process::exit(0);
	}
	*/

	//bot.polling().start().await.unwrap();

	Ok(())
}
