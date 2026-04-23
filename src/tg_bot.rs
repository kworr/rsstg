use crate::{
	Arc,
	Mutex,
	core::FeedList,
};

use std::{
	borrow::Cow,
	fmt,
	time::Duration,
};

use lazy_static::lazy_static;
use serde::{
	Deserialize,
	Serialize,
};
use regex::Regex;
use smol::Timer;
use stacked_errors::{
	bail,
	Result,
	StackableErr,
};
use tgbot::{
	api::{
		Client,
		ExecuteError
	},
	types::{
		AnswerCallbackQuery,
		Bot,
		ChatPeerId,
		EditMessageResult,
		EditMessageText,
		GetBot,
		InlineKeyboardButton,
		InlineKeyboardMarkup,
		Message,
		ParseMode,
		SendMessage,
	},
};

const CB_VERSION: u8 = 0;

lazy_static! {
	pub static ref RE_CLOSING: Regex = Regex::new(r"</[ \t]*(pre|code)[ \t]*>").unwrap();
}

// validate input as being postable in preformatted block, all html tags are fine, except tags that
// break the block - </pre> and </code>, we don't need to escape anything else, as telegram manages
// that
pub fn validate (text: &str) -> Result<&str> {
	if RE_CLOSING.is_match(text) {
		bail!("Telegram closing tag found.");
	} else {
		Ok(text)
	}
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Callback {
	// Edit one feed (version, name)
	Edit(u8, String),
	// List all feeds (version, name to show, page number)
	List(u8, String, usize),
	// Show root menu (version)
	Menu(u8),
}

impl Callback {
	pub fn edit <S>(text: S) -> Callback
	where S: Into<String> {
		Callback::Edit(CB_VERSION, text.into())
	}

	pub fn list <S>(text: S, page: usize) -> Callback
	where S: Into<String> {
		Callback::List(CB_VERSION, text.into(), page)
	}

	pub fn menu () -> Callback {
		Callback::Menu(CB_VERSION)
	}

	fn version (&self) -> u8 {
		match self {
			Callback::Edit(version, .. ) => *version,
			Callback::List(version, .. ) => *version,
			Callback::Menu(version) => *version,
		}
	}
}

impl fmt::Display for Callback {
	fn fmt (&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str(&toml::to_string(self).map_err(|_| fmt::Error)?)
	}
}

/// Produce new Keyboard Markup from current Callback
pub async fn get_kb (cb: &Callback, feeds: &Arc<Mutex<FeedList>>) -> Result<InlineKeyboardMarkup> {
	if cb.version() != CB_VERSION {
		bail!("Wrong callback version.");
	}
	let mark = match cb {
		Callback::Edit(_, _name) => { // XXX edit missing
			let kb: Vec<Vec<InlineKeyboardButton>> = vec![];
			InlineKeyboardMarkup::from(kb)
		},
		Callback::List(_, name, page) => {
			let mut kb = vec![];
			let feeds = feeds.lock_arc().await;
			let long = feeds.len() > 6;
			let (start, end) = if long {
				(page * 5, 5 + page * 5)
			} else {
				(0, 6)
			};
			let mut i = 0;
			if name.is_empty() {
				let feed_iter = feeds.iter().skip(start);
				for (id, name) in feed_iter {
					kb.push(vec![
						InlineKeyboardButton::for_callback_data(
							format!("{}. {}", id, name),
							Callback::edit(name).to_string()),
					]);
					i += 1;
					if i == end { break }
				}
			} else {
				let mut found = false;
				let mut first_page = None;
				for (id, feed_name) in feeds.iter() {
					if name == feed_name {
						found = true;
					}
					i += 1;
					kb.push(vec![
						InlineKeyboardButton::for_callback_data(
							format!("{}. {}", id, feed_name),
							Callback::list("xxx", *page).to_string()), // XXX edit
					]);
					if i == end {
						// page complete, if found we got the right page, if not - reset and
						// continue.
						if found {
							break
						} else {
							if first_page.is_none() {
								first_page = Some(kb);
							}
							kb = vec![];
							i = 0
						}
					}
				}
				if !found {
					// name not found, showing first page
					kb = first_page.unwrap_or_default();
				}
			}
			if long {
				kb.push(vec![
					InlineKeyboardButton::for_callback_data("<<",
						Callback::list("", if *page == 0 { *page } else { page - 1 } ).to_string()),
					InlineKeyboardButton::for_callback_data(">>",
						Callback::list("", page.saturating_add(1)).to_string()),
				]);
			}
			InlineKeyboardMarkup::from(kb)
		},
		Callback::Menu(_) => {
			let kb = vec![
				vec![
					InlineKeyboardButton::for_callback_data(
						"Add new channel",
						Callback::menu().to_string()), // new XXX
				],
				vec![
					InlineKeyboardButton::for_callback_data(
						"List channels",
						Callback::list("", 0).to_string()),
				],
			];
			InlineKeyboardMarkup::from(kb)
		},
	};
	Ok(mark)
}

pub enum MyMessage <'a> {
	Html { text: Cow<'a, str> },
	HtmlTo { text: Cow<'a, str>, to: ChatPeerId },
	HtmlToKb { text: Cow<'a, str>, to: ChatPeerId, kb: InlineKeyboardMarkup },
}

impl MyMessage <'_> {
	pub fn html <'a, S> (text: S) -> MyMessage<'a>
	where S: Into<Cow<'a, str>> {
		let text = text.into();
		MyMessage::Html { text }
	}
	
	pub fn html_to <'a, S> (text: S, to: ChatPeerId) -> MyMessage<'a>
	where S: Into<Cow<'a, str>> {
		let text = text.into();
		MyMessage::HtmlTo { text, to }
	}
	
	pub fn html_to_kb <'a, S> (text: S, to: ChatPeerId, kb: InlineKeyboardMarkup) -> MyMessage<'a>
	where S: Into<Cow<'a, str>> {
		let text = text.into();
		MyMessage::HtmlToKb { text, to, kb }
	}
	
	fn req (&self, tg: &Tg) -> SendMessage {
		match self {
			MyMessage::Html { text } =>
				SendMessage::new(tg.owner, text.as_ref())
					.with_parse_mode(ParseMode::Html),
			MyMessage::HtmlTo { text, to } =>
				SendMessage::new(*to, text.as_ref())
					.with_parse_mode(ParseMode::Html),
			MyMessage::HtmlToKb { text, to, kb } =>
				SendMessage::new(*to, text.as_ref())
					.with_parse_mode(ParseMode::Html)
					.with_reply_markup(kb.clone()),
		}
	}
}

#[derive(Clone)]
pub struct Tg {
	pub me: Bot,
	pub owner: ChatPeerId,
	pub client: Client,
}

impl Tg {
	/// Construct a new `Tg` instance from configuration.
	///
	/// The `settings` must provide the following keys:
	///  - `"api_key"` (string),
	///  - `"owner"` (integer chat id),
	///  - `"api_gateway"` (string).
	///
	/// The function initialises the client, configures the gateway and fetches the bot identity
	/// before returning the constructed `Tg`.
	pub async fn new (settings: &config::Config) -> Result<Tg> {
		let api_key = settings.get_string("api_key").stack()?;

		let owner = ChatPeerId::from(settings.get_int("owner").stack()?);
		// We don't use retries, as in async environment this will just get us stuck for extra
		// amount of time on simple requests. Just bail, show error and ack it in the code. In
		// other case we might got stuck with multiple open transactions in database.
		let client = Client::new(&api_key).stack()?
			.with_host(settings.get_string("api_gateway").stack()?)
			.with_max_retries(0);
		let me = client.execute(GetBot).await.stack()?;
		Ok(Tg {
			me,
			owner,
			client,
		})
	}

	/// Send a text message to a chat, using an optional target and parse mode.
	///
	/// # Returns
	/// The sent `Message` on success.
	pub async fn send (&self, msg: MyMessage<'_>) -> Result<Message> {
		self.client.execute(msg.req(self)).await.stack()
	}

	pub async fn answer_cb (&self, id: String, text: String) -> Result<bool> {
		self.client.execute(
			AnswerCallbackQuery::new(id)
				.with_text(text)
		).await.stack()
	}

	/// Create a copy of this `Tg` with the owner replaced by the given chat ID.
	///
	/// # Parameters
	/// - `owner`: The Telegram chat identifier to set as the new owner (expressed as an `i64`).
	///
	/// # Returns
	/// A new `Tg` instance identical to the original except its `owner` field is set to the provided chat ID.
	pub fn with_owner <O>(&self, owner: O) -> Tg
	where O: Into<i64> {
		Tg {
			owner: ChatPeerId::from(owner.into()),
			..self.clone()
		}
	}

	// XXX Can loop indefinitely if API calls results retry_after, add max retries?
	pub async fn update_message (&self, chat_id: i64, message_id: i64, text: String, feeds: &Arc<Mutex<FeedList>>, cb: Callback) -> Result<EditMessageResult> {
		loop {
			let req = EditMessageText::for_chat_message(chat_id, message_id, &text)
				.with_reply_markup(get_kb(&cb, feeds).await.stack()?);
			let res = self.client.execute(req).await;
			match res {
				Ok(res) => return Ok(res),
				Err(ref err) => {
					if let ExecuteError::Response(resp) = err
						&& let Some(delay) = resp.retry_after()
					{
						if delay > 60 {
							return res.context("Delay too big (>60), not waiting.");
						}
						Timer::after(Duration::from_secs(delay)).await;
					} else {
						return res.context("Can't update message");
					}
				},
			};
		}
	}
}
