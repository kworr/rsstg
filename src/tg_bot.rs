use crate::{
	Arc,
	Mutex,
};

use std::{
	borrow::Cow,
	collections::HashMap,
	fmt,
};

use serde::{
	Deserialize,
	Serialize,
};
use stacked_errors::{
	bail,
	Result,
	StackableErr,
};
use tgbot::{
	api::Client,
	types::{
		Bot,
		ChatPeerId,
		GetBot,
		InlineKeyboardButton,
		InlineKeyboardMarkup,
		Message,
		ParseMode,
		SendMessage,
	},
};

const CB_VERSION: u8 = 0;

#[derive(Serialize, Deserialize, Debug)]
pub enum Callback {
	// List all feeds (version, name to show, page number)
	List(u8, String, u8),
}

impl Callback {
	pub fn list (text: &str, page: u8) -> Callback {
		Callback::List(CB_VERSION, text.to_owned(), page)
	}

	fn version (&self) -> u8 {
		match self {
			Callback::List(version, .. ) => *version,
		}
	}
}

impl fmt::Display for Callback {
	fn fmt (&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str(&toml::to_string(self).map_err(|_| fmt::Error)?)
	}
}

/// Produce new Keyboard Markup from current Callback
pub async fn get_kb (cb: &Callback, feeds: Arc<Mutex<HashMap<i32, String>>>) -> Result<InlineKeyboardMarkup> {
	if cb.version() != CB_VERSION {
		bail!("Wrong callback version.");
	}
	let mark = match cb {
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
			for (id, name) in feeds.iter() {
				if i < start { continue }
				if i > end { break }
				i += 1;
				kb.push(vec![
					InlineKeyboardButton::for_callback_data(
						format!("{}. {}", id, name),
						Callback::list("xxx", *page).to_string()), // XXX edit
				])
			}
			if name.is_empty() {
				// no name - reverting to pages, any unknown number means last page
				kb.push(vec![
					InlineKeyboardButton::for_callback_data("1",
						Callback::list("xxx", 0).to_string()),
				])
			} else {
				kb.push(vec![
					InlineKeyboardButton::for_callback_data("1",
						Callback::list("xxx", 0).to_string()),
				])
			}
			if long {
				kb.push(vec![
					InlineKeyboardButton::for_callback_data("<<",
						Callback::list("", if *page == 0 { *page } else { page - 1 } ).to_string()),
					InlineKeyboardButton::for_callback_data(">>",
						Callback::list("", page + 1).to_string()),
				]);
			}
			InlineKeyboardMarkup::from(kb)
		},
	};
	Ok(mark)
}

pub enum MyMessage <'a> {
	Html { text: Cow<'a, str> },
	HtmlTo { text: Cow<'a, str>, to: ChatPeerId },
	HtmlToKb { text: Cow<'a, str>, to: ChatPeerId, kb: InlineKeyboardMarkup },
	Text { text: Cow<'a, str> },
	TextTo { text: Cow<'a, str>, to: ChatPeerId },
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
	
	pub fn text <'a, S> (text: S) -> MyMessage<'a>
	where S: Into<Cow<'a, str>> {
		let text = text.into();
		MyMessage::Text { text }
	}
	
	pub fn text_to <'a, S> (text: S, to: ChatPeerId) -> MyMessage<'a>
	where S: Into<Cow<'a, str>> {
		let text = text.into();
		MyMessage::TextTo { text, to }
	}
	
	fn req (&self, tg: &Tg) -> Result<SendMessage> {
		Ok(match self {
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
			MyMessage::Text { text } =>
				SendMessage::new(tg.owner, text.as_ref())
					.with_parse_mode(ParseMode::MarkdownV2),
			MyMessage::TextTo { text, to } =>
				SendMessage::new(*to, text.as_ref())
					.with_parse_mode(ParseMode::MarkdownV2),
		})
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
		self.client.execute(msg.req(self)?).await.stack()
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
}
