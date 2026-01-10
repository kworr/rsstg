use serde::{
	Deserialize,
	Serialize,
};
use stacked_errors::{
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

#[derive(Serialize, Deserialize, Debug)]
enum Callback {
	// List all feeds (version, name to show)
	List(u8, String),
}

fn get_kb (cb: &Callback) -> Result<InlineKeyboardMarkup> {
	let mark = InlineKeyboardMarkup::from(vec![vec![
		InlineKeyboardButton::for_callback_data("1",
			toml::to_string(&Callback::List(0,"xxx".to_owned())).stack()?),
	]]);
	Ok(mark)
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
	pub async fn send <S>(&self, msg: S, target: Option<ChatPeerId>, mode: Option<ParseMode>) -> Result<Message>
	where S: Into<String> {
		let msg = msg.into();

		let mode = mode.unwrap_or(ParseMode::Html);
		let target = target.unwrap_or(self.owner);
		self.client.execute(
			SendMessage::new(target, msg)
				.with_parse_mode(mode)
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
}
