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
		Message,
		ParseMode,
		SendMessage,
	},
};

#[derive(Clone)]
pub struct Tg {
	pub me: Bot,
	pub owner: ChatPeerId,
	pub client: Client,
}

impl Tg {
	/// Create a new `Tg` configured from application settings.
	///
	/// The `settings` must contain the following keys:
	/// - `"api_key"`: bot API token as a string.
	/// - `"owner"`: owner chat id as an integer.
	/// - `"api_gateway"`: base URL of the Telegram API gateway as a string.
	///
	/// The function initialises an HTTP client configured with the provided gateway,
	/// fetches the bot identity and returns a `Tg` instance ready for use.
	///
	/// # Examples
	///
	/// ```no_run
	/// # use config::Config;
	/// # use crate::tg_bot::Tg;
	/// # async fn _example() -> Result<(), Box<dyn std::error::Error>> {
	/// let mut settings = Config::default();
	/// settings.set("api_key", "BOT_TOKEN")?;
	/// settings.set("owner", 12345_i64)?;
	/// settings.set("api_gateway", "https://api.telegram.org")?;
	///
	/// let tg = Tg::new(&settings).await?;
	/// # Ok(()) }
	/// ```
	///
	/// # Returns
	///
	/// `Ok(Tg)` containing the initialised bot client and identity on success, or an error stacked via `stack()` on failure.
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

	/// Send a text message to the specified chat.
	///
	/// If `target` is `None`, the message is sent to the configured owner. If `mode` is `None`, `ParseMode::Html` is used.
	///
	/// # Returns
	///
	/// The sent `Message` on success.
	///
	/// # Examples
	///
	/// ```ignore
	/// // async context required
	/// let sent = tg.send("Hello, world!", None, None).await.unwrap();
	/// println!("sent message id = {}", sent.message_id);
	/// ```
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

	/// Return a copy of this `Tg` with its owner set to the supplied chat identifier.
	///
	/// The supplied `owner` is converted into an `i64` and wrapped as a `ChatPeerId`.
	///
	/// # Parameters
	///
	/// - `owner`: A value convertible into an `i64` representing the Telegram chat ID to set as the new owner.
	///
	/// # Returns
	///
	/// A `Tg` instance identical to the original except that its `owner` field is replaced by the provided chat ID.
	///
	/// # Examples
	///
	/// ```
	/// // assuming `tg` is an existing `Tg`
	/// let new_tg = tg.with_owner(123456789i64);
	/// assert_eq!(new_tg.owner, ChatPeerId::from(123456789i64));
	/// ```
	pub fn with_owner <O>(&self, owner: O) -> Tg
	where O: Into<i64> {
		Tg {
			owner: ChatPeerId::from(owner.into()),
			..self.clone()
		}
	}
}