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
	/// Construct a new `Tg` instance from configuration.
	///
	/// The `settings` must provide the following keys: `"api_key"` (string), `"owner"` (integer chat id),
	/// and `"api_gateway"` (string). The function initialises the HTTP client, configures the gateway
	/// and fetches the bot identity before returning the constructed `Tg`.
	///
	/// # Examples
	///
	/// ```no_run
	/// # use config::Config;
	/// # use tgbot::Tg;
	/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
	/// let settings = Config::builder()
	///     .set("api_key", "YOUR_API_KEY")?
	///     .set("owner", 123456789_i64)?
	///     .set("api_gateway", "https://api.telegram.org")?
	///     .build()?;
	///
	/// let tg = Tg::new(&settings).await?;
	/// # Ok(())
	/// # }
	/// ```
	///
	/// # Returns
	///
	/// `Ok(Tg)` containing the constructed `Tg` on success, `Err` otherwise.
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
	///
	/// The sent `Message` on success.
	///
	/// # Examples
	///
	/// ```no_run
	/// # // `tg` must be an initialized `Tg` instance available in scope.
	/// # async fn example(tg: &Tg) {
	/// let msg = tg.send("Hello, world!", None, None).await.unwrap();
	/// // `msg` contains the message returned by the Telegram API.
	/// # }
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

	/// Create a copy of this `Tg` with the owner replaced by the given chat ID.
	///
	/// # Parameters
	///
	/// - `owner`: The Telegram chat identifier to set as the new owner (expressed as an `i64`).
	///
	/// # Returns
	///
	/// A new `Tg` instance identical to the original except its `owner` field is set to the provided chat ID.
	///
	/// # Examples
	///
	/// ```no_run
	/// // given an existing `tg: Tg`
	/// let new_tg = tg.with_owner(123456789);
	/// // `new_tg.owner` now corresponds to chat id `123456789`
	/// ```
	pub fn with_owner (&self, owner: i64) -> Tg {
		Tg {
			owner: ChatPeerId::from(owner),
			..self.clone()
		}
	}
}