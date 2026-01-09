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

	pub fn with_owner (&self, owner: i64) -> Tg {
		Tg {
			owner: ChatPeerId::from(owner),
			..self.clone()
		}
	}
}
