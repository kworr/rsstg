use crate::{
	core::Core,
	tg_bot::{
		Callback,
		MyMessage,
		get_kb,
	},
};

use lazy_static::lazy_static;
use regex::Regex;
use sedregex::ReplaceCommand;
use stacked_errors::{
	Result,
	StackableErr,
	bail,
};
use tgbot::types::{
	ChatMember,
	ChatUsername,
	GetChat,
	GetChatAdministrators,
	Message,
};
use url::Url;

lazy_static! {
	static ref RE_USERNAME: Regex = Regex::new(r"^@([a-zA-Z][a-zA-Z0-9_]+)$").unwrap();
	static ref RE_IV_HASH: Regex = Regex::new(r"^[a-f0-9]{14}$").unwrap();
}

/// Sends an informational message to the message's chat linking to the bot help channel.
pub async fn start (core: &Core, msg: &Message) -> Result<()> {
	core.tg.send(MyMessage::text_to(
		"We are open\\. Probably\\. Visit [channel](https://t.me/rsstg_bot_help/3) for details\\.",
		msg.chat.get_id()
	)).await.stack()?;
	Ok(())
}

/// Send the sender's subscription list to the chat.
///
/// Retrieves the message sender's user ID, obtains their subscription list from `core`,
/// and sends the resulting reply into the message chat using MarkdownV2.
pub async fn list (core: &Core, msg: &Message) -> Result<()> {
	let sender = msg.sender.get_user_id()
		.stack_err("Ignoring unreal users.")?;
	let reply = core.list(sender).await.stack()?;
	core.tg.send(MyMessage::text_to(reply, msg.chat.get_id())).await.stack()?;
	Ok(())
}

pub async fn test (core: &Core, msg: &Message) -> Result<()> {
	let sender: i64 = msg.sender.get_user_id()
		.stack_err("Ignoring unreal users.")?.into();
	let feeds = core.get_feeds(sender).await.stack()?;
	let kb = get_kb(&Callback::list("", 0), feeds).await.stack()?;
	core.tg.send(MyMessage::html_to_kb(format!("List of feeds owned by {:?}:", msg.sender.get_user_username()), msg.chat.get_id(), kb)).await.stack()?;
	Ok(())
}

/// Handle channel-management commands that operate on a single numeric source ID.
///
/// This validates that exactly one numeric argument is provided, performs the requested
/// operation (check, clean, enable, delete, disable) against the database or core,
/// and sends the resulting reply to the chat.
///
/// # Parameters
///
/// - `core`: application core containing database and Telegram clients.
/// - `command`: command string (e.g. "/check", "/clean", "/enable", "/delete", "/disable").
/// - `msg`: incoming Telegram message that triggered the command; used to determine sender and chat.
/// - `words`: command arguments; expected to contain exactly one element that parses as a 32-bit integer.
pub async fn command (core: &Core, command: &str, msg: &Message, words: &[String]) -> Result<()> {
	let mut conn = core.db.begin().await.stack()?;
	let sender = msg.sender.get_user_id()
		.stack_err("Ignoring unreal users.")?;
	let reply = if words.len() == 1 {
		match words[0].parse::<i32>() {
			Err(err) => format!("I need a number.\n{}", &err).into(),
			Ok(number) => match command {
				"/check" => core.check(number, false, None).await
					.context("Channel check failed.")?.into(),
				"/clean" => conn.clean(number, sender).await.stack()?,
				"/enable" => conn.enable(number, sender).await.stack()?.into(),
				"/delete" => {
					let res = conn.delete(number, sender).await.stack()?;
					core.rm_feed(sender.into(), &number).await.stack()?;
					res
				}
				"/disable" => conn.disable(number, sender).await.stack()?.into(),
				_ => bail!("Command {command} {words:?} not handled."),
			},
		}
	} else {
		"This command needs exactly one number.".into()
	};
	core.tg.send(MyMessage::html_to(reply, msg.chat.get_id())).await.stack()?;
	Ok(())
}

/// Validate command arguments, check permissions and update or add a channel feed configuration in the database.
///
/// This function parses and validates parameters supplied by a user command (either "/update <id> ..." or "/add ..."),
/// verifies the channel username and feed URL, optionally validates an IV hash and a replacement regexp,
/// ensures both the bot and the command sender are administrators of the target channel, and performs the database update.
///
/// # Parameters
///
/// - `command` — the invoked command, expected to be either `"/update"` (followed by a numeric source id) or `"/add"`.
/// - `msg` — the incoming Telegram message; used to derive the command sender and target chat id for the reply.
/// - `words` — the command arguments: for `"/add"` expected `channel url [iv_hash|'-'] [url_re|'-']`; for `"/update"`
///   the first element must be a numeric `source_id` followed by the same parameters.
pub async fn update (core: &Core, command: &str, msg: &Message, words: &[String]) -> Result<()> {
	let sender = msg.sender.get_user_id()
		.stack_err("Ignoring unreal users.")?;
	let mut source_id: Option<i32> = None;
	let at_least = "Requires at least 3 parameters.";
	let mut i_words = words.iter();
	match command {
		"/update" => {
			let next_word = i_words.next().context(at_least)?;
			source_id = Some(next_word.parse::<i32>()
				.context(format!("I need a number, but got {next_word}."))?);
		},
		"/add" => {},
		_ => bail!("Passing {command} is not possible here."),
	};
	let (channel, url, iv_hash, url_re) = (
		i_words.next().context(at_least)?,
		i_words.next().context(at_least)?,
		i_words.next(),
		i_words.next());
	if ! RE_USERNAME.is_match(channel) {
		bail!("Usernames should be something like \"@\\[a\\-zA\\-Z]\\[a\\-zA\\-Z0\\-9\\_]+\", aren't they?\nNot {channel:?}");
	};
	{
		let parsed_url = Url::parse(url)
			.stack_err("Expecting a valid link to ATOM/RSS feed.")?;
		match parsed_url.scheme() {
			"http" | "https" => {},
			scheme => {
				bail!("Unsupported URL scheme: {scheme}");
			},
		};
	}
	let iv_hash = match iv_hash {
		Some(hash) => {
			match hash.as_ref() {
				"-" => None,
				thing => {
					if ! RE_IV_HASH.is_match(thing) {
						bail!("IV hash should be 14 hex digits.\nNot {thing:?}");
					};
					Some(thing)
				},
			}
		},
		None => None,
	};
	let url_re = match url_re {
		Some(re) => {
			match re.as_ref() {
				"-" => None,
				thing => {
					let _url_rex = ReplaceCommand::new(thing).context("Regexp parsing error:")?;
					Some(thing)
				}
			}
		},
		None => None,
	};
	let chat_id = ChatUsername::from(channel.as_ref());
	let channel_id = core.tg.client.execute(GetChat::new(chat_id.clone())).await.stack_err("getting GetChat")?.id;
	let chan_adm = core.tg.client.execute(GetChatAdministrators::new(chat_id)).await
		.context("Sorry, I have no access to that chat.")?;
	let (mut me, mut user) = (false, false);
	for admin in chan_adm {
		let member_id = match admin {
			ChatMember::Creator(member) => member.user.id,
			ChatMember::Administrator(member) => member.user.id,
			ChatMember::Left(_)
			| ChatMember::Kicked(_)
			| ChatMember::Member{..}
			| ChatMember::Restricted(_) => continue,
		};
		if member_id == core.tg.me.id {
			me = true;
		}
		if member_id == sender {
			user = true;
		}
	};
	if ! me   { bail!("I need to be admin on that channel."); };
	if ! user { bail!("You should be admin on that channel."); };
	let mut conn = core.db.begin().await.stack()?;
	let update = conn.update(source_id, channel, channel_id, url, iv_hash, url_re, sender).await.stack()?;
	core.tg.send(MyMessage::html_to(update, msg.chat.get_id())).await.stack()?;
	if command == "/add" {
		if let Some(new_record) = conn.get_one_name(sender, channel).await.stack()? {
			core.add_feed(sender.into(), new_record.source_id, new_record.channel).await.stack()?;
		} else {
			bail!("Failed to read data on freshly inserted source.");
		}
	};
	Ok(())
}
