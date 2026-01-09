use crate::core::Core;

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
	ParseMode::MarkdownV2,
};
use url::Url;

lazy_static! {
	static ref RE_USERNAME: Regex = Regex::new(r"^@([a-zA-Z][a-zA-Z0-9_]+)$").unwrap();
	static ref RE_IV_HASH: Regex = Regex::new(r"^[a-f0-9]{14}$").unwrap();
}

/// Sends an informational message to the message's chat linking to the bot help channel.
///
/// # Examples
///
/// ```no_run
/// # use crate::{Core, Message};
/// # async fn example(core: &Core, msg: &Message) {
/// start(core, msg).await.unwrap();
/// # }
/// ```
pub async fn start (core: &Core, msg: &Message) -> Result<()> {
	core.tg.send("We are open\\. Probably\\. Visit [channel](https://t.me/rsstg_bot_help/3) for details\\.",
		Some(msg.chat.get_id()), Some(MarkdownV2)).await.stack()?;
	Ok(())
}

/// Send the message sender's subscription list to the chat.
///
/// Looks up the sender's user ID, fetches their subscription list from `core`,
/// and sends the resulting reply to the message chat formatted as MarkdownV2.
///
/// # Examples
///
/// ```no_run
/// # use crate::{Core, Message};
/// # #[tokio::main]
/// # async fn main() -> crate::Result<()> {
/// let core: Core = unimplemented!();
/// let msg: Message = unimplemented!();
/// list(&core, &msg).await?;
/// # Ok(())
/// # }
/// ```
pub async fn list (core: &Core, msg: &Message) -> Result<()> {
	let sender = msg.sender.get_user_id()
		.stack_err("Ignoring unreal users.")?;
	let reply = core.list(sender).await.stack()?;
	core.tg.send(reply, Some(msg.chat.get_id()), Some(MarkdownV2)).await.stack()?;
	Ok(())
}

/// Handle a single-number channel-management command and reply to the chat.

///

/// Validates that exactly one numeric argument is provided, executes the requested

/// operation ("/check", "/clean", "/enable", "/delete", "/disable") against the

/// database or core, and sends the resulting reply into the originating chat.

/// If the argument parsing or permission checks fail, an explanatory message is sent.

///

/// # Parameters

///

/// - `command`: command string, one of "/check", "/clean", "/enable", "/delete", "/disable".

/// - `words`: command arguments; expected to contain exactly one element that parses as an `i32`.

///

/// # Examples

///

/// ```no_run

/// # use your_crate::{command, Core};

/// # use tgbot::types::Message;

/// # async fn example(core: &Core, msg: &Message) {

/// let args = vec!["42".to_string()];

/// let _ = command(core, "/check", msg, &args).await;

/// # }

/// ```
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
				"/delete" => conn.delete(number, sender).await.stack()?,
				"/disable" => conn.disable(number, sender).await.stack()?.into(),
				_ => bail!("Command {command} {words:?} not handled."),
			},
		}
	} else {
		"This command needs exacly one number.".into()
	};
	core.tg.send(reply, Some(msg.chat.get_id()), None).await.stack()?;
	Ok(())
}

/// Validate arguments, verify permissions and create or update a channel feed configuration.
///
/// Accepts either `"/add"` or `"/update <source_id>"` as `command`. For `"/add"` the expected `words` form is:
/// `channel url [iv_hash|'-'] [url_re|'-']`. For `"/update"` the first word must be a numeric `source_id` followed by
/// the same parameters. The function validates the channel username and feed URL, optionally validates an IV hash and a
/// replacement regexp, ensures both the bot and the command sender are administrators of the target channel, persists
/// the new or updated feed configuration to the database, and sends the resulting status message back to the command
/// chat.
///
/// # Examples
///
/// ```no_run
/// # async fn example(core: &crate::Core, msg: &crate::Message) -> crate::Result<()> {
/// let words = [ "@example_channel".to_string(),
///               "https://example.org/feed.xml".to_string(),
///               "-".to_string(), // no iv_hash
///               "-".to_string()  // no url_re
/// ];
/// crate::command::update(core, "/add", msg, &words).await?;
/// # Ok(()) }
/// ```
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
	/*
	let channel = match RE_USERNAME.captures(channel) {
		Some(caps) => match caps.get(1) {
			Some(data) => data.as_str(),
			None => bail!("No string found in channel name"),
		},
		None => {
			bail!("Usernames should be something like \"@\\[a\\-zA\\-Z]\\[a\\-zA\\-Z0\\-9\\_]+\", aren't they?\nNot {channel:?}");
		},
	};
	*/
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
	let channel_id = core.tg.client.execute(GetChat::new(chat_id.clone())).await.stack_err("gettting GetChat")?.id;
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
	core.tg.send(update, Some(msg.chat.get_id()), None).await.stack()?;
	Ok(())
}