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

lazy_static! {
	static ref RE_USERNAME: Regex = Regex::new(r"^@([a-zA-Z][a-zA-Z0-9_]+)$").unwrap();
	static ref RE_LINK: Regex = Regex::new(r"^https?://[a-zA-Z.0-9-]+/[-_a-zA-Z.:0-9/?=]+$").unwrap();
	static ref RE_IV_HASH: Regex = Regex::new(r"^[a-f0-9]{14}$").unwrap();
}

pub async fn start (core: &Core, msg: &Message) -> Result<()> {
	core.send("We are open\\. Probably\\. Visit [channel](https://t.me/rsstg_bot_help/3) for details\\.",
		Some(msg.chat.get_id()), Some(MarkdownV2)).await.stack()?;
	Ok(())
}

pub async fn list (core: &Core, msg: &Message) -> Result<()> {
	let sender = msg.sender.get_user_id()
		.stack_err("Ignoring unreal users.")?;
	let reply = core.list(sender).await.stack()?;
	core.send(reply, Some(msg.chat.get_id()), Some(MarkdownV2)).await.stack()?;
	Ok(())
}

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
	core.send(reply, Some(msg.chat.get_id()), None).await.stack()?;
	Ok(())
}

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
	if ! RE_LINK.is_match(url) {
		bail!("Link should be a link to atom/rss feed, something like \"https://domain/path\".\nNot {url:?}");
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
	let channel_id = core.tg.execute(GetChat::new(chat_id.clone())).await.stack_err("gettting GetChat")?.id;
	let chan_adm = core.tg.execute(GetChatAdministrators::new(chat_id)).await
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
		if member_id == core.me.id {
			me = true;
		}
		if member_id == sender {
			user = true;
		}
	};
	if ! me   { bail!("I need to be admin on that channel."); };
	if ! user { bail!("You should be admin on that channel."); };
	let mut conn = core.db.begin().await.stack()?;
	core.send(conn.update(source_id, channel, channel_id, url, iv_hash, url_re, sender).await.stack()?, Some(msg.chat.get_id()), None).await.stack()?;
	Ok(())
}
