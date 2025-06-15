use crate::core::Core;

use anyhow::{
	anyhow,
	bail,
	Context,
	Result,
};
use lazy_static::lazy_static;
use regex::Regex;
use sedregex::ReplaceCommand;
use tgbot::types::{
	ChatMember,
	ChatUsername,
	GetChat,
	GetChatAdministrators,
	Message,
	ParseMode::MarkdownV2,
};

lazy_static! {
	static ref RE_USERNAME: Regex = Regex::new(r"^@[a-zA-Z][a-zA-Z0-9_]+$").unwrap();
	static ref RE_LINK: Regex = Regex::new(r"^https?://[a-zA-Z.0-9-]+/[-_a-zA-Z.:0-9/?=]+$").unwrap();
	static ref RE_IV_HASH: Regex = Regex::new(r"^[a-f0-9]{14}$").unwrap();
}

pub async fn start(core: &Core, msg: &Message) -> Result<()> {
	core.send("We are open\\. Probably\\. Visit [channel](https://t.me/rsstg_bot_help/3) for details\\.",
		Some(msg.chat.get_id()), Some(MarkdownV2)).await?;
	Ok(())
}

pub async fn list(core: &Core, msg: &Message) -> Result<()> {
	let sender = msg.sender.get_user_id()
		.ok_or(anyhow!("Ignoring unreal users."))?;
	let reply = core.list(sender).await?;
	core.send(reply, Some(msg.chat.get_id()), Some(MarkdownV2)).await?;
	Ok(())
}

pub async fn command (core: &Core, msg: &Message, command: &[String]) -> Result<()> {
	let mut conn = core.db.begin().await?;
	let sender = msg.sender.get_user_id()
		.ok_or(anyhow!("Ignoring unreal users."))?;
	let reply = if command.len() >= 2 {
		match command[1].parse::<i32>() {
			Err(err) => format!("I need a number.\n{}", &err).into(),
			Ok(number) => match &command[0][..] {
				"/check" => core.check(number, false).await
					.context("Channel check failed.")?.into(),
				"/clean" => conn.clean(number, sender).await?,
				"/enable" => conn.enable(number, sender).await?.into(),
				"/delete" => conn.delete(number, sender).await?,
				"/disable" => conn.disable(number, sender).await?.into(),
				_ => bail!("Command {} not handled.", &command[0]),
			},
		}
	} else {
		"This command needs a number.".into()
	};
	core.send(reply, Some(msg.chat.get_id()), None).await?;
	Ok(())
}

pub async fn update (core: &Core, msg: &Message, command: &[String]) -> Result<()> {
	let sender = msg.sender.get_user_id()
		.ok_or(anyhow!("Ignoring unreal users."))?;
	let mut source_id: Option<i32> = None;
	let at_least = "Requires at least 3 parameters.";
	let mut i_command = command.iter();
	let first_word = i_command.next().context(at_least)?;
	match first_word.as_ref() {
		"/update" => {
			let next_word = i_command.next().context(at_least)?;
			source_id = Some(next_word.parse::<i32>()
				.context(format!("I need a number, but got {next_word}."))?);
		},
		"/add" => {},
		_ => bail!("Passing {first_word} is not possible here."),
	};
	let (channel, url, iv_hash, url_re) = (
		i_command.next().context(at_least)?,
		i_command.next().context(at_least)?,
		i_command.next(),
		i_command.next());
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
	let chat_id = ChatUsername::from(channel.clone());
	let channel_id = core.tg.execute(GetChat::new(chat_id.clone())).await?.id;
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
	let mut conn = core.db.begin().await?;
	core.send(conn.update(source_id, channel, channel_id, url, iv_hash, url_re, sender).await?, Some(msg.chat.get_id()), None).await?;
	Ok(())
}
