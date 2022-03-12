use anyhow::{bail, Context, Result};
use crate::core::Core;
use lazy_static::lazy_static;
use regex::Regex;
use sedregex::ReplaceCommand;
use std::borrow::Cow;

lazy_static! {
	static ref RE_USERNAME: Regex = Regex::new(r"^@[a-zA-Z][a-zA-Z0-9_]+$").unwrap();
	static ref RE_LINK: Regex = Regex::new(r"^https?://[a-zA-Z.0-9-]+/[-_a-zA-Z.0-9/?=]+$").unwrap();
	static ref RE_IV_HASH: Regex = Regex::new(r"^[a-f0-9]{14}$").unwrap();
}

pub async fn start(core: &Core, sender: telegram_bot::UserId) -> Result<()> {
	core.send("We are open. Probably. Visit [channel](https://t.me/rsstg_bot_help/3) for details.", Some(sender), None).await?;
	Ok(())
}

pub async fn list(core: &Core, sender: telegram_bot::UserId) -> Result<()> {
	core.send(core.list(sender).await?, Some(sender), Some(telegram_bot::types::ParseMode::MarkdownV2)).await?;
	Ok(())
}

pub async fn command(core: &Core, sender: telegram_bot::UserId, command: Vec<&str>) -> Result<()> {
	let msg: Cow<str> = match &command[1].parse::<i32>() {
		Err(err) => format!("I need a number.\n{}", &err).into(),
		Ok(number) => match command[0] {
			"/check" => core.check(number, sender, false).await
				.context("Channel check failed.")?,
			"/clean" => core.clean(number, sender).await?,
			"/enable" => core.enable(number, sender).await?.into(),
			"/delete" => core.delete(number, sender).await?,
			"/disable" => core.disable(number, sender).await?.into(),
			_ => bail!("Command {} not handled.", &command[0]),
		},
	};
	core.send(msg, Some(sender), None).await?;
	Ok(())
}

pub async fn update(core: &Core, sender: telegram_bot::UserId, command: Vec<&str>) -> Result<()> {
	let mut source_id: Option<i32> = None;
	let at_least = "Requires at least 3 parameters.";
	let first_word = command[0];
	let command = match first_word {
		"/update" => {
			source_id = Some(command[1].parse::<i32>()
				.context(format!("I need a number, but got {}.", command[1]))?);
			&command[2..]
		},
		"/add" => &command[1..],
		_ => bail!("Passing {} is not possible here.", command[1]),
	};
	let mut i_command = command.iter();
	let (channel, url, iv_hash, url_re) = (
		i_command.next().context(at_least)?,
		i_command.next().context(at_least)?,
		i_command.next(),
		i_command.next());
	if ! RE_USERNAME.is_match(channel) {
		bail!("Usernames should be something like \"@\\[a\\-zA\\-Z]\\[a\\-zA\\-Z0\\-9\\_]+\", aren't they?\nNot {:?}", &channel);
	};
	if ! RE_LINK.is_match(url) {
		bail!("Link should be a link to atom/rss feed, something like \"https://domain/path\".\nNot {:?}", &url);
	}
	let iv_hash = match iv_hash {
		Some(hash) => {
			match *hash {
				"-" => None,
				thing => {
					if ! RE_IV_HASH.is_match(thing) {
						bail!("IV hash should be 14 hex digits.\nNot {:?}", thing);
					};
					Some(thing)
				},
			}
		},
		None => None,
	};
	let url_re = match url_re {
		Some(re) => {
			match *re {
				"-" => None,
				thing => {
					let _url_rex = ReplaceCommand::new(thing).context("Regexp parsing error:")?;
					Some(thing)
				}
			}
		},
		None => None,
	};
	let channel_id = i64::from(core.tg.send(telegram_bot::GetChat::new(telegram_bot::ChatRef::ChannelUsername(channel.to_string()))).await?.id());
	let chan_adm = core.tg.send(telegram_bot::GetChatAdministrators::new(telegram_bot::ChatRef::ChannelUsername(channel.to_string()))).await
		.context("Sorry, I have no access to that chat.")?;
	let (mut me, mut user) = (false, false);
	for admin in chan_adm {
		if admin.user.id == core.my.id {
			me = true;
		};
		if admin.user.id == sender {
			user = true;
		};
	};
	if ! me   { bail!("I need to be admin on that channel."); };
	if ! user { bail!("You should be admin on that channel."); };
	core.send(core.update(source_id, channel, channel_id, url, iv_hash, url_re, sender).await?, Some(sender), None).await?;
	Ok(())
}
