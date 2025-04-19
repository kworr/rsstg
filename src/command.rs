use crate::core::Core;

use anyhow::{
	bail,
	Context,
	Result
};
use lazy_static::lazy_static;
use regex::Regex;
use sedregex::ReplaceCommand;
use std::borrow::Cow;
use teloxide::{
	Bot,
	dispatching::dialogue::GetChatId,
	payloads::GetChatAdministrators,
	requests::{
		Requester,
		ResponseResult
	},
	types::{
		Message,
		UserId,
	},
	utils::command::BotCommands,
};

lazy_static! {
	static ref RE_USERNAME: Regex = Regex::new(r"^@[a-zA-Z][a-zA-Z0-9_]+$").unwrap();
	static ref RE_LINK: Regex = Regex::new(r"^https?://[a-zA-Z.0-9-]+/[-_a-zA-Z.:0-9/?=]+$").unwrap();
	static ref RE_IV_HASH: Regex = Regex::new(r"^[a-f0-9]{14}$").unwrap();
}

#[derive(BotCommands, Clone)]
#[command(rename_rule = "lowercase", description = "Supported commands:")]
enum Command {
	#[command(description = "display this help.")]
	Help,
	#[command(description = "Does nothing.")]
	Start,
	#[commant(description = "List active subscriptions.")]
	List,
}

pub async fn cmd_handler(bot: Bot, msg: Message, cmd: Command) -> ResponseResult<()> {
	match cmd {
		Command::Help => bot.send_message(msg.chat.id, Command::descriptions().to_string()).await?,
		Command::Start => bot.send_message(msg.chat.id,
			"We are open. Probably. Visit [channel](https://t.me/rsstg_bot_help/3) for details.").await?,
		Command::List => bot.send_message(msg.chat.id, core.list(msg.from).await?).await?,
	};
	Ok(())
}

pub async fn list(core: &Core, sender: UserId) -> Result<()> {
	core.send(core.list(sender).await?, Some(sender), Some(telegram_bot::types::ParseMode::MarkdownV2)).await?;
	Ok(())
}

pub async fn command(core: &Core, sender: telegram_bot::UserId, command: Vec<&str>) -> Result<()> {
	if command.len() >= 2 {
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
	} else {
		core.send("This command needs a number.", Some(sender), None).await?;
	}
	Ok(())
}

pub async fn update(core: &Core, sender: telegram_bot::UserId, command: Vec<&str>) -> Result<()> {
	let mut source_id: Option<i32> = None;
	let at_least = "Requires at least 3 parameters.";
	let mut i_command = command.iter();
	let first_word = i_command.next().context(at_least)?;
	match *first_word {
		"/update" => {
			let next_word = i_command.next().context(at_least)?;
			source_id = Some(next_word.parse::<i32>()
				.context(format!("I need a number, but got {}.", next_word))?);
		},
		"/add" => {},
		_ => bail!("Passing {} is not possible here.", first_word),
	};
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
	let channel_id = i64::from(core.request(telegram_bot::GetChat::new(telegram_bot::ChatRef::ChannelUsername(channel.to_string()))).await?.id());
	let chan_adm = core.request(telegram_bot::GetChatAdministrators::new(telegram_bot::ChatRef::ChannelUsername(channel.to_string()))).await
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
