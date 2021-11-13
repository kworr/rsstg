mod command;
mod core;

use config;
use futures::StreamExt;
use tokio;

use telegram_bot::*;

#[macro_use]
extern crate lazy_static;

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
	let mut settings = config::Config::default();
	settings.merge(config::File::with_name("rsstg"))?;

	let core = core::Core::new(settings).await?;

	let mut stream = core.stream();
	stream.allowed_updates(&[AllowedUpdate::Message]);
	let mut reply_to: Option<UserId>;

	loop {
		reply_to = None;
		match stream.next().await {
			Some(update) => {
				if let Err(err) = handle(update?, &core, &mut reply_to).await {
					core.send(&format!("🛑 {:?}", err), reply_to)?;
				};
			},
			None => {
				core.send(&format!("🛑 None error."), None)?;
			}
		};
	}

	//Ok(())
}

async fn handle(update: telegram_bot::Update, core: &core::Core, mut _reply_to: &Option<UserId>) -> Result<()> {
	match update.kind {
		UpdateKind::Message(message) => {
			match message.kind {
				MessageKind::Text { ref data, .. } => {
					let sender = message.from.id;
					let words: Vec<&str> = data.split_whitespace().collect();
					match words[0] {
						"/check" | "/clean" | "/enable" | "/delete" | "/disable" => command::command(core, sender, words).await?,
						"/start" => command::start(core, sender).await?,
						"/list" => command::list(core, sender).await?,
						"/add" | "/update" => command::update(core, sender, words).await?,
						_ => {
						},
					};
				},
				_ => {
				},
			};
		},
		_ => {},
	};

	Ok(())
}
