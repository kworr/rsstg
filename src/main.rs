//! This is telegram bot to fetch RSS/ATOM feeds and post results on public
//! channels

#![warn(missing_docs)]

mod command;
mod core;
mod sql;
mod tg_bot;

use async_compat::Compat;
use stacked_errors::{
	Result,
	StackableErr,
};
use tgbot::handler::LongPoll;

fn main () -> Result<()> {
	smol::block_on(Compat::new(async {
		async_main().await.unwrap();
	}));

	Ok(())
}

async fn async_main () -> Result<()> {
	let settings = config::Config::builder()
		.set_default("api_gateway", "https://api.telegram.org").stack()?
		.add_source(config::File::with_name("rsstg"))
		.build()
		.stack()?;

	let core = core::Core::new(settings).await.stack()?;

	LongPoll::new(core.tg.client.clone(), core).run().await;

	Ok(())
}
