//! This is telegram bot to fetch RSS/ATOM feeds and post results on public
//! channels

#![warn(missing_docs)]

mod command;
mod core;
mod sql;

use anyhow::Result;
use tgbot::handler::LongPoll;

#[async_std::main]
async fn main() -> Result<()> {
	let settings = config::Config::builder()
		.add_source(config::File::with_name("rsstg"))
		.build()?;

	let core = core::Core::new(settings).await?;

	LongPoll::new(core.tg.clone(), core).run().await;

	Ok(())
}
