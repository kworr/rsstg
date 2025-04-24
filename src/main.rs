//! This is telegram bot to fetch RSS/ATOM feeds and post results on public
//! channels

#![warn(missing_docs)]

mod command;
mod core;
mod sql;

use anyhow::Result;

#[async_std::main]
async fn main() -> Result<()> {
	let settings = config::Config::builder()
		.add_source(config::File::with_name("rsstg"))
		.build()?;

	let mut core = core::Core::new(settings).await?;

	core.stream().await?;

	Ok(())
}
