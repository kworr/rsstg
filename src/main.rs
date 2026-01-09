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

/// Program entry point that initialises and runs the asynchronous bot core and its Telegram long-poll loop.
///
/// Returns `Ok(())` on successful completion.
///
/// # Examples
///
/// ```no_run
/// // Invoke the binary entry point; the function initialises the bot and starts long-polling.
/// let result = crate::main();
/// assert!(result.is_ok());
/// ```
fn main () -> Result<()> {
	smol::block_on(Compat::new(async {
		async_main().await.unwrap();
	}));

	Ok(())
}

/// Initialises configuration and the bot core, then runs the Telegram long-poll loop.
///
/// Loads configuration (setting a default `api_gateway`), constructs the application core,
/// and starts the long-polling loop that handles incoming Telegram updates.
///
/// # Examples
///
/// ```no_run
/// use smol::block_on;
/// block_on(crate::async_main()).unwrap();
/// ```
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