//! This is telegram bot to fetch RSS/ATOM feeds and post results on public
//! channels

#![warn(missing_docs)]

mod command;
mod core;
mod sql;

use async_compat::Compat;
use stacked_errors::{
	Result,
	StackableErr,
};
use tgbot::handler::LongPoll;

/// Program entry point that initialises and runs the asynchronous bot runtime.
///
/// This function drives the async runtime, invoking the core asynchronous initialisation
/// and long-poll loop, then returns on completion.
///
/// # Examples
///
/// ```no_run
/// fn main_wrapper() {
///     // In normal execution the binary's `main` calls this function.
///     // `main()` returns a `Result<()>` which can be unwrapped for simple examples.
///     rsstg::main().unwrap();
/// }
/// ```
///
/// Returns `Ok(())` on successful completion, or an error if startup fails.
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

	LongPoll::new(core.tg.clone(), core).run().await;

	Ok(())
}