//! agent-bridge: the bus from the shell, and the session relay. Wire-compatible with the Python CLI.

mod args;
mod commands;
mod relay;

use clap::Parser;
use clap::error::ErrorKind;

fn main() {
    std::panic::set_hook(Box::new(|info| {
        let what = info
            .payload()
            .downcast_ref::<&str>()
            .map(|s| s.to_string())
            .or_else(|| info.payload().downcast_ref::<String>().cloned())
            .unwrap_or_default();
        eprintln!("internal error: {}", what.split_whitespace().collect::<Vec<_>>().join(" "));
        std::process::exit(1);
    }));
    if let Err(e) = agent_bridge::config::get() {
        eprintln!("{e}");
        std::process::exit(1);
    }
    let cli = match args::Cli::try_parse() {
        Ok(cli) => cli,
        Err(e) if matches!(e.kind(), ErrorKind::DisplayHelp | ErrorKind::DisplayVersion) => e.exit(),
        Err(e) if e.kind() == ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand => {
            eprintln!("agent-bridge: the following arguments are required: COMMAND");
            std::process::exit(1);
        }
        Err(e) => {
            let text = e.to_string();
            let first = text.lines().next().unwrap_or("bad arguments").trim_start_matches("error: ");
            eprintln!("agent-bridge: {first}");
            std::process::exit(1);
        }
    };
    if let Err(e) = commands::run(cli.cmd) {
        eprintln!("{e}");
        std::process::exit(1);
    }
}
