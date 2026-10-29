//! agent-bridge: the bus from the shell, the session relay and the Claude Code hooks. Its output and errors are those
//! of v0.1.0's Python CLI, pinned by tests/golden/cli.txt.

mod args;
mod commands;
mod hooks;
mod ledger_raw;
mod relay;

use clap::Parser;
use clap::error::ErrorKind;

fn main() {
    // A hook must never block or break a session: whatever happens, it exits 0 and says nothing.
    if std::env::args_os().nth(1).is_some_and(|a| a == "hook") {
        std::panic::set_hook(Box::new(|_| std::process::exit(0)));
        if let Ok(cli) = args::Cli::try_parse() {
            let _ = commands::run(cli.cmd);
        }
        return;
    }
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
