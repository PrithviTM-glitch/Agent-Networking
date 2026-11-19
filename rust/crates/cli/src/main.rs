//! agent-bridge: the bus from the shell, the session relay and the Claude Code hooks. Its output and errors are those
//! of v0.1.0's Python CLI, pinned by tests/golden/cli.txt.

mod args;
mod commands;
mod doctor;
mod hooks;
mod ledger_raw;
mod ref_guard;
mod relay;

use clap::Parser;
use clap::error::ErrorKind;

fn main() {
    // A session hook must never block or break a session: whatever happens, it exits 0 and says nothing. The guards
    // are the opposite: one that fails refuses (guard 2, ref-guard 1), never lets a write through.
    if std::env::args_os().nth(1).is_some_and(|a| a == "hook") {
        let refuse = match std::env::args_os().nth(2) {
            Some(w) if w == "guard" => 2,
            Some(w) if w == "ref-guard" => 1,
            _ => 0,
        };
        if refuse == 0 {
            std::panic::set_hook(Box::new(|_| std::process::exit(0)));
        } else {
            std::panic::set_hook(Box::new(move |_| {
                eprintln!("blocked: the agent-bridge guard failed (internal error)");
                std::process::exit(refuse)
            }));
        }
        match args::Cli::try_parse() {
            Ok(cli) => {
                let _ = commands::run(cli.cmd);
            }
            // asked for by a person, not by Claude Code: the help prints as anywhere else
            Err(e) if e.kind() == ErrorKind::DisplayHelp => e.exit(),
            Err(_) => std::process::exit(refuse),
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
    agent_bridge::ledger::cancel_on_signals();
    let parsed = args::Cli::try_parse();
    // help and --version read no configuration, so they work whatever the environment holds
    if let Err(e) = &parsed
        && matches!(e.kind(), ErrorKind::DisplayHelp | ErrorKind::DisplayVersion)
    {
        e.exit()
    }
    if let Err(e) = agent_bridge::config::get() {
        eprintln!("{e}");
        std::process::exit(1);
    }
    let cli = match parsed {
        Ok(cli) => cli,
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
