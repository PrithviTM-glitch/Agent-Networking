//! The services' command line (ledgerd, historyd), as Python's argparse with allow_abbrev=False parses
//! [-h] [--health]: the same outcome, and on a bad argument the same stderr and exit status 2. Only no arguments (or
//! --health alone, repeated) ever reaches the service, so a stray flag cannot start a second one that takes the live
//! one's session over.

use crate::validation::py_repr;

#[derive(Debug, PartialEq)]
pub enum Mode {
    Serve,
    Health,
    Help,
    /// what to print on stderr before exiting with status 2
    Bad(String),
}

pub fn usage(prog: &str) -> String {
    format!("usage: {prog} [-h] [--health]")
}

pub fn mode(prog: &str, args: &[String]) -> Mode {
    let bad = |message: String| Mode::Bad(format!("{}\n{prog}: error: {message}\n", usage(prog)));
    let ignored =
        |name: &str, value: &str| bad(format!("argument {name}: ignored explicit argument {}", py_repr(value)));
    let (mut health, mut extras) = (false, Vec::new());
    for (i, arg) in args.iter().enumerate() {
        match arg.as_str() {
            // everything from -- on is a positional, and there are none
            "--" => {
                extras.extend(args[i..].iter().map(String::as_str));
                break;
            }
            "-h" | "--help" => return Mode::Help,
            "--health" => health = true,
            a if a.starts_with("--help=") => return ignored("-h/--help", &a["--help=".len()..]),
            a if a.starts_with("--health=") => return ignored("--health", &a["--health=".len()..]),
            a if a.starts_with("-h=") => return ignored("-h/--help", &a["-h=".len()..]),
            // a cluster of short flags: -h takes no argument, so the rest is more flags (or a dash, refused)
            a if a.starts_with("-h-") => return ignored("-h/--help", &a["-h".len()..]),
            a if a.starts_with("-h") => return Mode::Help,
            a => extras.push(a),
        }
    }
    match (extras.is_empty(), health) {
        (false, _) => bad(format!("unrecognized arguments: {}", extras.join(" "))),
        (true, true) => Mode::Health,
        (true, false) => Mode::Serve,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn of(args: &[&str]) -> Mode {
        mode("agent-bridge-historyd", &args.iter().map(|a| a.to_string()).collect::<Vec<_>>())
    }

    /// The last stderr line Python 3.12's argparse (allow_abbrev=False) prints for args.
    fn error(args: &[&str]) -> String {
        match of(args) {
            Mode::Bad(text) => {
                assert!(text.starts_with("usage: agent-bridge-historyd [-h] [--health]\n"), "{text}");
                text.lines().last().unwrap_or_default().to_owned()
            }
            other => format!("{other:?}"),
        }
    }

    #[test]
    fn like_argparse() {
        assert_eq!(of(&[]), Mode::Serve);
        assert_eq!(of(&["--health"]), Mode::Health);
        assert_eq!(of(&["--health", "--health"]), Mode::Health);
        for help in
            [&["-h"][..], &["--help"], &["--bogus", "-h"], &["-h", "--bogus"], &["-hx"], &["x", "--health", "-hh"]]
        {
            assert_eq!(of(help), Mode::Help, "{help:?}");
        }
        let e = "agent-bridge-historyd: error:";
        for (args, last) in [
            (&["--bogus"][..], "unrecognized arguments: --bogus"),
            (&["--health", "x"], "unrecognized arguments: x"),
            (&["serve"], "unrecognized arguments: serve"),
            (&[""], "unrecognized arguments: "),
            (&["--hea"], "unrecognized arguments: --hea"),
            (&["--he"], "unrecognized arguments: --he"),
            (&["-x"], "unrecognized arguments: -x"),
            (&["-"], "unrecognized arguments: -"),
            (&["--"], "unrecognized arguments: --"),
            (&["--", "x"], "unrecognized arguments: -- x"),
            (&["--health", "--", "-h"], "unrecognized arguments: -- -h"),
            (&["x", "y", "--z"], "unrecognized arguments: x y --z"),
            (&["--health=1"], "argument --health: ignored explicit argument '1'"),
            (&["--help=1"], "argument -h/--help: ignored explicit argument '1'"),
            (&["-h=1"], "argument -h/--help: ignored explicit argument '1'"),
            (&["--health=1", "-h"], "argument --health: ignored explicit argument '1'"),
            (&["-h-x"], "argument -h/--help: ignored explicit argument '-x'"),
            (&["--help="], "argument -h/--help: ignored explicit argument ''"),
            (&["--health="], "argument --health: ignored explicit argument ''"),
        ] {
            assert_eq!(error(args), format!("{e} {last}"), "{args:?}");
        }
    }
}
