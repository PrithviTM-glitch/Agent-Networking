//! `agent-bridge ledger-raw`, which `agent-bridge-ledger` runs: one ledgerd request exactly as given (any op, any
//! --as, no implicit init), the reply printed as JSON, exit status 1 when it is not ok. The arguments are read as the
//! Python client's argparse read them, usage errors included, so the old name behaves as it did.

use agent_bridge::json::{Map, dumps_map, float};
use agent_bridge::ledger;
use agent_bridge::validation::py_repr;
use serde_json::Value;

const PROG: &str = "agent-bridge-ledger";
const USAGE: &str = "usage: agent-bridge-ledger [-h] [--as ADDR] [--timeout TIMEOUT] op";
const HELP: &str = "

One ledgerd request, as given: OP, then the request's fields as --name value pairs (--ttl and --wait are numbers;
dashes in a name become underscores). Prints the reply as JSON; exit status 1 when ledgerd refuses or does not answer.

positional arguments:
  op

options:
  -h, --help         show this help message and exit
  --as ADDR
  --timeout TIMEOUT
";

struct Request {
    op: String,
    as_: Option<String>,
    timeout: Option<f64>,
    fields: Map,
}

enum Stop {
    Help,
    Usage(String),
}

/// A value for an option: argparse refuses one that looks like an option itself.
fn value_of(args: &[String], i: usize, name: &str) -> Result<String, Stop> {
    match args.get(i + 1) {
        Some(v) if !v.starts_with('-') || v == "-" || v.parse::<f64>().is_ok() => Ok(v.clone()),
        _ => Err(Stop::Usage(format!("argument {name}: expected one argument"))),
    }
}

/// Python's float() of a command-line string, or its error.
fn py_float(v: &str) -> Result<f64, String> {
    v.trim().parse::<f64>().map_err(|_| format!("could not convert string to float: {}", py_repr(v)))
}

fn parse(args: &[String]) -> Result<Request, Stop> {
    if args.iter().any(|a| a == "-h" || a == "--help") {
        return Err(Stop::Help);
    }
    let (mut op, mut as_, mut timeout, mut rest) = (None, None, None, Vec::new());
    let mut i = 0;
    while i < args.len() {
        let a = &args[i];
        if let Some(v) = a.strip_prefix("--as=") {
            as_ = Some(v.to_owned());
        } else if a == "--as" {
            as_ = Some(value_of(args, i, "--as")?);
            i += 1;
        } else if a == "--timeout" || a.starts_with("--timeout=") {
            let v = match a.strip_prefix("--timeout=") {
                Some(v) => v.to_owned(),
                None => {
                    i += 1;
                    value_of(args, i - 1, "--timeout")?
                }
            };
            let t = py_float(&v)
                .map_err(|_| Stop::Usage(format!("argument --timeout: invalid float value: {}", py_repr(&v))))?;
            timeout = Some(t);
        } else if op.is_none() && (!a.starts_with('-') || a == "-") {
            op = Some(a.clone());
        } else {
            rest.push(a.clone());
        }
        i += 1;
    }
    let op = op.ok_or_else(|| Stop::Usage("the following arguments are required: op".into()))?;
    if rest.len() % 2 == 1 || rest.iter().step_by(2).any(|k| !k.starts_with("--")) {
        return Err(Stop::Usage("fields go as --name value pairs".into()));
    }
    let mut fields = Map::new();
    for pair in rest.chunks(2) {
        let (k, v) = (&pair[0], &pair[1]);
        let value =
            if k == "--ttl" || k == "--wait" { float(py_float(v).map_err(Stop::Usage)?) } else { v.as_str().into() };
        fields.insert(k[2..].replace('-', "_"), value);
    }
    Ok(Request { op, as_, timeout, fields })
}

/// The exit status.
pub fn run(args: &[String]) -> i32 {
    let req = match parse(args) {
        Ok(req) => req,
        Err(Stop::Help) => {
            print!("{USAGE}{HELP}");
            return 0;
        }
        Err(Stop::Usage(message)) => {
            eprintln!("{USAGE}\n{PROG}: error: {message}");
            return 2;
        }
    };
    let reply = ledger::call_value(req.op.into(), req.as_.map(Value::from), req.fields, req.timeout);
    let (reply, status) = match reply {
        Ok(reply) => (reply, 0),
        Err(e) => (e.reply, 1),
    };
    println!("{}", dumps_map(&reply));
    status
}

#[cfg(test)]
mod tests {
    use super::*;

    fn usage(args: &[&str]) -> String {
        match parse(&args.iter().map(|a| a.to_string()).collect::<Vec<_>>()) {
            Err(Stop::Usage(m)) => m,
            Err(Stop::Help) => "help".into(),
            Ok(r) => format!("{} {:?} {:?} {}", r.op, r.as_, r.timeout, agent_bridge::json::dumps_map(&r.fields)),
        }
    }

    #[test]
    fn arguments_as_argparse_read_them() {
        assert_eq!(usage(&["dump", "--as", "a-1"]), r#"dump Some("a-1") None {}"#);
        assert_eq!(usage(&["--as", "a-1", "--timeout", "2", "dump"]), r#"dump Some("a-1") Some(2.0) {}"#);
        assert_eq!(
            usage(&["acquire", "--resource", "a/r", "--ttl", "5", "--max-level", "write"]),
            r#"acquire None None {"resource": "a/r", "ttl": 5.0, "max_level": "write"}"#
        );
        assert_eq!(usage(&["dump", "--bogus", "-h"]), "help");
        assert_eq!(usage(&[]), "the following arguments are required: op");
        assert_eq!(usage(&["dump", "--name"]), "fields go as --name value pairs");
        assert_eq!(usage(&["dump", "extra", "x"]), "fields go as --name value pairs");
        assert_eq!(usage(&["dump", "--as"]), "argument --as: expected one argument");
        assert_eq!(usage(&["dump", "--timeout", "soon"]), "argument --timeout: invalid float value: 'soon'");
        assert_eq!(usage(&["acquire", "--ttl", "x"]), "could not convert string to float: 'x'");
    }
}
