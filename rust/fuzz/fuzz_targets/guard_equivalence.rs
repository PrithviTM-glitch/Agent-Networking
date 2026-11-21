//! The guard decides the same for every spelling of one command: extra blanks and tabs, words quoted ('w', "w") or
//! backslash-escaped where the shell's quote removal gives back the same word. A command is drawn from words that
//! matter to the guard (mutators, git, paths into and around the locked lane, globs, variables, redirections).

#![no_main]

#[path = "common.rs"]
mod common;

use agent_bridge::guard::{ToolCall, decide};
use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;

#[rustfmt::skip]
const PROGS: &[&str] = &[
    "rm", "cp", "mv", "touch", "cat", "ls", "git", "make", "sed", "echo", "tee", "find", "cd", "chmod", "ln", "grep",
    "timeout", "nohup", "xargs", "sort", "dd",
];
#[rustfmt::skip]
const WORDS: &[&str] = &[
    "-f", "-rf", "-i", "-n", "-s", "-o", "-C", "--git-dir", "-c", "core.hooksPath=/dev/null", "commit", "-am", "wip",
    "status", "log", "config", "core.hooksPath", "-delete", "-name", "5", "s/a/b/", "of=x", "--", "x", "755",
];
#[rustfmt::skip]
const PATHS: &[&str] = &[
    "LANE/src/a.txt", "LANE", "LANE/src", "LANE/new", "b.txt", "via-link/src/a.txt", "hard.txt", "../lane/x",
    "src/a.txt", "/tmp/agent-bridge-fuzz-out", "c.txt", "REPO", "REPO/.git/config", "REPO/.git/refs/heads/lane", ".",
    "..",
];
#[rustfmt::skip]
const DYNAMIC: &[&str] = &["$X", "\"$X\"", "*.tmp", "src/*", "via-*/src/a.txt", "LANE/s*/a.*", "$(pwd)", "b.t?t"];
const OPS: &[&str] = &[";", "&&", "||", "|", "\n"];
const BLANKS: &[&str] = &[" ", "  ", "\t", " \t "];

#[derive(Arbitrary, Debug)]
enum Tok {
    Prog(u8),
    Word(u8),
    Path(u8),
    Dynamic(u8),
    Redirect(bool, u8),
}

#[derive(Arbitrary, Debug)]
struct Spelling {
    quote: u8,
    blank: u8,
}

#[derive(Arbitrary, Debug)]
struct Input {
    cmds: Vec<(u8, Vec<(Tok, Spelling)>)>,
    cwd: u8,
}

fn pick<'a>(list: &[&'a str], i: u8) -> &'a str {
    list[i as usize % list.len()]
}

/// A plain word spelled another way with the same meaning after quote removal; anything else is left alone.
fn spell(word: &str, s: &Spelling) -> String {
    let plain = word.chars().all(|c| c.is_ascii_alphanumeric() || "/._-:".contains(c));
    if !plain || word.contains('=') {
        return word.to_owned();
    }
    match s.quote % 4 {
        0 => word.to_owned(),
        1 => format!("'{word}'"),
        2 => format!("\"{word}\""),
        _ => format!("\\{word}"),
    }
}

/// The command spelled plainly (single blanks, no quoting) and varied.
fn render(input: &Input) -> (String, String) {
    let f = common::fixture();
    let (lane, repo) = (f.lane.display().to_string(), f.repo.display().to_string());
    let fill = |w: &str| w.replace("LANE", &lane).replace("REPO", &repo);
    let (mut plain, mut varied) = (String::new(), String::new());
    for (i, (op, toks)) in input.cmds.iter().take(4).enumerate() {
        if i > 0 {
            plain.push_str(&format!(" {} ", pick(OPS, *op)));
            varied.push_str(&format!("  {} ", pick(OPS, *op)));
        }
        for (j, (tok, s)) in toks.iter().take(6).enumerate() {
            if j > 0 {
                plain.push(' ');
                varied.push_str(pick(BLANKS, s.blank));
            }
            let (p, v) = match tok {
                Tok::Prog(i) => (pick(PROGS, *i).to_owned(), spell(pick(PROGS, *i), s)),
                Tok::Word(i) => (pick(WORDS, *i).to_owned(), spell(pick(WORDS, *i), s)),
                Tok::Path(i) => (fill(pick(PATHS, *i)), spell(&fill(pick(PATHS, *i)), s)),
                Tok::Dynamic(i) => (fill(pick(DYNAMIC, *i)), fill(pick(DYNAMIC, *i))),
                Tok::Redirect(append, i) => {
                    let op = if *append { ">>" } else { ">" };
                    let target = fill(pick(PATHS, *i));
                    (format!("{op}{target}"), format!("{op} {}", spell(&target, s)))
                }
            };
            plain.push_str(&p);
            varied.push_str(&v);
        }
    }
    (plain, varied)
}

fuzz_target!(|input: Input| {
    let f = common::fixture();
    let cwd = [&f.free, &f.lane, &f.repo][input.cwd as usize % 3].clone();
    let decided = |text: String| {
        let mut args = serde_json::Map::new();
        args.insert("command".into(), text.into());
        decide(&ToolCall { tool: "Bash".into(), input: args, cwd: cwd.clone() }, &common::Mock)
    };
    let (plain, varied) = render(&input);
    let (a, b) = (decided(plain.clone()), decided(varied.clone()));
    assert_eq!(a, b, "the same command decided differently:\n  {plain:?}\n  {varied:?}");
});
