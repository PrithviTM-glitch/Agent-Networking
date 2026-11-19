//! Just enough of a POSIX shell lexer to find what a command line would run: simple commands with their environment
//! assignments, words and redirections, the bodies of `$(...)` and backticks, and `sh -c` / `eval` / wrapper programs
//! unwrapped. Heredoc bodies are skipped (they are data). A word that the shell would expand at run time (a variable,
//! a command substitution, a glob) is marked dynamic: its value can't be known here, except that a word whose only
//! expansion is a glob is also marked `glob`, so the caller can expand it against the filesystem.

/// One word after quote removal; `dynamic` when the shell would still expand it, `glob` when that expansion is only
/// pathname expansion (`*`, `?`, `[`), with no variable or substitution.
#[derive(Debug, Clone, PartialEq)]
pub struct Word {
    pub text: String,
    pub dynamic: bool,
    pub glob: bool,
}

/// A simple command: `NAME=value ... program args ... redirections`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Cmd {
    pub env: Vec<(String, String)>,
    pub words: Vec<Word>,
    /// (operator, target) for every redirection that names a file, e.g. (">>", "out.txt")
    pub redirects: Vec<(String, Word)>,
}

impl Cmd {
    pub fn program(&self) -> Option<&str> {
        self.words.first().map(|w| w.text.rsplit('/').next().unwrap_or(&w.text).trim_start_matches('\\'))
    }
}

const MAX_DEPTH: usize = 4;
const KEYWORDS: [&str; 12] = ["then", "do", "else", "elif", "if", "while", "until", "!", "{", "}", "done", "fi"];
const SHELLS: [&str; 5] = ["sh", "bash", "zsh", "dash", "ksh"];
const WRAPPERS: [&str; 18] = [
    "env", "command", "exec", "nohup", "timeout", "nice", "ionice", "stdbuf", "time", "setsid", "sudo", "doas",
    "builtin", "chronic", "unbuffer", "xargs", "watch", "flock",
];
/// Wrapper options that take a value as the next word.
const WRAPPER_ARGS: [&str; 12] = ["-n", "-u", "-g", "-s", "-k", "-c", "-C", "-o", "-e", "-I", "-L", "-P"];

/// Every simple command `text` would run, with `sh -c`, `eval`, wrappers and command substitutions unwrapped.
/// Err when the text can't be lexed (an unterminated quote), or nests deeper than MAX_DEPTH.
pub fn commands(text: &str) -> Result<Vec<Cmd>, String> {
    let mut out = Vec::new();
    collect(text, 0, &mut out)?;
    Ok(out)
}

fn collect(text: &str, depth: usize, out: &mut Vec<Cmd>) -> Result<(), String> {
    if depth > MAX_DEPTH {
        return Err("commands nested too deeply".into());
    }
    let (cmds, nested) = lex(text)?;
    for inner in nested {
        collect(&inner, depth + 1, out)?;
    }
    for cmd in cmds {
        unwrap(cmd, depth, out)?;
    }
    Ok(())
}

/// Replace a wrapper or a shell with the command it runs.
fn unwrap(mut cmd: Cmd, depth: usize, out: &mut Vec<Cmd>) -> Result<(), String> {
    loop {
        // leading assignments, keywords
        while let Some(w) = cmd.words.first() {
            if let Some((k, v)) = assignment(&w.text).filter(|_| !w.dynamic || w.text.contains('=')) {
                cmd.env.push((k, v));
                cmd.words.remove(0);
            } else if KEYWORDS.contains(&w.text.as_str()) {
                cmd.words.remove(0);
            } else {
                break;
            }
        }
        let Some(prog) = cmd.program().map(str::to_owned) else {
            if !cmd.redirects.is_empty() {
                out.push(cmd); // a bare redirection, e.g. "> file"
            }
            return Ok(());
        };
        if matches!(prog.as_str(), "for" | "case" | "select" | "function") {
            return Ok(()); // the header names no program; its body's commands come separately
        }
        if SHELLS.contains(&prog.as_str())
            && let Some(i) = cmd.words.iter().position(|w| w.text == "-c")
        {
            match cmd.words.get(i + 1) {
                Some(script) if script.dynamic => {
                    cmd.words = vec![Word { text: prog, dynamic: false, glob: false }, script.clone()]; // judged as unknowable
                    out.push(cmd);
                }
                Some(script) => {
                    let script = script.text.clone();
                    if !cmd.redirects.is_empty() {
                        out.push(Cmd { env: vec![], words: vec![], redirects: cmd.redirects });
                    }
                    return collect(&script, depth + 1, out);
                }
                None => {}
            }
            return Ok(());
        }
        if prog == "eval" {
            let text: Vec<String> = cmd.words[1..].iter().map(|w| w.text.clone()).collect();
            return collect(&text.join(" "), depth + 1, out);
        }
        if WRAPPERS.contains(&prog.as_str()) {
            let mut rest = cmd.words.split_off(1);
            let is_timeout = prog == "timeout";
            let mut i = 0;
            while i < rest.len() {
                let t = &rest[i].text;
                if t == "--" {
                    i += 1;
                    break;
                } else if t.starts_with('-') && t.len() > 1 {
                    i += if WRAPPER_ARGS.contains(&t.as_str()) { 2 } else { 1 };
                } else if assignment(t).is_some() && prog == "env" {
                    if let Some((k, v)) = assignment(t) {
                        cmd.env.push((k, v));
                    }
                    i += 1;
                } else if is_timeout && t.chars().next().is_some_and(|c| c.is_ascii_digit()) {
                    i += 1; // the duration
                } else if prog == "flock" && i == 0 {
                    i += 1; // the lock file
                } else {
                    break;
                }
            }
            rest.drain(..i.min(rest.len()));
            if rest.is_empty() {
                return Ok(());
            }
            if prog == "xargs" {
                // the arguments come from stdin: mark the command's operands unknowable
                rest.push(Word { text: "<from stdin>".into(), dynamic: true, glob: false });
            }
            cmd.words = rest;
            continue;
        }
        out.push(cmd);
        return Ok(());
    }
}

fn assignment(text: &str) -> Option<(String, String)> {
    let (k, v) = text.split_once('=')?;
    let ok = k.chars().next().is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && k.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    ok.then(|| (k.to_owned(), v.to_owned()))
}

const OPS: [&str; 18] =
    ["&&", "||", ";;", "|&", "&>>", "&>", ">>", ">|", "<<-", "<<<", "<<", "<>", "<&", ">&", ";", "&", "|", ">"];

/// The simple commands of one shell text (not unwrapped), and the texts of its command substitutions.
#[allow(unused_assignments)] // the last end_cmd! resets state nothing reads again
fn lex(text: &str) -> Result<(Vec<Cmd>, Vec<String>), String> {
    let chars: Vec<char> = text.chars().collect();
    let (mut cmds, mut nested) = (Vec::new(), Vec::new());
    let mut cmd = Cmd::default();
    let (mut word, mut dynamic, mut in_word) = (String::new(), false, false);
    let mut subst = false; // the word has a variable or substitution, not only a glob
    let mut pending_redirect: Option<String> = None;
    let mut heredocs: Vec<(String, bool)> = Vec::new(); // (delimiter, strip tabs)
    let mut i = 0;

    macro_rules! end_word {
        () => {{
            if in_word {
                let w = Word { text: std::mem::take(&mut word), dynamic, glob: dynamic && !subst };
                if let Some(op) = pending_redirect.take() {
                    if op.starts_with("<<") && op != "<<<" {
                        heredocs.push((w.text.clone(), op == "<<-"));
                    } else if !((op == ">&" || op == "<&") && w.text.chars().all(|c| c.is_ascii_digit() || c == '-'))
                        && op != "<<<"
                    {
                        cmd.redirects.push((op, w));
                    }
                } else {
                    cmd.words.push(w);
                }
            }
            dynamic = false;
            subst = false;
            in_word = false;
        }};
    }
    macro_rules! end_cmd {
        () => {{
            end_word!();
            if !cmd.words.is_empty() || !cmd.redirects.is_empty() {
                cmds.push(std::mem::take(&mut cmd));
            }
        }};
    }

    while i < chars.len() {
        let c = chars[i];
        match c {
            ' ' | '\t' => end_word!(),
            '\n' => {
                end_cmd!();
                i += 1;
                // heredoc bodies are data: skip to each delimiter line
                for (delim, strip) in std::mem::take(&mut heredocs) {
                    loop {
                        if i >= chars.len() {
                            break;
                        }
                        let end = chars[i..].iter().position(|&c| c == '\n').map_or(chars.len(), |p| i + p);
                        let line: String = chars[i..end].iter().collect();
                        i = end + 1;
                        let line = if strip { line.trim_start_matches('\t').to_owned() } else { line };
                        if line == delim {
                            break;
                        }
                    }
                }
                continue;
            }
            '#' if !in_word => {
                while i < chars.len() && chars[i] != '\n' {
                    i += 1;
                }
                continue;
            }
            '\'' => {
                in_word = true;
                i += 1;
                while i < chars.len() && chars[i] != '\'' {
                    word.push(chars[i]);
                    i += 1;
                }
                if i >= chars.len() {
                    return Err("unterminated single quote".into());
                }
            }
            '"' => {
                in_word = true;
                i += 1;
                while i < chars.len() && chars[i] != '"' {
                    match chars[i] {
                        '\\' if i + 1 < chars.len() && "\"\\$`\n".contains(chars[i + 1]) => {
                            word.push(chars[i + 1]);
                            i += 2;
                            continue;
                        }
                        '$' | '`' => {
                            dynamic = true;
                            subst = true;
                            i = substitution(&chars, i, &mut word, &mut nested)?;
                            continue;
                        }
                        ch => word.push(ch),
                    }
                    i += 1;
                }
                if i >= chars.len() {
                    return Err("unterminated double quote".into());
                }
            }
            '\\' => {
                in_word = true;
                if i + 1 < chars.len() {
                    if chars[i + 1] != '\n' {
                        word.push(chars[i + 1]);
                    }
                    i += 2;
                    continue;
                }
            }
            '$' | '`' => {
                in_word = true;
                dynamic = true;
                subst = true;
                i = substitution(&chars, i, &mut word, &mut nested)?;
                continue;
            }
            '*' | '?' | '[' => {
                in_word = true;
                dynamic = true;
                word.push(c);
            }
            '(' | ')' => {
                end_cmd!();
            }
            ';' | '&' | '|' | '<' | '>' => {
                // a number right before a redirection is a file descriptor, not a word
                if in_word && !dynamic && (c == '<' || c == '>') && word.chars().all(|d| d.is_ascii_digit()) {
                    word.clear();
                    in_word = false;
                }
                end_word!();
                let rest: String = chars[i..chars.len().min(i + 3)].iter().collect();
                let op =
                    OPS.iter().find(|op| rest.starts_with(**op)).copied().unwrap_or(if c == '<' { "<" } else { ";" });
                i += op.chars().count();
                if op.contains('<') || op.contains('>') {
                    pending_redirect = Some(op.to_owned());
                } else {
                    end_cmd!();
                }
                continue;
            }
            _ => {
                in_word = true;
                word.push(c);
            }
        }
        i += 1;
    }
    end_cmd!();
    Ok((cmds, nested))
}

/// Copy a `$...` or backtick expansion starting at i into word; the body of a command substitution goes to nested.
/// Returns the index after it.
fn substitution(chars: &[char], mut i: usize, word: &mut String, nested: &mut Vec<String>) -> Result<usize, String> {
    if chars[i] == '`' {
        let start = i + 1;
        i = start;
        while i < chars.len() && chars[i] != '`' {
            i += if chars[i] == '\\' { 2 } else { 1 };
        }
        if i >= chars.len() {
            return Err("unterminated backquote".into());
        }
        nested.push(chars[start..i].iter().collect());
        word.extend(&chars[start - 1..=i]);
        return Ok(i + 1);
    }
    // '$'
    match chars.get(i + 1) {
        Some('(') => {
            let arithmetic = chars.get(i + 2) == Some(&'(');
            let start = i + 2;
            let (mut depth, mut j) = (1, start);
            while j < chars.len() && depth > 0 {
                match chars[j] {
                    '(' => depth += 1,
                    ')' => depth -= 1,
                    '\'' => {
                        j += 1;
                        while j < chars.len() && chars[j] != '\'' {
                            j += 1;
                        }
                    }
                    _ => {}
                }
                j += 1;
            }
            if depth > 0 {
                return Err("unterminated $(".into());
            }
            if !arithmetic {
                nested.push(chars[start..j - 1].iter().collect());
            }
            word.extend(&chars[i..j]);
            Ok(j)
        }
        Some('{') => {
            let end = chars[i..].iter().position(|&c| c == '}').map(|p| i + p).ok_or("unterminated ${")?;
            word.extend(&chars[i..=end]);
            Ok(end + 1)
        }
        _ => {
            word.push('$');
            Ok(i + 1)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn progs(text: &str) -> Vec<String> {
        commands(text).unwrap().iter().filter_map(|c| c.program().map(str::to_owned)).collect()
    }

    #[test]
    fn unwraps_what_runs() {
        assert_eq!(progs("cd x && rm -rf y; ls | wc -l"), ["cd", "rm", "ls", "wc"]);
        assert_eq!(progs("bash -c 'git commit -am x' && sh -c \"make\""), ["git", "make"]);
        assert_eq!(progs("eval 'rm a'"), ["rm"]);
        assert_eq!(progs("FOO=1 env BAR=2 nohup timeout 30 \\rm x"), ["rm"]);
        assert_eq!(progs("/usr/bin/git push"), ["git"]);
        assert_eq!(progs("echo $(rm -rf x) `touch y`"), ["rm", "touch", "echo"]);
        assert_eq!(progs("if test -f a; then rm a; fi"), ["test", "rm"]);
        assert_eq!(progs("(cd x; git push)"), ["cd", "git"]);
        assert_eq!(progs("cat > /tmp/x <<'E'\nrm -rf /\nE\nbash /tmp/x"), ["cat", "bash"], "heredoc bodies are data");
        let c = &commands("echo x >> out.txt 2>/dev/null >&2").unwrap()[0];
        assert_eq!(
            c.redirects.iter().map(|(o, w)| (o.as_str(), w.text.as_str())).collect::<Vec<_>>(),
            [(">>", "out.txt"), (">", "/dev/null")]
        );
        assert_eq!(c.words.len(), 2, "the 2 of 2> is a descriptor, not an argument");
        let x = &commands("echo a | xargs rm").unwrap()[1];
        assert!(x.words.last().unwrap().dynamic, "xargs operands come from stdin");
        assert!(commands("rm $D/x").unwrap()[0].words[1].dynamic);
        assert!(commands("rm src/*").unwrap()[0].words[1].dynamic);
        assert!(!commands("rm 'src/*'").unwrap()[0].words[1].dynamic, "quoted globs are literal");
        assert!(commands("rm src/*.tmp").unwrap()[0].words[1].glob, "a glob alone can be expanded");
        assert!(!commands("rm $D/*").unwrap()[0].words[1].glob, "a variable can't");
        assert!(commands("echo 'unterminated").is_err());
    }
}
