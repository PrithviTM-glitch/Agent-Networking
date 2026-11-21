"""The docs as a contract: what README.md and docs/ promise about the commands, the MCP server, the refusal texts and
themselves has to match the code. These are the checks bridge-public ran by hand before each release, made tests."""

import re
import subprocess

from conftest import BIN, ROOT

README = ROOT / "README.md"
ARCHITECTURE = ROOT / "docs" / "architecture.md"
DOCS = [README, *sorted((ROOT / "docs").glob("*.md"))]


def help_commands(*args):
    """The subcommands `agent-bridge ARGS --help` lists under "Commands:", without clap's own `help`."""
    out = subprocess.run([str(BIN / "agent-bridge"), *args, "--help"], capture_output=True, text=True, timeout=10)
    assert out.returncode == 0, out.stderr
    names, listing = [], False
    for line in out.stdout.splitlines():
        if line.startswith("Commands:"):
            listing = True
        elif listing and (m := re.match(r"  (\S+)", line)):
            names.append(m.group(1))
        elif listing and line and not line.startswith(" "):
            break
    return [n for n in names if n != "help"]


def test_every_cli_subcommand_is_in_the_architecture_doc():
    """Every subcommand `agent-bridge --help` lists, and every nested one (`worktree acquire`, ...), is named in
    backticks in docs/architecture.md."""
    text = ARCHITECTURE.read_text()
    missing = []
    for command in help_commands():
        if f"`{command}`" not in text:
            missing.append(command)
        for sub in help_commands(command) if command != "help" else []:
            if not re.search(rf"`[^`]*\b{re.escape(sub)}\b[^`]*`", text):
                missing.append(f"{command} {sub}")
    assert not missing, f"named by --help but not in docs/architecture.md: {missing}"


def test_the_readme_counts_the_mcp_tools():
    tools = len(re.findall(r"^@tool\(", (ROOT / "src" / "agent_bridge" / "mcp_server.py").read_text(), re.M))
    said = re.search(r"gives a Claude Code session (\d+) tools", README.read_text())
    assert said and int(said.group(1)) == tools, f"the README says {said and said.group(1)}, mcp_server.py has {tools}"


def sources():
    """Every Rust and Python source, with Rust's backslash string continuations joined, as one text to search."""
    files = [*(ROOT / "rust" / "crates").rglob("*.rs"), *(ROOT / "src" / "agent_bridge").rglob("*.py")]
    return "\n".join(re.sub(r"\\\n\s*", "", f.read_text(errors="replace")) for f in files)


REFUSAL = re.compile(r"`((?:blocked: |you may not |agent-bridge: )[^`]+)`")
# <placeholders>, and the ellipses docs use for the rest of a message, stand for text the code fills in.
PLACEHOLDER = re.compile(r"<[^<>]+>|\.\.\.|…")


def test_every_quoted_refusal_is_in_the_sources():
    """Every refusal-looking string the docs quote ("blocked: ...", "you may not ...", "agent-bridge: ...") occurs in
    the Rust or Python sources: each literal piece between its placeholders, verbatim."""
    code = sources()
    wrong, checked = [], 0
    for doc in (README, ARCHITECTURE):
        for quoted in REFUSAL.findall(doc.read_text()):
            quoted = " ".join(quoted.split())  # a quote wrapped across lines reads as one line
            pieces = [p.strip() for p in PLACEHOLDER.split(quoted) if len(p.strip()) >= 4]
            checked += 1
            if not pieces or any(p not in code for p in pieces):
                wrong.append(f"{doc.name}: {quoted}")
    assert checked >= 5, f"only {checked} quoted refusals found: the pattern no longer matches the docs"
    assert not wrong, "quoted refusals that no source contains:\n" + "\n".join(wrong)


def slugs(path):
    """GitHub's heading anchors for a Markdown file: lowercased, punctuation dropped, spaces as hyphens, repeats
    numbered."""
    seen, out, fence = {}, set(), False
    for line in path.read_text().splitlines():
        if line.startswith("```"):
            fence = not fence
        if fence or not (m := re.match(r"#{1,6} (.+)", line)):
            continue
        slug = re.sub(r"[^\w\- ]", "", m.group(1).strip().lower().replace("`", "")).replace(" ", "-")
        n = seen.get(slug, 0)
        seen[slug] = n + 1
        out.add(slug if n == 0 else f"{slug}-{n}")
    return out


def test_internal_links_resolve():
    """Every link from README.md or docs/*.md to an anchor or a file inside the repository resolves."""
    broken = []
    for doc in DOCS:
        for target in re.findall(r"\]\(([^)\s]+)\)", doc.read_text()):
            if re.match(r"[a-z]+:", target):
                continue  # http(s), mailto: not ours to check
            file, _, anchor = target.partition("#")
            dest = (doc.parent / file).resolve() if file else doc
            if not dest.exists():
                broken.append(f"{doc.name}: {target} (no such file)")
            elif anchor and dest.suffix == ".md" and anchor not in slugs(dest):
                broken.append(f"{doc.name}: {target} (no such heading)")
    assert not broken, "broken links:\n" + "\n".join(broken)


def test_every_test_file_has_a_readme_row():
    text = README.read_text()
    missing = [f.name for f in sorted((ROOT / "tests").glob("test_*.py")) if f"`tests/{f.name}`" not in text]
    assert not missing, f"test files with no row in the README's table: {missing}"
