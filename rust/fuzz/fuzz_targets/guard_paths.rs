//! guard::paths on any input: resolve, glob and lexical never panic; lexical never leaves its base unless the rest
//! climbs out with "..", and leaves no "." or ".." behind; glob's matches are absolute.

#![no_main]

#[path = "common.rs"]
mod common;

use agent_bridge::guard::paths::{glob, lexical, resolve};
use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use std::path::{Component, Path, PathBuf};

#[derive(Arbitrary, Debug)]
struct Input {
    base: Vec<String>,
    rest: String,
    pattern: String,
}

fuzz_target!(|input: Input| {
    if input.rest.contains('\0') || input.pattern.contains('\0') {
        return;
    }
    // lexical, on a base of plain components
    let mut base = PathBuf::from("/");
    for c in input.base.iter().take(8) {
        if !c.is_empty() && !c.contains(['/', '\0']) && c != "." && c != ".." {
            base.push(c);
        }
    }
    let out = lexical(&base, &input.rest);
    assert!(out.is_absolute(), "{out:?}");
    assert!(
        out.components().all(|c| matches!(c, Component::RootDir | Component::Normal(_))),
        "lexical left . or .. behind: {base:?} + {:?} = {out:?}",
        input.rest
    );
    let climbs = Path::new(&input.rest).components().any(|c| matches!(c, Component::ParentDir | Component::RootDir));
    if !climbs {
        assert!(out.starts_with(&base), "left the base without ..: {base:?} + {:?} = {out:?}", input.rest);
    }
    // resolve and glob against the fixture
    let f = common::fixture();
    for cwd in [&f.free, &f.lane] {
        assert!(resolve(&input.rest, cwd).is_absolute());
        if let Some(found) = glob(&input.pattern, cwd, 64) {
            assert!(!found.is_empty() && found.iter().all(|p| p.is_absolute()), "{:?} -> {found:?}", input.pattern);
        }
    }
});
