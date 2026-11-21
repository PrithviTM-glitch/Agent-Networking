//! The addresses register makes, as a type: a new agent's `<name>-<6 hex>` and a session's own `<project>-<8 hex>`
//! (shared by its relay and MCP server). Provisioning gives credentials to these and nothing else, through the same
//! `Address::parse`, so a shape one constructor makes can never be one provisioning refuses (0.3.2 accepted only
//! 6 hex, which would have left every session address anonymous).

use crate::validation::{ID_MAX, NAME_MAX, Result, check_ident_cap, is_ident};
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Address(String);

/// The hex suffix lengths register makes: a new agent's 3 random bytes, a session's 4 digest bytes.
const AGENT_HEX: usize = 6;
const SESSION_HEX: usize = 8;

impl Address {
    /// A new agent's address: name and 3 random bytes in hex.
    pub fn new_agent(name: &str) -> Result<Address> {
        check_ident_cap(name, "name", NAME_MAX)?;
        Ok(Address::with_suffix(name, &crate::ledger::random_bytes(AGENT_HEX / 2)))
    }

    /// A session's own address: project and the first 4 bytes of sha256(session) in hex.
    pub fn session(project: &str, session: &str) -> Address {
        let digest = Sha256::digest(session.as_bytes());
        Address::from_digest(project, [digest[0], digest[1], digest[2], digest[3]])
    }

    /// session's address from its 4 digest bytes; separate so a proof can range over every digest.
    pub fn from_digest(project: &str, digest: [u8; 4]) -> Address {
        Address::with_suffix(project, &digest)
    }

    /// name, "-" and bytes in lowercase hex. Built by hand rather than with format!, whose machinery a Kani proof
    /// can't get through.
    fn with_suffix(name: &str, bytes: &[u8]) -> Address {
        let mut text = String::with_capacity(name.len() + 1 + 2 * bytes.len());
        text.push_str(name);
        text.push('-');
        for b in bytes {
            for h in hex(*b) {
                text.push(h as char);
            }
        }
        Address(text)
    }

    /// text as an address register makes, or None. The one predicate provisioning uses.
    pub fn parse(text: &str) -> Option<Address> {
        (shaped(text.as_bytes()) && is_ident(text, ID_MAX)).then(|| Address(text.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A byte in lowercase hex: the encoding with_suffix uses, by hand (format!'s machinery is more than a Kani proof can
/// get through).
fn hex(b: u8) -> [u8; 2] {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    [HEX[usize::from(b >> 4)], HEX[usize::from(b & 0x0f)]]
}

/// A non-empty name, then after the last '-' exactly 6 or 8 lowercase hex digits. The last '-' is found by a plain
/// loop: rsplit_once's memrchr is more than a Kani proof can unwind.
fn shaped(text: &[u8]) -> bool {
    let mut dash = None;
    for (i, b) in text.iter().enumerate() {
        if *b == b'-' {
            dash = Some(i);
        }
    }
    let Some(dash) = dash else { return false };
    let suffix = &text[dash + 1..];
    dash > 0
        && matches!(suffix.len(), AGENT_HEX | SESSION_HEX)
        && suffix.iter().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

impl std::fmt::Display for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Kani (scripts/verify-kani.sh): over every digest, a session's address parses, so it is provisionable.
#[cfg(kani)]
mod proofs {
    use super::*;

    /// The bytes from_digest writes for project "p", with no heap: "p-" and the digest through the same hex(), and
    /// shaped() (parse's check) accepts them, for every digest.
    #[kani::proof]
    #[kani::unwind(12)]
    fn every_session_digest_makes_a_parsable_address() {
        let digest: [u8; 4] = kani::any();
        let mut text = [0u8; 10];
        text[0] = b'p';
        text[1] = b'-';
        for (i, b) in digest.iter().enumerate() {
            let [hi, lo] = hex(*b);
            text[2 + 2 * i] = hi;
            text[3 + 2 * i] = lo;
        }
        assert!(shaped(&text));
        assert!(text.iter().all(|b| crate::validation::is_ident_char(*b as char)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const UUID: &str = "[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}";
    const NAME: &str = "[A-Za-z0-9_][A-Za-z0-9_-]{0,39}";

    proptest! {
        /// What a session's relay and MCP server register as is always provisionable (the 0.3.2 bug).
        #[test]
        fn every_session_address_parses(session in UUID, project in NAME) {
            let made = crate::identity::session_address(&session);
            prop_assert!(Address::parse(&made).is_some(), "{made}");
            prop_assert!(crate::provision::provisionable(&made), "{made}");
            let ours = Address::session(&project, &session);
            prop_assert_eq!(Address::parse(ours.as_str()), Some(ours));
        }

        /// A new agent's address parses back to itself.
        #[test]
        fn every_new_agent_address_parses(name in NAME) {
            let made = Address::new_agent(&name).expect("a valid name");
            prop_assert_eq!(Address::parse(made.as_str()), Some(made.clone()));
            prop_assert!(crate::provision::provisionable(made.as_str()));
        }

        /// Nothing but those two shapes parses: no other suffix length, no uppercase, no missing name.
        #[test]
        fn other_shapes_do_not_parse(name in NAME, hex in "[0-9a-f]{0,12}") {
            let text = format!("{name}-{hex}");
            prop_assert_eq!(Address::parse(&text).is_some(), matches!(hex.len(), 6 | 8) && text.len() <= ID_MAX);
        }
    }

    #[test]
    fn reserved_and_odd_shapes() {
        for no in [
            "admin",
            "ledgerd",
            "historyd",
            "al-1",
            "al-A1B2C3",
            "-a1b2c3",
            "al-a1b2c3d",
            "al-a1b2c3d4e5",
            "a/b-a1b2c3",
        ] {
            assert_eq!(Address::parse(no), None, "{no}");
        }
        assert_eq!(Address::from_digest("p", [0, 0x0f, 0xa0, 0xff]).as_str(), "p-000fa0ff");
        for b in 0..=255u8 {
            let want: String = format!("q-{b:02x}{b:02x}{b:02x}{b:02x}");
            assert_eq!(Address::from_digest("q", [b; 4]).as_str(), want, "the same bytes format! wrote");
        }
    }
}
