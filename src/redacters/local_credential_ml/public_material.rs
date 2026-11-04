//! Recognises text that a credential-shaped line can hold but that is not a secret: public
//! keys and certificates, fields that are public by definition, hashes and signatures, and
//! values that decode to words or to a counting sequence.
//!
//! The classifier reads one line and rates how much it looks like a credential. It cannot
//! tell a public key from a private one when the line is a bare run of base64, nor a
//! `"x": "…"` coordinate of a public JWK from a secret, so this module decides that from the
//! parts of the line it can see and from the PEM block around it.

use base64::alphabet;
use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};
use base64::Engine;
use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::ops::Range;

/// Words that name data which is public by definition: the members of a public JWK, the
/// parts of a signature, fingerprints and certificates. An identifier is public when any of
/// its words is one of these.
const PUBLIC_FIELD_WORDS: &[&str] = &[
    "kid",
    "crv",
    "kty",
    "sig",
    "signature",
    "ciphertext",
    "tag",
    "nonce",
    "pub",
    "public",
    "thumbprint",
    "fingerprint",
    "certificate",
    "cert",
];

/// Identifiers of one or two characters that name public data (`x` and `y` of a JWK). Longer
/// identifiers are matched by their words, so `x_api_key` stays a key.
const PUBLIC_SHORT_FIELDS: &[&str] = &["x", "y", "n", "e", "ct", "pk", "iv"];

/// Type names and keywords that stand between a variable name and its value
/// (`const KEY: &str = "…"`) and are not the name of the value.
const DECLARATION_WORDS: &[&str] = &[
    "str", "string", "u8", "u16", "u32", "u64", "usize", "i32", "i64", "vec", "bytes", "bool",
    "const", "static", "let", "var", "val", "final", "mut", "pub",
];

/// Prefixes that a hash algorithm puts before the digest in lock files and integrity fields.
const HASH_PREFIXES: &[&str] = &[
    "h1:", "sha1-", "sha256-", "sha384-", "sha512-", "sha256:", "sha512:", "md5:",
];

/// Letters in a row, in a decoded value, that make it text and not random bytes. Seven is
/// where a random 32-byte key has about a 1 in 2,000 chance of holding such a run.
const TEXT_RUN_LETTERS: usize = 7;

/// Share of the steps between neighbouring bytes that must be equal for the bytes to count
/// as a sequence (`00 01 02 …`).
const SEQUENCE_SHARE: f64 = 0.8;

/// Shortest hex or base64 value that is decoded to look for text or a sequence.
const MIN_DECODED_VALUE_LENGTH: usize = 8;

/// Fewest bytes in which a repeated value or a constant step is a pattern and not a chance.
const MIN_SEQUENCE_BYTES: usize = 16;

/// Shortest run of letters that is judged to be an alphabet.
const MIN_ALPHABET_LENGTH: usize = 12;

const LENIENT_BASE64: GeneralPurpose = GeneralPurpose::new(
    &alphabet::STANDARD,
    GeneralPurposeConfig::new()
        .with_decode_allow_trailing_bits(true)
        .with_decode_padding_mode(DecodePaddingMode::Indifferent),
);

#[derive(Debug)]
pub struct PublicMaterial {
    pem_begin: Regex,
    ssh_public_key: Regex,
    name_before_separator: Regex,
}

impl PublicMaterial {
    pub fn new() -> Result<Self, regex::Error> {
        Ok(Self {
            pem_begin: Regex::new(r"-----BEGIN ([A-Z0-9 ]+)-----")?,
            ssh_public_key: Regex::new(
                r"\b(?:ssh-(?:rsa|ed25519|dss)|ecdsa-sha2-nistp\d+|sk-ssh-ed25519@openssh\.com)\s+AAAA",
            )?,
            name_before_separator: Regex::new(r#"([A-Za-z_][A-Za-z0-9_-]*)["']?\s*[:=]"#)?,
        })
    }

    /// The byte ranges of the PEM blocks of `text` that are not private keys: public keys,
    /// certificates and requests, from their `BEGIN` marker to their `END` marker. A block
    /// with no `END` marker is not a block.
    pub fn public_blocks(&self, text: &str) -> Vec<Range<usize>> {
        self.pem_begin
            .captures_iter(text)
            .filter_map(|captures| {
                let label = &captures[1];
                let begin = captures.get(0)?;
                if label.contains("PRIVATE") {
                    return None;
                }
                let end_marker = format!("-----END {label}-----");
                let end = begin.end() + text[begin.end()..].find(&end_marker)? + end_marker.len();
                Some(begin.start()..end)
            })
            .collect()
    }

    /// Whether the whole `line` is public material whatever it holds, such as an SSH public
    /// key.
    pub fn is_public_line(&self, line: &str) -> bool {
        self.ssh_public_key.is_match(line)
    }

    /// Whether the `value` on `line` is public material: named by a public field, prefixed by
    /// a hash algorithm, or shaped like something other than a secret.
    pub fn is_public_value(&self, line: &str, value: Range<usize>) -> bool {
        let text = &line[value.clone()];
        HASH_PREFIXES.iter().any(|prefix| text.starts_with(prefix))
            || self.is_named_public(&line[..value.start])
            || is_alphabet(text)
            || decodes_to_non_secret(text)
    }

    /// Whether the last name assigned or declared in `before`, the text of the line before a
    /// value, is a public field.
    fn is_named_public(&self, before: &str) -> bool {
        let names: Vec<&str> = self
            .name_before_separator
            .captures_iter(before)
            .filter_map(|captures| captures.get(1))
            .map(|name| name.as_str())
            .collect();
        let Some(name) = names
            .into_iter()
            .rev()
            .find(|name| !DECLARATION_WORDS.contains(&name.to_ascii_lowercase().as_str()))
        else {
            return false;
        };
        if name.len() <= 2 {
            return PUBLIC_SHORT_FIELDS.contains(&name.to_ascii_lowercase().as_str());
        }
        identifier_words(name)
            .iter()
            .any(|word| PUBLIC_FIELD_WORDS.contains(&word.as_str()))
    }
}

/// The lowercase words of an identifier in snake, kebab or camel case.
fn identifier_words(identifier: &str) -> Vec<String> {
    let mut words = vec![String::new()];
    let mut previous_is_lower = false;
    for character in identifier.chars() {
        if !character.is_alphanumeric() || (character.is_uppercase() && previous_is_lower) {
            words.push(String::new());
        }
        if character.is_alphanumeric() {
            words
                .last_mut()
                .expect("words starts with one entry")
                .extend(character.to_lowercase());
        }
        previous_is_lower = character.is_lowercase() || character.is_ascii_digit();
    }
    words.retain(|word| !word.is_empty());
    words
}

/// Letters only, none used twice: an alphabet or a permutation, such as the checksum letters of
/// a national ID. A random secret of that length over 52 letters repeats one almost surely.
fn is_alphabet(text: &str) -> bool {
    if text.len() < MIN_ALPHABET_LENGTH || !text.chars().all(|c| c.is_ascii_alphabetic()) {
        return false;
    }
    let mut seen = HashSet::new();
    text.chars().all(|c| seen.insert(c.to_ascii_lowercase()))
}

/// Hex or base64 whose bytes are words (`wrapped`, `YELLOW SUBMARINE`) or a counting sequence.
fn decodes_to_non_secret(text: &str) -> bool {
    let Some(bytes) = decode(text) else {
        return false;
    };
    holds_text_run(&bytes) || is_sequence(&bytes)
}

fn decode(text: &str) -> Option<Vec<u8>> {
    if text.len() < MIN_DECODED_VALUE_LENGTH {
        return None;
    }
    if text.len().is_multiple_of(2) && text.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return (0..text.len())
            .step_by(2)
            .map(|index| u8::from_str_radix(&text[index..index + 2], 16).ok())
            .collect();
    }
    let is_base64 = text
        .trim_end_matches('=')
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/' | b'-' | b'_'));
    if !is_base64 || text.len() - text.trim_end_matches('=').len() > 2 {
        return None;
    }
    let standard: String = text
        .trim_end_matches('=')
        .chars()
        .map(|c| match c {
            '-' => '+',
            '_' => '/',
            other => other,
        })
        .collect();
    LENIENT_BASE64.decode(standard).ok()
}

fn holds_text_run(bytes: &[u8]) -> bool {
    let mut run = 0;
    for byte in bytes {
        if byte.is_ascii_alphabetic() || matches!(byte, b' ' | b'-' | b'_') {
            run += 1;
            if run >= TEXT_RUN_LETTERS {
                return true;
            }
        } else {
            run = 0;
        }
    }
    false
}

/// Bytes that step by the same amount between neighbours for most of their length, or that
/// take only a few values.
fn is_sequence(bytes: &[u8]) -> bool {
    if bytes.len() < MIN_SEQUENCE_BYTES {
        return false;
    }
    let mut steps: HashMap<u8, usize> = HashMap::new();
    for pair in bytes.windows(2) {
        *steps.entry(pair[1].wrapping_sub(pair[0])).or_default() += 1;
    }
    let most_common = steps.values().copied().max().unwrap_or(0);
    let mut distinct = bytes.to_vec();
    distinct.sort_unstable();
    distinct.dedup();
    most_common as f64 >= SEQUENCE_SHARE * (bytes.len() - 1) as f64 || distinct.len() <= 4
}

#[cfg(test)]
mod tests {
    use super::*;

    fn material() -> PublicMaterial {
        PublicMaterial::new().unwrap()
    }

    fn public(line: &str, value: &str) -> bool {
        let start = line.find(value).unwrap();
        material().is_public_value(line, start..start + value.len())
    }

    const HEX_VALUE: &str = "a4c123b1612dd272d1371c17149d439536b3216fdaeeb975729fae923d5a4fd1";

    #[test]
    fn the_members_of_a_public_jwk_are_public() {
        for line in [
            r#"  x: "YP7UuiVanTHJYet0xjVtaMBJuJI7Yfps5mliLmDyn7Y","#,
            r#"  "y":"eQP-EAi4vJmkGunpVii8ZPLxsgwtfp9Rd6PClNRGIpk","#,
            r#"  kid: "DOvxvJiAdIqVWIkFt5hDtCunXLF0BV4-JGv4f-ALSm0","#,
        ] {
            let value = line.split('"').rev().nth(1).unwrap();
            assert!(public(line, value), "{line}");
        }
    }

    #[test]
    fn a_field_is_named_by_the_identifier_before_its_separator() {
        assert!(public(
            &format!(r#"const P256_PUBLIC_KEY: &str = "{HEX_VALUE}";"#),
            HEX_VALUE
        ));
        assert!(public(
            &format!(r#"let tlsCertificate = "{HEX_VALUE}""#),
            HEX_VALUE
        ));
    }

    #[test]
    fn a_key_or_a_secret_is_not_public_and_neither_is_a_longer_name_with_an_x() {
        for line in [
            format!(r#"key: "{HEX_VALUE}""#),
            format!(r#"value: "{HEX_VALUE}""#),
            format!(r#"x_api_key: "{HEX_VALUE}""#),
            format!(r#"aeadKey: "{HEX_VALUE}""#),
            format!(r#"client_secret = "{HEX_VALUE}""#),
        ] {
            assert!(!public(&line, HEX_VALUE), "{line}");
        }
    }

    #[test]
    fn a_public_name_earlier_on_the_line_does_not_make_a_later_secret_public() {
        let line = format!(r#"{{"public": 1, "api_key": "{HEX_VALUE}"}}"#);
        assert!(!public(&line, HEX_VALUE));
    }

    #[test]
    fn a_value_after_a_hash_algorithm_is_a_hash() {
        for line in [
            "github.com/pkg/errors v0.9.1 h1:FEBLx1zS214owpjy7qsBeixbURkuhQAwrK5UwLGTwt4=",
            r#""integrity": "sha512-Gq4s3MJ1xHy2c1kQk8Qy6c4tSbWWb8m7TkYQe5QJyW9ZpP0O1KMWqqH2Gm4cTq6tb1UyWc3DtfEtSR0fwQ==""#,
        ] {
            let value = line
                .split(['"', ' '])
                .find(|word| word.starts_with("h1:") || word.starts_with("sha512-"))
                .unwrap();
            assert!(public(line, value), "{line}");
        }
    }

    #[test]
    fn letters_that_never_repeat_are_an_alphabet_and_not_a_secret() {
        assert!(public(
            r#"const ALPHABET: &str = "ABCDGHIKLMPQRVWXYZ";"#,
            "ABCDGHIKLMPQRVWXYZ"
        ));
        assert!(!public(
            r#"const TOKEN: &str = "qWeRtYuIoPaSdFgqKlZw";"#,
            "qWeRtYuIoPaSdFgqKlZw"
        ));
    }

    #[test]
    fn a_value_that_decodes_to_words_is_not_a_secret() {
        assert!(public(r#"let value = "d3JhcHBlZA";"#, "d3JhcHBlZA"));
        assert!(public(
            r#"let marker = "/wEAcmVjb3JkLWJ5dGVzLW1hcmtlcg==";"#,
            "/wEAcmVjb3JkLWJ5dGVzLW1hcmtlcg=="
        ));
        let yellow_submarine = "59454c4c4f57205355424d4152494e452059454c4c4f57205355424d4152494e";
        assert!(public(
            &format!(r#"key: "{yellow_submarine}""#),
            yellow_submarine
        ));
    }

    #[test]
    fn a_counting_sequence_of_bytes_is_a_test_value_and_random_hex_is_not() {
        let counting = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
        assert!(public(&format!(r#"key: "{counting}""#), counting));
        assert!(!public(&format!(r#"key: "{HEX_VALUE}""#), HEX_VALUE));
    }

    #[test]
    fn a_ssh_public_key_line_is_public() {
        let material = material();
        assert!(material.is_public_line(
            "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIHN0cmluZ3RoYXRsb29rc2xpa2VhbmVkMjU1MTlrZXk user@host"
        ));
        assert!(!material.is_public_line("ssh deploy@host with the password Tr0ub4dor&3"));
    }

    #[test]
    fn public_pem_blocks_are_found_and_private_ones_are_not() {
        let text = "a\n-----BEGIN PUBLIC KEY-----\nMIIB\nIjAN\n-----END PUBLIC KEY-----\nb\n-----BEGIN PRIVATE KEY-----\nMIIE\n-----END PRIVATE KEY-----\n-----BEGIN CERTIFICATE-----\nMIIC\n-----END CERTIFICATE-----\n";
        let blocks: Vec<&str> = material()
            .public_blocks(text)
            .into_iter()
            .map(|range| &text[range])
            .collect();
        assert_eq!(
            blocks,
            [
                "-----BEGIN PUBLIC KEY-----\nMIIB\nIjAN\n-----END PUBLIC KEY-----",
                "-----BEGIN CERTIFICATE-----\nMIIC\n-----END CERTIFICATE-----"
            ]
        );
    }
}
