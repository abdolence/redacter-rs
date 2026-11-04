//! The value checks of the `generic-api-key` rule, which ports the rule of the same name from
//! [gitleaks](https://github.com/gitleaks/gitleaks) (MIT licence, `gitleaks.LICENSE`). The stopword
//! list is the one of that rule in `config/gitleaks.toml` at commit
//! `5f6ec56cfa6f8c67648d863582c7ae2f206d5c1c`.

use super::quoted_credential_value;
use std::collections::BTreeMap;

/// A value is kept only above this many bits of Shannon entropy per character; equal to it is
/// dropped, as gitleaks does.
const MIN_ENTROPY: f64 = 3.5;

/// Lowercase words, one per line. A value that contains one of them, in any case, is a name or
/// a placeholder rather than a secret.
const STOPWORDS: &str = include_str!("generic_api_key_stopwords.txt");

/// A candidate of the `generic-api-key` rule is a secret when it is random enough, is not made
/// of letters only and contains no stopword. The value is ASCII, which is what the pattern
/// captures.
pub fn generic_api_key_value(value: &str) -> bool {
    shannon_entropy(value) > MIN_ENTROPY
        && quoted_credential_value(value)
        && !has_stopword(value)
        && !is_name(value)
}

/// The longest segment mixing letters and digits that still reads as a name, by the case of its
/// letters. A lower case segment has no word boundaries to check, so a random lower case key
/// (`key-3ax6xnjp29jd6fds4gc373sgvjxteol0`) passes for words and numbers unless its length
/// rules it out; names such as `pkcs8` or `sha256` are short.
const MAX_LOWER_CASE_NAME_SEGMENT: usize = 8;
const MAX_MIXED_CASE_NAME_SEGMENT: usize = 13;

/// Whether `value` is a cipher, identifier or file name: every segment between separators is
/// made of words and numbers, and there is at least one letter. The key name of the pattern
/// also matches in prose (`the Cloud KMS key, ChaCha20-Poly1305.`), where the next token is a
/// name. A value of hex digits only is a digest or a hex-encoded key, whose runs of `a` to `f`
/// would pass for words.
fn is_name(value: &str) -> bool {
    let is_hex = value
        .bytes()
        .all(|byte| byte.is_ascii_hexdigit() || !byte.is_ascii_alphanumeric());
    if is_hex || !value.bytes().any(|byte| byte.is_ascii_alphabetic()) {
        return false;
    }
    value
        .split(|c: char| !c.is_ascii_alphanumeric())
        .all(is_name_segment)
}

/// A segment of letters and digits is a name when it is short enough for its case and each run
/// of letters in it is a word.
fn is_name_segment(segment: &str) -> bool {
    let has_letters = segment.bytes().any(|byte| byte.is_ascii_alphabetic());
    let has_digits = segment.bytes().any(|byte| byte.is_ascii_digit());
    if has_letters && has_digits {
        let longest = if segment.bytes().any(|byte| byte.is_ascii_uppercase()) {
            MAX_MIXED_CASE_NAME_SEGMENT
        } else {
            MAX_LOWER_CASE_NAME_SEGMENT
        };
        if segment.len() > longest {
            return false;
        }
    }
    segment
        .split(|c: char| !c.is_ascii_alphabetic())
        .filter(|run| !run.is_empty())
        .all(is_word_run)
}

/// A run of letters that is upper case (`KEY`), or a sequence of words: lower case ones of two
/// letters or more, or a capital and two lower case letters or more (`pkcs`, `ChaCha`).
fn is_word_run(run: &str) -> bool {
    if run.len() >= 2 && run.bytes().all(|byte| byte.is_ascii_uppercase()) {
        return true;
    }
    let mut rest = run;
    while !rest.is_empty() {
        let word = rest
            .strip_prefix(|c: char| c.is_ascii_uppercase())
            .unwrap_or(rest);
        let lowercase = word.bytes().take_while(u8::is_ascii_lowercase).count();
        if lowercase < 2 {
            return false;
        }
        rest = &word[lowercase..];
    }
    true
}

fn has_stopword(value: &str) -> bool {
    let lowered = value.to_ascii_lowercase();
    STOPWORDS.lines().any(|word| lowered.contains(word))
}

/// Entropy of the bytes of an ASCII `text`, in bits per character.
fn shannon_entropy(text: &str) -> f64 {
    let mut counts: BTreeMap<u8, u32> = BTreeMap::new();
    for byte in text.bytes() {
        *counts.entry(byte).or_default() += 1;
    }
    let total = f64::from(u32::try_from(text.len()).unwrap_or(u32::MAX));
    counts
        .values()
        .map(|&count| {
            let probability = f64::from(count) / total;
            -probability * probability.log2()
        })
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entropy_equal_to_the_floor_is_dropped() {
        // 8 characters once and 4 twice in 16 give exactly 3.5 bits per character.
        assert!(!generic_api_key_value("q7m3v9t2hkpshkps"));
        assert!(generic_api_key_value("q7m3v9t2hkpshkpz"));
    }

    #[test]
    fn a_stopword_inside_the_value_drops_it() {
        assert!(generic_api_key_value("x9Kq2pro7Zw4mT"));
        assert!(!generic_api_key_value("x9Kq2proc7Zw4mT"));
        assert!(!generic_api_key_value("x9Kq2PROC7Zw4mT"));
    }

    #[test]
    fn letters_only_values_are_dropped() {
        assert!(!generic_api_key_value("kJhGfDsAqWeRtYuIoP"));
    }

    #[test]
    fn a_value_made_of_words_and_numbers_is_a_name_and_not_a_key() {
        for name in [
            "ChaCha20-Poly1305.",
            "from_pkcs8_der",
            "Aes256KeyWrap",
            "KEY_ALG_RSA_1024",
            "kms_aead_operation=",
            "123456789/env",
        ] {
            assert!(!generic_api_key_value(name), "{name}");
        }
    }

    #[test]
    fn a_random_value_stays_a_key() {
        let dropped: Vec<&str> = [
            "k8Jd2Lw9QzX7vNb3RtY5",
            "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",
            "tkn_9fK2mXv7LqPz4RtWb8Yc",
            "123e4567-e89b-12d3-a456-426614174000",
            "0123456789abcdef0123456789abcdef",
            "key-3ax6xnjp29jd6fds4gc373sgvjxteol0",
            "rff5uqlp3ony7qac",
            "7pkoj58azs50hgvnvckt",
            "gfstsyj2dcsew5hemdj64lnkhato8jcb",
            "0boyvjpt1uqz7nhh0jtuqqbkwxmj8ujystaenqka",
        ]
        .into_iter()
        .filter(|key| !generic_api_key_value(key))
        .collect();
        assert_eq!(dropped, Vec::<&str>::new());
    }
}
