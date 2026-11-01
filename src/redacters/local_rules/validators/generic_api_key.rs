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
    shannon_entropy(value) > MIN_ENTROPY && quoted_credential_value(value) && !has_stopword(value)
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
        assert!(generic_api_key_value("abc123pro456xyz"));
        assert!(!generic_api_key_value("x9Kq2proc7Zw4mT"));
        assert!(!generic_api_key_value("x9Kq2PROC7Zw4mT"));
    }

    #[test]
    fn letters_only_values_are_dropped() {
        assert!(!generic_api_key_value("kJhGfDsAqWeRtYuIoP"));
    }
}
