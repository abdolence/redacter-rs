//! Canadian identifiers: the Social Insurance Number.

use super::super::checksums::{digits_of, luhn_any_length};

/// Canadian SIN: 9 digits, Luhn.
pub fn canadian_sin(value: &str) -> bool {
    let digits = digits_of(value);
    digits.len() == 9 && luhn_any_length(&digits)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canadian_sin_luhn() {
        for (value, expected) in [
            ("046 454 286", true), // published (Wikipedia)
            ("123456782", true),
            ("123-456-782", true),
            ("123456783", false),
        ] {
            assert_eq!(canadian_sin(value), expected, "{value}");
        }
    }
}
