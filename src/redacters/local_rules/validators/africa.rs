//! Identifiers of Africa.

use super::checksums::{digits_of, luhn_any_length};
use super::dates::{pair, valid_date};

/// South African ID `YYMMDDSSSSCAZ`: C is the citizenship (0 or 1), Z a Luhn check digit.
pub fn south_african_id(value: &str) -> bool {
    let digits = digits_of(value);
    digits.len() == 13
        && digits[10] <= 1
        && valid_date(None, pair(&digits, 2), pair(&digits, 4))
        && luhn_any_length(&digits)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn south_african_id_luhn_date_and_citizenship() {
        for (value, expected) in [
            ("8001015009087", true), // commonly cited example
            ("9001015009086", true),
            ("9001015009185", true),  // citizenship digit 1
            ("9013015009081", false), // Luhn valid, month 13
            ("9001015009087", false),
        ] {
            assert_eq!(south_african_id(value), expected, "{value}");
        }
    }
}
