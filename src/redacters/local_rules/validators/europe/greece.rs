//! Greek identifiers: the AMKA and the AFM.

use super::super::checksums::{digits_of, luhn_any_length, weighted_sum};
use super::super::dates::{pair, valid_date};

/// Greek AMKA: `DDMMYY` and five digits, the last a Luhn check over all eleven.
pub fn greek_amka(value: &str) -> bool {
    let digits = digits_of(value);
    digits.len() == 11
        && valid_date(None, pair(&digits, 2), pair(&digits, 0))
        && luhn_any_length(&digits)
}

/// Greek AFM (tax number): 9 digits, the last is Σ of the first eight with weights 2^8 down
/// to 2^1, modulo 11, modulo 10.
pub fn greek_afm(value: &str) -> bool {
    let digits = digits_of(value);
    digits.len() == 9
        && weighted_sum(&digits[..8], &[256, 128, 64, 32, 16, 8, 4, 2]) % 11 % 10 == digits[8]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(validator: fn(&str) -> bool, cases: &[(&str, bool)]) {
        for (value, expected) in cases {
            assert_eq!(validator(value), *expected, "{value}");
        }
    }

    #[test]
    fn greek_amka_luhn_and_date() {
        check(
            greek_amka,
            &[
                ("01019012341", true),
                ("32139012341", false), // Luhn valid, day 32
                ("01019012342", false),
            ],
        );
    }

    #[test]
    fn greek_afm_powers_of_two() {
        check(
            greek_afm,
            &[
                ("090000045", true), // commonly cited example
                ("123456783", true),
                ("987654324", true),
                ("123456784", false),
            ],
        );
    }
}
