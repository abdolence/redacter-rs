//! Portuguese identifiers: the NIF.

use super::super::checksums::{digits_of, weighted_sum};

/// Portuguese NIF: 9 digits, the first 1-3 or 5-9 (the categories for persons and
/// organisations). The check digit is 11 minus Σ weights 9..2 modulo 11, or 0 when that
/// remainder is below 2.
pub fn portuguese_nif(value: &str) -> bool {
    let digits = digits_of(value);
    if digits.len() != 9 || !matches!(digits[0], 1..=3 | 5..=9) {
        return false;
    }
    let remainder = weighted_sum(&digits[..8], &[9, 8, 7, 6, 5, 4, 3, 2]) % 11;
    (if remainder < 2 { 0 } else { 11 - remainder }) == digits[8]
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
    fn portuguese_nif_mod_11() {
        check(
            portuguese_nif,
            &[
                ("123456789", true), // commonly cited example
                ("212345672", true),
                ("212 345 672", true),
                ("412345676", false), // check valid, first digit 4
                ("212345673", false),
                ("21234567", false),
            ],
        );
    }
}
