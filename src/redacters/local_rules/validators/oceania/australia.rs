//! Australian identifiers: the Tax File Number.

use super::super::checksums::{digits_of, weighted_sum};

/// Australian TFN: 9 digits, Σ weights 1 4 3 7 5 8 6 9 10 divisible by 11.
pub fn australian_tfn(value: &str) -> bool {
    let digits = digits_of(value);
    digits.len() == 9 && weighted_sum(&digits, &[1, 4, 3, 7, 5, 8, 6, 9, 10]).is_multiple_of(11)
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
    fn australian_tfn_mod_11() {
        check(
            australian_tfn,
            &[
                ("123 456 782", true), // commonly cited example
                ("876543210", true),
                ("876543211", false),
                ("12345678", false), // legacy 8-digit numbers are not covered
            ],
        );
    }
}
