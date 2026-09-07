//! Indian identifiers: the Aadhaar number.

use super::super::checksums::{digits_of, verhoeff_valid};

/// Indian Aadhaar: 12 digits not starting with 0 or 1, the last a Verhoeff check digit.
pub fn indian_aadhaar(value: &str) -> bool {
    let digits = digits_of(value);
    digits.len() == 12 && digits[0] >= 2 && verhoeff_valid(&digits)
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
    fn indian_aadhaar_verhoeff() {
        check(
            indian_aadhaar,
            &[
                ("999941057058", true), // commonly cited sample
                ("2345 6789 0124", true),
                ("234567890125", false),
                ("134567890124", false), // first digit 1
            ],
        );
    }
}
