//! Croatian identifiers: the OIB.

use super::super::checksums::{digits_of, iso7064_mod_11_10_check};

/// Croatian OIB: 11 digits, ISO 7064 MOD 11,10 over the first ten.
pub fn croatian_oib(value: &str) -> bool {
    let digits = digits_of(value);
    digits.len() == 11 && iso7064_mod_11_10_check(&digits[..10]) == digits[10]
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
    fn croatian_oib_iso7064() {
        check(
            croatian_oib,
            &[
                ("69435151530", true), // commonly cited example
                ("12345678903", true),
                ("98765432106", true),
                ("12345678904", false),
            ],
        );
    }
}
