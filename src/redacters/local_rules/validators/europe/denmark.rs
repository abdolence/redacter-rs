//! Danish identifiers: the CPR number.

use super::super::checksums::digits_of;
use super::super::dates::{pair, valid_date};

/// Danish CPR: `DDMMYY-SSSS`. The modulus-11 check was dropped in 2007, so only the date is
/// verified and the rule that uses this validator is keyword-anchored.
pub fn danish_cpr(value: &str) -> bool {
    let digits = digits_of(value);
    digits.len() == 10 && valid_date(None, pair(&digits, 2), pair(&digits, 0))
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
    fn danish_cpr_date_only() {
        check(
            danish_cpr,
            &[
                ("010203-1234", true),
                ("0102031234", true),
                ("290204-1234", true),
                ("300203-1234", false), // 30 February
                ("311303-1234", false), // month 13
                ("010203-123", false),
            ],
        );
    }
}
