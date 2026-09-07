//! Norwegian identifiers: the fødselsnummer.

use super::super::checksums::{digits_of, mod11_complement};
use super::super::dates::{pair, valid_date};

/// Norwegian fødselsnummer: `DDMMYYIIIK1K2`. K1 uses weights 3 7 6 1 8 9 4 5 2 over the
/// nine digits before it and K2 uses 5 4 3 2 7 6 5 4 3 2 over the ten before it; each is
/// 11 minus the weighted sum modulo 11 (11 read as 0, 10 not issued). A D-number for
/// people without residence adds 40 to the day.
pub fn norwegian_fodselsnummer(value: &str) -> bool {
    let digits = digits_of(value);
    if digits.len() != 11 {
        return false;
    }
    let mut day = pair(&digits, 0);
    if day > 40 {
        day -= 40;
    }
    valid_date(None, pair(&digits, 2), day)
        && mod11_complement(&digits[..9], &[3, 7, 6, 1, 8, 9, 4, 5, 2]) == Some(digits[9])
        && mod11_complement(&digits[..10], &[5, 4, 3, 2, 7, 6, 5, 4, 3, 2]) == Some(digits[10])
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
    fn norwegian_fodselsnummer_two_control_digits() {
        check(
            norwegian_fodselsnummer,
            &[
                ("01019012480", true),
                ("010190 12480", true),
                ("15068512333", true),
                ("41019012393", true),  // D-number, day + 40
                ("01019012481", false), // second control digit wrong
                ("32019012351", false), // controls valid, day 32
                ("0101901248", false),
            ],
        );
    }
}
