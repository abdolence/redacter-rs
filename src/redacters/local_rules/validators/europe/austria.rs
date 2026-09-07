//! Austrian identifiers: the Sozialversicherungsnummer.

use super::super::checksums::{digits_of, weighted_sum};
use super::super::dates::{pair, valid_date};

/// Austrian Sozialversicherungsnummer `LLLP DDMMYY`: P is Σ of the other nine digits with
/// weights 3 7 9 5 8 4 2 1 6 modulo 11 (10 is not issued); the serial starts at 100.
pub fn austrian_svnr(value: &str) -> bool {
    let digits = digits_of(value);
    if digits.len() != 10 || digits[0] == 0 {
        return false;
    }
    if !valid_date(None, pair(&digits, 6), pair(&digits, 4)) {
        return false;
    }
    let sum =
        weighted_sum(&digits[..3], &[3, 7, 9]) + weighted_sum(&digits[4..], &[5, 8, 4, 2, 1, 6]);
    sum % 11 == digits[3]
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
    fn austrian_svnr_check_digit_and_date() {
        check(
            austrian_svnr,
            &[
                ("1237 010180", true), // commonly cited example
                ("1237010180", true),
                ("9991 311299", true),
                ("5553 150685", true),
                ("1238 320180", false), // check valid, day 32
                ("1237 010181", false),
                ("0237 010180", false), // serial starts with 0
            ],
        );
    }
}
