//! Bulgarian identifiers: the EGN.

use super::super::checksums::{digits_of, weighted_sum};
use super::super::dates::{pair, valid_date};

/// Bulgarian EGN `YYMMDDRRRC`: the month has 40 added for the 2000s (20 for the 1800s, not
/// accepted). C is Σ weights 2 4 8 5 10 9 7 3 6 modulo 11, with 10 read as 0.
pub fn bulgarian_egn(value: &str) -> bool {
    let digits = digits_of(value);
    if digits.len() != 10 {
        return false;
    }
    let (century, month) = match pair(&digits, 2) {
        month @ 1..=12 => (1900, month),
        month @ 41..=52 => (2000, month - 40),
        _ => return false,
    };
    valid_date(Some(century + pair(&digits, 0)), month, pair(&digits, 4))
        && weighted_sum(&digits[..9], &[2, 4, 8, 5, 10, 9, 7, 3, 6]) % 11 % 10 == digits[9]
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
    fn bulgarian_egn_check_and_century_month() {
        check(
            bulgarian_egn,
            &[
                ("6101057509", true), // commonly cited example
                ("9001011238", true),
                ("0541011239", true),  // January 2005
                ("9013011234", false), // check valid, month 13
                ("9001011237", false),
            ],
        );
    }
}
