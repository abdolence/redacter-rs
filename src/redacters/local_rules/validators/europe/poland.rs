//! Polish identifiers: the PESEL.

use super::super::checksums::{digits_of, weighted_sum};
use super::super::dates::{pair, valid_date};

/// Polish PESEL: `YYMMDDSSSSC`; the month carries the century (1-12 for the 1900s, 21-32 for
/// the 2000s; the 1800s and 2100s offsets are not accepted). C is `(10 - Σ weights 1 3 7 9
/// 1 3 7 9 1 3 mod 10) mod 10`.
pub fn polish_pesel(value: &str) -> bool {
    let digits = digits_of(value);
    if digits.len() != 11 {
        return false;
    }
    let (century, month) = match pair(&digits, 2) {
        month @ 1..=12 => (1900, month),
        month @ 21..=32 => (2000, month - 20),
        _ => return false,
    };
    valid_date(Some(century + pair(&digits, 0)), month, pair(&digits, 4))
        && (10 - weighted_sum(&digits[..10], &[1, 3, 7, 9, 1, 3, 7, 9, 1, 3]) % 10) % 10
            == digits[10]
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
    fn polish_pesel_control_digit_and_century_month() {
        check(
            polish_pesel,
            &[
                ("44051401359", true), // published (Wikipedia)
                ("90010112349", true),
                ("05210112349", true),  // January 2005
                ("44135401356", false), // control valid, day 54
                ("90810112343", false), // control valid, 1800s month offset
                ("44051401358", false), // control wrong
            ],
        );
    }
}
