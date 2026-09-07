//! Belgian identifiers: the national number.

use super::super::checksums::digits_of;
use super::super::dates::{pair, valid_date};

/// Belgian national number `YY.MM.DD-SSS.CC`: CC is 97 minus the first nine digits modulo
/// 97; for people born from 2000 a 2 is prefixed before the division. The check that passes
/// tells the century, so the date is verified with it. Month 00 (unknown) and the +20/+40
/// "bis" offsets for foreign residents are not accepted.
pub fn belgian_national_number(value: &str) -> bool {
    let digits = digits_of(value);
    if digits.len() != 11 {
        return false;
    }
    let base = digits[..9]
        .iter()
        .fold(0u64, |acc, &d| acc * 10 + u64::from(d));
    let check = u64::from(pair(&digits, 9));
    let (month, day) = (pair(&digits, 2), pair(&digits, 4));
    if 97 - base % 97 == check {
        return valid_date(Some(1900 + pair(&digits, 0)), month, day);
    }
    if 97 - (2_000_000_000 + base) % 97 == check {
        return valid_date(Some(2000 + pair(&digits, 0)), month, day);
    }
    false
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
    fn belgian_national_number_mod_97_with_century() {
        check(
            belgian_national_number,
            &[
                ("85.07.30-033.28", true), // published (Wikipedia)
                ("85073003328", true),
                ("90.01.01-123.95", true),
                ("05.01.01-123.85", true), // born 2005: 2 prefixed before the division
                ("05.13.01-123.72", false), // check valid, month 13
                ("85.07.30-033.29", false), // check wrong
            ],
        );
    }
}
