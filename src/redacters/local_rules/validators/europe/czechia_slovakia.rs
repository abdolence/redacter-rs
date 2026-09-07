//! Czech and Slovak identifiers: the shared rodné číslo.

use super::super::checksums::digits_of;
use super::super::dates::{pair, valid_date};

/// Czech and Slovak rodné číslo in the 10-digit form issued since 1954: `YYMMDD/SSSC`, women
/// have 50 added to the month. The whole number is divisible by 11, except that numbers
/// whose first nine digits leave remainder 10 end in 0 instead.
pub fn czech_slovak_rodne_cislo(value: &str) -> bool {
    let digits = digits_of(value);
    if digits.len() != 10 {
        return false;
    }
    let year = pair(&digits, 0);
    let year = if year >= 54 { 1900 + year } else { 2000 + year };
    let mut month = pair(&digits, 2);
    if month > 50 {
        month -= 50;
    }
    if !valid_date(Some(year), month, pair(&digits, 4)) {
        return false;
    }
    let number = digits.iter().fold(0u64, |acc, &d| acc * 10 + u64::from(d));
    number % 11 == 0 || ((number / 10) % 11 == 10 && digits[9] == 0)
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
    fn czech_slovak_rodne_cislo_divisible_by_eleven() {
        check(
            czech_slovak_rodne_cislo,
            &[
                ("780123/3540", true), // commonly cited example
                ("7801233540", true),
                ("900101/1239", true),
                ("905101/1233", true),  // month + 50, female
                ("901301/1238", false), // divisible by 11, month 13
                ("900101/1238", false), // not divisible by 11
                ("530101/123", false),  // pre-1954 nine-digit form is not covered
            ],
        );
    }
}
