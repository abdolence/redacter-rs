//! Identifiers of Central, Eastern and South-Eastern Europe.

use super::checksums::{digits_of, weighted_sum};
use super::dates::{pair, valid_date};

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
