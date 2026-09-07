//! Swedish identifiers: the personnummer and samordningsnummer.

use super::super::checksums::{digits_of, luhn_any_length};
use super::super::dates::{pair, valid_date};

/// Swedish personnummer and samordningsnummer: `YYMMDD-NNNC` (10 digits; `+` instead of
/// `-` for people over 100) or `YYYYMMDD-NNNC`. Luhn over the 10-digit form; a
/// coordination number has 60 added to the day.
pub fn swedish_personnummer(value: &str) -> bool {
    let digits = digits_of(value);
    let (year, body) = match digits.len() {
        10 => (None, &digits[..]),
        12 => (
            Some(pair(&digits, 0) * 100 + pair(&digits, 2)),
            &digits[2..],
        ),
        _ => return false,
    };
    let mut day = pair(body, 4);
    if day > 60 {
        day -= 60;
    }
    valid_date(year, pair(body, 2), day) && luhn_any_length(body)
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
    fn swedish_personnummer_luhn_and_date() {
        check(
            swedish_personnummer,
            &[
                ("811218-9876", true), // published (Skatteverket)
                ("8112189876", true),
                ("811218+9876", true), // over 100 years old
                ("811278-9873", true), // coordination number, day + 60
                ("19811218-9876", true),
                ("198112189876", true),
                ("200002291235", true),  // 29 February 2000
                ("190002291235", false), // 1900 was not a leap year
                ("8113189875", false),   // Luhn passes, month 13
                ("811218-9877", false),  // Luhn fails
                ("811218987", false),    // 9 digits
            ],
        );
    }
}
