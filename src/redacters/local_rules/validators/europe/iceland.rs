//! Icelandic identifiers: the kennitala.

use super::super::checksums::{digits_of, mod11_complement};
use super::super::dates::{pair, valid_date};

/// Icelandic kennitala: `DDMMYY-NNCM`, C the control digit (11 minus Σ weights 3 2 7 6 5 4
/// 3 2 modulo 11, 11 read as 0, 10 not issued), M the century (8 for the 1800s, 9 for the
/// 1900s, 0 for the 2000s). Company numbers add 40 to the day and are not personal data,
/// so they fail the date check on purpose.
pub fn icelandic_kennitala(value: &str) -> bool {
    let digits = digits_of(value);
    if digits.len() != 10 {
        return false;
    }
    let century = match digits[9] {
        8 => 1800,
        9 => 1900,
        0 => 2000,
        _ => return false,
    };
    valid_date(
        Some(century + pair(&digits, 4)),
        pair(&digits, 2),
        pair(&digits, 0),
    ) && mod11_complement(&digits[..8], &[3, 2, 7, 6, 5, 4, 3, 2]) == Some(digits[8])
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
    fn icelandic_kennitala_control_digit_and_century() {
        check(
            icelandic_kennitala,
            &[
                ("120174-3399", true), // published (Wikipedia)
                ("010190-1269", true),
                ("290200-2450", true),  // 29 February 2000
                ("320174-3339", false), // control valid, day 32
                ("120174-3389", false), // control wrong
                ("120174-3391", false), // century digit 1
            ],
        );
    }
}
