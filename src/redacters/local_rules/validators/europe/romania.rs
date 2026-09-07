//! Romanian identifiers: the CNP.

use super::super::checksums::{digits_of, weighted_sum};
use super::super::dates::{pair, valid_date};

/// Romanian CNP `SYYMMDDJJNNNC`: S is the sex-and-century digit (1-2 1900s, 3-4 1800s, 5-6
/// 2000s, 7-9 foreign residents with the century unknown), JJ the county (01-46, 51, 52 or
/// 70). C is Σ weights 2 7 9 1 4 6 3 5 8 2 7 9 modulo 11, with 10 read as 1.
pub fn romanian_cnp(value: &str) -> bool {
    let digits = digits_of(value);
    if digits.len() != 13 {
        return false;
    }
    let century = match digits[0] {
        1 | 2 => Some(1900),
        3 | 4 => Some(1800),
        5 | 6 => Some(2000),
        7..=9 => None,
        _ => return false,
    };
    let year = century.map(|century| century + pair(&digits, 1));
    if !valid_date(year, pair(&digits, 3), pair(&digits, 5)) {
        return false;
    }
    if !matches!(pair(&digits, 7), 1..=46 | 51 | 52 | 70) {
        return false;
    }
    let control = weighted_sum(&digits[..12], &[2, 7, 9, 1, 4, 6, 3, 5, 8, 2, 7, 9]) % 11;
    (if control == 10 { 1 } else { control }) == digits[12]
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
    fn romanian_cnp_check_century_and_county() {
        check(
            romanian_cnp,
            &[
                ("1900101123457", true),
                ("5000115401231", true),  // born 2000, county 40
                ("1900101991231", false), // check valid, county 99
                ("1900101123458", false),
                ("0900101123457", false), // first digit 0
            ],
        );
    }
}
