//! Luxembourg identifiers: the matricule.

use super::super::checksums::{digits_of, luhn_any_length, verhoeff_check_digit};
use super::super::dates::{pair, valid_date};

/// Luxembourg matricule `YYYYMMDDSSSLV`: L is the Luhn check digit and V the Verhoeff check
/// digit, both over the first eleven digits.
pub fn luxembourg_matricule(value: &str) -> bool {
    let digits = digits_of(value);
    if digits.len() != 13 {
        return false;
    }
    let year = pair(&digits, 0) * 100 + pair(&digits, 2);
    valid_date(Some(year), pair(&digits, 4), pair(&digits, 6))
        && luhn_any_length(&digits[..12])
        && verhoeff_check_digit(&digits[..11]) == digits[12]
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
    fn luxembourg_matricule_luhn_and_verhoeff() {
        check(
            luxembourg_matricule,
            &[
                ("1893120105732", true), // published (Wikipedia)
                ("1990010112385", true),
                ("2004022900721", true),  // 29 February 2004
                ("2005022900718", false), // checks valid, 29 February 2005
                ("1990010112386", false), // Verhoeff digit wrong
                ("1990010112395", false), // Luhn digit wrong
            ],
        );
    }
}
