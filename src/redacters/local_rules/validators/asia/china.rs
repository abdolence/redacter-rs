//! Chinese identifiers: the resident identity number.

use super::super::checksums::{digits_of, iso7064_mod_11_2_check};
use super::super::dates::{pair, valid_date};

/// Chinese resident identity number (GB 11643): a 6-digit region, `YYYYMMDD`, a 3-digit
/// sequence and an ISO 7064 MOD 11-2 check character.
pub fn chinese_resident_id(value: &str) -> bool {
    if !value.is_ascii() || value.len() != 18 {
        return false;
    }
    let digits = digits_of(&value[..17]);
    if digits.len() != 17 {
        return false;
    }
    let year = pair(&digits, 6) * 100 + pair(&digits, 8);
    valid_date(Some(year), pair(&digits, 10), pair(&digits, 12))
        && iso7064_mod_11_2_check(&digits) == char::from(value.as_bytes()[17])
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
    fn chinese_resident_id_iso7064_and_date() {
        check(
            chinese_resident_id,
            &[
                ("11010519491231002X", true), // commonly cited example
                ("110105199001011232", true),
                ("110105199002301231", false), // check valid, 30 February
                ("110105199001011233", false),
                ("11010519491231002x", false), // lowercase check character
            ],
        );
    }
}
