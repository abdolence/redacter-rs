//! Identifiers of Asia and the Pacific.

use super::checksums::{digits_of, iso7064_mod_11_2_check, verhoeff_valid, weighted_sum};
use super::dates::{pair, valid_date};

/// Australian TFN: 9 digits, Σ weights 1 4 3 7 5 8 6 9 10 divisible by 11.
pub fn australian_tfn(value: &str) -> bool {
    let digits = digits_of(value);
    digits.len() == 9 && weighted_sum(&digits, &[1, 4, 3, 7, 5, 8, 6, 9, 10]).is_multiple_of(11)
}

/// Indian Aadhaar: 12 digits not starting with 0 or 1, the last a Verhoeff check digit.
pub fn indian_aadhaar(value: &str) -> bool {
    let digits = digits_of(value);
    digits.len() == 12 && digits[0] >= 2 && verhoeff_valid(&digits)
}

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
    fn australian_tfn_mod_11() {
        check(
            australian_tfn,
            &[
                ("123 456 782", true), // commonly cited example
                ("876543210", true),
                ("876543211", false),
                ("12345678", false), // legacy 8-digit numbers are not covered
            ],
        );
    }

    #[test]
    fn indian_aadhaar_verhoeff() {
        check(
            indian_aadhaar,
            &[
                ("999941057058", true), // commonly cited sample
                ("2345 6789 0124", true),
                ("234567890125", false),
                ("134567890124", false), // first digit 1
            ],
        );
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
