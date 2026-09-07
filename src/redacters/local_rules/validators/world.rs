//! Identifiers outside Europe.

use super::checksums::{
    digits_of, iso7064_mod_11_2_check, luhn_any_length, verhoeff_valid, weighted_sum,
};
use super::dates::{pair, valid_date};

/// SSN `AAA-GG-SSSS` with the SSA exclusions, or an ITIN (area 900-999, group in the
/// issued ranges 70-88, 90-92, 94-99).
pub fn us_ssn_or_itin(value: &str) -> bool {
    let digits = digits_of(value);
    if digits.len() != 9 {
        return false;
    }
    let area = digits[0] * 100 + digits[1] * 10 + digits[2];
    let group = digits[3] * 10 + digits[4];
    let serial = digits[5] * 1000 + digits[6] * 100 + digits[7] * 10 + digits[8];
    if group == 0 || serial == 0 || area == 0 || area == 666 {
        return false;
    }
    if area >= 900 {
        return matches!(group, 70..=88 | 90..=92 | 94..=99);
    }
    true
}

/// Canadian SIN: 9 digits, Luhn.
pub fn canadian_sin(value: &str) -> bool {
    let digits = digits_of(value);
    digits.len() == 9 && luhn_any_length(&digits)
}

/// Australian TFN: 9 digits, Σ weights 1 4 3 7 5 8 6 9 10 divisible by 11.
pub fn australian_tfn(value: &str) -> bool {
    let digits = digits_of(value);
    digits.len() == 9 && weighted_sum(&digits, &[1, 4, 3, 7, 5, 8, 6, 9, 10]).is_multiple_of(11)
}

/// Brazilian CPF `NNN.NNN.NNN-CC`: each check digit is 11 minus the weighted sum of the
/// digits before it (weights counting down from 10, then from 11) modulo 11, or 0 when
/// that is below 2. Numbers made of one repeated digit pass the arithmetic and are
/// rejected.
pub fn brazilian_cpf(value: &str) -> bool {
    let digits = digits_of(value);
    if digits.len() != 11 || digits.iter().all(|&d| d == digits[0]) {
        return false;
    }
    cpf_check_digit(&digits[..9]) == digits[9] && cpf_check_digit(&digits[..10]) == digits[10]
}

fn cpf_check_digit(digits: &[u32]) -> u32 {
    let top = u32::try_from(digits.len()).unwrap_or(0) + 1;
    let sum: u32 = digits
        .iter()
        .enumerate()
        .map(|(i, &d)| d * (top - u32::try_from(i).unwrap_or(0)))
        .sum();
    match sum % 11 {
        0 | 1 => 0,
        remainder => 11 - remainder,
    }
}

/// Indian Aadhaar: 12 digits not starting with 0 or 1, the last a Verhoeff check digit.
pub fn indian_aadhaar(value: &str) -> bool {
    let digits = digits_of(value);
    digits.len() == 12 && digits[0] >= 2 && verhoeff_valid(&digits)
}

/// South African ID `YYMMDDSSSSCAZ`: C is the citizenship (0 or 1), Z a Luhn check digit.
pub fn south_african_id(value: &str) -> bool {
    let digits = digits_of(value);
    digits.len() == 13
        && digits[10] <= 1
        && valid_date(None, pair(&digits, 2), pair(&digits, 4))
        && luhn_any_length(&digits)
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
    fn canadian_sin_luhn() {
        check(
            canadian_sin,
            &[
                ("046 454 286", true), // published (Wikipedia)
                ("123456782", true),
                ("123-456-782", true),
                ("123456783", false),
            ],
        );
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
    fn brazilian_cpf_two_check_digits() {
        check(
            brazilian_cpf,
            &[
                ("111.444.777-35", true), // commonly cited example
                ("12345678909", true),
                ("98765432100", true),
                ("11111111111", false), // repeated digit, arithmetically valid
                ("12345678900", false),
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
    fn south_african_id_luhn_date_and_citizenship() {
        check(
            south_african_id,
            &[
                ("8001015009087", true), // commonly cited example
                ("9001015009086", true),
                ("9001015009185", true),  // citizenship digit 1
                ("9013015009081", false), // Luhn valid, month 13
                ("9001015009087", false),
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

    #[test]
    fn ssn_rejects_reserved_areas_and_zero_groups() {
        assert!(us_ssn_or_itin("123-45-6789"));
        assert!(!us_ssn_or_itin("000-45-6789"));
        assert!(!us_ssn_or_itin("666-45-6789"));
        assert!(!us_ssn_or_itin("123-00-6789"));
        assert!(!us_ssn_or_itin("123-45-0000"));
        assert!(!us_ssn_or_itin("901-45-6789"));
    }

    #[test]
    fn itin_accepts_only_its_group_ranges() {
        assert!(us_ssn_or_itin("912-70-1234"));
        assert!(us_ssn_or_itin("912-99-1234"));
        assert!(!us_ssn_or_itin("912-89-1234"));
    }
}
