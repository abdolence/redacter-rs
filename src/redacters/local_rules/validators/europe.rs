//! Identifiers of Western and Southern Europe. Each function documents the format and the
//! published check it applies.

use super::checksums::{
    alphanumerics_of, digits_of, dutch_eleven_test, iso7064_mod_11_10_check, luhn_any_length,
};

pub fn dutch_bsn(value: &str) -> bool {
    let digits = digits_of(value);
    digits.len() == 9 && dutch_eleven_test(&digits)
}

const DNI_LETTERS: &[u8] = b"TRWAGMYFPDXBNJZSQVHLCKE";

pub fn spanish_dni_nie(value: &str) -> bool {
    let compact = alphanumerics_of(value);
    let bytes = compact.as_bytes();
    if bytes.len() != 9 {
        return false;
    }
    let mut digits = String::with_capacity(8);
    match bytes[0] {
        b'X' => digits.push('0'),
        b'Y' => digits.push('1'),
        b'Z' => digits.push('2'),
        c if c.is_ascii_digit() => digits.push(char::from(c)),
        _ => return false,
    }
    digits.push_str(&compact[1..8]);
    let Ok(number) = digits.parse::<usize>() else {
        return false;
    };
    DNI_LETTERS[number % 23] == bytes[8]
}

const CF_ODD: [u32; 36] = [
    1, 0, 5, 7, 9, 13, 15, 17, 19, 21, // 0-9
    1, 0, 5, 7, 9, 13, 15, 17, 19, 21, 2, 4, 18, 20, 11, 3, 6, 8, 12, 14, 16, 10, 22, 25, 24,
    23, // A-Z
];

fn cf_index(c: u8) -> Option<usize> {
    match c {
        b'0'..=b'9' => Some(usize::from(c - b'0')),
        b'A'..=b'Z' => Some(usize::from(c - b'A') + 10),
        _ => None,
    }
}

pub fn italian_codice_fiscale(value: &str) -> bool {
    let compact = alphanumerics_of(value);
    let bytes = compact.as_bytes();
    if bytes.len() != 16 {
        return false;
    }
    let mut sum = 0;
    for (i, &c) in bytes[..15].iter().enumerate() {
        let Some(index) = cf_index(c) else {
            return false;
        };
        // Positions are 1-based in the specification: odd positions use the odd table,
        // even positions use the plain value (digits 0-9, letters 0-25).
        sum += if i % 2 == 0 {
            CF_ODD[index]
        } else if index >= 10 {
            index as u32 - 10
        } else {
            index as u32
        };
    }
    bytes[15] == b'A' + (sum % 26) as u8
}

pub fn french_nir(value: &str) -> bool {
    let compact = alphanumerics_of(value);
    if compact.len() != 15 {
        return false;
    }
    let (number, key) = compact.split_at(13);
    let number = number.replace("2A", "19").replace("2B", "18");
    let (Ok(number), Ok(key)) = (number.parse::<u64>(), key.parse::<u64>()) else {
        return false;
    };
    97 - (number % 97) == key
}

/// ISO 7064 MOD 11,10 as used by the German tax identification number.
pub fn german_steuer_id(value: &str) -> bool {
    let digits = digits_of(value);
    if digits.len() != 11 || digits[0] == 0 {
        return false;
    }
    iso7064_mod_11_10_check(&digits[..10]) == digits[10]
}

pub fn uk_nino(value: &str) -> bool {
    let compact = alphanumerics_of(value);
    let bytes = compact.as_bytes();
    if bytes.len() != 9 {
        return false;
    }
    let (first, second) = (bytes[0], bytes[1]);
    if b"DFIQUV".contains(&first) || b"DFIQUVO".contains(&second) {
        return false;
    }
    if !matches!(bytes[8], b'A'..=b'D') || !bytes[2..8].iter().all(u8::is_ascii_digit) {
        return false;
    }
    let prefix = &compact[..2];
    !["BG", "GB", "NK", "KN", "TN", "NT", "ZZ"].contains(&prefix)
}

pub fn eu_vat(value: &str) -> bool {
    let compact = alphanumerics_of(value);
    if compact.len() < 4 {
        return false;
    }
    let (country, body) = compact.split_at(2);
    let digits = digits_of(body);
    match country {
        // ISO 7064 MOD 11,10 over 9 digits.
        "DE" => digits.len() == 9 && iso7064_mod_11_10_check(&digits[..8]) == digits[8],
        "FR" => {
            if body.len() != 11 || !body[..2].bytes().all(|b| b.is_ascii_digit()) {
                return body.len() == 11;
            }
            let (Ok(key), Ok(siren)) = (body[..2].parse::<u64>(), body[2..].parse::<u64>()) else {
                return false;
            };
            (12 + 3 * (siren % 97)) % 97 == key
        }
        "IT" => digits.len() == 11 && luhn_any_length(&digits),
        "NL" => digits.len() == 11 && &body[9..10] == "B" && dutch_eleven_test(&digits),
        "BE" => {
            if digits.len() != 10 {
                return false;
            }
            let base: u64 = digits[..8]
                .iter()
                .fold(0, |acc, &d| acc * 10 + u64::from(d));
            let check: u64 = u64::from(digits[8]) * 10 + u64::from(digits[9]);
            97 - (base % 97) == check
        }
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dutch_bsn_eleven_test() {
        assert!(dutch_bsn("111222333"));
        assert!(!dutch_bsn("111222334"));
    }

    #[test]
    fn spanish_dni_and_nie_control_letter() {
        assert!(spanish_dni_nie("12345678Z"));
        assert!(!spanish_dni_nie("12345678A"));
        assert!(spanish_dni_nie("X1234567L"));
        assert!(!spanish_dni_nie("X1234567Z"));
    }

    #[test]
    fn italian_codice_fiscale_control_character() {
        assert!(italian_codice_fiscale("RSSMRA85M01H501Q"));
        assert!(!italian_codice_fiscale("RSSMRA85M01H501A"));
    }

    #[test]
    fn french_nir_key() {
        assert!(french_nir("2 69 05 49 588 157 80"));
        assert!(!french_nir("2 69 05 49 588 157 81"));
        assert!(french_nir("1 55 05 2A 123 456 48"));
    }

    #[test]
    fn german_steuer_id_mod_11_10() {
        assert!(german_steuer_id("86095742719"));
        assert!(!german_steuer_id("86095742718"));
        assert!(!german_steuer_id("06095742719"));
    }

    #[test]
    fn uk_nino_prefix_rules() {
        assert!(uk_nino("AB 12 34 56 C"));
        assert!(!uk_nino("QQ 12 34 56 C"));
        assert!(!uk_nino("BG 12 34 56 C"));
        assert!(!uk_nino("DA 12 34 56 C"));
        assert!(!uk_nino("AO 12 34 56 C"));
    }

    #[test]
    fn uk_nino_rejects_a_bad_suffix_or_non_digit_body() {
        assert!(!uk_nino("AB 12 34 56 E"));
        assert!(!uk_nino("ABCDEFGHI"));
        assert!(!uk_nino("AB123456E"));
    }

    #[test]
    fn eu_vat_checks_the_countries_with_a_known_algorithm() {
        assert!(eu_vat("DE136695976"));
        assert!(!eu_vat("DE136695977"));
        assert!(eu_vat("FR40303265045"));
        assert!(!eu_vat("FR41303265045"));
        assert!(eu_vat("IT00743110157"));
        assert!(!eu_vat("IT00743110158"));
        assert!(eu_vat("NL123456782B01"));
        assert!(!eu_vat("NL123456783B01"));
        assert!(eu_vat("BE0403019261"));
        assert!(!eu_vat("BE0403019262"));
        assert!(eu_vat("ESA12345674"));
    }
}
