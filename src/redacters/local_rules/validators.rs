//! Checksum and range validators applied to regex candidates. Each takes the raw matched
//! text, separators included, and answers whether the value is structurally valid.

use std::net::{Ipv4Addr, Ipv6Addr};

pub type Validator = fn(&str) -> bool;

fn digits_of(value: &str) -> Vec<u32> {
    value.chars().filter_map(|c| c.to_digit(10)).collect()
}

fn alphanumerics_of(value: &str) -> String {
    value
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .map(|c| c.to_ascii_uppercase())
        .collect()
}

fn luhn_any_length(digits: &[u32]) -> bool {
    let sum: u32 = digits
        .iter()
        .rev()
        .enumerate()
        .map(|(i, &d)| {
            if i % 2 == 1 {
                let doubled = d * 2;
                if doubled > 9 {
                    doubled - 9
                } else {
                    doubled
                }
            } else {
                d
            }
        })
        .sum();
    sum.is_multiple_of(10)
}

pub fn luhn(value: &str) -> bool {
    let digits = digits_of(value);
    if !(13..=19).contains(&digits.len()) {
        return false;
    }
    luhn_any_length(&digits)
}

/// ISO 7064 mod 97-10 over the IBAN rearranged with the country code and check digits moved
/// to the end and letters expanded to two digits (A=10 .. Z=35).
fn mod97(value: &str) -> u32 {
    let mut remainder: u32 = 0;
    for c in value.chars() {
        let chunk = if c.is_ascii_digit() {
            c.to_digit(10).unwrap_or(0)
        } else {
            u32::from(c.to_ascii_uppercase()) - u32::from('A') + 10
        };
        let width = if chunk >= 10 { 100 } else { 10 };
        remainder = (remainder * width + chunk) % 97;
    }
    remainder
}

const IBAN_LENGTHS: &[(&str, usize)] = &[
    ("AT", 20),
    ("BE", 16),
    ("BG", 22),
    ("CH", 21),
    ("CY", 28),
    ("CZ", 24),
    ("DE", 22),
    ("DK", 18),
    ("EE", 20),
    ("ES", 24),
    ("FI", 18),
    ("FR", 27),
    ("GB", 22),
    ("GR", 27),
    ("HR", 21),
    ("HU", 28),
    ("IE", 22),
    ("IS", 26),
    ("IT", 27),
    ("LI", 21),
    ("LT", 20),
    ("LU", 20),
    ("LV", 21),
    ("MC", 27),
    ("MT", 31),
    ("NL", 18),
    ("NO", 15),
    ("PL", 28),
    ("PT", 25),
    ("RO", 24),
    ("SE", 24),
    ("SI", 19),
    ("SK", 24),
    ("SM", 27),
    ("UA", 29),
];

pub fn iban(value: &str) -> bool {
    let compact = alphanumerics_of(value);
    if compact.len() < 15 || compact.len() > 34 {
        return false;
    }
    let country = &compact[..2];
    if let Some((_, expected)) = IBAN_LENGTHS.iter().find(|(c, _)| *c == country) {
        if compact.len() != *expected {
            return false;
        }
    }
    let rearranged = format!("{}{}", &compact[4..], &compact[..4]);
    mod97(&rearranged) == 1
}

/// A phone candidate: `min` to 15 digits once separators are stripped, and not a run of one
/// repeated digit (`+1 111 111 1111` is a placeholder, not a number).
fn phone_digit_count(value: &str, min: usize) -> bool {
    let digits = digits_of(value);
    (min..=15).contains(&digits.len()) && digits.iter().any(|&d| d != digits[0])
}

pub fn phone_digits(value: &str) -> bool {
    phone_digit_count(value, 8)
}

/// National forms carry no country code, so the spec's 9-digit minimum applies: it is what
/// separates a phone number from a dotted date or amount of 8 digits.
pub fn phone_national_digits(value: &str) -> bool {
    phone_digit_count(value, 9)
}

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

/// The Dutch eleven test: the first eight digits weighted 9 down to 2, minus the ninth, is a
/// multiple of 11. Shared by the BSN and the Dutch VAT number. `digits` must hold at least 9.
fn dutch_eleven_test(digits: &[u32]) -> bool {
    let weighted: i64 = digits[..8]
        .iter()
        .enumerate()
        .map(|(i, &d)| i64::from(d) * (9 - i as i64))
        .sum();
    (weighted - i64::from(digits[8])).rem_euclid(11) == 0
}

/// ISO 7064 MOD 11,10 over `digits`, returning the check digit that must follow them. Shared
/// by the German tax identification number and the German VAT number.
fn iso7064_mod_11_10_check(digits: &[u32]) -> u32 {
    let mut product = 10;
    for &d in digits {
        let mut sum = (d + product) % 10;
        if sum == 0 {
            sum = 10;
        }
        product = (sum * 2) % 11;
    }
    (11 - product) % 10
}

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

pub fn ipv4(value: &str) -> bool {
    value.parse::<Ipv4Addr>().is_ok()
}

pub fn ipv6(value: &str) -> bool {
    value.parse::<Ipv6Addr>().is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn luhn_accepts_standard_test_cards() {
        for card in [
            "4111 1111 1111 1111",
            "5500-0000-0000-0004",
            "340000000000009",
            "6011000000000004",
        ] {
            assert!(luhn(card), "{card}");
        }
    }

    #[test]
    fn luhn_rejects_a_wrong_check_digit_and_short_numbers() {
        assert!(!luhn("4111 1111 1111 1112"));
        assert!(!luhn("123456789012"));
        assert!(!luhn("12345678901234567890"));
    }

    #[test]
    fn iban_accepts_iso_examples() {
        for value in [
            "GB82 WEST 1234 5698 7654 32",
            "DE89370400440532013000",
            "FR1420041010050500013M02606",
            "NL91ABNA0417164300",
        ] {
            assert!(iban(value), "{value}");
        }
    }

    #[test]
    fn iban_rejects_a_wrong_checksum_and_wrong_length() {
        assert!(!iban("GB82 WEST 1234 5698 7654 33"));
        assert!(!iban("DE8937040044053201300"));
    }

    #[test]
    fn phone_digits_requires_eight_to_fifteen_varied_digits() {
        assert!(phone_digits("+1 (555) 123-4567"));
        assert!(!phone_digits("+1 111 111 1111"));
        assert!(!phone_digits("+12 345"));
        assert!(!phone_digits("+1234567890123456"));
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

    #[test]
    fn ip_validators_delegate_to_std_parsing() {
        assert!(ipv4("192.168.0.1"));
        assert!(!ipv4("192.168.0.300"));
        assert!(ipv6("2001:db8::1"));
        assert!(!ipv6("2001:db8:::1"));
    }
}
