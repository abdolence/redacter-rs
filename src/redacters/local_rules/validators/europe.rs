//! Identifiers of Western and Southern Europe. Each function documents the format and the
//! published check it applies.

use super::checksums::{
    alphanumerics_of, digits_of, dutch_eleven_test, ean13_check_digit, iso7064_mod_11_10_check,
    luhn_any_length, verhoeff_check_digit, weighted_sum,
};
use super::dates::{pair, valid_date};

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

/// Belgian national number `YY.MM.DD-SSS.CC`: CC is 97 minus the first nine digits modulo
/// 97; for people born from 2000 a 2 is prefixed before the division. The check that passes
/// tells the century, so the date is verified with it. Month 00 (unknown) and the +20/+40
/// "bis" offsets for foreign residents are not accepted.
pub fn belgian_national_number(value: &str) -> bool {
    let digits = digits_of(value);
    if digits.len() != 11 {
        return false;
    }
    let base = digits[..9]
        .iter()
        .fold(0u64, |acc, &d| acc * 10 + u64::from(d));
    let check = u64::from(pair(&digits, 9));
    let (month, day) = (pair(&digits, 2), pair(&digits, 4));
    if 97 - base % 97 == check {
        return valid_date(Some(1900 + pair(&digits, 0)), month, day);
    }
    if 97 - (2_000_000_000 + base) % 97 == check {
        return valid_date(Some(2000 + pair(&digits, 0)), month, day);
    }
    false
}

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

/// Portuguese NIF: 9 digits, the first 1-3 or 5-9 (the categories for persons and
/// organisations). The check digit is 11 minus Σ weights 9..2 modulo 11, or 0 when that
/// remainder is below 2.
pub fn portuguese_nif(value: &str) -> bool {
    let digits = digits_of(value);
    if digits.len() != 9 || !matches!(digits[0], 1..=3 | 5..=9) {
        return false;
    }
    let remainder = weighted_sum(&digits[..8], &[9, 8, 7, 6, 5, 4, 3, 2]) % 11;
    (if remainder < 2 { 0 } else { 11 - remainder }) == digits[8]
}

const PPS_CONTROL: &[u8; 23] = b"WABCDEFGHIJKLMNOPQRSTUV";

/// Irish PPS number: 7 digits, a check letter and (since 2013) a second letter. The check
/// letter is at index (Σ weights 8..2 + 9 × the second letter's value, A = 1) modulo 23 of
/// `WABCDEFGHIJKLMNOPQRSTUV`; a legacy `W` second letter counts 0.
pub fn irish_pps(value: &str) -> bool {
    let compact = alphanumerics_of(value);
    let bytes = compact.as_bytes();
    if !(8..=9).contains(&bytes.len()) {
        return false;
    }
    let digits = digits_of(&compact[..7]);
    if digits.len() != 7 {
        return false;
    }
    let mut sum = weighted_sum(&digits, &[8, 7, 6, 5, 4, 3, 2]);
    if let Some(&second) = bytes.get(8) {
        if !second.is_ascii_uppercase() {
            return false;
        }
        if second != b'W' {
            sum += 9 * u32::from(second - b'A' + 1);
        }
    }
    PPS_CONTROL[(sum % 23) as usize] == bytes[7]
}

/// Swiss AHV number `756.NNNN.NNNN.NC`: an EAN-13 with the 756 country prefix.
pub fn swiss_ahv(value: &str) -> bool {
    let digits = digits_of(value);
    digits.len() == 13 && digits[..3] == [7, 5, 6] && ean13_check_digit(&digits[..12]) == digits[12]
}

/// Austrian Sozialversicherungsnummer `LLLP DDMMYY`: P is Σ of the other nine digits with
/// weights 3 7 9 5 8 4 2 1 6 modulo 11 (10 is not issued); the serial starts at 100.
pub fn austrian_svnr(value: &str) -> bool {
    let digits = digits_of(value);
    if digits.len() != 10 || digits[0] == 0 {
        return false;
    }
    if !valid_date(None, pair(&digits, 6), pair(&digits, 4)) {
        return false;
    }
    let sum =
        weighted_sum(&digits[..3], &[3, 7, 9]) + weighted_sum(&digits[4..], &[5, 8, 4, 2, 1, 6]);
    sum % 11 == digits[3]
}

/// Greek AMKA: `DDMMYY` and five digits, the last a Luhn check over all eleven.
pub fn greek_amka(value: &str) -> bool {
    let digits = digits_of(value);
    digits.len() == 11
        && valid_date(None, pair(&digits, 2), pair(&digits, 0))
        && luhn_any_length(&digits)
}

/// Greek AFM (tax number): 9 digits, the last is Σ of the first eight with weights 2^8 down
/// to 2^1, modulo 11, modulo 10.
pub fn greek_afm(value: &str) -> bool {
    let digits = digits_of(value);
    digits.len() == 9
        && weighted_sum(&digits[..8], &[256, 128, 64, 32, 16, 8, 4, 2]) % 11 % 10 == digits[8]
}

/// UK driving licence number: five surname letters (padded with 9), then the decade digit,
/// the month (+50 for women), the day, the year digit, two initials, a check digit and two
/// check characters. Only the date fields can be verified.
pub fn uk_driving_licence(value: &str) -> bool {
    let compact = alphanumerics_of(value);
    if compact.len() != 16 {
        return false;
    }
    let digits = digits_of(&compact[5..11]);
    if digits.len() != 6 {
        return false;
    }
    let mut month = pair(&digits, 1);
    if month > 50 {
        month -= 50;
    }
    valid_date(None, month, pair(&digits, 3))
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
    fn belgian_national_number_mod_97_with_century() {
        check(
            belgian_national_number,
            &[
                ("85.07.30-033.28", true), // published (Wikipedia)
                ("85073003328", true),
                ("90.01.01-123.95", true),
                ("05.01.01-123.85", true), // born 2005: 2 prefixed before the division
                ("05.13.01-123.72", false), // check valid, month 13
                ("85.07.30-033.29", false), // check wrong
            ],
        );
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

    #[test]
    fn portuguese_nif_mod_11() {
        check(
            portuguese_nif,
            &[
                ("123456789", true), // commonly cited example
                ("212345672", true),
                ("212 345 672", true),
                ("412345676", false), // check valid, first digit 4
                ("212345673", false),
                ("21234567", false),
            ],
        );
    }

    #[test]
    fn irish_pps_mod_23_with_second_letter() {
        check(
            irish_pps,
            &[
                ("1234567FA", true), // published (Wikipedia)
                ("7654321G", true),
                ("7654321PA", true),
                ("1234567TW", true), // legacy W second letter counts 0
                ("7654322G", false),
                ("1234567FB", false), // second letter changes the sum
                ("1234567F1", false),
            ],
        );
    }

    #[test]
    fn swiss_ahv_ean13() {
        check(
            swiss_ahv,
            &[
                ("756.9217.0769.85", true), // published (ZAS)
                ("7561234123413", true),
                ("756.1234.1234.14", false),
                ("757.9217.0769.85", false),
            ],
        );
    }

    #[test]
    fn austrian_svnr_check_digit_and_date() {
        check(
            austrian_svnr,
            &[
                ("1237 010180", true), // commonly cited example
                ("1237010180", true),
                ("9991 311299", true),
                ("5553 150685", true),
                ("1238 320180", false), // check valid, day 32
                ("1237 010181", false),
                ("0237 010180", false), // serial starts with 0
            ],
        );
    }

    #[test]
    fn greek_amka_luhn_and_date() {
        check(
            greek_amka,
            &[
                ("01019012341", true),
                ("32139012341", false), // Luhn valid, day 32
                ("01019012342", false),
            ],
        );
    }

    #[test]
    fn uk_driving_licence_date_fields() {
        check(
            uk_driving_licence,
            &[
                ("MORGA657054SM9IJ", true), // DVLA specimen
                ("SMITH710101AB9CD", true),
                ("morga657054sm9ij", true), // lowercase: alphanumerics_of upper-cases first
                ("MORGA657354SM9IJ", false), // day 35
                ("MORGA663054SM9IJ", false), // month 63 (13 after the +50)
                ("MORGA657054SM9I", false), // 15 characters
            ],
        );
    }

    #[test]
    fn greek_afm_powers_of_two() {
        check(
            greek_afm,
            &[
                ("090000045", true), // commonly cited example
                ("123456783", true),
                ("987654324", true),
                ("123456784", false),
            ],
        );
    }
}
