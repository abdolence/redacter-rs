//! United Kingdom identifiers: the National Insurance number, driving licence number and
//! postcode.

use super::super::checksums::{alphanumerics_of, digits_of};
use super::super::dates::{pair, valid_date};

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

/// Storage-capacity abbreviations that share the UK postcode's `<digit><letter><letter>`
/// inward-code shape (`4GB`, `2TB`): a postcode's own inward code never spells one of these
/// in practice, so a match ending in one is a hardware spec, not an address.
const UK_POSTCODE_STORAGE_UNITS: &[&str] = &["GB", "TB", "MB", "KB", "PB", "EB"];

pub fn uk_postcode(value: &str) -> bool {
    let compact = alphanumerics_of(value);
    if compact.len() < 2 {
        return false;
    }
    let inward_letters = &compact[compact.len() - 2..];
    !UK_POSTCODE_STORAGE_UNITS.contains(&inward_letters)
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
    fn uk_postcode_rejects_storage_capacity_inward_codes() {
        check(
            uk_postcode,
            &[
                ("SW1A 1AA", true),
                ("M1 1AE", true),
                ("B33 8TH", true),
                ("CR2 6XH", true),
                ("DN55 1PT", true),
                ("GIR 0AA", true),
                ("S9 4GB", false),
                ("PC3 2GB", false),
                ("X5 2TB", false),
            ],
        );
    }
}
