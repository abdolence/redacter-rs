//! Checksum and range validators applied to regex candidates. Each takes the raw matched
//! text, separators included, and answers whether the value is structurally valid. The
//! generic validators live here; national identifiers are grouped by region.

mod africa;
mod americas;
mod asia;
mod checksums;
mod dates;
mod europe;
mod oceania;

pub use africa::*;
pub use americas::*;
pub use asia::*;
pub use europe::*;
pub use oceania::*;

pub use dates::birth_date;
#[cfg(test)]
pub use dates::MONTHS;

use checksums::{alphanumerics_of, digits_of, luhn_any_length, mod97};
use std::net::{Ipv4Addr, Ipv6Addr};

pub type Validator = fn(&str) -> bool;

pub fn luhn(value: &str) -> bool {
    let digits = digits_of(value);
    if !(13..=19).contains(&digits.len()) {
        return false;
    }
    luhn_any_length(&digits)
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
    fn ip_validators_delegate_to_std_parsing() {
        assert!(ipv4("192.168.0.1"));
        assert!(!ipv4("192.168.0.300"));
        assert!(ipv6("2001:db8::1"));
        assert!(!ipv6("2001:db8:::1"));
    }
}
