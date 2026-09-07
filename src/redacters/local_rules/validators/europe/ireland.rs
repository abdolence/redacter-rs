//! Irish identifiers: the PPS number and the Eircode.

use super::super::checksums::{alphanumerics_of, digits_of, weighted_sum};

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

/// Irish Eircode unique identifier: the published character set mixes 15 letters and the 10
/// digits with no rule against an all-digit draw, but no real Eircode has ever been observed
/// with one, and a purely numeric 4-character tail is what lets a routing-key-shaped prefix
/// (`H12`, `F12`) plus an ordinary 4-digit number (a year, a short code) pass as an Eircode.
/// Requiring at least one letter trades that theoretical, unobserved case for rejecting the
/// common false positive.
pub fn irish_eircode(value: &str) -> bool {
    let compact = alphanumerics_of(value);
    compact.len() >= 4
        && compact[compact.len() - 4..]
            .chars()
            .any(|c| c.is_ascii_alphabetic())
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
    fn irish_eircode_requires_a_letter_in_the_identifier() {
        check(
            irish_eircode,
            &[
                ("D02 X285", true),
                ("A65 F4E2", true),
                ("T12 YT20", true),
                ("H12 2024", false),
                ("F12 2024", false),
                ("D6W 1234", false),
            ],
        );
    }
}
