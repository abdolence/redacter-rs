//! Albanian identifiers: the NID.

use super::super::checksums::{alphanumerics_of, digits_of};
use super::super::dates::{pair, valid_date};

/// Albanian NID: a decade letter (`A` for 1900-1909 up to `M` for 2020-2029), the year within
/// the decade, the month (+50 for women), the day, a three-digit serial and a check letter
/// whose algorithm is not published, so only the date is verified.
pub fn albanian_nid(value: &str) -> bool {
    let compact = alphanumerics_of(value);
    let bytes = compact.as_bytes();
    if bytes.len() != 10 || !bytes[0].is_ascii_uppercase() || !bytes[9].is_ascii_alphabetic() {
        return false;
    }
    let digits = digits_of(&compact[1..9]);
    if digits.len() != 8 {
        return false;
    }
    let decade = u32::from(bytes[0] - b'A');
    if decade > 12 {
        return false;
    }
    let mut month = pair(&digits, 1);
    if month > 50 {
        month -= 50;
    }
    valid_date(
        Some(1900 + decade * 10 + digits[0]),
        month,
        pair(&digits, 3),
    )
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
    fn albanian_nid_decade_letter_and_date() {
        check(
            albanian_nid,
            &[
                ("I05101999Q", true),  // 1980, January (month 51 = 1, female), day 1
                ("A00101999Q", true),  // 1900
                ("i05101999q", true),  // lowercase: alphanumerics_of upper-cases first
                ("I05132999Q", false), // day 32
                ("Z05101999Q", false), // decade letter beyond M
                ("I0510199Q", false),
            ],
        );
    }
}
