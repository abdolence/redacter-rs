//! Identifiers outside Europe.

use super::checksums::digits_of;

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

#[cfg(test)]
mod tests {
    use super::*;

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
