//! Swiss identifiers: the AHV number.

use super::super::checksums::{digits_of, ean13_check_digit};

/// Swiss AHV number `756.NNNN.NNNN.NC`: an EAN-13 with the 756 country prefix.
pub fn swiss_ahv(value: &str) -> bool {
    let digits = digits_of(value);
    digits.len() == 13 && digits[..3] == [7, 5, 6] && ean13_check_digit(&digits[..12]) == digits[12]
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
}
