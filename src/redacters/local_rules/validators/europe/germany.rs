//! German identifiers: the tax identification number.

use super::super::checksums::{digits_of, iso7064_mod_11_10_check};

/// ISO 7064 MOD 11,10 as used by the German tax identification number.
pub fn german_steuer_id(value: &str) -> bool {
    let digits = digits_of(value);
    if digits.len() != 11 || digits[0] == 0 {
        return false;
    }
    iso7064_mod_11_10_check(&digits[..10]) == digits[10]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn german_steuer_id_mod_11_10() {
        assert!(german_steuer_id("86095742719"));
        assert!(!german_steuer_id("86095742718"));
        assert!(!german_steuer_id("06095742719"));
    }
}
