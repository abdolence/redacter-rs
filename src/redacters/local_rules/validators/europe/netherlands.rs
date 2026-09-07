//! Dutch identifiers: the BSN.

use super::super::checksums::{digits_of, dutch_eleven_test};

pub fn dutch_bsn(value: &str) -> bool {
    let digits = digits_of(value);
    digits.len() == 9 && dutch_eleven_test(&digits)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dutch_bsn_eleven_test() {
        assert!(dutch_bsn("111222333"));
        assert!(!dutch_bsn("111222334"));
    }
}
