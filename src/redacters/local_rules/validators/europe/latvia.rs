//! Latvian identifiers: the personas kods.

use super::super::checksums::{digits_of, weighted_sum};
use super::super::dates::{pair, valid_date};

/// Latvian personas kods: the pre-2017 form `DDMMYY-CSSSK` (C the century: 0 for the 1800s,
/// 1 for the 1900s, 2 for the 2000s) or the current `32SSSS-SSSSK` without a date. K is
/// `(1101 - Σ weights 1 6 3 7 9 10 5 8 4 2) mod 11 mod 10`.
pub fn latvian_personas_kods(value: &str) -> bool {
    let digits = digits_of(value);
    if digits.len() != 11 {
        return false;
    }
    let control = (1101 - weighted_sum(&digits[..10], &[1, 6, 3, 7, 9, 10, 5, 8, 4, 2])) % 11 % 10;
    if control != digits[10] {
        return false;
    }
    if digits[0] == 3 && digits[1] == 2 {
        return true;
    }
    let century = match digits[6] {
        0 => 1800,
        1 => 1900,
        2 => 2000,
        _ => return false,
    };
    valid_date(
        Some(century + pair(&digits, 4)),
        pair(&digits, 2),
        pair(&digits, 0),
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
    fn latvian_personas_kods_old_and_new_forms() {
        check(
            latvian_personas_kods,
            &[
                ("161175-19997", true), // commonly cited example
                ("010190-11231", true),
                ("320000-12340", true), // post-2017 form, no date
                ("32000012340", true),
                ("010190-31232", false), // control valid, century digit 3
                ("010190-11232", false), // control wrong
            ],
        );
    }
}
