//! Turkish identifiers: the TCKN.

use super::super::checksums::digits_of;

/// Turkish TCKN: 11 digits, the first non-zero. Digit 10 is `(7 × (d1+d3+d5+d7+d9) -
/// (d2+d4+d6+d8)) mod 10` and digit 11 is the sum of the first ten modulo 10.
pub fn turkish_tckn(value: &str) -> bool {
    let digits = digits_of(value);
    if digits.len() != 11 || digits[0] == 0 {
        return false;
    }
    let odd: i64 = (0..9).step_by(2).map(|i| i64::from(digits[i])).sum();
    let even: i64 = (1..8).step_by(2).map(|i| i64::from(digits[i])).sum();
    (7 * odd - even).rem_euclid(10) == i64::from(digits[9])
        && digits[..10].iter().sum::<u32>() % 10 == digits[10]
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
    fn turkish_tckn_two_check_digits() {
        check(
            turkish_tckn,
            &[
                ("10000000146", true), // commonly cited example
                ("12345678950", true),
                ("98765432150", true),
                ("12345678951", false),
                ("02345678950", false), // first digit 0
            ],
        );
    }
}
