//! Estonian and Lithuanian identifiers: the shared personal code format.

use super::super::checksums::{digits_of, weighted_sum};
use super::super::dates::{pair, valid_date};

/// Estonian isikukood and Lithuanian asmens kodas share one format: `GYYMMDDSSSC`, G the
/// sex-and-century digit (1-2 for the 1800s, 3-4 for the 1900s, 5-6 for the 2000s). C is
/// the weighted sum modulo 11 with weights 1 2 3 4 5 6 7 8 9 1; when that is 10 the weights
/// 3 4 5 6 7 8 9 1 2 3 are used instead, and a second 10 becomes 0.
pub fn baltic_personal_code(value: &str) -> bool {
    let digits = digits_of(value);
    if digits.len() != 11 {
        return false;
    }
    let century = match digits[0] {
        1 | 2 => 1800,
        3 | 4 => 1900,
        5 | 6 => 2000,
        _ => return false,
    };
    if !valid_date(
        Some(century + pair(&digits, 1)),
        pair(&digits, 3),
        pair(&digits, 5),
    ) {
        return false;
    }
    let mut control = weighted_sum(&digits[..10], &[1, 2, 3, 4, 5, 6, 7, 8, 9, 1]) % 11;
    if control == 10 {
        control = weighted_sum(&digits[..10], &[3, 4, 5, 6, 7, 8, 9, 1, 2, 3]) % 11;
        if control == 10 {
            control = 0;
        }
    }
    control == digits[10]
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
    fn baltic_personal_code_two_weightings_and_century() {
        check(
            baltic_personal_code,
            &[
                ("37605030299", true), // published (Estonia)
                ("33309240064", true), // published (Lithuania)
                ("39001011237", true),
                ("60402290124", true), // 29 February 2004
                ("48503152346", true),
                ("50502290126", false), // control valid, 29 February 2005
                ("70001011234", false), // control valid, century digit 7
                ("39001011238", false), // control wrong
            ],
        );
    }
}
