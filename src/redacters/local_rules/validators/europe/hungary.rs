//! Hungarian identifiers: the személyi szám and the TAJ social insurance number.

use super::super::checksums::{digits_of, weighted_sum};
use super::super::dates::{pair, valid_date};

/// Hungarian személyi szám `MYYMMDDSSSC`: M is the sex-and-century digit (1-4 for the 1900s,
/// 5-8 for the 2000s). C is the weighted sum modulo 11 with weights 1..10 for numbers issued
/// since 1997 or 10..1 for older ones; either is accepted and a remainder of 10 is never
/// issued.
pub fn hungarian_personal_number(value: &str) -> bool {
    let digits = digits_of(value);
    if digits.len() != 11 {
        return false;
    }
    let century = match digits[0] {
        1..=4 => 1900,
        5..=8 => 2000,
        _ => return false,
    };
    if !valid_date(
        Some(century + pair(&digits, 1)),
        pair(&digits, 3),
        pair(&digits, 5),
    ) {
        return false;
    }
    let ascending = weighted_sum(&digits[..10], &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]) % 11;
    let descending = weighted_sum(&digits[..10], &[10, 9, 8, 7, 6, 5, 4, 3, 2, 1]) % 11;
    [ascending, descending].contains(&digits[10])
}

/// Hungarian TAJ (social insurance number): 9 digits, the last is Σ of the first eight with
/// weights 3 and 7 alternating, modulo 10.
pub fn hungarian_taj(value: &str) -> bool {
    let digits = digits_of(value);
    digits.len() == 9 && weighted_sum(&digits[..8], &[3, 7, 3, 7, 3, 7, 3, 7]) % 10 == digits[8]
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
    fn hungarian_personal_number_both_weightings() {
        check(
            hungarian_personal_number,
            &[
                ("19001011249", true), // ascending weights (since 1997)
                ("28503152344", true), // descending weights (before 1997)
                ("1 900101 1249", true),
                ("50502290122", false), // check valid, 29 February 2005
                ("19001011248", false),
                ("90010112349", false), // sex-and-century digit 9
            ],
        );
    }

    #[test]
    fn hungarian_taj_alternating_weights() {
        check(
            hungarian_taj,
            &[
                ("123456788", true),
                ("123 456 788", true),
                ("876543212", true),
                ("123456789", false),
            ],
        );
    }
}
