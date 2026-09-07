//! Identifiers of the former Yugoslavia: the JMBG shared by Slovenia, Serbia, Bosnia and
//! Herzegovina, Montenegro and North Macedonia.

use super::super::checksums::{digits_of, mod11_complement};
use super::super::dates::{pair, valid_date};

/// JMBG (Serbia, Bosnia and Herzegovina, Montenegro and North Macedonia) and the Slovenian
/// EMŠO `DDMMYYYRRSSSK`: YYY is the last three digits of the year (900-999 for the 1900s,
/// else the 2000s). K is 11 minus Σ weights 7 6 5 4 3 2 7 6 5 4 3 2 modulo 11, with 11 read
/// as 0 and 10 not issued.
pub fn jmbg(value: &str) -> bool {
    let digits = digits_of(value);
    if digits.len() != 13 {
        return false;
    }
    let yyy = digits[4] * 100 + pair(&digits, 5);
    let year = if yyy >= 900 { 1000 + yyy } else { 2000 + yyy };
    valid_date(Some(year), pair(&digits, 2), pair(&digits, 0))
        && mod11_complement(&digits[..12], &[7, 6, 5, 4, 3, 2, 7, 6, 5, 4, 3, 2])
            == Some(digits[12])
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
    fn jmbg_control_digit_and_three_digit_year() {
        check(
            jmbg,
            &[
                ("0101006500006", true), // published (Wikipedia), 1 January 2006
                ("0101985501239", true),
                ("2902004501234", true),  // 29 February 2004
                ("3201985501234", false), // check valid, day 32
                ("0101985501230", false),
            ],
        );
    }
}
