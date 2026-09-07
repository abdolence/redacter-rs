//! Nordic and Baltic personal identity numbers. All embed a birth date; the check digits
//! follow the published algorithm named on each function.

use super::super::checksums::{digits_of, luhn_any_length, mod11_complement, weighted_sum};
use super::super::dates::{pair, valid_date};

/// Swedish personnummer and samordningsnummer: `YYMMDD-NNNC` (10 digits; `+` instead of
/// `-` for people over 100) or `YYYYMMDD-NNNC`. Luhn over the 10-digit form; a
/// coordination number has 60 added to the day.
pub fn swedish_personnummer(value: &str) -> bool {
    let digits = digits_of(value);
    let (year, body) = match digits.len() {
        10 => (None, &digits[..]),
        12 => (
            Some(pair(&digits, 0) * 100 + pair(&digits, 2)),
            &digits[2..],
        ),
        _ => return false,
    };
    let mut day = pair(body, 4);
    if day > 60 {
        day -= 60;
    }
    valid_date(year, pair(body, 2), day) && luhn_any_length(body)
}

/// Norwegian fødselsnummer: `DDMMYYIIIK1K2`. K1 uses weights 3 7 6 1 8 9 4 5 2 over the
/// nine digits before it and K2 uses 5 4 3 2 7 6 5 4 3 2 over the ten before it; each is
/// 11 minus the weighted sum modulo 11 (11 read as 0, 10 not issued). A D-number for
/// people without residence adds 40 to the day.
pub fn norwegian_fodselsnummer(value: &str) -> bool {
    let digits = digits_of(value);
    if digits.len() != 11 {
        return false;
    }
    let mut day = pair(&digits, 0);
    if day > 40 {
        day -= 40;
    }
    valid_date(None, pair(&digits, 2), day)
        && mod11_complement(&digits[..9], &[3, 7, 6, 1, 8, 9, 4, 5, 2]) == Some(digits[9])
        && mod11_complement(&digits[..10], &[5, 4, 3, 2, 7, 6, 5, 4, 3, 2]) == Some(digits[10])
}

/// Danish CPR: `DDMMYY-SSSS`. The modulus-11 check was dropped in 2007, so only the date is
/// verified and the rule that uses this validator is keyword-anchored.
pub fn danish_cpr(value: &str) -> bool {
    let digits = digits_of(value);
    digits.len() == 10 && valid_date(None, pair(&digits, 2), pair(&digits, 0))
}

const HETU_CONTROL: &[u8; 31] = b"0123456789ABCDEFHJKLMNPRSTUVWXY";

/// Finnish henkilötunnus: `DDMMYYCZZZQ`, C the century marker (`+` 1800s; `-`, `Y`, `X`,
/// `W`, `V`, `U` 1900s; `A` to `F` 2000s), ZZZ the individual number from 002, Q the
/// control character at index (DDMMYYZZZ mod 31) of `0123456789ABCDEFHJKLMNPRSTUVWXY`.
pub fn finnish_hetu(value: &str) -> bool {
    if !value.is_ascii() || value.len() != 11 {
        return false;
    }
    let bytes = value.as_bytes();
    let century = match bytes[6] {
        b'+' => 1800,
        b'-' | b'Y' | b'X' | b'W' | b'V' | b'U' => 1900,
        b'A'..=b'F' => 2000,
        _ => return false,
    };
    let date = digits_of(&value[..6]);
    let individual = digits_of(&value[7..10]);
    if date.len() != 6 || individual.len() != 3 {
        return false;
    }
    if individual[0] * 100 + pair(&individual, 1) < 2 {
        return false;
    }
    if !valid_date(
        Some(century + pair(&date, 4)),
        pair(&date, 2),
        pair(&date, 0),
    ) {
        return false;
    }
    let number = date
        .iter()
        .chain(&individual)
        .fold(0u64, |acc, &d| acc * 10 + u64::from(d));
    HETU_CONTROL[(number % 31) as usize] == bytes[10]
}

/// Icelandic kennitala: `DDMMYY-NNCM`, C the control digit (11 minus Σ weights 3 2 7 6 5 4
/// 3 2 modulo 11, 11 read as 0, 10 not issued), M the century (8 for the 1800s, 9 for the
/// 1900s, 0 for the 2000s). Company numbers add 40 to the day and are not personal data,
/// so they fail the date check on purpose.
pub fn icelandic_kennitala(value: &str) -> bool {
    let digits = digits_of(value);
    if digits.len() != 10 {
        return false;
    }
    let century = match digits[9] {
        8 => 1800,
        9 => 1900,
        0 => 2000,
        _ => return false,
    };
    valid_date(
        Some(century + pair(&digits, 4)),
        pair(&digits, 2),
        pair(&digits, 0),
    ) && mod11_complement(&digits[..8], &[3, 2, 7, 6, 5, 4, 3, 2]) == Some(digits[8])
}

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
    fn swedish_personnummer_luhn_and_date() {
        check(
            swedish_personnummer,
            &[
                ("811218-9876", true), // published (Skatteverket)
                ("8112189876", true),
                ("811218+9876", true), // over 100 years old
                ("811278-9873", true), // coordination number, day + 60
                ("19811218-9876", true),
                ("198112189876", true),
                ("200002291235", true),  // 29 February 2000
                ("190002291235", false), // 1900 was not a leap year
                ("8113189875", false),   // Luhn passes, month 13
                ("811218-9877", false),  // Luhn fails
                ("811218987", false),    // 9 digits
            ],
        );
    }

    #[test]
    fn norwegian_fodselsnummer_two_control_digits() {
        check(
            norwegian_fodselsnummer,
            &[
                ("01019012480", true),
                ("010190 12480", true),
                ("15068512333", true),
                ("41019012393", true),  // D-number, day + 40
                ("01019012481", false), // second control digit wrong
                ("32019012351", false), // controls valid, day 32
                ("0101901248", false),
            ],
        );
    }

    #[test]
    fn danish_cpr_date_only() {
        check(
            danish_cpr,
            &[
                ("010203-1234", true),
                ("0102031234", true),
                ("290204-1234", true),
                ("300203-1234", false), // 30 February
                ("311303-1234", false), // month 13
                ("010203-123", false),
            ],
        );
    }

    #[test]
    fn finnish_hetu_control_character_and_century_marker() {
        check(
            finnish_hetu,
            &[
                ("131052-308T", true), // published (DVV)
                ("010594Y9021", true), // published (DVV), 2023 marker
                ("010190-123M", true),
                ("010105A123P", true),
                ("310252-308Y", false), // control valid, 31 February
                ("131052-308U", false), // control wrong
                ("131052x308T", false), // unknown century marker
                ("131052-308", false),
            ],
        );
    }

    #[test]
    fn icelandic_kennitala_control_digit_and_century() {
        check(
            icelandic_kennitala,
            &[
                ("120174-3399", true), // published (Wikipedia)
                ("010190-1269", true),
                ("290200-2450", true),  // 29 February 2000
                ("320174-3339", false), // control valid, day 32
                ("120174-3389", false), // control wrong
                ("120174-3391", false), // century digit 1
            ],
        );
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
