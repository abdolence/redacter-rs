//! Finnish identifiers: the henkilötunnus.

use super::super::checksums::digits_of;
use super::super::dates::{pair, valid_date};

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

#[cfg(test)]
mod tests {
    use super::*;

    fn check(validator: fn(&str) -> bool, cases: &[(&str, bool)]) {
        for (value, expected) in cases {
            assert_eq!(validator(value), *expected, "{value}");
        }
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
}
