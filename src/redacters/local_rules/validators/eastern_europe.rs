//! Identifiers of Central, Eastern and South-Eastern Europe.

use super::checksums::{
    alphanumerics_of, digits_of, iso7064_mod_11_10_check, mod11_complement, weighted_sum,
};
use super::dates::{pair, valid_date};

/// Polish PESEL: `YYMMDDSSSSC`; the month carries the century (1-12 for the 1900s, 21-32 for
/// the 2000s; the 1800s and 2100s offsets are not accepted). C is `(10 - Σ weights 1 3 7 9
/// 1 3 7 9 1 3 mod 10) mod 10`.
pub fn polish_pesel(value: &str) -> bool {
    let digits = digits_of(value);
    if digits.len() != 11 {
        return false;
    }
    let (century, month) = match pair(&digits, 2) {
        month @ 1..=12 => (1900, month),
        month @ 21..=32 => (2000, month - 20),
        _ => return false,
    };
    valid_date(Some(century + pair(&digits, 0)), month, pair(&digits, 4))
        && (10 - weighted_sum(&digits[..10], &[1, 3, 7, 9, 1, 3, 7, 9, 1, 3]) % 10) % 10
            == digits[10]
}

/// Czech and Slovak rodné číslo in the 10-digit form issued since 1954: `YYMMDD/SSSC`, women
/// have 50 added to the month. The whole number is divisible by 11, except that numbers
/// whose first nine digits leave remainder 10 end in 0 instead.
pub fn czech_slovak_rodne_cislo(value: &str) -> bool {
    let digits = digits_of(value);
    if digits.len() != 10 {
        return false;
    }
    let year = pair(&digits, 0);
    let year = if year >= 54 { 1900 + year } else { 2000 + year };
    let mut month = pair(&digits, 2);
    if month > 50 {
        month -= 50;
    }
    if !valid_date(Some(year), month, pair(&digits, 4)) {
        return false;
    }
    let number = digits.iter().fold(0u64, |acc, &d| acc * 10 + u64::from(d));
    number % 11 == 0 || ((number / 10) % 11 == 10 && digits[9] == 0)
}

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

/// Bulgarian EGN `YYMMDDRRRC`: the month has 40 added for the 2000s (20 for the 1800s, not
/// accepted). C is Σ weights 2 4 8 5 10 9 7 3 6 modulo 11, with 10 read as 0.
pub fn bulgarian_egn(value: &str) -> bool {
    let digits = digits_of(value);
    if digits.len() != 10 {
        return false;
    }
    let (century, month) = match pair(&digits, 2) {
        month @ 1..=12 => (1900, month),
        month @ 41..=52 => (2000, month - 40),
        _ => return false,
    };
    valid_date(Some(century + pair(&digits, 0)), month, pair(&digits, 4))
        && weighted_sum(&digits[..9], &[2, 4, 8, 5, 10, 9, 7, 3, 6]) % 11 % 10 == digits[9]
}

/// Croatian OIB: 11 digits, ISO 7064 MOD 11,10 over the first ten.
pub fn croatian_oib(value: &str) -> bool {
    let digits = digits_of(value);
    digits.len() == 11 && iso7064_mod_11_10_check(&digits[..10]) == digits[10]
}

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

/// Romanian CNP `SYYMMDDJJNNNC`: S is the sex-and-century digit (1-2 1900s, 3-4 1800s, 5-6
/// 2000s, 7-9 foreign residents with the century unknown), JJ the county (01-46, 51, 52 or
/// 70). C is Σ weights 2 7 9 1 4 6 3 5 8 2 7 9 modulo 11, with 10 read as 1.
pub fn romanian_cnp(value: &str) -> bool {
    let digits = digits_of(value);
    if digits.len() != 13 {
        return false;
    }
    let century = match digits[0] {
        1 | 2 => Some(1900),
        3 | 4 => Some(1800),
        5 | 6 => Some(2000),
        7..=9 => None,
        _ => return false,
    };
    let year = century.map(|century| century + pair(&digits, 1));
    if !valid_date(year, pair(&digits, 3), pair(&digits, 5)) {
        return false;
    }
    if !matches!(pair(&digits, 7), 1..=46 | 51 | 52 | 70) {
        return false;
    }
    let control = weighted_sum(&digits[..12], &[2, 7, 9, 1, 4, 6, 3, 5, 8, 2, 7, 9]) % 11;
    (if control == 10 { 1 } else { control }) == digits[12]
}

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

/// Albanian NID: a decade letter (`A` for 1900-1909 up to `M` for 2020-2029), the year within
/// the decade, the month (+50 for women), the day, a three-digit serial and a check letter
/// whose algorithm is not published, so only the date is verified.
pub fn albanian_nid(value: &str) -> bool {
    let compact = alphanumerics_of(value);
    let bytes = compact.as_bytes();
    if bytes.len() != 10 || !bytes[0].is_ascii_uppercase() || !bytes[9].is_ascii_alphabetic() {
        return false;
    }
    let digits = digits_of(&compact[1..9]);
    if digits.len() != 8 {
        return false;
    }
    let decade = u32::from(bytes[0] - b'A');
    if decade > 12 {
        return false;
    }
    let mut month = pair(&digits, 1);
    if month > 50 {
        month -= 50;
    }
    valid_date(
        Some(1900 + decade * 10 + digits[0]),
        month,
        pair(&digits, 3),
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
    fn polish_pesel_control_digit_and_century_month() {
        check(
            polish_pesel,
            &[
                ("44051401359", true), // published (Wikipedia)
                ("90010112349", true),
                ("05210112349", true),  // January 2005
                ("44135401356", false), // control valid, day 54
                ("90810112343", false), // control valid, 1800s month offset
                ("44051401358", false), // control wrong
            ],
        );
    }

    #[test]
    fn czech_slovak_rodne_cislo_divisible_by_eleven() {
        check(
            czech_slovak_rodne_cislo,
            &[
                ("780123/3540", true), // commonly cited example
                ("7801233540", true),
                ("900101/1239", true),
                ("905101/1233", true),  // month + 50, female
                ("901301/1238", false), // divisible by 11, month 13
                ("900101/1238", false), // not divisible by 11
                ("530101/123", false),  // pre-1954 nine-digit form is not covered
            ],
        );
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

    #[test]
    fn bulgarian_egn_check_and_century_month() {
        check(
            bulgarian_egn,
            &[
                ("6101057509", true), // commonly cited example
                ("9001011238", true),
                ("0541011239", true),  // January 2005
                ("9013011234", false), // check valid, month 13
                ("9001011237", false),
            ],
        );
    }

    #[test]
    fn croatian_oib_iso7064() {
        check(
            croatian_oib,
            &[
                ("69435151530", true), // commonly cited example
                ("12345678903", true),
                ("98765432106", true),
                ("12345678904", false),
            ],
        );
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

    #[test]
    fn romanian_cnp_check_century_and_county() {
        check(
            romanian_cnp,
            &[
                ("1900101123457", true),
                ("5000115401231", true),  // born 2000, county 40
                ("1900101991231", false), // check valid, county 99
                ("1900101123458", false),
                ("0900101123457", false), // first digit 0
            ],
        );
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

    #[test]
    fn albanian_nid_decade_letter_and_date() {
        check(
            albanian_nid,
            &[
                ("I05101999Q", true),  // 1980, January (month 51 = 1, female), day 1
                ("A00101999Q", true),  // 1900
                ("i05101999q", true),  // lowercase: alphanumerics_of upper-cases first
                ("I05132999Q", false), // day 32
                ("Z05101999Q", false), // decade letter beyond M
                ("I0510199Q", false),
            ],
        );
    }
}
