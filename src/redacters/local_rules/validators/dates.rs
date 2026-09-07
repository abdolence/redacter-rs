//! Calendar helpers for identifiers that embed a birth date.

use std::time::{SystemTime, UNIX_EPOCH};

/// Year of the current UTC date, 1970 if the clock is before the epoch.
pub(super) fn current_year() -> u32 {
    let days = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs() / 86_400);
    civil_year_from_days(days)
}

/// Year of the proleptic Gregorian date `days` after 1970-01-01: the year part of Howard
/// Hinnant's `civil_from_days`, for non-negative day counts only.
fn civil_year_from_days(days: u64) -> u32 {
    let z = days + 719_468;
    let era = z / 146_097;
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let shifted_month = (5 * day_of_year + 2) / 153;
    let month = if shifted_month < 10 {
        shifted_month + 3
    } else {
        shifted_month - 9
    };
    let year = year_of_era + era * 400 + u64::from(month <= 2);
    u32::try_from(year).unwrap_or(u32::MAX)
}

fn is_leap_year(year: u32) -> bool {
    year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400))
}

fn days_in_month(year: Option<u32>, month: u32) -> u32 {
    match month {
        4 | 6 | 9 | 11 => 30,
        2 => match year {
            Some(year) if !is_leap_year(year) => 28,
            _ => 29,
        },
        _ => 31,
    }
}

/// True for a real calendar date. With the year unknown (two-digit years whose century the
/// format does not encode) 29 February is accepted. A known year must lie in 1850..=the
/// current year: these are birth dates, and nobody alive was born before 1850.
pub(super) fn valid_date(year: Option<u32>, month: u32, day: u32) -> bool {
    if !(1..=12).contains(&month) || day == 0 {
        return false;
    }
    if year.is_some_and(|year| !(1850..=current_year()).contains(&year)) {
        return false;
    }
    day <= days_in_month(year, month)
}

/// The two digits at `digits[at..at + 2]` as one number. Callers check the length first.
pub(super) fn pair(digits: &[u32], at: usize) -> u32 {
    digits[at] * 10 + digits[at + 1]
}

/// Month names, lowercase, in the languages the `birth-date` rule supports (English,
/// German, French, Spanish, Italian, Dutch, Portuguese, Polish, Swedish), with the ASCII
/// spellings of the accented ones and the common abbreviations. Names shared by several
/// languages appear once.
pub const MONTHS: &[(&str, u32)] = &[
    ("january", 1),
    ("jan", 1),
    ("januar", 1),
    ("janvier", 1),
    ("enero", 1),
    ("gennaio", 1),
    ("januari", 1),
    ("janeiro", 1),
    ("stycznia", 1),
    ("styczeń", 1),
    ("styczen", 1),
    ("february", 2),
    ("feb", 2),
    ("februar", 2),
    ("février", 2),
    ("fevrier", 2),
    ("febrero", 2),
    ("febbraio", 2),
    ("februari", 2),
    ("fevereiro", 2),
    ("lutego", 2),
    ("luty", 2),
    ("march", 3),
    ("mar", 3),
    ("märz", 3),
    ("maerz", 3),
    ("mrz", 3),
    ("mars", 3),
    ("marzo", 3),
    ("maart", 3),
    ("março", 3),
    ("marco", 3),
    ("marca", 3),
    ("marzec", 3),
    ("april", 4),
    ("apr", 4),
    ("avril", 4),
    ("abril", 4),
    ("aprile", 4),
    ("kwietnia", 4),
    ("kwiecień", 4),
    ("kwiecien", 4),
    ("may", 5),
    ("mai", 5),
    ("mayo", 5),
    ("maggio", 5),
    ("mei", 5),
    ("maio", 5),
    ("maja", 5),
    ("maj", 5),
    ("june", 6),
    ("jun", 6),
    ("juni", 6),
    ("juin", 6),
    ("junio", 6),
    ("giugno", 6),
    ("junho", 6),
    ("czerwca", 6),
    ("czerwiec", 6),
    ("july", 7),
    ("jul", 7),
    ("juli", 7),
    ("juillet", 7),
    ("julio", 7),
    ("luglio", 7),
    ("julho", 7),
    ("lipca", 7),
    ("lipiec", 7),
    ("august", 8),
    ("aug", 8),
    ("août", 8),
    ("aout", 8),
    ("agosto", 8),
    ("augustus", 8),
    ("augusti", 8),
    ("sierpnia", 8),
    ("sierpień", 8),
    ("sierpien", 8),
    ("september", 9),
    ("sep", 9),
    ("sept", 9),
    ("septembre", 9),
    ("septiembre", 9),
    ("setiembre", 9),
    ("settembre", 9),
    ("setembro", 9),
    ("września", 9),
    ("wrzesnia", 9),
    ("wrzesień", 9),
    ("wrzesien", 9),
    ("october", 10),
    ("oct", 10),
    ("oktober", 10),
    ("okt", 10),
    ("octobre", 10),
    ("octubre", 10),
    ("ottobre", 10),
    ("outubro", 10),
    ("października", 10),
    ("pazdziernika", 10),
    ("październik", 10),
    ("pazdziernik", 10),
    ("november", 11),
    ("nov", 11),
    ("novembre", 11),
    ("noviembre", 11),
    ("novembro", 11),
    ("listopada", 11),
    ("listopad", 11),
    ("december", 12),
    ("dec", 12),
    ("dezember", 12),
    ("dez", 12),
    ("décembre", 12),
    ("decembre", 12),
    ("diciembre", 12),
    ("dicembre", 12),
    ("dezembro", 12),
    ("grudnia", 12),
    ("grudzień", 12),
    ("grudzien", 12),
];

fn month_number(word: &str) -> Option<u32> {
    let word = word.trim_end_matches('.').to_lowercase();
    MONTHS
        .iter()
        .find(|(name, _)| *name == word)
        .map(|(_, month)| *month)
}

fn plausible_birth_date(year: u32, month: u32, day: u32) -> bool {
    year >= 1900 && valid_date(Some(year), month, day)
}

/// A date of birth as captured by the `birth-date` rule: ISO `1985-03-14`, numeric
/// `14.03.1985` or `03/14/1985` (day-first and month-first are both tried), or a day, a
/// month name from `MONTHS` and a four-digit year in either order. Years are limited to
/// 1900..=the current year.
pub fn birth_date(value: &str) -> bool {
    let numbers: Vec<u32> = value
        .split(|c: char| !c.is_ascii_digit())
        .filter(|part| !part.is_empty())
        .filter_map(|part| part.parse().ok())
        .collect();
    let month_word = value
        .split(|c: char| !c.is_alphabetic())
        .find_map(month_number);
    let (year, month, day) = match (month_word, numbers.as_slice()) {
        // `14 March 1985`, `March 14, 1985`, `1985 March 14`
        (Some(month), [first, second]) => {
            if *first > 31 {
                (*first, month, *second)
            } else {
                (*second, month, *first)
            }
        }
        // `1985-03-14`
        (None, [year, month, day]) if *year > 31 => (*year, *month, *day),
        // `14.03.1985` or `03/14/1985`: whichever order is a real date
        (None, [first, second, year]) => {
            return plausible_birth_date(*year, *second, *first)
                || plausible_birth_date(*year, *first, *second);
        }
        _ => return false,
    };
    plausible_birth_date(year, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn civil_year_from_days_at_known_boundaries() {
        assert_eq!(civil_year_from_days(0), 1970);
        assert_eq!(civil_year_from_days(11_016), 2000); // 2000-02-29
        assert_eq!(civil_year_from_days(19_723), 2024); // 2024-01-01
        assert_eq!(civil_year_from_days(20_453), 2025); // 2025-12-31
        assert_eq!(civil_year_from_days(20_454), 2026); // 2026-01-01
    }

    #[test]
    fn current_year_is_not_before_2026() {
        assert!(current_year() >= 2026);
    }

    #[test]
    fn valid_date_handles_leap_years_and_the_year_window() {
        for (year, month, day, expected) in [
            (Some(2000), 2, 29, true),
            (Some(1900), 2, 29, false),
            (Some(2004), 2, 29, true),
            (None, 2, 29, true),
            (None, 2, 30, false),
            (Some(2024), 4, 31, false),
            (Some(2024), 12, 31, true),
            (Some(1849), 1, 1, false),
            (Some(1850), 1, 1, true),
            (Some(current_year()), 1, 1, true),
            (Some(current_year() + 1), 1, 1, false),
            (None, 13, 1, false),
            (None, 0, 1, false),
            (None, 1, 0, false),
        ] {
            assert_eq!(
                valid_date(year, month, day),
                expected,
                "{year:?}-{month}-{day}"
            );
        }
        assert_eq!(pair(&[1, 9, 8, 5], 2), 85);
    }

    #[test]
    fn birth_date_accepts_the_supported_shapes_and_languages() {
        for (value, expected) in [
            ("1985-03-14", true),
            ("14.03.1985", true),
            ("14-03-1985", true),
            ("03/14/1985", true), // month first
            ("14/03/1985", true), // day first
            ("14 March 1985", true),
            ("14th March 1985", true),
            ("March 14, 1985", true),
            ("14 Sept. 1985", true),
            ("14. März 1985", true),
            ("14 mars 1985", true),
            ("1er janvier 1985", true),
            ("14 de marzo de 1985", true),
            ("14 marzo 1985", true),
            ("14 maart 1985", true),
            ("14 de março de 1985", true),
            ("14 marca 1985", true),
            ("14 augusti 1985", true),
            ("2024-04-30", true),  // a toddler is a person too
            ("1985-02-29", false), // not a leap year
            ("31.04.1985", false),
            ("13/13/1985", false),
            ("14 Smarch 1985", false),
            ("14 March 1899", false),
            ("14 March 2999", false),
            ("14 March", false),
        ] {
            assert_eq!(birth_date(value), expected, "{value}");
        }
    }

    #[test]
    fn month_names_map_to_months() {
        assert_eq!(month_number("MÄRZ"), Some(3));
        assert_eq!(month_number("sept."), Some(9));
        assert_eq!(month_number("października"), Some(10));
        assert_eq!(month_number("de"), None);
        for (name, month) in MONTHS {
            assert!((1..=12).contains(month), "{name}");
            assert!(
                name.chars().count() <= 12,
                "{name}: the pattern allows 12 letters"
            );
            assert_eq!(*name, name.to_lowercase(), "{name}");
        }
    }
}
