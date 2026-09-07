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
}
