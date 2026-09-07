//! The EU VAT number, a single validator covering several member states' own check digits.

use super::super::checksums::{
    alphanumerics_of, digits_of, dutch_eleven_test, iso7064_mod_11_10_check, luhn_any_length,
};

pub fn eu_vat(value: &str) -> bool {
    let compact = alphanumerics_of(value);
    if compact.len() < 4 {
        return false;
    }
    let (country, body) = compact.split_at(2);
    let digits = digits_of(body);
    match country {
        // ISO 7064 MOD 11,10 over 9 digits.
        "DE" => digits.len() == 9 && iso7064_mod_11_10_check(&digits[..8]) == digits[8],
        "FR" => {
            if body.len() != 11 || !body[..2].bytes().all(|b| b.is_ascii_digit()) {
                return body.len() == 11;
            }
            let (Ok(key), Ok(siren)) = (body[..2].parse::<u64>(), body[2..].parse::<u64>()) else {
                return false;
            };
            (12 + 3 * (siren % 97)) % 97 == key
        }
        "IT" => digits.len() == 11 && luhn_any_length(&digits),
        "NL" => digits.len() == 11 && &body[9..10] == "B" && dutch_eleven_test(&digits),
        "BE" => {
            if digits.len() != 10 {
                return false;
            }
            let base: u64 = digits[..8]
                .iter()
                .fold(0, |acc, &d| acc * 10 + u64::from(d));
            let check: u64 = u64::from(digits[8]) * 10 + u64::from(digits[9]);
            97 - (base % 97) == check
        }
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eu_vat_checks_the_countries_with_a_known_algorithm() {
        assert!(eu_vat("DE136695976"));
        assert!(!eu_vat("DE136695977"));
        assert!(eu_vat("FR40303265045"));
        assert!(!eu_vat("FR41303265045"));
        assert!(eu_vat("IT00743110157"));
        assert!(!eu_vat("IT00743110158"));
        assert!(eu_vat("NL123456782B01"));
        assert!(!eu_vat("NL123456783B01"));
        assert!(eu_vat("BE0403019261"));
        assert!(!eu_vat("BE0403019262"));
        assert!(eu_vat("ESA12345674"));
    }
}
