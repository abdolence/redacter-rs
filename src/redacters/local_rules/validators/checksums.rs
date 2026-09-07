//! Check-digit arithmetic shared by the national identifier validators. Every function takes
//! digits already extracted with `digits_of` (so each is 0..=9); the callers decide lengths,
//! date fields and which digit is the check.

pub(super) fn digits_of(value: &str) -> Vec<u32> {
    value.chars().filter_map(|c| c.to_digit(10)).collect()
}

pub(super) fn alphanumerics_of(value: &str) -> String {
    value
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .map(|c| c.to_ascii_uppercase())
        .collect()
}

pub(super) fn luhn_any_length(digits: &[u32]) -> bool {
    let sum: u32 = digits
        .iter()
        .rev()
        .enumerate()
        .map(|(i, &d)| {
            if i % 2 == 1 {
                let doubled = d * 2;
                if doubled > 9 {
                    doubled - 9
                } else {
                    doubled
                }
            } else {
                d
            }
        })
        .sum();
    sum.is_multiple_of(10)
}

/// ISO 7064 mod 97-10 over the IBAN rearranged with the country code and check digits moved
/// to the end and letters expanded to two digits (A=10 .. Z=35).
pub(super) fn mod97(value: &str) -> u32 {
    let mut remainder: u32 = 0;
    for c in value.chars() {
        let chunk = if c.is_ascii_digit() {
            c.to_digit(10).unwrap_or(0)
        } else {
            u32::from(c.to_ascii_uppercase()) - u32::from('A') + 10
        };
        let width = if chunk >= 10 { 100 } else { 10 };
        remainder = (remainder * width + chunk) % 97;
    }
    remainder
}

/// The Dutch eleven test: the first eight digits weighted 9 down to 2, minus the ninth, is a
/// multiple of 11. Shared by the BSN and the Dutch VAT number. `digits` must hold at least 9.
pub(super) fn dutch_eleven_test(digits: &[u32]) -> bool {
    let weighted: i64 = digits[..8]
        .iter()
        .enumerate()
        .map(|(i, &d)| i64::from(d) * (9 - i as i64))
        .sum();
    (weighted - i64::from(digits[8])).rem_euclid(11) == 0
}

/// ISO 7064 MOD 11,10 over `digits`, returning the check digit that must follow them. Shared
/// by the German tax identification number and the German VAT number.
pub(super) fn iso7064_mod_11_10_check(digits: &[u32]) -> u32 {
    let mut product = 10;
    for &d in digits {
        let mut sum = (d + product) % 10;
        if sum == 0 {
            sum = 10;
        }
        product = (sum * 2) % 11;
    }
    (11 - product) % 10
}

/// Σ digit × weight over the shorter of the two slices.
pub(super) fn weighted_sum(digits: &[u32], weights: &[u32]) -> u32 {
    digits.iter().zip(weights).map(|(d, w)| d * w).sum()
}

/// `11 - (weighted sum mod 11)`, the form used by the Norwegian, Icelandic and ex-Yugoslav
/// numbers: 11 is read as 0 and 10 means no valid control digit exists.
pub(super) fn mod11_complement(digits: &[u32], weights: &[u32]) -> Option<u32> {
    match 11 - weighted_sum(digits, weights) % 11 {
        11 => Some(0),
        10 => None,
        control => Some(control),
    }
}

/// ISO 7064 MOD 11-2 check character (GB 11643 resident identity numbers): the weights are
/// successive powers of two modulo 11 counted from the right, and the remainder selects a
/// character of `10X98765432`.
pub(super) fn iso7064_mod_11_2_check(digits: &[u32]) -> char {
    const CHARS: &[u8; 11] = b"10X98765432";
    let mut weight = 2;
    let mut sum = 0;
    for &d in digits.iter().rev() {
        sum += d * weight;
        weight = (weight * 2) % 11;
    }
    char::from(CHARS[(sum % 11) as usize])
}

/// EAN-13 / GS1 check digit over the twelve leading digits (weights 1 and 3 alternating
/// from the left).
pub(super) fn ean13_check_digit(digits: &[u32]) -> u32 {
    let sum: u32 = digits
        .iter()
        .enumerate()
        .map(|(i, &d)| if i % 2 == 1 { d * 3 } else { d })
        .sum();
    (10 - sum % 10) % 10
}

const VERHOEFF_D: [[u8; 10]; 10] = [
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
    [1, 2, 3, 4, 0, 6, 7, 8, 9, 5],
    [2, 3, 4, 0, 1, 7, 8, 9, 5, 6],
    [3, 4, 0, 1, 2, 8, 9, 5, 6, 7],
    [4, 0, 1, 2, 3, 9, 5, 6, 7, 8],
    [5, 9, 8, 7, 6, 0, 4, 3, 2, 1],
    [6, 5, 9, 8, 7, 1, 0, 4, 3, 2],
    [7, 6, 5, 9, 8, 2, 1, 0, 4, 3],
    [8, 7, 6, 5, 9, 3, 2, 1, 0, 4],
    [9, 8, 7, 6, 5, 4, 3, 2, 1, 0],
];
const VERHOEFF_P: [[u8; 10]; 8] = [
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
    [1, 5, 7, 6, 2, 8, 3, 0, 9, 4],
    [5, 8, 0, 3, 7, 9, 6, 1, 4, 2],
    [8, 9, 1, 6, 0, 4, 3, 5, 2, 7],
    [9, 4, 5, 3, 1, 2, 6, 8, 7, 0],
    [4, 2, 8, 6, 5, 7, 3, 9, 0, 1],
    [2, 7, 9, 3, 8, 0, 6, 4, 1, 5],
    [7, 0, 4, 6, 9, 1, 3, 2, 5, 8],
];
const VERHOEFF_INV: [u8; 10] = [0, 4, 3, 2, 1, 5, 6, 7, 8, 9];

/// Runs the Verhoeff dihedral-group product from the right, with the position offset the
/// two uses need (0 to validate a number ending in its check digit, 1 to compute one).
fn verhoeff_run(digits: &[u32], offset: usize) -> usize {
    let mut c = 0usize;
    for (i, &d) in digits.iter().rev().enumerate() {
        // `d` is 0..=9 by construction (`digits_of`), so both lookups are in range.
        let p = VERHOEFF_P[(i + offset) % 8][d as usize];
        c = usize::from(VERHOEFF_D[c][usize::from(p)]);
    }
    c
}

/// True when the last digit is the Verhoeff check digit of the digits before it.
pub(super) fn verhoeff_valid(digits: &[u32]) -> bool {
    verhoeff_run(digits, 0) == 0
}

/// The Verhoeff check digit that must follow `digits`.
pub(super) fn verhoeff_check_digit(digits: &[u32]) -> u32 {
    u32::from(VERHOEFF_INV[verhoeff_run(digits, 1)])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn weighted_sum_zips_the_shorter_slice() {
        assert_eq!(weighted_sum(&[1, 2, 3], &[10, 20]), 50);
        assert_eq!(weighted_sum(&[], &[10, 20]), 0);
    }

    #[test]
    fn mod11_complement_maps_eleven_to_zero_and_ten_to_none() {
        // weighted sum 22 -> remainder 0 -> 11 -> 0
        assert_eq!(mod11_complement(&[2], &[11]), Some(0));
        // weighted sum 1 -> 11 - 1 = 10 -> not issuable
        assert_eq!(mod11_complement(&[1], &[1]), None);
        // weighted sum 4 -> 7
        assert_eq!(mod11_complement(&[2, 2], &[1, 1]), Some(7));
    }

    #[test]
    fn iso7064_mod_11_2_check_character() {
        // Chinese resident id example widely used in the GB 11643 literature.
        let digits = digits_of("11010519491231002");
        assert_eq!(iso7064_mod_11_2_check(&digits), 'X');
        assert_eq!(iso7064_mod_11_2_check(&digits_of("11010519900101123")), '2');
    }

    #[test]
    fn ean13_check_digit_matches_a_published_barcode() {
        // EAN-13 example from the GS1 check digit documentation: 4006381333931.
        assert_eq!(ean13_check_digit(&digits_of("400638133393")), 1);
    }

    #[test]
    fn verhoeff_accepts_the_published_example_and_computes_its_digit() {
        // Verhoeff's own example: the check digit of 236 is 3.
        assert_eq!(verhoeff_check_digit(&[2, 3, 6]), 3);
        assert!(verhoeff_valid(&[2, 3, 6, 3]));
        assert!(!verhoeff_valid(&[2, 3, 6, 4]));
        // Every single-digit transposition of 2363 is caught.
        for (i, j) in [(0, 1), (1, 2), (2, 3)] {
            let mut swapped = [2, 3, 6, 3];
            swapped.swap(i, j);
            assert!(
                !verhoeff_valid(&swapped) || swapped == [2, 3, 6, 3],
                "{swapped:?}"
            );
        }
    }
}
