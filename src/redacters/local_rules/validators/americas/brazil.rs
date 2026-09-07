//! Brazilian identifiers: the CPF.

use super::super::checksums::digits_of;

/// Brazilian CPF `NNN.NNN.NNN-CC`: each check digit is 11 minus the weighted sum of the
/// digits before it (weights counting down from 10, then from 11) modulo 11, or 0 when
/// that is below 2. Numbers made of one repeated digit pass the arithmetic and are
/// rejected.
pub fn brazilian_cpf(value: &str) -> bool {
    let digits = digits_of(value);
    if digits.len() != 11 || digits.iter().all(|&d| d == digits[0]) {
        return false;
    }
    cpf_check_digit(&digits[..9]) == digits[9] && cpf_check_digit(&digits[..10]) == digits[10]
}

fn cpf_check_digit(digits: &[u32]) -> u32 {
    let top = u32::try_from(digits.len()).unwrap_or(0) + 1;
    let sum: u32 = digits
        .iter()
        .enumerate()
        .map(|(i, &d)| d * (top - u32::try_from(i).unwrap_or(0)))
        .sum();
    match sum % 11 {
        0 | 1 => 0,
        remainder => 11 - remainder,
    }
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
    fn brazilian_cpf_two_check_digits() {
        check(
            brazilian_cpf,
            &[
                ("111.444.777-35", true), // commonly cited example
                ("12345678909", true),
                ("98765432100", true),
                ("11111111111", false), // repeated digit, arithmetically valid
                ("12345678900", false),
            ],
        );
    }
}
