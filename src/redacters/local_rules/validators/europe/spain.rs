//! Spanish identifiers: the DNI and NIE.

use super::super::checksums::alphanumerics_of;

const DNI_LETTERS: &[u8] = b"TRWAGMYFPDXBNJZSQVHLCKE";

pub fn spanish_dni_nie(value: &str) -> bool {
    let compact = alphanumerics_of(value);
    let bytes = compact.as_bytes();
    if bytes.len() != 9 {
        return false;
    }
    let mut digits = String::with_capacity(8);
    match bytes[0] {
        b'X' => digits.push('0'),
        b'Y' => digits.push('1'),
        b'Z' => digits.push('2'),
        c if c.is_ascii_digit() => digits.push(char::from(c)),
        _ => return false,
    }
    digits.push_str(&compact[1..8]);
    let Ok(number) = digits.parse::<usize>() else {
        return false;
    };
    DNI_LETTERS[number % 23] == bytes[8]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spanish_dni_and_nie_control_letter() {
        assert!(spanish_dni_nie("12345678Z"));
        assert!(!spanish_dni_nie("12345678A"));
        assert!(spanish_dni_nie("X1234567L"));
        assert!(!spanish_dni_nie("X1234567Z"));
    }
}
