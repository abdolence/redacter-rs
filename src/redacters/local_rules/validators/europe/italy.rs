//! Italian identifiers: the codice fiscale.

use super::super::checksums::alphanumerics_of;

const CF_ODD: [u32; 36] = [
    1, 0, 5, 7, 9, 13, 15, 17, 19, 21, // 0-9
    1, 0, 5, 7, 9, 13, 15, 17, 19, 21, 2, 4, 18, 20, 11, 3, 6, 8, 12, 14, 16, 10, 22, 25, 24,
    23, // A-Z
];

fn cf_index(c: u8) -> Option<usize> {
    match c {
        b'0'..=b'9' => Some(usize::from(c - b'0')),
        b'A'..=b'Z' => Some(usize::from(c - b'A') + 10),
        _ => None,
    }
}

pub fn italian_codice_fiscale(value: &str) -> bool {
    let compact = alphanumerics_of(value);
    let bytes = compact.as_bytes();
    if bytes.len() != 16 {
        return false;
    }
    let mut sum = 0;
    for (i, &c) in bytes[..15].iter().enumerate() {
        let Some(index) = cf_index(c) else {
            return false;
        };
        // Positions are 1-based in the specification: odd positions use the odd table,
        // even positions use the plain value (digits 0-9, letters 0-25).
        sum += if i % 2 == 0 {
            CF_ODD[index]
        } else if index >= 10 {
            index as u32 - 10
        } else {
            index as u32
        };
    }
    bytes[15] == b'A' + (sum % 26) as u8
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn italian_codice_fiscale_control_character() {
        assert!(italian_codice_fiscale("RSSMRA85M01H501Q"));
        assert!(!italian_codice_fiscale("RSSMRA85M01H501A"));
    }
}
