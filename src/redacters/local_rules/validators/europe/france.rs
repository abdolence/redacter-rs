//! French identifiers: the NIR (social security number).

use super::super::checksums::alphanumerics_of;

pub fn french_nir(value: &str) -> bool {
    let compact = alphanumerics_of(value);
    if compact.len() != 15 {
        return false;
    }
    let (number, key) = compact.split_at(13);
    let number = number.replace("2A", "19").replace("2B", "18");
    let (Ok(number), Ok(key)) = (number.parse::<u64>(), key.parse::<u64>()) else {
        return false;
    };
    97 - (number % 97) == key
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn french_nir_key() {
        assert!(french_nir("2 69 05 49 588 157 80"));
        assert!(!french_nir("2 69 05 49 588 157 81"));
        assert!(french_nir("1 55 05 2A 123 456 48"));
    }
}
