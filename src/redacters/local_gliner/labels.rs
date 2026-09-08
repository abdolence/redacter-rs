//! The default zero-shot label set fed to the model as prompt text.
//!
//! `gliner_multi_pii-v1` has no fixed label taxonomy: any free-text label works as a prompt.
//! This list is PII only. `organization` is deliberately absent: measured against this model,
//! it over-redacts team and desk names (`Marketing team`, `The Support Desk`) that are not PII.

pub const DEFAULT_LABELS: &[&str] = &[
    "person",
    "address",
    "email",
    "phone number",
    "date of birth",
    "passport number",
    "national id number",
    "credit card number",
    "iban",
    "ip address",
    "username",
    "medical condition",
    "postal code",
    "license plate number",
];

pub fn default_labels() -> Vec<String> {
    DEFAULT_LABELS
        .iter()
        .map(|label| label.to_string())
        .collect()
}

/// Splits `labels` into batches of at most `max_types`, the model's limit on how many entity
/// types one forward pass can score. Each batch becomes an independent prompt over the same
/// windows; callers union the resulting spans.
pub fn batch_labels(labels: &[String], max_types: usize) -> Vec<&[String]> {
    if max_types == 0 {
        return Vec::new();
    }
    labels.chunks(max_types).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_labels_has_no_organization() {
        let labels = default_labels();
        assert!(!labels.is_empty());
        assert!(
            !labels.iter().any(|label| label == "organization"),
            "{labels:?}"
        );
    }

    #[test]
    fn batches_respect_max_types_and_cover_every_label() {
        let labels: Vec<String> = (0..60).map(|i| format!("label{i}")).collect();
        let batches = batch_labels(&labels, 25);
        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0].len(), 25);
        assert_eq!(batches[1].len(), 25);
        assert_eq!(batches[2].len(), 10);
        let total: usize = batches.iter().map(|b| b.len()).sum();
        assert_eq!(total, labels.len());
    }

    #[test]
    fn a_single_small_batch_is_not_split() {
        let labels = default_labels();
        let batches = batch_labels(&labels, 25);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), labels.len());
    }

    #[test]
    fn zero_max_types_yields_no_batches() {
        let labels = default_labels();
        assert!(batch_labels(&labels, 0).is_empty());
    }
}
