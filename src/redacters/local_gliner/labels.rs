//! The default zero-shot label set fed to the model as prompt text.
//!
//! `gliner_multi_pii-v1` has no fixed label taxonomy: any free-text label works as a prompt.
//! This list is PII only. `organization` is deliberately absent: measured against this model,
//! it over-redacts team and desk names (`Marketing team`, `The Support Desk`) that are not PII.

use crate::redacters::Category;

pub const DEFAULT_LABELS: [(&str, Category); 14] = [
    ("person", Category::Person),
    ("address", Category::Address),
    ("email", Category::Contact),
    ("phone number", Category::Contact),
    ("date of birth", Category::DateOfBirth),
    ("passport number", Category::IdNumber),
    ("national id number", Category::IdNumber),
    ("credit card number", Category::Financial),
    ("iban", Category::Financial),
    ("ip address", Category::Network),
    ("username", Category::Person),
    ("medical condition", Category::Other),
    ("postal code", Category::Address),
    ("license plate number", Category::IdNumber),
];

pub fn default_labels() -> Vec<String> {
    DEFAULT_LABELS
        .iter()
        .map(|(label, _)| label.to_string())
        .collect()
}

/// The category of a finding the model labelled `label`, ignoring case: that of the default
/// label of the name, `other` for a label the user added.
#[cfg_attr(not(test), expect(dead_code, reason = "wired in with --categories"))]
pub fn label_category(label: &str) -> Category {
    DEFAULT_LABELS
        .iter()
        .find(|(default, _)| default.eq_ignore_ascii_case(label))
        .map_or(Category::Other, |(_, category)| *category)
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
    fn every_default_label_but_medical_condition_is_in_a_named_category() {
        for (label, category) in DEFAULT_LABELS {
            assert_eq!(
                category != Category::Other,
                label != "medical condition",
                "{label}"
            );
        }
    }

    #[test]
    fn a_label_is_placed_ignoring_case_and_a_label_the_user_added_is_other() {
        assert_eq!(label_category("Phone Number"), Category::Contact);
        assert_eq!(label_category("employer"), Category::Other);
    }

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
