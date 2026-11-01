//! The categories `--categories` chose, and the errors of choosing them.

use crate::args::RedacterType;
use crate::redacters::{Category, Detections};
use clap::ValueEnum;
use serde::Serialize;
use std::collections::BTreeSet;
use std::fmt::Display;

/// The categories `--categories` chose: at least one, never all of them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct Categories(BTreeSet<Category>);

impl Categories {
    /// The chosen categories, or `None` when `chosen` names every category or none, which
    /// limits nothing.
    pub fn limiting(chosen: impl IntoIterator<Item = Category>) -> Option<Self> {
        let chosen: BTreeSet<Category> = chosen.into_iter().collect();
        let limits_something =
            !chosen.is_empty() && chosen.len() < Category::value_variants().len();
        limits_something.then_some(Self(chosen))
    }

    pub fn contains(&self, category: Category) -> bool {
        self.0.contains(&category)
    }

    /// Accepts the `values` the user named for `option` when all of them are in the chosen
    /// categories, else fails on the first one outside.
    pub fn check<T: Display>(
        &self,
        option: &'static str,
        values: &[T],
        category: impl Fn(&T) -> Category,
    ) -> Result<(), CategoriesError> {
        match values.iter().find(|value| !self.contains(category(value))) {
            Some(value) => Err(CategoriesError::ValueOutside {
                option,
                value: value.to_string(),
                category: category(value),
            }),
            None => Ok(()),
        }
    }

    /// `defaults` without those outside the chosen categories.
    pub fn filter<T>(
        &self,
        defaults: impl IntoIterator<Item = T>,
        category: impl Fn(&T) -> Category,
    ) -> Vec<T> {
        defaults
            .into_iter()
            .filter(|value| self.contains(category(value)))
            .collect()
    }
}

impl Display for Categories {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (position, category) in self.0.iter().enumerate() {
            if position > 0 {
                f.write_str(", ")?;
            }
            write!(f, "{category}")?;
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CategoriesError {
    #[error("{option} {value} is in the {category} category, which --categories leaves out")]
    ValueOutside {
        option: &'static str,
        value: String,
        category: Category,
    },
    #[error("{redacter} finds nothing in the chosen categories: {categories}")]
    NothingToFind {
        redacter: RedacterType,
        categories: Categories,
    },
    #[error("{option} has no effect with --categories, which reads images with OCR")]
    NoEffect { option: &'static str },
    #[error("{redacter} cannot limit cp to the chosen categories, only scan can")]
    CopyUnsupported { redacter: RedacterType },
    #[error("{redacter} named {values} value(s) it could not place in the text, so they cannot be redacted")]
    Unlocated {
        redacter: RedacterType,
        values: usize,
    },
}

impl Detections {
    /// Drops the findings and the unlocated values whose label `redacter` places outside
    /// `categories`.
    pub fn retain_categories(&mut self, redacter: RedacterType, categories: &Categories) {
        self.found
            .retain(|detection| categories.contains(detection.label.category(redacter)));
        self.unlocated
            .retain(|label| categories.contains(label.category(redacter)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::redacters::{Detection, DetectionLabel, DetectionLocation};

    fn chosen(categories: &[Category]) -> Categories {
        Categories::limiting(categories.iter().copied()).unwrap()
    }

    #[test]
    fn secrets_is_credential_and_prints_as_credential() {
        let parsed = Category::from_str("secrets", true).unwrap();
        assert_eq!(parsed, Category::Credential);
        assert_eq!(parsed.to_string(), "credential");
    }

    #[test]
    fn choosing_every_category_limits_nothing() {
        assert_eq!(
            Categories::limiting(Category::value_variants().iter().copied()),
            None
        );
        assert!(Categories::limiting([Category::Credential]).is_some());
    }

    #[test]
    fn the_chosen_categories_print_in_the_order_of_the_enum() {
        let categories = chosen(&[Category::Credential, Category::Contact]);
        assert_eq!(categories.to_string(), "contact, credential");
    }

    #[test]
    fn check_fails_on_the_first_value_outside_with_its_category() {
        let categories = chosen(&[Category::Credential]);
        let error = categories
            .check("--local-rules", &["secrets", "email", "phone"], |name| {
                if *name == "secrets" {
                    Category::Credential
                } else {
                    Category::Contact
                }
            })
            .unwrap_err();
        assert!(matches!(
            error,
            CategoriesError::ValueOutside {
                option: "--local-rules",
                ref value,
                category: Category::Contact,
            } if value == "email"
        ));
    }

    #[test]
    fn filter_keeps_only_the_defaults_in_the_chosen_categories() {
        let categories = chosen(&[Category::Credential, Category::Network]);
        let kept = categories.filter(
            [
                ("password", Category::Credential),
                ("name", Category::Person),
                ("ip", Category::Network),
            ],
            |(_, category)| *category,
        );
        assert_eq!(
            kept.iter().map(|(name, _)| *name).collect::<Vec<_>>(),
            ["password", "ip"]
        );
    }

    #[test]
    fn retain_categories_drops_the_findings_and_unlocated_values_outside() {
        let found = ["email", "secret-assignment", "employee-number"]
            .into_iter()
            .map(|label| Detection {
                label: DetectionLabel::new(label),
                location: DetectionLocation::File,
                score: None,
                value: String::new(),
            })
            .collect::<Vec<_>>();
        let mut detections = Detections::from(found);
        detections.unlocated = ["email", "secret-assignment"]
            .into_iter()
            .map(DetectionLabel::new)
            .collect();
        detections.retain_categories(
            RedacterType::LocalRules,
            &chosen(&[Category::Credential, Category::Other]),
        );
        assert_eq!(
            detections.unlocated,
            [DetectionLabel::new("secret-assignment")]
        );
        assert_eq!(
            detections
                .found
                .iter()
                .map(|detection| detection.label.to_string())
                .collect::<Vec<_>>(),
            ["secret-assignment", "employee-number"]
        );
    }
}
