//! The categories `--categories` chose, and the errors of choosing them.

use crate::args::RedacterType;
use crate::redacters::{mask_detections, Category, Detection, Detections, TextMask};
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

    /// `text` with the findings of `redacter` in `categories` masked as `mask` says and
    /// everything else untouched, and the findings that were masked. Fails when a value in
    /// the categories was named but not placed in `text`, since it cannot be masked.
    pub fn masked_within(
        mut self,
        text: &str,
        redacter: RedacterType,
        mask: TextMask,
        categories: &Categories,
    ) -> Result<(String, Vec<Detection>), CategoriesError> {
        self.retain_categories(redacter, categories);
        if !self.unlocated.is_empty() {
            return Err(CategoriesError::Unlocated {
                redacter,
                values: self.unlocated.len(),
            });
        }
        Ok((mask_detections(text, &self.found, mask), self.found))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::redacters::{Detection, DetectionLabel, DetectionLocation, TextLines};

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

    fn located(text: &str, value: &str, label: &str) -> Detection {
        let start = text.find(value).unwrap();
        TextLines::new(text)
            .detection(start..start + value.len(), DetectionLabel::new(label), None)
            .unwrap()
    }

    #[test]
    fn masking_within_the_categories_leaves_the_rest_of_the_text_alone() {
        let text = "key=hunter2-Secret mail=ops@example.com";
        let detections = Detections::from(vec![
            located(text, "hunter2-Secret", "password"),
            located(text, "ops@example.com", "email_address"),
        ]);

        let (masked, replaced) = detections
            .masked_within(
                text,
                RedacterType::GeminiLlm,
                TextMask::EveryCharacter,
                &chosen(&[Category::Credential]),
            )
            .unwrap();

        assert_eq!(masked, "key=XXXXXXXXXXXXXX mail=ops@example.com");
        assert_eq!(
            replaced
                .iter()
                .map(|d| d.value.as_str())
                .collect::<Vec<_>>(),
            ["hunter2-Secret"]
        );
    }

    #[test]
    fn a_value_in_the_categories_that_was_not_placed_cannot_be_masked() {
        let text = "key=hunter2-Secret";
        let detections = Detections {
            unlocated: vec![DetectionLabel::new("password")],
            ..Detections::default()
        };

        let error = detections
            .masked_within(
                text,
                RedacterType::GeminiLlm,
                TextMask::EveryCharacter,
                &chosen(&[Category::Credential]),
            )
            .unwrap_err();

        assert!(matches!(
            error,
            CategoriesError::Unlocated {
                redacter: RedacterType::GeminiLlm,
                values: 1
            }
        ));
    }

    #[test]
    fn a_value_outside_the_categories_that_was_not_placed_is_no_error() {
        let text = "mail=ops@example.com";
        let detections = Detections {
            unlocated: vec![DetectionLabel::new("email_address")],
            ..Detections::default()
        };

        let (masked, replaced) = detections
            .masked_within(
                text,
                RedacterType::GeminiLlm,
                TextMask::EveryCharacter,
                &chosen(&[Category::Credential]),
            )
            .unwrap();

        assert_eq!(masked, text);
        assert!(replaced.is_empty());
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
