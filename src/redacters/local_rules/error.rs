use crate::errors::AppError;

/// Configuration problems in the local rules redacter. Every variant names the value
/// that was wrong so the user can find it in their command line or rule file.
#[derive(Debug, thiserror::Error)]
pub enum LocalRulesError {
    #[error("rule `{rule}` has an invalid regex: {reason}")]
    InvalidRegex { rule: String, reason: String },
    #[error("rule name `{name}` is used more than once")]
    DuplicateRule { name: String },
    #[error("rule `{name}` has an empty dictionary")]
    EmptyDictionary { name: String },
    #[error("rule `{name}` has a dictionary word that is empty or only whitespace")]
    EmptyDictionaryWord { name: String },
    #[error("inline rule `{value}` must have the form name=regex")]
    InvalidInlineRule { value: String },
    #[error("rule file {path}: {reason}")]
    RuleFile { path: String, reason: String },
    #[error("no local rules are enabled: every group is disabled and no custom rules were given")]
    NoRules,
}

impl From<LocalRulesError> for AppError {
    fn from(err: LocalRulesError) -> Self {
        AppError::RedacterConfigError {
            message: err.to_string(),
        }
    }
}
