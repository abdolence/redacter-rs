use crate::errors::AppError;

/// Configuration problems in the local rules redacter. Every variant names the value
/// that was wrong so the user can find it in their command line or rule file.
#[derive(Debug, thiserror::Error)]
pub enum LocalRulesError {
    #[error("rule `{rule}` has an invalid regex: {reason}")]
    InvalidRegex { rule: String, reason: String },
    #[error("rule name `{name}` is used more than once")]
    DuplicateRule { name: String },
    #[error("rule name `{name}` is the name of a built-in rule")]
    BuiltinRuleName { name: String },
    #[error("rule `{name}` has an empty dictionary")]
    EmptyDictionary { name: String },
    #[error("rule `{name}` has a dictionary word that is empty or only whitespace")]
    EmptyDictionaryWord { name: String },
    #[error("rule `{name}` is a dictionary, which has no capture group to choose")]
    CaptureGroupOnDictionary { name: String },
    #[error(
        "rule `{name}` redacts capture group {capture_group}, but its regex has {groups} groups \
         counting the whole match"
    )]
    CaptureGroupOutOfRange {
        name: String,
        capture_group: usize,
        groups: usize,
    },
    #[error("rule `{rule}` names the validator `{validator}`, which is not a built-in rule with a value check")]
    UnknownValidator { rule: String, validator: String },
    #[error("rule `{rule}` finds nothing in its example `{example}`")]
    ExampleNotMatched { rule: String, example: String },
    #[error("rule `{rule}` finds a value in its counter-example `{example}`")]
    CounterExampleMatched { rule: String, example: String },
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
