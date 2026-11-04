use crate::redacters::local_model::LocalModelError;
use crate::redacters::LocalRulesError;

/// Failures of the local credential ML redacter. Every variant names what was wrong: the file,
/// the shape, or the value the model produced.
#[derive(Debug, thiserror::Error)]
pub enum LocalCredentialMlError {
    #[error(transparent)]
    Model(#[from] LocalModelError),
    #[error("{path} maps the classes to `{actual}`, expected `0: clean, 1: credential`")]
    ClassLabels { path: String, actual: String },
    #[error("{path} gives the temperature {value}, which is not a positive number")]
    InvalidTemperature { path: String, value: f32 },
    #[error("model produced the score {value}, which is not between 0 and 1")]
    InvalidScore { value: f32 },
    #[error("cannot compile the rules that place a credential on a line: {0}")]
    Rules(#[from] LocalRulesError),
    #[error("cannot compile the patterns that recognise public material: {0}")]
    PublicMaterialPatterns(#[from] regex::Error),
}
