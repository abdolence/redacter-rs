use crate::redacters::LocalRulesError;

/// Failures of the local credential ML redacter. Every variant names what was wrong: the file, the
/// graph node, the shape, or the value the model produced.
#[derive(Debug, thiserror::Error)]
pub enum LocalCredentialMlError {
    #[error("cannot load credential model {path}: {source}")]
    ModelLoad {
        path: String,
        #[source]
        source: rten::LoadError,
    },
    #[error("cannot read model file {path}: {reason}")]
    Config { path: String, reason: String },
    #[error("{path} maps the classes to `{actual}`, expected `0: clean, 1: credential`")]
    ClassLabels { path: String, actual: String },
    #[error("{path} gives the temperature {value}, which is not a positive number")]
    Temperature { path: String, value: f64 },
    #[error("cannot load tokenizer {path}: {reason}")]
    TokenizerLoad { path: String, reason: String },
    #[error("model graph has no node named `{name}`")]
    MissingGraphNode { name: String },
    #[error("cannot tokenize a line: {reason}")]
    Encode { reason: String },
    #[error("model inference failed: {reason}")]
    Inference { reason: String },
    #[error("model output has shape {actual}, expected {expected}")]
    OutputShape { expected: String, actual: String },
    #[error("model produced the score {value}, which is not between 0 and 1")]
    InvalidScore { value: f32 },
    #[error("inference task failed: {reason}")]
    InferenceTask { reason: String },
    #[error("cannot compile the rules that place a credential on a line: {0}")]
    Rules(#[from] LocalRulesError),
}
