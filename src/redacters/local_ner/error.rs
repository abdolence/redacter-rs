/// Failures of the local NER redacter. Every variant names what was wrong: the file, the
/// label, the graph node, the token, the shape, or the value the user passed.
#[derive(Debug, thiserror::Error)]
pub enum LocalNerError {
    #[error("cannot load NER model {path}: {source}")]
    ModelLoad {
        path: String,
        #[source]
        source: rten::LoadError,
    },
    #[error("cannot load tokenizer {path}: {reason}")]
    TokenizerLoad { path: String, reason: String },
    #[error("model config {path}: {reason}")]
    Config { path: String, reason: String },
    #[error(
        "model config has an unsupported label `{label}`; expected O or B-/I- PER, ORG, LOC, DATE"
    )]
    UnknownLabel { label: String },
    #[error("model config has an invalid id2label table: {reason}")]
    InvalidLabels { reason: String },
    #[error("model graph has no node named `{name}`")]
    MissingGraphNode { name: String },
    #[error("tokenizer has no `{token}` token")]
    MissingSpecialToken { token: String },
    #[error("cannot tokenize the text: {reason}")]
    Encode { reason: String },
    #[error("model inference failed: {reason}")]
    Inference { reason: String },
    #[error("model output has shape {actual}, expected {expected}")]
    OutputShape { expected: String, actual: String },
    #[error("inference task failed: {reason}")]
    InferenceTask { reason: String },
    #[error("no entity types selected for the local-ner redacter; valid values: per, org, loc")]
    NoEntities,
    #[error("local-ner minimum score {value} is outside 0.0..=1.0")]
    InvalidMinScore { value: f32 },
}
