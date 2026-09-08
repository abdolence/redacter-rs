/// Failures of the local GLiNER redacter. Every variant names what was wrong: the file, the
/// config field, the graph node, the token, the shape, or the value the user passed.
#[derive(Debug, thiserror::Error)]
pub enum LocalGlinerError {
    #[error("cannot load GLiNER model {path}: {source}")]
    ModelLoad {
        path: String,
        #[source]
        source: rten::LoadError,
    },
    #[error("cannot load tokenizer {path}: {reason}")]
    TokenizerLoad { path: String, reason: String },
    #[error("model config {path}: {reason}")]
    Config { path: String, reason: String },
    #[error("model graph has no node named `{name}`")]
    MissingGraphNode { name: String },
    #[error("tokenizer has no `{token}` token")]
    MissingSpecialToken { token: String },
    #[error("cannot tokenize `{text}`: {reason}")]
    Encode { text: String, reason: String },
    #[error("model inference failed: {reason}")]
    Inference { reason: String },
    #[error("model output has shape {actual}, expected {expected}")]
    OutputShape { expected: String, actual: String },
    #[error("inference task failed: {reason}")]
    InferenceTask { reason: String },
    #[error("no labels given to the local-gliner redacter")]
    NoLabels,
    #[error("local-gliner label is empty or all whitespace")]
    EmptyLabel,
    #[error("local-gliner minimum score {value} is outside 0.0..=1.0")]
    InvalidMinScore { value: f32 },
    #[error(
        "a label batch prompt of {prompt_tokens} tokens leaves no room for any text word within max_len {max_len}"
    )]
    PromptTooLong {
        prompt_tokens: usize,
        max_len: usize,
    },
}
