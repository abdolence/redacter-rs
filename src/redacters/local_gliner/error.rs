use crate::redacters::local_model::LocalModelError;

/// Failures of the local GLiNER redacter. Every variant names what was wrong: the file, the
/// config field, the shape, or the value the user passed.
#[derive(Debug, thiserror::Error)]
pub enum LocalGlinerError {
    #[error(transparent)]
    Model(#[from] LocalModelError),
    #[error("no labels given to the local-gliner redacter")]
    NoLabels,
    #[error("local-gliner label is empty or all whitespace")]
    EmptyLabel,
    #[error(
        "a label batch prompt of {prompt_tokens} tokens leaves no room for any text word within max_len {max_len}"
    )]
    PromptTooLong {
        prompt_tokens: usize,
        max_len: usize,
    },
}
