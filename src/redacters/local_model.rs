//! Loading and running the rten models behind the local redacters.

use crate::AppResult;
use rten::{Model, NodeId};
use serde::de::DeserializeOwned;
use std::fmt::Display;
use std::path::Path;
use std::sync::Arc;
use tokenizers::{Tokenizer, TruncationParams};

/// Failures of loading a model and its files, shared by the local redacters. Every variant
/// names what was wrong: the file, the graph node or the token.
#[derive(Debug, thiserror::Error)]
pub enum LocalModelError {
    #[error("cannot load {model} model {path}: {source}")]
    ModelLoad {
        model: &'static str,
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
    #[error("cannot tokenize the text: {reason}")]
    Encode { reason: String },
}

impl LocalModelError {
    pub fn config(path: &Path, reason: impl Display) -> Self {
        Self::Config {
            path: path.display().to_string(),
            reason: reason.to_string(),
        }
    }
}

/// Reads and parses a JSON file next to the model.
pub fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T, LocalModelError> {
    let bytes = std::fs::read(path).map_err(|err| LocalModelError::config(path, err))?;
    serde_json::from_slice(&bytes).map_err(|err| LocalModelError::config(path, err))
}

/// Loads an ONNX model; `model` names it in the error.
pub fn load_model(path: &Path, model: &'static str) -> Result<Model, LocalModelError> {
    Model::load_file(path).map_err(|source| LocalModelError::ModelLoad {
        model,
        path: path.display().to_string(),
        source,
    })
}

/// Loads `tokenizer.json` with padding off and `truncation` as given, whatever the file says.
/// A redacter that windows the token stream itself passes `None`: a `truncation` block left
/// in the file (the copies next to the binary and the legacy directories are checked against
/// nothing) would cut each text at one window and copy the rest of it unredacted.
pub fn load_tokenizer(
    path: &Path,
    truncation: Option<TruncationParams>,
) -> Result<Tokenizer, LocalModelError> {
    let load_error = |reason: String| LocalModelError::TokenizerLoad {
        path: path.display().to_string(),
        reason,
    };
    let mut tokenizer = Tokenizer::from_file(path).map_err(|err| load_error(err.to_string()))?;
    tokenizer
        .with_truncation(truncation)
        .map_err(|err| load_error(err.to_string()))?;
    tokenizer.with_padding(None);
    Ok(tokenizer)
}

#[cfg(any(feature = "local-ner", feature = "local-gliner"))]
pub fn special_token(tokenizer: &Tokenizer, token: &str) -> Result<u32, LocalModelError> {
    tokenizer
        .token_to_id(token)
        .ok_or_else(|| LocalModelError::MissingSpecialToken {
            token: token.to_string(),
        })
}

pub fn graph_node(model: &Model, name: &str) -> Result<NodeId, LocalModelError> {
    model
        .find_node(name)
        .ok_or_else(|| LocalModelError::MissingGraphNode {
            name: name.to_string(),
        })
}

/// Runs `work` on the blocking pool with shared handles to the engine and its options, so
/// inference does not stall the async pipeline.
pub async fn run_blocking<E, O, T>(
    engine: &Arc<E>,
    options: &O,
    work: impl FnOnce(&E, &O) -> AppResult<T> + Send + 'static,
) -> AppResult<T>
where
    E: Send + Sync + 'static,
    O: Clone + Send + 'static,
    T: Send + 'static,
{
    let engine = Arc::clone(engine);
    let options = options.clone();
    tokio::task::spawn_blocking(move || work(&engine, &options)).await?
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A tokenizer.json carrying a `truncation` block, the shape a legacy or hand-placed
    /// copy can still have: one word per token, and every text cut at 8 tokens unless the
    /// loader turns truncation off.
    fn truncating_tokenizer_file() -> tempfile::NamedTempFile {
        let vocab: String = (0..40)
            .map(|id| format!("\"w{id}\": {id}"))
            .collect::<Vec<_>>()
            .join(", ");
        let json = format!(
            r#"{{
  "version": "1.0",
  "truncation": {{"direction": "Right", "max_length": 8, "strategy": "LongestFirst", "stride": 0}},
  "padding": null,
  "added_tokens": [],
  "normalizer": null,
  "pre_tokenizer": {{"type": "Whitespace"}},
  "post_processor": null,
  "decoder": null,
  "model": {{"type": "WordLevel", "vocab": {{{vocab}, "[UNK]": 40}}, "unk_token": "[UNK]"}}
}}"#
        );
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), json).unwrap();
        file
    }

    fn encoded_length(tokenizer: &Tokenizer) -> usize {
        let text: Vec<String> = (0..30).map(|id| format!("w{id}")).collect();
        let encoding = tokenizer.encode(text.join(" ").as_str(), false).unwrap();
        encoding.get_ids().len()
    }

    #[test]
    fn a_tokenizer_loaded_without_truncation_never_truncates_a_long_text() {
        let file = truncating_tokenizer_file();
        let tokenizer = load_tokenizer(file.path(), None).unwrap();
        assert_eq!(encoded_length(&tokenizer), 30);
    }

    #[test]
    fn a_tokenizer_loaded_with_truncation_cuts_at_the_given_length() {
        let file = truncating_tokenizer_file();
        let truncation = TruncationParams {
            max_length: 12,
            ..TruncationParams::default()
        };
        let tokenizer = load_tokenizer(file.path(), Some(truncation)).unwrap();
        assert_eq!(encoded_length(&tokenizer), 12);
    }

    #[test]
    fn a_missing_tokenizer_file_names_itself() {
        let err = load_tokenizer(Path::new("/nonexistent/tokenizer.json"), None).unwrap_err();
        match &err {
            LocalModelError::TokenizerLoad { path, .. } => {
                assert_eq!(path, "/nonexistent/tokenizer.json")
            }
            other => panic!("expected TokenizerLoad, got {other}"),
        }
    }
}
