//! The rten and tokenizers side of the local GLiNER redacter: loads the pinned model once and
//! scores windows for the pure pipeline.
//!
//! rten's ONNX loader downcasts every `int64`/`bool` graph input to `int32`, so every tensor
//! built here is `i32` rather than the `int64`/`bool` the exported graph declares.

use super::error::LocalGlinerError;
use super::labels::batch_labels;
use super::pipeline::{
    candidates_to_findings, greedy_decode, score_document, GlinerOptions, Logits, WindowInputs,
    WindowScorer, OVERLAP_WORDS,
};
use crate::model_store::ModelFiles;
use crate::redacters::text_spans::{apply_redaction, merge_findings, Finding, REDACTED};
use rten::{Model, NodeId};
use rten_tensor::prelude::*;
use rten_tensor::NdTensor;
use std::path::Path;
use tokenizers::Tokenizer;

/// The fields of `gliner_config.json` the pipeline needs. The three string fields are
/// validated rather than merely read: they name the pre/post-processing this engine
/// implements, and a config file that disagrees would silently score with the wrong shapes.
#[derive(Debug)]
struct GlinerConfigValues {
    max_len: usize,
    max_width: usize,
    max_types: usize,
    ent_token: String,
    sep_token: String,
}

fn read_config(path: &Path) -> Result<GlinerConfigValues, LocalGlinerError> {
    let config_error = |reason: String| LocalGlinerError::Config {
        path: path.display().to_string(),
        reason,
    };
    let bytes = std::fs::read(path).map_err(|err| config_error(err.to_string()))?;
    let config: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|err| config_error(err.to_string()))?;

    let field_usize = |name: &str| -> Result<usize, LocalGlinerError> {
        config
            .get(name)
            .and_then(|value| value.as_u64())
            .and_then(|value| usize::try_from(value).ok())
            .ok_or_else(|| config_error(format!("no {name} number")))
    };
    let field_str = |name: &str| -> Result<String, LocalGlinerError> {
        config
            .get(name)
            .and_then(|value| value.as_str())
            .map(str::to_string)
            .ok_or_else(|| config_error(format!("no {name} string")))
    };
    let expect_str = |name: &str, expected: &str| -> Result<(), LocalGlinerError> {
        let actual = field_str(name)?;
        if actual != expected {
            return Err(config_error(format!(
                "{name} is `{actual}`, this engine only implements `{expected}`"
            )));
        }
        Ok(())
    };

    let max_len = field_usize("max_len")?;
    let max_width = field_usize("max_width")?;
    let max_types = field_usize("max_types")?;
    let ent_token = field_str("ent_token")?;
    let sep_token = field_str("sep_token")?;
    expect_str("span_mode", "markerV0")?;
    expect_str("subtoken_pooling", "first")?;
    expect_str("words_splitter_type", "whitespace")?;

    if max_len <= 2 {
        return Err(config_error(format!(
            "max_len {max_len} leaves no room for a single text word"
        )));
    }
    if max_width == 0 {
        return Err(config_error("max_width is 0".to_string()));
    }
    if max_types == 0 {
        return Err(config_error("max_types is 0".to_string()));
    }

    Ok(GlinerConfigValues {
        max_len,
        max_width,
        max_types,
        ent_token,
        sep_token,
    })
}

/// Loads `tokenizer.json` with truncation and padding turned off. The pipeline windows the
/// word stream itself and needs every word: a `truncation` block left in the file would cut
/// each text at one window and copy the rest of it unredacted.
fn load_tokenizer(path: &Path) -> Result<Tokenizer, LocalGlinerError> {
    let load_error = |reason: String| LocalGlinerError::TokenizerLoad {
        path: path.display().to_string(),
        reason,
    };
    let mut tokenizer = Tokenizer::from_file(path).map_err(|err| load_error(err.to_string()))?;
    tokenizer
        .with_truncation(None)
        .map_err(|err| load_error(err.to_string()))?;
    tokenizer.with_padding(None);
    Ok(tokenizer)
}

fn special_token(tokenizer: &Tokenizer, token: &str) -> Result<u32, LocalGlinerError> {
    tokenizer
        .token_to_id(token)
        .ok_or_else(|| LocalGlinerError::MissingSpecialToken {
            token: token.to_string(),
        })
}

fn graph_node(model: &Model, name: &str) -> Result<NodeId, LocalGlinerError> {
    model
        .find_node(name)
        .ok_or_else(|| LocalGlinerError::MissingGraphNode {
            name: name.to_string(),
        })
}

/// The loaded model, tokenizer and config. `Send + Sync`: rten's `Model::run` and the
/// tokenizer's `encode` take `&self`, so one engine behind an `Arc` serves every file.
pub struct GlinerEngine {
    model: Model,
    tokenizer: Tokenizer,
    cls: u32,
    sep: u32,
    ent: u32,
    gliner_sep: u32,
    max_len: usize,
    max_width: usize,
    max_types: usize,
    input_ids: NodeId,
    attention_mask: NodeId,
    words_mask: NodeId,
    text_lengths: NodeId,
    span_idx: NodeId,
    span_mask: NodeId,
    logits: NodeId,
}

impl GlinerEngine {
    /// Loads `gliner_config.json`, `tokenizer.json` and `model.onnx` from the resolved model
    /// directory. Every missing or unexpected piece is a typed error naming it.
    pub fn load(files: &ModelFiles) -> Result<Self, LocalGlinerError> {
        let config = read_config(&files.path("gliner_config.json"))?;
        let tokenizer = load_tokenizer(&files.path("tokenizer.json"))?;
        let cls = special_token(&tokenizer, "[CLS]")?;
        let sep = special_token(&tokenizer, "[SEP]")?;
        let ent = special_token(&tokenizer, &config.ent_token)?;
        let gliner_sep = special_token(&tokenizer, &config.sep_token)?;
        let model_path = files.path("model.onnx");
        let model =
            Model::load_file(&model_path).map_err(|source| LocalGlinerError::ModelLoad {
                path: model_path.display().to_string(),
                source,
            })?;
        Ok(Self {
            input_ids: graph_node(&model, "input_ids")?,
            attention_mask: graph_node(&model, "attention_mask")?,
            words_mask: graph_node(&model, "words_mask")?,
            text_lengths: graph_node(&model, "text_lengths")?,
            span_idx: graph_node(&model, "span_idx")?,
            span_mask: graph_node(&model, "span_mask")?,
            logits: graph_node(&model, "logits")?,
            model,
            tokenizer,
            cls,
            sep,
            ent,
            gliner_sep,
            max_len: config.max_len,
            max_width: config.max_width,
            max_types: config.max_types,
        })
    }

    /// Subtoken ids for one word or label string, without special tokens. `is_split_into_words`
    /// in the Python processor is equivalent to encoding each pre-split word on its own, since
    /// the tokenizer's Metaspace pre-tokenizer is configured with `prepend_scheme: always`.
    fn subtoken_ids(&self, text: &str) -> Result<Vec<u32>, LocalGlinerError> {
        self.tokenizer
            .encode(text, false)
            .map(|encoding| encoding.get_ids().to_vec())
            .map_err(|err| LocalGlinerError::Encode {
                text: text.to_string(),
                reason: err.to_string(),
            })
    }

    /// Byte spans of the selected labels, merged and sorted.
    pub fn find(
        &self,
        text: &str,
        options: &GlinerOptions,
    ) -> Result<Vec<Finding>, LocalGlinerError> {
        let words = super::pipeline::split_words(text);
        if words.is_empty() {
            return Ok(Vec::new());
        }
        let word_ids: Vec<Vec<u32>> = words
            .iter()
            .map(|word| self.subtoken_ids(word.text))
            .collect::<Result<_, _>>()?;

        let label_batches = batch_labels(&options.labels, self.max_types);
        let mut label_ids: Vec<Vec<Vec<u32>>> = Vec::with_capacity(label_batches.len());
        for batch in &label_batches {
            let mut ids = Vec::with_capacity(batch.len());
            for label in *batch {
                ids.push(self.subtoken_ids(label)?);
            }
            label_ids.push(ids);
        }

        let candidates = score_document(
            self,
            self.cls,
            self.sep,
            self.ent,
            self.gliner_sep,
            &words,
            &word_ids,
            &label_batches,
            &label_ids,
            self.max_len,
            self.max_width,
            OVERLAP_WORDS,
            options.min_score,
        )?;
        Ok(merge_findings(candidates_to_findings(greedy_decode(
            candidates,
        ))))
    }

    /// The text with every finding replaced by `[REDACTED]`, plus the findings.
    pub fn redact_text(
        &self,
        text: &str,
        options: &GlinerOptions,
    ) -> Result<(String, Vec<Finding>), LocalGlinerError> {
        let findings = self.find(text, options)?;
        Ok((apply_redaction(text, &findings, REDACTED), findings))
    }
}

/// `local_gliner::mod::LocalGlinerRedacter` shares one engine across `spawn_blocking` calls
/// behind an `Arc`; this stops compiling the moment `Model` or `Tokenizer` stops being
/// `Send + Sync`.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<GlinerEngine>();
};

impl WindowScorer for GlinerEngine {
    fn score_window(
        &self,
        inputs: &WindowInputs,
        num_labels: usize,
    ) -> Result<Logits, LocalGlinerError> {
        let n = inputs.ids.len();
        let num_spans = inputs.span_idx.len();
        let ids = NdTensor::from_data([1, n], inputs.ids.clone());
        let attention = NdTensor::from_data([1, n], vec![1i32; n]);
        let words_mask = NdTensor::from_data([1, n], inputs.words_mask.clone());
        let text_lengths = NdTensor::from_data([1, 1], vec![inputs.num_words as i32]);
        let span_idx_flat: Vec<i32> = inputs.span_idx.iter().flat_map(|pair| *pair).collect();
        let span_idx = NdTensor::from_data([1, num_spans, 2], span_idx_flat);
        let span_mask = NdTensor::from_data([1, num_spans], inputs.span_mask.clone());

        let [output] = self
            .model
            .run_n(
                vec![
                    (self.input_ids, ids.view().into()),
                    (self.attention_mask, attention.view().into()),
                    (self.words_mask, words_mask.view().into()),
                    (self.text_lengths, text_lengths.view().into()),
                    (self.span_idx, span_idx.view().into()),
                    (self.span_mask, span_mask.view().into()),
                ],
                [self.logits],
                None,
            )
            .map_err(|err| LocalGlinerError::Inference {
                reason: err.to_string(),
            })?;

        let output: NdTensor<f32, 4> =
            output
                .try_into()
                .map_err(|err| LocalGlinerError::OutputShape {
                    expected: "a rank-4 f32 tensor".to_string(),
                    actual: format!("{err:?}"),
                })?;
        let [batch, num_words, max_width, out_labels] = output.shape();
        if batch != 1 || num_words != inputs.num_words || out_labels != num_labels {
            return Err(LocalGlinerError::OutputShape {
                expected: format!("[1, {}, _, {num_labels}]", inputs.num_words),
                actual: format!("[{batch}, {num_words}, {max_width}, {out_labels}]"),
            });
        }
        Ok(Logits {
            num_words,
            max_width,
            num_labels: out_labels,
            data: output.to_vec(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const REAL_CONFIG: &str = r#"{
  "class_token_index": 250103,
  "max_len": 384,
  "max_width": 12,
  "max_types": 25,
  "ent_token": "<<ENT>>",
  "sep_token": "<<SEP>>",
  "span_mode": "markerV0",
  "subtoken_pooling": "first",
  "words_splitter_type": "whitespace"
}"#;

    fn config_file(contents: &str) -> tempfile::NamedTempFile {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), contents).unwrap();
        file
    }

    #[test]
    fn read_config_takes_the_shape_fields_from_the_real_file() {
        let file = config_file(REAL_CONFIG);
        let config = read_config(file.path()).unwrap();
        assert_eq!(config.max_len, 384);
        assert_eq!(config.max_width, 12);
        assert_eq!(config.max_types, 25);
        assert_eq!(config.ent_token, "<<ENT>>");
        assert_eq!(config.sep_token, "<<SEP>>");
    }

    #[test]
    fn read_config_names_the_file_and_the_missing_key() {
        let file = config_file(r#"{"max_len": 384}"#);
        let err = read_config(file.path()).unwrap_err();
        match &err {
            LocalGlinerError::Config { path, reason } => {
                assert_eq!(path, &file.path().display().to_string());
                assert!(reason.contains("max_width"), "{reason}");
            }
            other => panic!("expected Config, got {other}"),
        }

        let missing = Path::new("/nonexistent/gliner_config.json");
        assert!(matches!(
            read_config(missing).unwrap_err(),
            LocalGlinerError::Config { .. }
        ));
    }

    #[test]
    fn read_config_rejects_an_unsupported_span_mode() {
        let json = REAL_CONFIG.replace("markerV0", "other");
        let file = config_file(&json);
        let err = read_config(file.path()).unwrap_err();
        match &err {
            LocalGlinerError::Config { reason, .. } => {
                assert!(reason.contains("span_mode"), "{reason}");
                assert!(reason.contains("markerV0"), "{reason}");
            }
            other => panic!("expected Config, got {other}"),
        }
    }

    #[test]
    fn read_config_rejects_a_zero_shape_field() {
        let json = REAL_CONFIG.replace("\"max_width\": 12", "\"max_width\": 0");
        let file = config_file(&json);
        assert!(matches!(
            read_config(file.path()).unwrap_err(),
            LocalGlinerError::Config { .. }
        ));
    }

    /// A tokenizer.json carrying a `truncation` block, the shape a legacy or hand-placed copy
    /// can still have: one word per token, and every text cut at 8 tokens unless the loader
    /// turns truncation off.
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

    #[test]
    fn the_loaded_tokenizer_never_truncates_a_long_text() {
        let file = truncating_tokenizer_file();
        let tokenizer = load_tokenizer(file.path()).unwrap();
        let text: Vec<String> = (0..30).map(|id| format!("w{id}")).collect();
        let text = text.join(" ");
        let encoding = tokenizer.encode(text.as_str(), false).unwrap();
        assert_eq!(
            encoding.get_ids().len(),
            30,
            "the truncation block in the file must be turned off at load"
        );
    }

    #[test]
    fn a_missing_tokenizer_file_names_itself() {
        let err = load_tokenizer(Path::new("/nonexistent/tokenizer.json")).unwrap_err();
        match &err {
            LocalGlinerError::TokenizerLoad { path, .. } => {
                assert_eq!(path, "/nonexistent/tokenizer.json")
            }
            other => panic!("expected TokenizerLoad, got {other}"),
        }
    }

    use crate::model_store::{DownloadModels, ModelId, ModelStore, ModelStoreOptions};
    use crate::reporter::AppReporter;
    use console::Term;

    /// `DownloadModels::No`: the 1.16 GB model is placed in the cache by hand for these tests
    /// and must never be fetched over the network by a test run.
    async fn engine() -> GlinerEngine {
        let term = Term::stdout();
        let reporter = AppReporter::from(&term);
        let options = ModelStoreOptions {
            download: DownloadModels::No,
            models_dir: None,
        };
        let store = ModelStore::new(&options, &reporter);
        let files = store.resolve(ModelId::GlinerMultiPii).await.unwrap();
        GlinerEngine::load(&files).unwrap()
    }

    fn default_options() -> GlinerOptions {
        GlinerOptions {
            labels: super::super::labels::default_labels(),
            min_score: 0.5,
        }
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-local-gliner"), ignore)]
    async fn loads_the_pinned_model_and_scores_a_window() {
        let engine = engine().await;
        assert_eq!(engine.max_width, 12);
        assert_eq!(engine.max_types, 25);
        let findings = engine
            .find("Alice Smith called from London.", &default_options())
            .unwrap();
        assert!(
            findings.iter().any(|f| f.rule.as_str() == "person"),
            "{findings:?}"
        );
    }
}
