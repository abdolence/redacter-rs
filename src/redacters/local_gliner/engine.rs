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
use crate::addon_store::AddonFiles;
use crate::redacters::local_model::{
    graph_node, load_model, load_tokenizer, read_json, special_token, LocalModelError,
};
use crate::redacters::text_spans::{merge_findings, Finding};
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

fn read_config(path: &Path) -> Result<GlinerConfigValues, LocalModelError> {
    let config_error = |reason: String| LocalModelError::config(path, reason);
    let config: serde_json::Value = read_json(path)?;

    let field_usize = |name: &str| -> Result<usize, LocalModelError> {
        config
            .get(name)
            .and_then(|value| value.as_u64())
            .and_then(|value| usize::try_from(value).ok())
            .ok_or_else(|| config_error(format!("no {name} number")))
    };
    let field_str = |name: &str| -> Result<String, LocalModelError> {
        config
            .get(name)
            .and_then(|value| value.as_str())
            .map(str::to_string)
            .ok_or_else(|| config_error(format!("no {name} string")))
    };
    let expect_str = |name: &str, expected: &str| -> Result<(), LocalModelError> {
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
    pub fn load(files: &AddonFiles) -> Result<Self, LocalGlinerError> {
        let config = read_config(&files.dir().join("gliner_config.json"))?;
        let tokenizer = load_tokenizer(&files.dir().join("tokenizer.json"), None)?;
        let cls = special_token(&tokenizer, "[CLS]")?;
        let sep = special_token(&tokenizer, "[SEP]")?;
        let ent = special_token(&tokenizer, &config.ent_token)?;
        let gliner_sep = special_token(&tokenizer, &config.sep_token)?;
        let model = load_model(&files.dir().join("model.onnx"), "GLiNER")?;
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
        let encoding =
            self.tokenizer
                .encode(text, false)
                .map_err(|err| LocalModelError::Encode {
                    reason: err.to_string(),
                })?;
        Ok(encoding.get_ids().to_vec())
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
            LocalModelError::Config { path, reason } => {
                assert_eq!(path, &file.path().display().to_string());
                assert!(reason.contains("max_width"), "{reason}");
            }
            other => panic!("expected Config, got {other}"),
        }

        let missing = Path::new("/nonexistent/gliner_config.json");
        assert!(matches!(
            read_config(missing).unwrap_err(),
            LocalModelError::Config { .. }
        ));
    }

    #[test]
    fn read_config_rejects_an_unsupported_span_mode() {
        let json = REAL_CONFIG.replace("markerV0", "other");
        let file = config_file(&json);
        let err = read_config(file.path()).unwrap_err();
        match &err {
            LocalModelError::Config { reason, .. } => {
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
            LocalModelError::Config { .. }
        ));
    }

    use crate::addon_store::{Addon, AddonStore, AddonStoreOptions};
    use crate::redacters::Score;

    /// The 1.16 GB model is installed with `redacter addons install local-gliner`; a test
    /// never downloads it.
    fn engine() -> GlinerEngine {
        let files = AddonStore::new(&AddonStoreOptions::default())
            .find(Addon::LocalGliner)
            .unwrap();
        GlinerEngine::load(&files).unwrap()
    }

    fn default_options() -> GlinerOptions {
        GlinerOptions {
            labels: super::super::labels::default_labels(),
            min_score: Score::from_static(0.5),
        }
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-local-gliner"), ignore)]
    async fn loads_the_pinned_model_and_scores_a_window() {
        let engine = engine();
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
