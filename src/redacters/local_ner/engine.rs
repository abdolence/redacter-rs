//! The rten and tokenizers side of the local NER redacter: loads the pinned model once and
//! scores windows for the pure pipeline.

use super::error::LocalNerError;
use super::pipeline::{
    collapse_words, merge_spans, tag_tokens, LabelSet, Logits, NerOptions, TokenizedText,
    WindowScorer, OVERLAP,
};
use crate::model_store::ModelFiles;
use crate::redacters::text_spans::{apply_redaction, merge_findings, Finding, REDACTED};
use rten::{Model, NodeId};
use rten_tensor::prelude::*;
use rten_tensor::NdTensor;
use std::collections::BTreeMap;
use std::path::Path;
use tokenizers::Tokenizer;

/// The loaded model, tokenizer and label table. `Send + Sync`: rten's `Model::run` and the
/// tokenizer's `encode` take `&self`, so one engine behind an `Arc` serves every file.
pub struct NerEngine {
    model: Model,
    tokenizer: Tokenizer,
    labels: LabelSet,
    cls: u32,
    sep: u32,
    input_ids: NodeId,
    attention_mask: NodeId,
    logits: NodeId,
    /// Tokens per window without `[CLS]` and `[SEP]`: `max_position_embeddings - 2`.
    body: usize,
}

impl NerEngine {
    /// Loads `config.json`, `tokenizer.json` and `model_uint8.onnx` from the resolved model
    /// directory. Every missing piece is a typed error naming it.
    pub fn load(files: &ModelFiles) -> Result<Self, LocalNerError> {
        let (labels, body) = read_config(&files.path("config.json"))?;
        let tokenizer_path = files.path("tokenizer.json");
        let tokenizer =
            Tokenizer::from_file(&tokenizer_path).map_err(|err| LocalNerError::TokenizerLoad {
                path: tokenizer_path.display().to_string(),
                reason: err.to_string(),
            })?;
        let cls = special_token(&tokenizer, "[CLS]")?;
        let sep = special_token(&tokenizer, "[SEP]")?;
        let model_path = files.path("model_uint8.onnx");
        let model = Model::load_file(&model_path).map_err(|source| LocalNerError::ModelLoad {
            path: model_path.display().to_string(),
            source,
        })?;
        let input_ids = graph_node(&model, "input_ids")?;
        let attention_mask = graph_node(&model, "attention_mask")?;
        let logits =
            model
                .output_ids()
                .first()
                .copied()
                .ok_or_else(|| LocalNerError::MissingGraphNode {
                    name: "<first output>".to_string(),
                })?;
        Ok(Self {
            model,
            tokenizer,
            labels,
            cls,
            sep,
            input_ids,
            attention_mask,
            logits,
            body,
        })
    }

    pub fn labels(&self) -> &LabelSet {
        &self.labels
    }

    /// Byte-offset tokens of `text`, without special tokens.
    pub fn tokenize(&self, text: &str) -> Result<TokenizedText, LocalNerError> {
        let encoding = self
            .tokenizer
            .encode(text, false)
            .map_err(|err| LocalNerError::Encode {
                reason: err.to_string(),
            })?;
        Ok(TokenizedText {
            ids: encoding.get_ids().to_vec(),
            offsets: encoding.get_offsets().to_vec(),
            word_ids: encoding.get_word_ids().to_vec(),
        })
    }

    /// Byte spans of the selected entities, merged and sorted.
    pub fn find(&self, text: &str, options: &NerOptions) -> Result<Vec<Finding>, LocalNerError> {
        let tokens = self.tokenize(text)?;
        if tokens.ids.is_empty() {
            return Ok(Vec::new());
        }
        let tags = tag_tokens(
            &tokens,
            self,
            self.cls,
            self.sep,
            self.body,
            OVERLAP,
            self.labels().len(),
        )?;
        let words = collapse_words(text, &tokens, &tags, self.labels());
        Ok(merge_findings(merge_spans(text, &words, options)))
    }

    /// The text with every finding replaced by `[REDACTED]`, plus the findings.
    pub fn redact_text(
        &self,
        text: &str,
        options: &NerOptions,
    ) -> Result<(String, Vec<Finding>), LocalNerError> {
        let findings = self.find(text, options)?;
        Ok((apply_redaction(text, &findings, REDACTED), findings))
    }
}

/// Task 7 shares one engine across `spawn_blocking` calls behind an `Arc`; this stops
/// compiling the moment `Model` or `Tokenizer` stops being `Send + Sync`.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<NerEngine>();
};

impl WindowScorer for NerEngine {
    fn score_window(&self, ids_with_cls_sep: &[u32]) -> Result<Logits, LocalNerError> {
        let n = ids_with_cls_sep.len();
        let mut input = Vec::with_capacity(n);
        for &id in ids_with_cls_sep {
            input.push(i32::try_from(id).map_err(|_| LocalNerError::Inference {
                reason: format!("token id {id} does not fit the model's int32 input"),
            })?);
        }
        let input = NdTensor::from_data([1, n], input);
        let mask = NdTensor::from_data([1, n], vec![1i32; n]);
        let [output] = self
            .model
            .run_n(
                vec![
                    (self.input_ids, input.view().into()),
                    (self.attention_mask, mask.view().into()),
                ],
                [self.logits],
                None,
            )
            .map_err(|err| LocalNerError::Inference {
                reason: err.to_string(),
            })?;
        let output: NdTensor<f32, 3> =
            output
                .try_into()
                .map_err(|err| LocalNerError::OutputShape {
                    expected: "a rank-3 f32 tensor".to_string(),
                    actual: format!("{err:?}"),
                })?;
        let [batch, rows, cols] = output.shape();
        if batch != 1 {
            return Err(LocalNerError::OutputShape {
                expected: format!("[1, {n}, {}]", self.labels.len()),
                actual: format!("[{batch}, {rows}, {cols}]"),
            });
        }
        Ok(Logits {
            rows,
            cols,
            data: output.to_vec(),
        })
    }
}

/// Reads `id2label` and `max_position_embeddings` from `config.json`.
fn read_config(path: &Path) -> Result<(LabelSet, usize), LocalNerError> {
    let config_error = |reason: String| LocalNerError::Config {
        path: path.display().to_string(),
        reason,
    };
    let bytes = std::fs::read(path).map_err(|err| config_error(err.to_string()))?;
    let config: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|err| config_error(err.to_string()))?;
    let id2label = config
        .get("id2label")
        .and_then(|value| value.as_object())
        .ok_or_else(|| config_error("no id2label object".to_string()))?;
    let mut map = BTreeMap::new();
    for (key, value) in id2label {
        let id: usize = key
            .parse()
            .map_err(|_| config_error(format!("id2label key `{key}` is not a number")))?;
        let label = value
            .as_str()
            .ok_or_else(|| config_error(format!("id2label value for `{key}` is not a string")))?;
        map.insert(id, label.to_string());
    }
    let labels = LabelSet::from_id2label(&map)?;
    let max_positions = config
        .get("max_position_embeddings")
        .and_then(|value| value.as_u64())
        .ok_or_else(|| config_error("no max_position_embeddings number".to_string()))?;
    let body = usize::try_from(max_positions)
        .ok()
        .and_then(|positions| positions.checked_sub(2))
        .filter(|body| *body > 0)
        .ok_or_else(|| {
            config_error(format!(
                "max_position_embeddings {max_positions} leaves no room for tokens"
            ))
        })?;
    Ok((labels, body))
}

fn special_token(tokenizer: &Tokenizer, token: &str) -> Result<u32, LocalNerError> {
    tokenizer
        .token_to_id(token)
        .ok_or_else(|| LocalNerError::MissingSpecialToken {
            token: token.to_string(),
        })
}

fn graph_node(model: &Model, name: &str) -> Result<NodeId, LocalNerError> {
    model
        .find_node(name)
        .ok_or_else(|| LocalNerError::MissingGraphNode {
            name: name.to_string(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model_store::{DownloadModels, ModelId, ModelStore, ModelStoreOptions};
    use crate::redacters::local_ner::pipeline::{Entity, Tag};
    use crate::reporter::AppReporter;
    use console::Term;

    const REAL_CONFIG: &str = r#"{
  "id2label": {"0": "O", "1": "B-DATE", "2": "I-DATE", "3": "B-PER", "4": "I-PER",
               "5": "B-ORG", "6": "I-ORG", "7": "B-LOC", "8": "I-LOC"},
  "max_position_embeddings": 512,
  "model_type": "distilbert"
}"#;

    /// Twelve labels, so the keys sort lexically as 0, 1, 10, 11, 2, ... — a table this size
    /// is only read correctly by parsing the keys as numbers.
    const WIDE_CONFIG: &str = r#"{
  "id2label": {"0": "O", "1": "B-PER", "2": "I-PER", "3": "B-ORG", "4": "I-ORG",
               "5": "B-LOC", "6": "I-LOC", "7": "B-DATE", "8": "I-DATE", "9": "O",
               "10": "B-LOC", "11": "I-PER"},
  "max_position_embeddings": 514
}"#;

    fn config_file(contents: &str) -> tempfile::NamedTempFile {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), contents).unwrap();
        file
    }

    #[test]
    fn read_config_takes_labels_and_window_body_from_the_file() {
        let file = config_file(REAL_CONFIG);
        let (labels, body) = read_config(file.path()).unwrap();
        assert_eq!(labels.len(), 9);
        assert_eq!(body, 510);
        assert_eq!(labels.tag(3), Some(Tag::Begin(Entity::Per)));

        let file = config_file(WIDE_CONFIG);
        let (labels, body) = read_config(file.path()).unwrap();
        assert_eq!(labels.len(), 12);
        assert_eq!(body, 512);
        assert_eq!(labels.tag(10), Some(Tag::Begin(Entity::Loc)));
        assert_eq!(labels.tag(11), Some(Tag::Inside(Entity::Per)));
    }

    #[test]
    fn read_config_names_the_file_and_the_missing_key() {
        let file = config_file(r#"{"max_position_embeddings": 512}"#);
        let err = read_config(file.path()).unwrap_err();
        match &err {
            LocalNerError::Config { path, reason } => {
                assert_eq!(path, &file.path().display().to_string());
                assert!(reason.contains("id2label"), "{reason}");
            }
            other => panic!("expected Config, got {other}"),
        }

        let file = config_file(r#"{"id2label": {"0": "O"}}"#);
        let err = read_config(file.path()).unwrap_err();
        assert!(
            matches!(&err, LocalNerError::Config { reason, .. } if reason.contains("max_position_embeddings")),
            "{err}"
        );

        let file = config_file(r#"{"id2label": {"0": "O"}, "max_position_embeddings": 2}"#);
        assert!(matches!(
            read_config(file.path()).unwrap_err(),
            LocalNerError::Config { .. }
        ));

        let file = config_file(
            r#"{"id2label": {"0": "O", "1": "B-MISC"}, "max_position_embeddings": 512}"#,
        );
        assert!(matches!(
            read_config(file.path()).unwrap_err(),
            LocalNerError::UnknownLabel { .. }
        ));

        let missing = Path::new("/nonexistent/config.json");
        assert!(matches!(
            read_config(missing).unwrap_err(),
            LocalNerError::Config { .. }
        ));
    }

    async fn engine() -> NerEngine {
        let term = Term::stdout();
        let reporter = AppReporter::from(&term);
        let options = ModelStoreOptions {
            download: DownloadModels::Yes,
            models_dir: None,
        };
        let store = ModelStore::new(&options, &reporter).unwrap();
        let files = store.resolve(ModelId::NerMultilingualHrl).await.unwrap();
        NerEngine::load(&files).unwrap()
    }

    fn all_entities() -> NerOptions {
        NerOptions {
            entities: Entity::all(),
            min_score: 0.5,
        }
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-local-ner"), ignore)]
    async fn loads_the_pinned_model_and_scores_a_window() {
        let engine = engine().await;
        assert_eq!(engine.labels().len(), 9);
        let tokens = engine.tokenize("John lives in London.").unwrap();
        let mut ids = vec![engine.cls];
        ids.extend(&tokens.ids);
        ids.push(engine.sep);
        let logits = engine.score_window(&ids).unwrap();
        assert_eq!((logits.rows, logits.cols), (ids.len(), 9));
        assert_eq!(logits.data.len(), ids.len() * 9);
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-local-ner"), ignore)]
    async fn tokenizer_offsets_are_byte_offsets_on_char_boundaries() {
        let engine = engine().await;
        let text = "Zürich liegt in der Schweiz.";
        let tokens = engine.tokenize(text).unwrap();
        assert_eq!(tokens.ids.len(), tokens.offsets.len());
        assert_eq!(tokens.ids.len(), tokens.word_ids.len());
        for &(start, end) in &tokens.offsets {
            assert!(
                text.is_char_boundary(start) && text.is_char_boundary(end),
                "{start}..{end}"
            );
        }
        let (start, end) = tokens.offsets[0];
        assert_eq!(start, 0);
        assert!(
            "Zürich".starts_with(&text[start..end]) && end > 0,
            "{}",
            &text[start..end]
        );
        assert!(engine.tokenize("").unwrap().ids.is_empty());
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-local-ner"), ignore)]
    async fn finds_people_organisations_and_locations() {
        let engine = engine().await;
        let text = "Angela Merkel besuchte Siemens in München.";
        let findings = engine.find(text, &all_entities()).unwrap();
        let found: Vec<(&str, &str)> = findings
            .iter()
            .map(|f| (f.rule.as_str(), &text[f.start..f.end]))
            .collect();
        assert!(found.contains(&("per", "Angela Merkel")), "{found:?}");
        assert!(found.contains(&("org", "Siemens")), "{found:?}");
        assert!(found.contains(&("loc", "München")), "{found:?}");
        let (redacted, _) = engine.redact_text(text, &all_entities()).unwrap();
        assert_eq!(redacted, "[REDACTED] besuchte [REDACTED] in [REDACTED].");

        // The model is multilingual: French and Spanish must work as well as German. On
        // "Emmanuel Macron a visite Renault a Lyon" the model tags Renault `per`, so the
        // French organisation is checked on a sentence where it heads the subject instead.
        let text = "Emmanuel Macron a visité Renault à Lyon.";
        let findings = engine.find(text, &all_entities()).unwrap();
        let found: Vec<(&str, &str)> = findings
            .iter()
            .map(|f| (f.rule.as_str(), &text[f.start..f.end]))
            .collect();
        assert!(found.contains(&("per", "Emmanuel Macron")), "{found:?}");
        assert!(found.contains(&("loc", "Lyon")), "{found:?}");

        let text = "Le siège social de Renault se trouve à Boulogne-Billancourt.";
        let findings = engine.find(text, &all_entities()).unwrap();
        let found: Vec<(&str, &str)> = findings
            .iter()
            .map(|f| (f.rule.as_str(), &text[f.start..f.end]))
            .collect();
        assert!(found.contains(&("org", "Renault")), "{found:?}");
        assert!(
            found.contains(&("loc", "Boulogne-Billancourt")),
            "{found:?}"
        );

        let text = "María García trabaja en Telefónica en Madrid.";
        let findings = engine.find(text, &all_entities()).unwrap();
        let found: Vec<(&str, &str)> = findings
            .iter()
            .map(|f| (f.rule.as_str(), &text[f.start..f.end]))
            .collect();
        assert!(found.contains(&("per", "María García")), "{found:?}");
        assert!(found.contains(&("org", "Telefónica")), "{found:?}");
        assert!(found.contains(&("loc", "Madrid")), "{found:?}");
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-local-ner"), ignore)]
    async fn long_text_runs_several_windows_end_to_end() {
        let engine = engine().await;
        let sentence = "Der Kunde Hans Müller aus Berlin arbeitet bei der Siemens AG. ";
        let text = sentence.repeat(80);
        let tokens = engine.tokenize(&text).unwrap();
        assert!(tokens.ids.len() > 1000, "{}", tokens.ids.len());
        let (redacted, findings) = engine.redact_text(&text, &all_entities()).unwrap();
        assert!(!redacted.contains("Hans Müller"), "a name survived");
        let people = findings.iter().filter(|f| f.rule.as_str() == "per").count();
        assert!(
            people >= 80,
            "{people} people in {} findings",
            findings.len()
        );
    }
}
