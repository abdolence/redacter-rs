//! The rten and tokenizers side of the local credential ML redacter: loads the pinned classifier
//! once and rates one line at a time.

use super::error::LocalCredentialMlError;
use super::extraction::CredentialExtractor;
use crate::addon_store::AddonFiles;
use crate::redacters::text_spans::Finding;
use crate::redacters::Score;
use rten::{Model, NodeId};
use rten_tensor::prelude::*;
use rten_tensor::NdTensor;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::Path;
use tokenizers::{Tokenizer, TruncationParams};

/// Tokens of a line the model reads, `[CLS]` and `[SEP]` included. Longer lines are cut: the
/// model was trained on lines of this length and its card sets the same limit.
const MAX_LINE_TOKENS: usize = 128;

/// The labels `config.json` must give the two output classes, in class order.
const CLASS_LABELS: [&str; 2] = ["clean", "credential"];

/// Temperature of the model's calibration (`temperature` in `policy.json`): the logits are
/// divided by it before the softmax, which makes the score a probability the default
/// threshold of 0.74 is meaningful for.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Temperature(f32);

impl TryFrom<f32> for Temperature {
    type Error = f32;

    fn try_from(value: f32) -> Result<Self, f32> {
        if value.is_finite() && value > 0.0 {
            Ok(Self(value))
        } else {
            Err(value)
        }
    }
}

#[derive(Deserialize)]
struct ModelConfig {
    id2label: BTreeMap<String, String>,
}

#[derive(Deserialize)]
struct Policy {
    temperature: f32,
}

/// The loaded classifier and the rules that place its verdicts. `Send + Sync`: rten's
/// `Model::run` and the tokenizer's `encode` take `&self`, so one engine behind an `Arc`
/// serves every file.
pub struct CredentialMlEngine {
    model: Model,
    tokenizer: Tokenizer,
    input_ids: NodeId,
    attention_mask: NodeId,
    logits: NodeId,
    temperature: Temperature,
    extractor: CredentialExtractor,
}

impl CredentialMlEngine {
    /// Loads `config.json`, `policy.json`, `tokenizer.json` and `model.onnx` from the resolved
    /// addon directory. Every missing or unexpected piece is a typed error naming it.
    pub fn load(files: &AddonFiles) -> Result<Self, LocalCredentialMlError> {
        check_class_labels(&files.dir().join("config.json"))?;
        let temperature = read_temperature(&files.dir().join("policy.json"))?;
        let tokenizer = load_tokenizer(&files.dir().join("tokenizer.json"))?;
        let model_path = files.dir().join("model.onnx");
        let model =
            Model::load_file(&model_path).map_err(|source| LocalCredentialMlError::ModelLoad {
                path: model_path.display().to_string(),
                source,
            })?;
        let input_ids = graph_node(&model, "input_ids")?;
        let attention_mask = graph_node(&model, "attention_mask")?;
        let logits = graph_node(&model, "logits")?;
        Ok(Self {
            model,
            tokenizer,
            input_ids,
            attention_mask,
            logits,
            temperature,
            extractor: CredentialExtractor::new()?,
        })
    }

    /// Byte spans of the credentials in `text`: every private key block, and one or more per
    /// line the model rates at `min_score` or more, merged and sorted.
    pub fn find(
        &self,
        text: &str,
        min_score: Score,
    ) -> Result<Vec<Finding>, LocalCredentialMlError> {
        self.extractor
            .find(text, min_score, |line| self.score_line(line))
    }

    /// The model's probability that `line` holds a credential.
    fn score_line(&self, line: &str) -> Result<Score, LocalCredentialMlError> {
        let encoding =
            self.tokenizer
                .encode(line, true)
                .map_err(|err| LocalCredentialMlError::Encode {
                    reason: err.to_string(),
                })?;
        let ids = encoding
            .get_ids()
            .iter()
            .map(|&id| {
                i32::try_from(id).map_err(|_| LocalCredentialMlError::Inference {
                    reason: format!("token id {id} does not fit the model's int32 input"),
                })
            })
            .collect::<Result<Vec<i32>, _>>()?;
        let tokens = ids.len();
        let input = NdTensor::from_data([1, tokens], ids);
        let mask = NdTensor::from_data([1, tokens], vec![1i32; tokens]);
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
            .map_err(|err| LocalCredentialMlError::Inference {
                reason: err.to_string(),
            })?;
        let output: NdTensor<f32, 2> =
            output
                .try_into()
                .map_err(|err| LocalCredentialMlError::OutputShape {
                    expected: "a rank-2 f32 tensor".to_string(),
                    actual: format!("{err:?}"),
                })?;
        let [batch, classes] = output.shape();
        if batch != 1 || classes != 2 {
            return Err(LocalCredentialMlError::OutputShape {
                expected: "[1, 2]".to_string(),
                actual: format!("[{batch}, {classes}]"),
            });
        }
        // Class 0 is `clean`, class 1 `credential` (checked against `config.json` at load);
        // the softmax of two classes is a sigmoid of their difference.
        let credential =
            1.0 / (1.0 + ((output[[0, 0]] - output[[0, 1]]) / self.temperature.0).exp());
        Score::try_from(credential)
            .map_err(|_| LocalCredentialMlError::InvalidScore { value: credential })
    }
}

/// This stops compiling the moment `Model` or `Tokenizer` stops being `Send + Sync`.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<CredentialMlEngine>();
};

fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T, LocalCredentialMlError> {
    let config_error = |reason: String| LocalCredentialMlError::Config {
        path: path.display().to_string(),
        reason,
    };
    let bytes = std::fs::read(path).map_err(|err| config_error(err.to_string()))?;
    serde_json::from_slice(&bytes).map_err(|err| config_error(err.to_string()))
}

/// Checks that `config.json` names class 0 `clean` and class 1 `credential`, the order the
/// score is computed in.
fn check_class_labels(path: &Path) -> Result<(), LocalCredentialMlError> {
    let config: ModelConfig = read_json(path)?;
    let labels: Vec<&str> = config.id2label.values().map(String::as_str).collect();
    let expected_keys = (0..CLASS_LABELS.len()).map(|class| class.to_string());
    if labels == CLASS_LABELS && config.id2label.keys().cloned().eq(expected_keys) {
        return Ok(());
    }
    Err(LocalCredentialMlError::ClassLabels {
        path: path.display().to_string(),
        actual: config
            .id2label
            .iter()
            .map(|(class, label)| format!("{class}: {label}"))
            .collect::<Vec<_>>()
            .join(", "),
    })
}

fn read_temperature(path: &Path) -> Result<Temperature, LocalCredentialMlError> {
    let policy: Policy = read_json(path)?;
    Temperature::try_from(policy.temperature).map_err(|value| {
        LocalCredentialMlError::InvalidTemperature {
            path: path.display().to_string(),
            value,
        }
    })
}

fn load_tokenizer(path: &Path) -> Result<Tokenizer, LocalCredentialMlError> {
    let load_error = |reason: String| LocalCredentialMlError::TokenizerLoad {
        path: path.display().to_string(),
        reason,
    };
    let mut tokenizer = Tokenizer::from_file(path).map_err(|err| load_error(err.to_string()))?;
    tokenizer
        .with_truncation(Some(TruncationParams {
            max_length: MAX_LINE_TOKENS,
            ..TruncationParams::default()
        }))
        .map_err(|err| load_error(err.to_string()))?;
    tokenizer.with_padding(None);
    Ok(tokenizer)
}

fn graph_node(model: &Model, name: &str) -> Result<NodeId, LocalCredentialMlError> {
    model
        .find_node(name)
        .ok_or_else(|| LocalCredentialMlError::MissingGraphNode {
            name: name.to_string(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn json_file(content: &str) -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(content.as_bytes()).unwrap();
        file
    }

    #[test]
    fn the_class_order_of_the_model_card_is_accepted() {
        let file = json_file(r#"{"id2label": {"0": "clean", "1": "credential"}}"#);
        check_class_labels(file.path()).unwrap();
    }

    #[test]
    fn swapped_or_shifted_classes_are_rejected() {
        for config in [
            r#"{"id2label": {"0": "credential", "1": "clean"}}"#,
            r#"{"id2label": {"1": "clean", "2": "credential"}}"#,
        ] {
            let file = json_file(config);
            assert!(
                matches!(
                    check_class_labels(file.path()),
                    Err(LocalCredentialMlError::ClassLabels { .. })
                ),
                "{config}"
            );
        }
    }

    #[test]
    fn a_config_without_labels_or_a_missing_file_is_a_config_error() {
        let file = json_file("{}");
        assert!(matches!(
            check_class_labels(file.path()),
            Err(LocalCredentialMlError::Config { .. })
        ));
        assert!(matches!(
            check_class_labels(Path::new("/nonexistent/config.json")),
            Err(LocalCredentialMlError::Config { .. })
        ));
    }

    #[test]
    fn the_temperature_comes_from_the_policy_file() {
        let file = json_file(r#"{"temperature": 2.0954017693748623, "threshold": 0.74}"#);
        assert_eq!(
            read_temperature(file.path()).unwrap(),
            Temperature(2.095_401_8)
        );
    }

    #[test]
    fn a_temperature_the_scores_cannot_use_is_rejected() {
        for policy in [
            r#"{"temperature": 0}"#,
            r#"{"temperature": -1.5}"#,
            r#"{"temperature": 1e-50}"#,
            r#"{"temperature": 1e300}"#,
        ] {
            let file = json_file(policy);
            assert!(
                matches!(
                    read_temperature(file.path()),
                    Err(LocalCredentialMlError::InvalidTemperature { .. })
                ),
                "{policy}"
            );
        }
    }
}
