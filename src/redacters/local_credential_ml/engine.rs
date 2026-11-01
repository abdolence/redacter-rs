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
use std::path::Path;
use tokenizers::{Tokenizer, TruncationParams};

/// Tokens of a line the model reads, `[CLS]` and `[SEP]` included. Longer lines are cut: the
/// model was trained on lines of this length and its card sets the same limit.
const MAX_LINE_TOKENS: usize = 128;

/// Temperature of the model's calibration (`policy.json` of the model card): the logits are
/// divided by it before the softmax, which makes the score a probability the default
/// threshold of 0.74 is meaningful for.
const TEMPERATURE: f32 = 2.095_401_8;

/// The loaded classifier and the rules that place its verdicts. `Send + Sync`: rten's
/// `Model::run` and the tokenizer's `encode` take `&self`, so one engine behind an `Arc`
/// serves every file.
pub struct CredentialMlEngine {
    model: Model,
    tokenizer: Tokenizer,
    input_ids: NodeId,
    attention_mask: NodeId,
    logits: NodeId,
    extractor: CredentialExtractor,
}

impl CredentialMlEngine {
    /// Loads `tokenizer.json` and `model.onnx` from the resolved addon directory. Every
    /// missing piece is a typed error naming it.
    pub fn load(files: &AddonFiles) -> Result<Self, LocalCredentialMlError> {
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
            extractor: CredentialExtractor::new()?,
        })
    }

    /// Byte spans of the credentials in `text`, one or more per line the model rates at
    /// `min_score` or more, merged and sorted.
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
        // Class 0 is `clean`, class 1 `credential`; the softmax of two classes is a sigmoid
        // of their difference.
        let credential = 1.0 / (1.0 + ((output[[0, 0]] - output[[0, 1]]) / TEMPERATURE).exp());
        Score::try_from(credential)
            .map_err(|_| LocalCredentialMlError::InvalidScore { value: credential })
    }
}

/// This stops compiling the moment `Model` or `Tokenizer` stops being `Send + Sync`.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<CredentialMlEngine>();
};

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
