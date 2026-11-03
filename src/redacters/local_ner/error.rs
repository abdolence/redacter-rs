use crate::redacters::local_model::LocalModelError;

/// Failures of the local NER redacter. Every variant names what was wrong: the file, the
/// label, the shape, or the value the user passed.
#[derive(Debug, thiserror::Error)]
pub enum LocalNerError {
    #[error(transparent)]
    Model(#[from] LocalModelError),
    #[error(
        "model config has an unsupported label `{label}`; expected O or B-/I- PER, ORG, LOC, DATE"
    )]
    UnknownLabel { label: String },
    #[error("model config has an invalid id2label table: {reason}")]
    InvalidLabels { reason: String },
    #[error("no entity types selected for the local-ner redacter; valid values: per, org, loc")]
    NoEntities,
}
