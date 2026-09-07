//! Offline named-entity redacter: a multilingual token-classification model on rten finds
//! people, organisations and locations; the pure pipeline turns its logits into byte spans.

mod error;
pub mod pipeline;

pub use error::LocalNerError;
