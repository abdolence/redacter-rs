//! How a model's answer ended. An answer that was filtered, cut short or empty is an error
//! for the file it belongs to: read as text it would be a clean scan or an empty redacted
//! file.

use crate::errors::AppError;
use crate::AppResult;
use std::fmt::{Display, Formatter};
use std::future::Future;

/// Why a model did not complete its answer. It names the provider's finish reason and the
/// blocked categories, never any content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelStop {
    /// A safety filter stopped the answer. Whether the answer is blocked varies from one
    /// attempt to the next, so it is retried.
    Safety { categories: Vec<String> },
    /// The provider gave no reason and stopped; retried.
    Other,
    /// The answer reached the output limit and is cut short.
    MaxTokens,
    /// The provider withheld the answer for a reason another attempt does not change, named
    /// as the provider names it.
    Withheld(String),
    /// The provider rejected the prompt and produced no answer, named by its block reason.
    PromptBlocked(String),
}

impl ModelStop {
    /// The stop of a Gemini API candidate, or `None` when it finished normally.
    /// `finish_reason` is the API's own name for it, which both Gemini endpoints share.
    pub fn from_gemini(finish_reason: &str, blocked_categories: Vec<String>) -> Option<Self> {
        match finish_reason {
            "STOP" | "FINISH_REASON_UNSPECIFIED" => None,
            "SAFETY" => Some(Self::Safety {
                categories: blocked_categories,
            }),
            "OTHER" => Some(Self::Other),
            "MAX_TOKENS" => Some(Self::MaxTokens),
            other => Some(Self::Withheld(other.to_string())),
        }
    }

    fn is_transient(&self) -> bool {
        matches!(self, Self::Safety { .. } | Self::Other)
    }
}

impl Display for ModelStop {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Safety { categories } if categories.is_empty() => f.write_str("SAFETY"),
            Self::Safety { categories } => {
                write!(f, "SAFETY (blocked: {})", categories.join(", "))
            }
            Self::Other => f.write_str("OTHER"),
            Self::MaxTokens => f.write_str("the output limit was reached"),
            Self::Withheld(reason) => f.write_str(reason),
            Self::PromptBlocked(reason) => write!(f, "the prompt was blocked ({reason})"),
        }
    }
}

/// What a Gemini API candidate carried, as the two Gemini endpoints report it.
pub struct GeminiCandidate<'a> {
    pub finish_reason: &'a str,
    pub blocked_categories: Vec<String>,
    pub text: String,
}

/// The text a model returned and how it ended.
#[derive(Debug)]
pub struct ModelAnswer {
    text: String,
    stop: Option<ModelStop>,
}

impl ModelAnswer {
    pub fn new(text: String, stop: Option<ModelStop>) -> Self {
        Self { text, stop }
    }

    /// The first candidate of a Gemini API response, or when there is none, the reason the
    /// prompt was blocked.
    pub fn from_gemini(
        candidate: Option<GeminiCandidate<'_>>,
        prompt_block_reason: Option<&str>,
    ) -> Self {
        match (candidate, prompt_block_reason) {
            (Some(candidate), _) => Self::new(
                candidate.text,
                ModelStop::from_gemini(candidate.finish_reason, candidate.blocked_categories),
            ),
            (None, Some(reason)) => Self::new(
                String::new(),
                Some(ModelStop::PromptBlocked(reason.to_string())),
            ),
            (None, None) => Self::new(String::new(), None),
        }
    }

    /// The text of an answer that finished normally and says something.
    pub fn into_text(self) -> AppResult<String> {
        match self.stop {
            Some(stop) => Err(AppError::ModelAnswerStopped(stop)),
            None if self.text.trim().is_empty() => Err(AppError::EmptyModelAnswer),
            None => Ok(self.text),
        }
    }

    /// The text of a redaction. Text that was blank to begin with may come back empty.
    pub fn into_redacted_text(self, input_was_blank: bool) -> AppResult<String> {
        match self.stop {
            None if input_was_blank => Ok(self.text),
            _ => self.into_text(),
        }
    }
}

/// The result of `attempt`, which runs a second time when the first failed in a way that
/// another attempt can change. One retry is the bound: a filter that keeps stopping the same
/// text is an answer.
pub async fn retrying_once<T, Attempt, Pending>(mut attempt: Attempt) -> AppResult<T>
where
    Attempt: FnMut() -> Pending,
    Pending: Future<Output = AppResult<T>>,
{
    match attempt().await {
        Err(error) if error.is_transient_model_failure() => {
            tracing::warn!(%error, "the model's answer failed, asking once more");
            attempt().await
        }
        result => result,
    }
}

impl AppError {
    /// The model's answer failed in a way that asking again can change.
    pub fn is_transient_model_failure(&self) -> bool {
        match self {
            Self::ModelAnswerStopped(stop) => stop.is_transient(),
            Self::EmptyModelAnswer => true,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    fn answer_stopped_by(finish_reason: &str) -> ModelAnswer {
        ModelAnswer::from_gemini(
            Some(GeminiCandidate {
                finish_reason,
                blocked_categories: vec!["DANGEROUS_CONTENT".to_string()],
                text: String::new(),
            }),
            None,
        )
    }

    fn stop_of(result: AppResult<String>) -> ModelStop {
        match result {
            Err(AppError::ModelAnswerStopped(stop)) => stop,
            other => panic!("expected a stopped answer, got {other:?}"),
        }
    }

    #[test]
    fn a_safety_stop_names_the_blocked_categories() {
        assert_eq!(
            stop_of(answer_stopped_by("SAFETY").into_text()),
            ModelStop::Safety {
                categories: vec!["DANGEROUS_CONTENT".to_string()]
            }
        );
    }

    #[test]
    fn other_finish_reasons_are_named_as_the_provider_names_them() {
        assert_eq!(
            stop_of(answer_stopped_by("MAX_TOKENS").into_text()),
            ModelStop::MaxTokens
        );
        assert_eq!(
            stop_of(answer_stopped_by("PROHIBITED_CONTENT").into_text()),
            ModelStop::Withheld("PROHIBITED_CONTENT".to_string())
        );
    }

    #[test]
    fn a_stop_keeps_no_text_even_when_the_answer_had_some() {
        let answer = ModelAnswer::from_gemini(
            Some(GeminiCandidate {
                finish_reason: "MAX_TOKENS",
                blocked_categories: Vec::new(),
                text: "[{\"text\": \"Ana".to_string(),
            }),
            None,
        );
        assert_eq!(stop_of(answer.into_text()), ModelStop::MaxTokens);
    }

    #[test]
    fn a_response_without_candidates_names_the_reason_the_prompt_was_blocked() {
        let answer = ModelAnswer::from_gemini(None, Some("PROHIBITED_CONTENT"));
        assert_eq!(
            stop_of(answer.into_text()),
            ModelStop::PromptBlocked("PROHIBITED_CONTENT".to_string())
        );
    }

    #[test]
    fn a_normal_stop_without_text_is_an_empty_answer() {
        for answer in [
            answer_stopped_by("STOP"),
            ModelAnswer::from_gemini(None, None),
        ] {
            assert!(matches!(
                answer.into_text(),
                Err(AppError::EmptyModelAnswer)
            ));
        }
    }

    #[test]
    fn a_normal_stop_with_text_is_the_text() {
        let answer = ModelAnswer::new("[]".to_string(), None);
        assert_eq!(answer.into_text().unwrap(), "[]");
    }

    #[test]
    fn a_redaction_of_blank_text_may_be_empty_and_any_other_may_not() {
        assert_eq!(
            ModelAnswer::new(String::new(), None)
                .into_redacted_text(true)
                .unwrap(),
            ""
        );
        assert!(matches!(
            ModelAnswer::new(String::new(), None).into_redacted_text(false),
            Err(AppError::EmptyModelAnswer)
        ));
        assert_eq!(
            stop_of(answer_stopped_by("SAFETY").into_redacted_text(true)),
            ModelStop::Safety {
                categories: vec!["DANGEROUS_CONTENT".to_string()]
            }
        );
    }

    async fn attempts_until_done(finish_reasons: &[&str]) -> (AppResult<String>, usize) {
        let attempts = Cell::new(0);
        let result = retrying_once(|| {
            let finish_reason = finish_reasons[attempts.get()];
            attempts.set(attempts.get() + 1);
            async move {
                let text = if finish_reason == "STOP" { "[]" } else { "" };
                ModelAnswer::from_gemini(
                    Some(GeminiCandidate {
                        finish_reason,
                        blocked_categories: Vec::new(),
                        text: text.to_string(),
                    }),
                    None,
                )
                .into_text()
            }
        })
        .await;
        (result, attempts.get())
    }

    #[tokio::test]
    async fn a_safety_stop_is_retried_once_and_a_second_answer_is_taken() {
        let (result, attempts) = attempts_until_done(&["SAFETY", "STOP"]).await;
        assert_eq!(result.unwrap(), "[]");
        assert_eq!(attempts, 2);
    }

    #[tokio::test]
    async fn a_stop_that_repeats_ends_as_the_error_after_one_retry() {
        let (result, attempts) = attempts_until_done(&["SAFETY", "SAFETY", "STOP"]).await;
        assert!(matches!(stop_of(result), ModelStop::Safety { .. }));
        assert_eq!(attempts, 2);
    }

    #[tokio::test]
    async fn other_and_an_empty_stop_are_retried() {
        assert_eq!(attempts_until_done(&["OTHER", "STOP"]).await.1, 2);
        let attempts = Cell::new(0);
        let result = retrying_once(|| {
            attempts.set(attempts.get() + 1);
            let text = if attempts.get() == 1 { "" } else { "[]" };
            async move { ModelAnswer::new(text.to_string(), None).into_text() }
        })
        .await;
        assert_eq!(result.unwrap(), "[]");
    }

    #[tokio::test]
    async fn a_truncated_or_withheld_answer_is_not_retried() {
        for finish_reason in [
            "MAX_TOKENS",
            "PROHIBITED_CONTENT",
            "BLOCKLIST",
            "SPII",
            "RECITATION",
        ] {
            let (result, attempts) = attempts_until_done(&[finish_reason, "STOP"]).await;
            assert!(result.is_err(), "{finish_reason}");
            assert_eq!(attempts, 1, "{finish_reason}");
        }
    }
}
