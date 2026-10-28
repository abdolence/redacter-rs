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
    /// The provider stopped without saying why; retried.
    Unexplained,
    /// The answer reached the output limit and is cut short.
    MaxTokens,
    /// The answer was withheld for a reason another attempt does not change: the provider's
    /// own finish reason, spelled as it spells it, or a plain-words description of what the
    /// response lacked (`no choices`, `no finish_reason`, `refusal`).
    Withheld(String),
    /// The provider rejected the prompt and produced no answer, named by its block reason.
    PromptBlocked(String),
}

impl ModelStop {
    /// The stop of a Gemini API candidate, or `None` when it finished with `STOP` and
    /// nothing else. `finish_reason` is the API's own name for it, which both Gemini
    /// endpoints share, or the number of a reason this build does not know.
    fn from_gemini(
        finish_reason: Result<&str, i32>,
        blocked_categories: Vec<String>,
    ) -> Option<Self> {
        match finish_reason {
            Ok("STOP") => None,
            Ok("SAFETY") => Some(Self::Safety {
                categories: blocked_categories,
            }),
            Ok("OTHER") => Some(Self::Unexplained),
            Ok("MAX_TOKENS") => Some(Self::MaxTokens),
            Ok(other) => Some(Self::Withheld(other.to_string())),
            Err(number) => Some(Self::Withheld(format!(
                "unrecognised finish reason {number}"
            ))),
        }
    }
}

impl Display for ModelStop {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Safety { categories } if categories.is_empty() => f.write_str("SAFETY"),
            Self::Safety { categories } => {
                write!(f, "SAFETY (blocked: {})", categories.join(", "))
            }
            Self::Unexplained => f.write_str("OTHER"),
            Self::MaxTokens => f.write_str("the output limit was reached"),
            Self::Withheld(reason) => f.write_str(reason),
            Self::PromptBlocked(reason) => write!(f, "the prompt was blocked ({reason})"),
        }
    }
}

/// What a Gemini API candidate carried, as the two Gemini endpoints report it.
pub struct GeminiCandidate {
    pub finish_reason: Result<&'static str, i32>,
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
        candidate: Option<GeminiCandidate>,
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

    /// The text of a redaction answer once the random input separator, which models
    /// sometimes echo around their answer despite being told not to, is gone. An answer
    /// without the separator is returned untouched, whitespace included; an echo is trimmed
    /// because the whitespace around it belongs to the echo, not to the document. An answer
    /// made of nothing but separators is not a redacted copy of a document that had content.
    pub fn into_text_without_separator(self, separator: &str) -> AppResult<String> {
        let text = self.into_text()?;
        let text = if text.contains(separator) {
            text.replace(separator, "").trim().to_string()
        } else {
            text
        };
        if text.trim().is_empty() {
            return Err(AppError::EmptyModelAnswer);
        }
        Ok(text)
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
        Err(error)
            if matches!(
                error,
                AppError::ModelAnswerStopped(ModelStop::Safety { .. } | ModelStop::Unexplained)
                    | AppError::EmptyModelAnswer
            ) =>
        {
            tracing::warn!(%error, "the model's answer failed, asking once more");
            attempt().await
        }
        result => result,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    fn answer_stopped_by(finish_reason: Result<&'static str, i32>) -> ModelAnswer {
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
            stop_of(answer_stopped_by(Ok("SAFETY")).into_text()),
            ModelStop::Safety {
                categories: vec!["DANGEROUS_CONTENT".to_string()]
            }
        );
    }

    #[test]
    fn other_finish_reasons_are_named_as_the_provider_names_them() {
        assert_eq!(
            stop_of(answer_stopped_by(Ok("MAX_TOKENS")).into_text()),
            ModelStop::MaxTokens
        );
        assert_eq!(
            stop_of(answer_stopped_by(Ok("PROHIBITED_CONTENT")).into_text()),
            ModelStop::Withheld("PROHIBITED_CONTENT".to_string())
        );
    }

    #[test]
    fn an_unspecified_finish_reason_is_a_stop() {
        assert_eq!(
            stop_of(answer_stopped_by(Ok("FINISH_REASON_UNSPECIFIED")).into_text()),
            ModelStop::Withheld("FINISH_REASON_UNSPECIFIED".to_string())
        );
    }

    #[test]
    fn an_echoed_separator_is_removed_from_around_the_answer() {
        for echoed in [
            "---1234 Hello, [REDACTED] ---1234",
            "---1234\nHello, [REDACTED]\n---1234\n",
        ] {
            let answer = ModelAnswer::new(echoed.to_string(), None);
            assert_eq!(
                answer.into_text_without_separator("---1234").unwrap(),
                "Hello, [REDACTED]"
            );
        }
    }

    #[test]
    fn an_answer_without_the_separator_keeps_its_whitespace() {
        let answer = ModelAnswer::new("  Hello, [REDACTED]\n".to_string(), None);
        assert_eq!(
            answer.into_text_without_separator("---1234").unwrap(),
            "  Hello, [REDACTED]\n"
        );
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
            answer_stopped_by(Ok("STOP")),
            ModelAnswer::from_gemini(None, None),
        ] {
            assert!(matches!(
                answer.into_text(),
                Err(AppError::EmptyModelAnswer)
            ));
        }
    }

    async fn attempts_until_done(finish_reasons: &[&'static str]) -> (AppResult<String>, usize) {
        let attempts = Cell::new(0);
        let result = retrying_once(|| {
            let finish_reason = finish_reasons[attempts.get()];
            attempts.set(attempts.get() + 1);
            async move {
                let text = if finish_reason == "STOP" { "[]" } else { "" };
                ModelAnswer::from_gemini(
                    Some(GeminiCandidate {
                        finish_reason: Ok(finish_reason),
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
    async fn a_truncated_answer_is_not_retried() {
        let (result, attempts) = attempts_until_done(&["MAX_TOKENS", "STOP"]).await;
        assert_eq!(stop_of(result), ModelStop::MaxTokens);
        assert_eq!(attempts, 1);
    }
}
