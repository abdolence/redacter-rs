//! How a model's answer ended. An answer that was filtered, cut short or empty is an error
//! for the file it belongs to: read as text it would be a clean scan or an empty redacted
//! file.

use crate::errors::AppError;
use crate::AppResult;
use rand::RngExt;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::time::Duration;

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

const BACKOFF_BASE: Duration = Duration::from_millis(500);

/// How many times a call is made when the provider throttles it, the first included.
pub const THROTTLED_ATTEMPTS: u32 = 5;

/// The result of `attempt`, which is made again after a pause while the provider throttles it
/// (see [`AppError::is_throttled`]), up to [`THROTTLED_ATTEMPTS`] attempts in all. The pause
/// doubles with each failure, with jitter so that calls throttled together do not return
/// together. Any other failure is returned at once.
pub async fn retrying_throttled<T, Attempt, Pending>(mut attempt: Attempt) -> AppResult<T>
where
    Attempt: FnMut() -> Pending,
    Pending: Future<Output = AppResult<T>>,
{
    let mut failures = 0;
    loop {
        match attempt().await {
            Err(error) if error.is_throttled() && failures + 1 < THROTTLED_ATTEMPTS => {
                let pause = backoff_pause(failures);
                tracing::debug!(%error, ?pause, "the provider throttled the call, trying again");
                tokio::time::sleep(pause).await;
                failures += 1;
            }
            result => return result,
        }
    }
}

/// The pause after the `failures`-th failure in a row: between half and all of 500 ms doubled
/// for each earlier failure.
fn backoff_pause(failures: u32) -> Duration {
    let ceiling = BACKOFF_BASE.saturating_mul(2u32.saturating_pow(failures));
    rand::rng().random_range(ceiling / 2..=ceiling)
}

#[cfg(test)]
mod tests {
    use super::*;
    use gcloud_sdk::tonic::Status;
    use std::cell::Cell;

    #[derive(Clone, Copy)]
    enum Call {
        Throttled,
        Denied,
        Answered,
    }

    /// What `retrying_throttled` gives when its attempts go as `calls` say, the last one for
    /// every attempt after them, with the attempts made and the time they took.
    async fn retried_over(calls: &[Call]) -> (AppResult<&'static str>, u32, Duration) {
        let made = Cell::new(0u32);
        let started = tokio::time::Instant::now();
        let result = retrying_throttled(|| {
            let call = calls[(made.get() as usize).min(calls.len() - 1)];
            made.set(made.get() + 1);
            async move {
                match call {
                    Call::Throttled => Err(Status::resource_exhausted("quota").into()),
                    Call::Denied => Err(AppError::SystemError {
                        message: "denied".to_string(),
                    }),
                    Call::Answered => Ok("answer"),
                }
            }
        })
        .await;
        (result, made.get(), started.elapsed())
    }

    #[tokio::test(start_paused = true)]
    async fn a_call_throttled_twice_is_made_again_and_succeeds() {
        let (result, made, _) =
            retried_over(&[Call::Throttled, Call::Throttled, Call::Answered]).await;

        assert_eq!(result.unwrap(), "answer");
        assert_eq!(made, 3);
    }

    #[tokio::test(start_paused = true)]
    async fn a_call_throttled_every_time_fails_after_five_attempts_with_growing_pauses() {
        let (result, made, waited) = retried_over(&[Call::Throttled]).await;

        assert!(result.unwrap_err().is_throttled());
        assert_eq!(made, THROTTLED_ATTEMPTS);
        // Four pauses, each between half and all of 500 ms, 1 s, 2 s and 4 s.
        assert!(
            (Duration::from_millis(3750)..=Duration::from_millis(7500)).contains(&waited),
            "{waited:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_failure_that_is_not_throttling_is_not_repeated() {
        let (result, made, waited) = retried_over(&[Call::Denied]).await;

        assert!(matches!(result, Err(AppError::SystemError { .. })));
        assert_eq!(made, 1);
        assert_eq!(waited, Duration::ZERO);
    }

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
    async fn a_safety_or_unexplained_stop_is_retried_once_and_any_other_is_not() {
        let stopped_by_safety = || ModelStop::Safety {
            categories: Vec::new(),
        };
        for (finish_reasons, expected_attempts, expected) in [
            (&["SAFETY", "STOP"][..], 2, Ok("[]".to_string())),
            (&["OTHER", "STOP"][..], 2, Ok("[]".to_string())),
            (
                &["SAFETY", "SAFETY", "STOP"][..],
                2,
                Err(stopped_by_safety()),
            ),
            (&["MAX_TOKENS", "STOP"][..], 1, Err(ModelStop::MaxTokens)),
        ] {
            let (result, attempts) = attempts_until_done(finish_reasons).await;
            let outcome = match result {
                Ok(text) => Ok(text),
                Err(AppError::ModelAnswerStopped(stop)) => Err(stop),
                Err(other) => panic!("{finish_reasons:?} ended in {other:?}"),
            };
            assert_eq!(outcome, expected, "{finish_reasons:?}");
            assert_eq!(attempts, expected_attempts, "{finish_reasons:?}");
        }
    }

    #[tokio::test]
    async fn an_empty_answer_is_retried() {
        let attempts = Cell::new(0);
        let result = retrying_once(|| {
            attempts.set(attempts.get() + 1);
            let text = if attempts.get() == 1 { "" } else { "[]" };
            async move { ModelAnswer::new(text.to_string(), None).into_text() }
        })
        .await;
        assert_eq!(result.unwrap(), "[]");
    }
}
