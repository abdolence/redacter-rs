//! Offline credential redacter: a line classifier on rten finds the lines that hold a
//! password, key or token, and the `local-rules` secret rules place the value on each one.

mod engine;
mod error;
mod extraction;

pub use error::LocalCredentialMlError;

use crate::addon_store::{Addon, AddonStore};
use crate::args::RedacterType;
use crate::errors::AppError;
use crate::file_systems::FileSystemRef;
use crate::redacters::text_spans::table_cells::{find_in_cell, redact_rows};
use crate::redacters::text_spans::{apply_redaction, REDACTED};
use crate::redacters::{
    text_or_table_support, Detection, Detections, RedactSupport, Redacter, RedacterDataItem,
    RedacterDataItemContent, Score,
};
use crate::reporter::AppReporter;
use crate::AppResult;
use engine::CredentialMlEngine;
use rvstruct::ValueStruct;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub struct LocalCredentialMlRedacterOptions {
    pub min_score: Score,
}

/// Offline credential redacter. The engine is loaded once per run and shared through an
/// `Arc`, so inference can run on a blocking thread while the async pipeline waits.
#[derive(Clone)]
pub struct LocalCredentialMlRedacter<'a> {
    engine: Arc<CredentialMlEngine>,
    min_score: Score,
    reporter: &'a AppReporter<'a>,
}

impl<'a> LocalCredentialMlRedacter<'a> {
    pub async fn new(
        options: LocalCredentialMlRedacterOptions,
        reporter: &'a AppReporter<'a>,
        addons: &AddonStore,
    ) -> AppResult<Self> {
        let files = addons.find(Addon::LocalCredentialMl)?;
        reporter.report(format!(
            "Loading local credential model from {}",
            files.dir().to_string_lossy()
        ))?;
        let engine = CredentialMlEngine::load(&files)?;
        Ok(Self {
            engine: Arc::new(engine),
            min_score: options.min_score,
            reporter,
        })
    }

    /// Runs `work` on the blocking pool with a shared handle to the engine.
    async fn run_blocking<T, F>(&self, work: F) -> AppResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&CredentialMlEngine, Score) -> AppResult<T> + Send + 'static,
    {
        let engine = Arc::clone(&self.engine);
        let min_score = self.min_score;
        tokio::task::spawn_blocking(move || work(&engine, min_score))
            .await
            .map_err(|err| LocalCredentialMlError::InferenceTask {
                reason: err.to_string(),
            })?
    }
}

impl<'a> Redacter for LocalCredentialMlRedacter<'a> {
    async fn redact(&self, input: RedacterDataItem) -> AppResult<RedacterDataItem> {
        let (content, findings) = match input.content {
            RedacterDataItemContent::Value(text) => {
                let (redacted, findings) = self
                    .run_blocking(move |engine, min_score| {
                        let findings = engine.find(&text, min_score)?;
                        Ok((apply_redaction(&text, &findings, REDACTED), findings.len()))
                    })
                    .await?;
                (RedacterDataItemContent::Value(redacted), findings)
            }
            RedacterDataItemContent::Table { headers, rows } => {
                let (headers, rows, findings) = self
                    .run_blocking(move |engine, min_score| {
                        let (rows, findings) = redact_rows(rows, |column, cell| {
                            find_in_cell(&headers, column, cell, &mut |text| {
                                engine.find(text, min_score)
                            })
                        })?;
                        Ok((headers, rows, findings))
                    })
                    .await?;
                (RedacterDataItemContent::Table { headers, rows }, findings)
            }
            RedacterDataItemContent::Image { .. } | RedacterDataItemContent::Pdf { .. } => {
                return Err(AppError::unsupported_content())
            }
        };
        if findings > 0 {
            self.reporter.report_debug(format!(
                "local-credential-ml: {} finding(s) in {}",
                findings,
                input.file_ref.relative_path.value()
            ));
        }
        Ok(RedacterDataItem {
            content,
            file_ref: input.file_ref,
        })
    }

    async fn redact_support(&self, file_ref: &FileSystemRef) -> AppResult<RedactSupport> {
        Ok(text_or_table_support(file_ref))
    }

    async fn detect(&self, input: &RedacterDataItem) -> AppResult<Detections> {
        match &input.content {
            RedacterDataItemContent::Value(text) => {
                let text = text.clone();
                self.run_blocking(move |engine, min_score| {
                    Detection::in_text(&text, engine.find(&text, min_score)?)
                })
                .await
            }
            RedacterDataItemContent::Table { headers, rows } => {
                let (headers, rows) = (headers.clone(), rows.clone());
                self.run_blocking(move |engine, min_score| {
                    Detection::in_table_cells(&rows, |column, cell| {
                        find_in_cell(&headers, column, cell, &mut |text| {
                            engine.find(text, min_score)
                        })
                    })
                })
                .await
            }
            RedacterDataItemContent::Image { .. } | RedacterDataItemContent::Pdf { .. } => {
                Err(AppError::unsupported_content())
            }
        }
    }

    fn redacter_type(&self) -> RedacterType {
        RedacterType::LocalCredentialMl
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::addon_store::AddonStoreOptions;
    use crate::redacters::{DetectionLabel, DetectionLocation};
    use console::Term;
    use mime::Mime;

    /// The model is installed with `redacter addons install local-credential-ml`; a test never
    /// downloads it.
    async fn redacter<'a>(reporter: &'a AppReporter<'a>) -> LocalCredentialMlRedacter<'a> {
        let store = AddonStore::new(&AddonStoreOptions::default());
        let options = LocalCredentialMlRedacterOptions {
            min_score: Score::from_static(0.74),
        };
        LocalCredentialMlRedacter::new(options, reporter, &store)
            .await
            .unwrap()
    }

    fn item(name: &str, media_type: Mime, content: RedacterDataItemContent) -> RedacterDataItem {
        RedacterDataItem {
            content,
            file_ref: FileSystemRef {
                relative_path: name.into(),
                media_type: Some(media_type),
                file_size: None,
            },
        }
    }

    fn credentials_note() -> String {
        std::fs::read_to_string("test-fixtures/credentials/credentials-note.txt").unwrap()
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-local-credential-ml"), ignore)]
    async fn redacts_the_credentials_and_keeps_the_rest_of_the_note() {
        let term = Term::stdout();
        let reporter = AppReporter::from(&term);
        let redacter = redacter(&reporter).await;
        let redacted = redacter
            .redact(item(
                "credentials-note.txt",
                mime::TEXT_PLAIN,
                RedacterDataItemContent::Value(credentials_note()),
            ))
            .await
            .unwrap();
        let RedacterDataItemContent::Value(out) = redacted.content else {
            panic!("text stays text");
        };
        assert_eq!(
            out,
            "Deploy notes for the team.\n\
             The password is [REDACTED] for the staging box (prose).\n\
             Google OAuth: [REDACTED]\n\
             GitHub: [REDACTED]\n\
             The release is on Thursday and the rollback plan is in the wiki.\n"
        );
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-local-credential-ml"), ignore)]
    async fn detects_the_credentials_with_the_model_score_and_one_label() {
        let term = Term::stdout();
        let reporter = AppReporter::from(&term);
        let redacter = redacter(&reporter).await;
        let text = credentials_note();
        let detections = redacter
            .detect(&item(
                "credentials-note.txt",
                mime::TEXT_PLAIN,
                RedacterDataItemContent::Value(text.clone()),
            ))
            .await
            .unwrap()
            .found;
        let values: Vec<&str> = detections
            .iter()
            .map(|detection| detection.value.as_str())
            .collect();
        assert_eq!(
            values,
            [
                "Tr0ub4dor&3",
                "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",
                "ghp_Ab1Cd2Ef3Gh4Ij5Kl6Mn7Op8Qr9St0Uv1Wx2"
            ]
        );
        for detection in &detections {
            assert_eq!(detection.label, DetectionLabel::new("credential"));
            assert!(
                detection
                    .score
                    .is_some_and(|score| score >= Score::from_static(0.74)),
                "{detection:?}"
            );
        }
        let DetectionLocation::Text {
            start, end, line, ..
        } = detections[0].location
        else {
            panic!("a text item yields text locations");
        };
        assert_eq!((&text[start..end], line), ("Tr0ub4dor&3", 2));
    }

    /// A cell is scored behind its column header, so a bare value is read as `key: value`.
    #[tokio::test]
    #[cfg_attr(not(feature = "ci-local-credential-ml"), ignore)]
    async fn redacts_a_credential_cell_and_leaves_the_other_cells() {
        let term = Term::stdout();
        let reporter = AppReporter::from(&term);
        let redacter = redacter(&reporter).await;
        let redacted = redacter
            .redact(item(
                "services.csv",
                mime::TEXT_CSV,
                RedacterDataItemContent::Table {
                    headers: vec!["service".to_string(), "api_key".to_string()],
                    rows: vec![vec![
                        "billing".to_string(),
                        "ghp_Ab1Cd2Ef3Gh4Ij5Kl6Mn7Op8Qr9St0Uv1Wx2".to_string(),
                    ]],
                },
            ))
            .await
            .unwrap();
        let RedacterDataItemContent::Table { headers, rows } = redacted.content else {
            panic!("table stays table");
        };
        assert_eq!(headers, vec!["service", "api_key"]);
        assert_eq!(rows, vec![vec!["billing", "[REDACTED]"]]);
    }
}
