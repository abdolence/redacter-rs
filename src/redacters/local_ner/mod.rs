//! Offline named-entity redacter: a multilingual token-classification model on rten finds
//! people, organisations and locations; the pure pipeline turns its logits into byte spans.

mod engine;
mod error;
pub mod pipeline;

pub use error::LocalNerError;
pub use pipeline::Entity;

use crate::args::RedacterType;
use crate::file_systems::FileSystemRef;
use crate::model_store::{ModelId, ModelStore};
use crate::redacters::{
    text_or_table_support, unsupported_type_error, RedactSupport, Redacter, RedacterDataItem,
    RedacterDataItemContent,
};
use crate::reporter::AppReporter;
use crate::AppResult;
use engine::NerEngine;
use pipeline::NerOptions;
use rvstruct::ValueStruct;
use std::collections::BTreeSet;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub struct LocalNerRedacterOptions {
    pub entities: BTreeSet<Entity>,
    pub min_score: f32,
}

impl LocalNerRedacterOptions {
    /// Rejects an empty entity list and a score outside `0.0..=1.0` (NaN included).
    pub fn validated(entities: BTreeSet<Entity>, min_score: f32) -> Result<Self, LocalNerError> {
        if entities.is_empty() {
            return Err(LocalNerError::NoEntities);
        }
        if !(0.0..=1.0).contains(&min_score) {
            return Err(LocalNerError::InvalidMinScore { value: min_score });
        }
        Ok(Self {
            entities,
            min_score,
        })
    }
}

/// Redacts every table cell that could hold a name, leaving the rest byte for byte. A cell
/// without a letter cannot hold a name, so it never reaches the model: inference on it would
/// cost a tokenizer pass and a forward pass to find nothing.
fn redact_rows<F>(
    rows: Vec<Vec<String>>,
    mut redact_cell: F,
) -> Result<(Vec<Vec<String>>, usize), LocalNerError>
where
    F: FnMut(&str) -> Result<(String, usize), LocalNerError>,
{
    let mut findings = 0usize;
    let mut redacted_rows = Vec::with_capacity(rows.len());
    for row in rows {
        let mut redacted_row = Vec::with_capacity(row.len());
        for cell in row {
            if cell.chars().any(char::is_alphabetic) {
                let (redacted, cell_findings) = redact_cell(&cell)?;
                findings += cell_findings;
                redacted_row.push(redacted);
            } else {
                redacted_row.push(cell);
            }
        }
        redacted_rows.push(redacted_row);
    }
    Ok((redacted_rows, findings))
}

/// Offline NER redacter. The engine is loaded once per run and shared through an `Arc`, so
/// inference can run on a blocking thread while the async pipeline waits.
#[derive(Clone)]
pub struct LocalNerRedacter<'a> {
    engine: Arc<NerEngine>,
    options: NerOptions,
    reporter: &'a AppReporter<'a>,
}

impl<'a> LocalNerRedacter<'a> {
    pub async fn new(
        options: LocalNerRedacterOptions,
        reporter: &'a AppReporter<'a>,
        models: &ModelStore<'_>,
    ) -> AppResult<Self> {
        let files = models.resolve(ModelId::NerMultilingualHrl).await?;
        reporter.report(format!(
            "Loading local NER model from {}",
            files.dir().to_string_lossy()
        ))?;
        let engine = NerEngine::load(&files)?;
        Ok(Self {
            engine: Arc::new(engine),
            options: NerOptions {
                entities: options.entities,
                min_score: options.min_score,
            },
            reporter,
        })
    }

    /// Runs `work` on the blocking pool with a shared handle to the engine.
    async fn run_blocking<T, F>(&self, work: F) -> AppResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&NerEngine, &NerOptions) -> Result<T, LocalNerError> + Send + 'static,
    {
        let engine = Arc::clone(&self.engine);
        let options = self.options.clone();
        let result = tokio::task::spawn_blocking(move || work(&engine, &options))
            .await
            .map_err(|err| LocalNerError::InferenceTask {
                reason: err.to_string(),
            })?;
        Ok(result?)
    }
}

impl<'a> Redacter for LocalNerRedacter<'a> {
    async fn redact(&self, input: RedacterDataItem) -> AppResult<RedacterDataItem> {
        let (content, findings) = match input.content {
            RedacterDataItemContent::Value(text) => {
                let (redacted, findings) = self
                    .run_blocking(move |engine, options| {
                        let (redacted, findings) = engine.redact_text(&text, options)?;
                        Ok((redacted, findings.len()))
                    })
                    .await?;
                (RedacterDataItemContent::Value(redacted), findings)
            }
            RedacterDataItemContent::Table { headers, rows } => {
                let (rows, findings) = self
                    .run_blocking(move |engine, options| {
                        redact_rows(rows, |cell| {
                            let (redacted, cell_findings) = engine.redact_text(cell, options)?;
                            Ok((redacted, cell_findings.len()))
                        })
                    })
                    .await?;
                (RedacterDataItemContent::Table { headers, rows }, findings)
            }
            RedacterDataItemContent::Image { .. } | RedacterDataItemContent::Pdf { .. } => {
                return Err(unsupported_type_error())
            }
        };
        if findings > 0 {
            self.reporter.report_debug(format!(
                "local-ner: {} finding(s) in {}",
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

    fn redacter_type(&self) -> RedacterType {
        RedacterType::LocalNer
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model_store::{DownloadModels, ModelStoreOptions};
    use crate::redacters::test_support::TEST_DOCUMENTS_DIR;
    use console::Term;
    use mime::Mime;

    #[test]
    fn options_default_to_every_entity_and_reject_bad_values() {
        let options = LocalNerRedacterOptions::validated(Entity::all(), 0.5).unwrap();
        assert_eq!(options.entities, Entity::all());
        assert_eq!(options.min_score, 0.5);
        assert!(
            LocalNerRedacterOptions::validated([Entity::Per].into_iter().collect(), 0.0).is_ok()
        );
        assert!(LocalNerRedacterOptions::validated(Entity::all(), 1.0).is_ok());
        let err = LocalNerRedacterOptions::validated(BTreeSet::new(), 0.5).unwrap_err();
        assert!(matches!(err, LocalNerError::NoEntities), "{err}");
        for bad in [-0.1_f32, 1.5, f32::NAN] {
            let err = LocalNerRedacterOptions::validated(Entity::all(), bad).unwrap_err();
            assert!(
                matches!(err, LocalNerError::InvalidMinScore { .. }),
                "{bad}: {err}"
            );
        }
    }

    /// The redacter answers `redact_support` with the shared text/table rule, so the media
    /// types it accepts are asserted through the very function it delegates to.
    #[test]
    fn supports_text_and_csv_but_not_images_or_pdf() {
        let supports = |media_type: Option<Mime>| {
            text_or_table_support(&FileSystemRef {
                relative_path: "sample".into(),
                media_type,
                file_size: None,
            })
        };
        assert_eq!(supports(Some(mime::TEXT_PLAIN)), RedactSupport::Supported);
        assert_eq!(
            supports(Some(mime::APPLICATION_JSON)),
            RedactSupport::Supported
        );
        assert_eq!(supports(Some(mime::TEXT_CSV)), RedactSupport::Supported);
        assert_eq!(supports(Some(mime::IMAGE_PNG)), RedactSupport::Unsupported);
        assert_eq!(
            supports(Some(mime::APPLICATION_PDF)),
            RedactSupport::Unsupported
        );
        assert_eq!(supports(None), RedactSupport::Unsupported);
    }

    /// The skip is what keeps a numeric column from paying for a tokenizer and a forward
    /// pass per cell, so it is asserted against a fake that records everything it is given.
    #[test]
    fn cells_without_letters_never_reach_the_model() {
        let rows = vec![
            vec![
                "1".to_string(),
                "Maria Garcia Lopez".to_string(),
                " 42.5 ".to_string(),
            ],
            vec!["-7".to_string(), "Osaka".to_string(), String::new()],
        ];
        let mut seen: Vec<String> = Vec::new();
        let (redacted, findings) = redact_rows(rows, |cell| {
            seen.push(cell.to_string());
            Ok(("[REDACTED]".to_string(), 1))
        })
        .unwrap();
        assert_eq!(seen, ["Maria Garcia Lopez", "Osaka"]);
        assert_eq!(findings, 2);
        assert_eq!(
            redacted,
            vec![
                vec!["1", "[REDACTED]", " 42.5 "],
                vec!["-7", "[REDACTED]", ""],
            ]
        );
    }

    async fn redacter<'a>(reporter: &'a AppReporter<'a>) -> LocalNerRedacter<'a> {
        let options = ModelStoreOptions {
            download: DownloadModels::Yes,
            models_dir: None,
        };
        let store = ModelStore::new(&options, reporter).unwrap();
        let options = LocalNerRedacterOptions::validated(Entity::all(), 0.5).unwrap();
        LocalNerRedacter::new(options, reporter, &store)
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

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-local-ner"), ignore)]
    async fn redacts_the_names_in_the_text_fixtures() {
        let term = Term::stdout();
        let reporter = AppReporter::from(&term);
        let redacter = redacter(&reporter).await;
        assert_eq!(redacter.redacter_type(), RedacterType::LocalNer);
        let text =
            std::fs::read_to_string(format!("{TEST_DOCUMENTS_DIR}customer-note.txt")).unwrap();
        assert!(
            text.contains("John Michael Smith") && text.contains("#A-10422"),
            "fixture changed"
        );
        let redacted = redacter
            .redact(item(
                "customer-note.txt",
                mime::TEXT_PLAIN,
                RedacterDataItemContent::Value(text),
            ))
            .await
            .unwrap();
        let RedacterDataItemContent::Value(out) = redacted.content else {
            panic!("text stays text");
        };
        assert!(!out.contains("John Michael Smith"), "{out}");
        assert!(!out.contains("London"), "{out}");
        assert!(out.contains("#A-10422"), "{out}");
        assert!(out.contains("[REDACTED]"), "{out}");

        let json = std::fs::read_to_string(format!("{TEST_DOCUMENTS_DIR}customer.json")).unwrap();
        let redacted = redacter
            .redact(item(
                "customer.json",
                mime::APPLICATION_JSON,
                RedacterDataItemContent::Value(json),
            ))
            .await
            .unwrap();
        let RedacterDataItemContent::Value(out) = redacted.content else {
            panic!("text stays text");
        };
        serde_json::from_str::<serde_json::Value>(&out).unwrap();
        assert!(!out.contains("John Michael Smith"), "{out}");
    }

    /// A one-word cell gives the model no sentence to read. Probed against this exact
    /// model, "Madrid", "Paris", "London" and "Barcelona" alone are tagged `O` at every
    /// score floor down to 0.0, while "Osaka" is tagged `LOC`; the same names inside the
    /// text fixtures are found. The table path therefore asserts the person column, the
    /// untouched id column and the city column exactly as the model leaves it, rather than
    /// an expectation the model does not meet.
    #[tokio::test]
    #[cfg_attr(not(feature = "ci-local-ner"), ignore)]
    async fn redacts_name_cells_and_keeps_headers_and_the_id_column() {
        let term = Term::stdout();
        let reporter = AppReporter::from(&term);
        let redacter = redacter(&reporter).await;
        let redacted = redacter
            .redact(item(
                "customers.csv",
                mime::TEXT_CSV,
                RedacterDataItemContent::Table {
                    headers: vec![
                        "id".to_string(),
                        "full_name".to_string(),
                        "city".to_string(),
                    ],
                    rows: vec![
                        vec![
                            "1".to_string(),
                            "Maria Garcia Lopez".to_string(),
                            "Madrid".to_string(),
                        ],
                        vec![
                            "2".to_string(),
                            "Akira Tanaka".to_string(),
                            "Osaka".to_string(),
                        ],
                    ],
                },
            ))
            .await
            .unwrap();
        let RedacterDataItemContent::Table { headers, rows } = redacted.content else {
            panic!("table stays table");
        };
        assert_eq!(headers, vec!["id", "full_name", "city"]);
        assert_eq!(rows[0], vec!["1", "[REDACTED]", "Madrid"]);
        assert_eq!(rows[1], vec!["2", "[REDACTED]", "[REDACTED]"]);
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-local-ner"), ignore)]
    async fn redacts_the_spike_multilingual_fixture() {
        let term = Term::stdout();
        let reporter = AppReporter::from(&term);
        let redacter = redacter(&reporter).await;
        let text =
            std::fs::read_to_string("experiments/spike-ner/fixtures/multilingual.txt").unwrap();
        let redacted = redacter
            .redact(item(
                "multilingual.txt",
                mime::TEXT_PLAIN,
                RedacterDataItemContent::Value(text),
            ))
            .await
            .unwrap();
        let RedacterDataItemContent::Value(out) = redacted.content else {
            panic!("text stays text");
        };
        for name in [
            "Klaus Bergmann",
            "Sophie Lefebvre",
            "Carlos Ramírez Ortega",
            "Berlin",
            "Paris",
            "Madrid",
        ] {
            assert!(!out.contains(name), "{name} survived:\n{out}");
        }
        assert!(out.contains("Bestellung"), "{out}");
    }
}
