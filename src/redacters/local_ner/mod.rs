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
use crate::redacters::text_spans::{apply_redaction, Finding, REDACTED};
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

/// Redacts every table cell that could hold a name, leaving the rest byte for byte. The
/// predicate is "holds no letter", not "is a number": a name needs letters, so a cell without
/// one cannot hold a name and never reaches the model, which would cost a tokenizer pass and
/// a forward pass to find nothing.
///
/// Each cell is scored with its column header in front of it, `"city: Madrid"` rather than
/// `"Madrid"`: a one-word cell gives the model no sentence, and it tags a bare city as `O` at
/// every score floor. The prefix is scoring context only. Findings are mapped back onto the
/// cell by `shift_into_cell` and the redaction is applied to the original cell text, so no
/// header text can reach the output.
fn redact_rows<F>(
    headers: &[String],
    rows: Vec<Vec<String>>,
    mut find_in: F,
) -> Result<(Vec<Vec<String>>, usize), LocalNerError>
where
    F: FnMut(&str) -> Result<Vec<Finding>, LocalNerError>,
{
    let mut findings = 0usize;
    let mut redacted_rows = Vec::with_capacity(rows.len());
    for row in rows {
        let mut redacted_row = Vec::with_capacity(row.len());
        for (column, cell) in row.into_iter().enumerate() {
            if !cell.chars().any(char::is_alphabetic) {
                redacted_row.push(cell);
                continue;
            }
            // A row wider than the header list, or an empty header, leaves the cell alone.
            let prefix = match headers.get(column) {
                Some(header) if !header.is_empty() => format!("{header}: "),
                _ => String::new(),
            };
            let scored = if prefix.is_empty() {
                find_in(&cell)?
            } else {
                find_in(&format!("{prefix}{cell}"))?
            };
            let in_cell = shift_into_cell(scored, prefix.len());
            findings += in_cell.len();
            redacted_row.push(apply_redaction(&cell, &in_cell, REDACTED));
        }
        redacted_rows.push(redacted_row);
    }
    Ok((redacted_rows, findings))
}

/// Maps findings on `"{prefix}{cell}"` back onto `cell`. A finding that ends inside the
/// prefix is the model matching the header itself and is dropped; one that crosses the
/// boundary keeps only the part that falls in the cell. `prefix_len` is a byte length and the
/// cell starts exactly there, so every clamped offset stays on a character boundary.
fn shift_into_cell(findings: Vec<Finding>, prefix_len: usize) -> Vec<Finding> {
    findings
        .into_iter()
        .filter(|finding| finding.end > prefix_len)
        .map(|mut finding| {
            finding.start = finding.start.max(prefix_len) - prefix_len;
            finding.end -= prefix_len;
            finding
        })
        .collect()
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
                let (headers, rows, findings) = self
                    .run_blocking(move |engine, options| {
                        let (rows, findings) =
                            redact_rows(&headers, rows, |text| engine.find(text, options))?;
                        Ok((headers, rows, findings))
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
    use crate::redacters::text_spans::RuleName;
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

    fn finding(start: usize, end: usize) -> Finding {
        Finding {
            start,
            end,
            rule: RuleName::new("loc"),
        }
    }

    /// Runs `redact_rows` over a single row with a fake engine that records the exact text
    /// it was handed and answers each call with the next queued finding list. Returns the
    /// redacted row, what the engine saw, and the finding count.
    fn run_row(
        headers: &[&str],
        row: &[&str],
        replies: Vec<Vec<Finding>>,
    ) -> (Vec<String>, Vec<String>, usize) {
        let headers: Vec<String> = headers.iter().map(|h| h.to_string()).collect();
        let rows = vec![row.iter().map(|cell| cell.to_string()).collect::<Vec<_>>()];
        let mut seen: Vec<String> = Vec::new();
        let mut replies = replies.into_iter();
        let (rows, findings) = redact_rows(&headers, rows, |text| {
            seen.push(text.to_string());
            Ok(replies.next().unwrap_or_default())
        })
        .unwrap();
        (rows.into_iter().next().unwrap(), seen, findings)
    }

    /// The skip is what keeps a column of numbers from paying for a tokenizer and a forward
    /// pass per cell, so it is asserted against a fake that records everything it is given.
    #[test]
    fn cells_without_letters_never_reach_the_model() {
        let headers: Vec<String> = ["id", "full_name", "amount"]
            .iter()
            .map(|h| h.to_string())
            .collect();
        let rows = vec![
            vec![
                "1".to_string(),
                "Maria Garcia Lopez".to_string(),
                " 42.5 ".to_string(),
            ],
            vec!["-7".to_string(), "Osaka".to_string(), String::new()],
        ];
        let mut seen: Vec<String> = Vec::new();
        let (redacted, findings) = redact_rows(&headers, rows, |text| {
            seen.push(text.to_string());
            Ok(vec![finding(text.len() - 5, text.len())])
        })
        .unwrap();
        assert_eq!(seen, ["full_name: Maria Garcia Lopez", "full_name: Osaka"]);
        assert_eq!(findings, 2);
        assert_eq!(
            redacted,
            vec![
                vec!["1", "Maria Garcia [REDACTED]", " 42.5 "],
                vec!["-7", "[REDACTED]", ""],
            ]
        );
    }

    /// A one-word cell gives the model no sentence to read, so the column header is put in
    /// front of it as scoring context. It is context only: it is never part of the value the
    /// redacter writes back.
    #[test]
    fn the_column_header_is_given_to_the_model_and_never_reaches_the_output() {
        let (row, seen, findings) = run_row(
            &["id", "city"],
            &["1", "Madrid"],
            vec![vec![finding(6, 12)]],
        );
        assert_eq!(seen, ["city: Madrid"]);
        assert_eq!(row, ["1", "[REDACTED]"]);
        assert_eq!(findings, 1);
        assert!(!row.iter().any(|cell| cell.contains("city")), "{row:?}");
    }

    /// The prefix shifts every offset the model reports. A finding that stays inside the
    /// header is the model matching the header itself and is dropped; one that crosses the
    /// boundary keeps only the part that falls in the cell.
    #[test]
    fn findings_in_the_prefix_are_dropped_and_crossing_ones_are_clamped() {
        let (row, seen, findings) = run_row(&["city"], &["Madrid"], vec![vec![finding(0, 4)]]);
        assert_eq!(seen, ["city: Madrid"]);
        assert_eq!(row, ["Madrid"]);
        assert_eq!(findings, 0);

        let (row, _, findings) = run_row(&["city"], &["Madrid"], vec![vec![finding(3, 9)]]);
        assert_eq!(row, ["[REDACTED]rid"]);
        assert_eq!(findings, 1);

        let (row, _, findings) = run_row(&["city"], &["Madrid"], vec![vec![finding(8, 12)]]);
        assert_eq!(row, ["Ma[REDACTED]"]);
        assert_eq!(findings, 1);
    }

    /// Without a header there is nothing to prefix with, and the cell is scored alone.
    #[test]
    fn a_cell_with_no_header_is_scored_on_its_own() {
        let (row, seen, findings) = run_row(&[""], &["Madrid"], vec![vec![finding(0, 6)]]);
        assert_eq!(seen, ["Madrid"]);
        assert_eq!(row, ["[REDACTED]"]);
        assert_eq!(findings, 1);

        // A row wider than the header list: the extra column has no header to prefix with.
        let (row, seen, _) = run_row(
            &["city"],
            &["Madrid", "Osaka"],
            vec![vec![], vec![finding(0, 5)]],
        );
        assert_eq!(seen, ["city: Madrid", "Osaka"]);
        assert_eq!(row, ["Madrid", "[REDACTED]"]);
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

    /// Probed against this exact model, a bare `"Madrid"`, `"Paris"`, `"London"` or
    /// `"Barcelona"` is tagged `O` at every score floor down to 0.0, while `"Osaka"` is
    /// tagged `LOC` and `"city: Madrid"` yields `Finding { 6..12, "loc" }`. That is why each
    /// cell is scored behind its column header, and why this test asserts the city column is
    /// redacted, the person column still is, and no cell comes back carrying a header word.
    #[tokio::test]
    #[cfg_attr(not(feature = "ci-local-ner"), ignore)]
    async fn redacts_name_and_city_cells_behind_their_column_headers() {
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
        assert_eq!(rows[0], vec!["1", "[REDACTED]", "[REDACTED]"]);
        assert_eq!(rows[1], vec!["2", "[REDACTED]", "[REDACTED]"]);
        for cell in rows.iter().flatten() {
            for header in ["id", "full_name", "city"] {
                assert!(
                    !cell.contains(header),
                    "{cell:?} carries the header {header:?}"
                );
            }
        }
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
