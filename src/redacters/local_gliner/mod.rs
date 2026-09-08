//! Offline zero-shot PII redacter: a GLiNER span model on rten scores free-text labels
//! against a document; the pure pipeline turns its logits into byte spans.

mod engine;
mod error;
pub mod labels;
pub mod pipeline;

pub use error::LocalGlinerError;

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
use engine::GlinerEngine;
use pipeline::GlinerOptions;
use rvstruct::ValueStruct;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub struct LocalGlinerRedacterOptions {
    pub labels: Vec<String>,
    pub min_score: f32,
}

impl LocalGlinerRedacterOptions {
    /// Rejects an empty or all-whitespace label, an empty label list and a score outside
    /// `0.0..=1.0` (NaN included). Every label is trimmed, since the model reads it as
    /// prompt text and leading or trailing whitespace changes nothing but the token count.
    pub fn validated(labels: Vec<String>, min_score: f32) -> Result<Self, LocalGlinerError> {
        if labels.is_empty() {
            return Err(LocalGlinerError::NoLabels);
        }
        let mut trimmed = Vec::with_capacity(labels.len());
        for label in labels {
            let label = label.trim().to_string();
            if label.is_empty() {
                return Err(LocalGlinerError::EmptyLabel);
            }
            trimmed.push(label);
        }
        if !(0.0..=1.0).contains(&min_score) {
            return Err(LocalGlinerError::InvalidMinScore { value: min_score });
        }
        Ok(Self {
            labels: trimmed,
            min_score,
        })
    }
}

/// Redacts every table cell that could hold a labelled span, leaving the rest byte for byte.
/// The predicate is "holds no letter", not "is a number": a cell without one cannot hold text
/// worth scoring and never reaches the model, which would cost a tokenizer pass and a forward
/// pass to find nothing.
///
/// Each cell is scored with its column header in front of it, `"city: Madrid"` rather than
/// `"Madrid"`: a one-word cell gives the model no sentence, and short bare values are tagged
/// at a lower score than the same value read as a sentence. The prefix is scoring context
/// only. Findings are mapped back onto the cell by `shift_into_cell` and the redaction is
/// applied to the original cell text, so no header text can reach the output.
fn redact_rows<F>(
    headers: &[String],
    rows: Vec<Vec<String>>,
    mut find_in: F,
) -> Result<(Vec<Vec<String>>, usize), LocalGlinerError>
where
    F: FnMut(&str) -> Result<Vec<Finding>, LocalGlinerError>,
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

/// Offline GLiNER redacter. The engine is loaded once per run and shared through an `Arc`,
/// so inference can run on a blocking thread while the async pipeline waits.
#[derive(Clone)]
pub struct LocalGlinerRedacter<'a> {
    engine: Arc<GlinerEngine>,
    options: GlinerOptions,
    reporter: &'a AppReporter<'a>,
}

impl<'a> LocalGlinerRedacter<'a> {
    pub async fn new(
        options: LocalGlinerRedacterOptions,
        reporter: &'a AppReporter<'a>,
        models: &ModelStore<'_>,
    ) -> AppResult<Self> {
        let files = models.resolve(ModelId::GlinerMultiPii).await?;
        reporter.report(format!(
            "Loading local GLiNER model from {}",
            files.dir().to_string_lossy()
        ))?;
        let engine = GlinerEngine::load(&files)?;
        Ok(Self {
            engine: Arc::new(engine),
            options: GlinerOptions {
                labels: options.labels,
                min_score: options.min_score,
            },
            reporter,
        })
    }

    /// Runs `work` on the blocking pool with a shared handle to the engine.
    async fn run_blocking<T, F>(&self, work: F) -> AppResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&GlinerEngine, &GlinerOptions) -> Result<T, LocalGlinerError> + Send + 'static,
    {
        let engine = Arc::clone(&self.engine);
        let options = self.options.clone();
        let result = tokio::task::spawn_blocking(move || work(&engine, &options))
            .await
            .map_err(|err| LocalGlinerError::InferenceTask {
                reason: err.to_string(),
            })?;
        Ok(result?)
    }
}

impl<'a> Redacter for LocalGlinerRedacter<'a> {
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
                "local-gliner: {} finding(s) in {}",
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
        RedacterType::LocalGliner
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
    fn options_default_labels_and_reject_bad_values() {
        let options = LocalGlinerRedacterOptions::validated(labels::default_labels(), 0.5).unwrap();
        assert_eq!(options.labels, labels::default_labels());
        assert_eq!(options.min_score, 0.5);
        assert!(LocalGlinerRedacterOptions::validated(vec!["person".to_string()], 0.0).is_ok());
        assert!(LocalGlinerRedacterOptions::validated(vec!["person".to_string()], 1.0).is_ok());

        let err = LocalGlinerRedacterOptions::validated(Vec::new(), 0.5).unwrap_err();
        assert!(matches!(err, LocalGlinerError::NoLabels), "{err}");

        let err = LocalGlinerRedacterOptions::validated(vec!["  ".to_string()], 0.5).unwrap_err();
        assert!(matches!(err, LocalGlinerError::EmptyLabel), "{err}");

        for bad in [-0.1_f32, 1.5, f32::NAN] {
            let err =
                LocalGlinerRedacterOptions::validated(vec!["person".to_string()], bad).unwrap_err();
            assert!(
                matches!(err, LocalGlinerError::InvalidMinScore { .. }),
                "{bad}: {err}"
            );
        }
    }

    #[test]
    fn labels_are_trimmed() {
        let options =
            LocalGlinerRedacterOptions::validated(vec![" email \n".to_string()], 0.5).unwrap();
        assert_eq!(options.labels, vec!["email".to_string()]);
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
            rule: RuleName::new("person"),
        }
    }

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

    #[test]
    fn a_cell_with_no_header_is_scored_on_its_own() {
        let (row, seen, findings) = run_row(&[""], &["Madrid"], vec![vec![finding(0, 6)]]);
        assert_eq!(seen, ["Madrid"]);
        assert_eq!(row, ["[REDACTED]"]);
        assert_eq!(findings, 1);

        let (row, seen, _) = run_row(
            &["city"],
            &["Madrid", "Osaka"],
            vec![vec![], vec![finding(0, 5)]],
        );
        assert_eq!(seen, ["city: Madrid", "Osaka"]);
        assert_eq!(row, ["Madrid", "[REDACTED]"]);
    }

    async fn redacter<'a>(reporter: &'a AppReporter<'a>) -> LocalGlinerRedacter<'a> {
        let options = ModelStoreOptions {
            download: DownloadModels::No,
            models_dir: None,
        };
        let store = ModelStore::new(&options, reporter);
        let options = LocalGlinerRedacterOptions::validated(labels::default_labels(), 0.5).unwrap();
        LocalGlinerRedacter::new(options, reporter, &store)
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
    #[cfg_attr(not(feature = "ci-local-gliner"), ignore)]
    async fn redacts_the_pii_in_the_customer_note_and_keeps_the_order_and_time() {
        let term = Term::stdout();
        let reporter = AppReporter::from(&term);
        let redacter = redacter(&reporter).await;
        assert_eq!(redacter.redacter_type(), RedacterType::LocalGliner);
        let text =
            std::fs::read_to_string(format!("{TEST_DOCUMENTS_DIR}customer-note.txt")).unwrap();
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
        for pii in [
            "John Michael Smith",
            "john.smith@example.com",
            "+1 (555) 123-4567",
            "14 March 1985",
            "221B Baker Street, London NW1 6XE",
            "4111 1111 1111 1111",
        ] {
            assert!(!out.contains(pii), "{pii} survived:\n{out}");
        }
        assert!(out.contains("#A-10422"), "{out}");
        assert!(out.contains("6pm"), "{out}");
        assert!(out.contains("[REDACTED]"), "{out}");
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-local-gliner"), ignore)]
    async fn redacts_the_contextual_fixture_and_keeps_the_non_pii_controls() {
        let term = Term::stdout();
        let reporter = AppReporter::from(&term);
        let redacter = redacter(&reporter).await;
        let fixture_path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/test-fixtures/documents/contextual-en.txt"
        );
        let text = std::fs::read_to_string(fixture_path)
            .unwrap_or_else(|err| panic!("read {fixture_path}: {err}"));
        let redacted = redacter
            .redact(item(
                "contextual-en.txt",
                mime::TEXT_PLAIN,
                RedacterDataItemContent::Value(text),
            ))
            .await
            .unwrap();
        let RedacterDataItemContent::Value(out) = redacted.content else {
            panic!("text stays text");
        };
        for pii in [
            "jonas petersen",
            "type 2 diabetes",
            "12/03/1984",
            "@mira_k84",
            "mkowalczyk",
            "14 Rosewood Lane, Kettering",
            "Mira Kowalczyk",
        ] {
            assert!(!out.contains(pii), "{pii} survived:\n{out}");
        }
        assert!(out.contains("Apple Watch"), "{out}");
        assert!(out.contains("The Support Desk"), "{out}");
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-local-gliner"), ignore)]
    async fn redacts_table_cells_behind_their_column_headers() {
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
                        "email".to_string(),
                    ],
                    rows: vec![
                        vec![
                            "1".to_string(),
                            "Maria Garcia Lopez".to_string(),
                            "maria.garcia@example.org".to_string(),
                        ],
                        vec![
                            "2".to_string(),
                            "Akira Tanaka".to_string(),
                            "akira.tanaka@example.net".to_string(),
                        ],
                    ],
                },
            ))
            .await
            .unwrap();
        let RedacterDataItemContent::Table { headers, rows } = redacted.content else {
            panic!("table stays table");
        };
        assert_eq!(headers, vec!["id", "full_name", "email"]);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], "1");
        assert_eq!(rows[1][0], "2");
        for cell in rows.iter().flatten() {
            assert!(!cell.contains('@'), "{cell:?}");
        }
    }
}
