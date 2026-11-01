mod error;
mod rules;
mod user_rules;
mod validators;

pub use error::LocalRulesError;
pub use rules::{RuleGroup, RuleSet};
pub use user_rules::{load_rules_file, parse_inline_rule, UserRule};
#[cfg(feature = "local-credential-ml")]
pub(crate) use validators::quoted_credential_value;
// Only referenced by the CLI's test suite (`args.rs`), which asserts the exact matcher a
// parsed rule produced; production code only ever constructs `UserRule` through
// `parse_inline_rule`/`load_rules_file` and never names `UserMatcher` itself.
#[cfg(test)]
pub use user_rules::UserMatcher;

use crate::args::RedacterType;
use crate::errors::AppError;
use crate::file_systems::FileSystemRef;
use crate::redacters::text_spans::table_cells::redact_rows;
use crate::redacters::{
    text_or_table_support, Detection, Detections, RedactSupport, Redacted, Redacter,
    RedacterDataItem, RedacterDataItemContent, TextMask,
};
use crate::reporter::AppReporter;
use crate::AppResult;
use rvstruct::ValueStruct;
use std::collections::BTreeSet;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct LocalRulesRedacterOptions {
    pub groups: BTreeSet<RuleGroup>,
    pub user_rules: Vec<UserRule>,
}

/// Offline redacter: curated regexes with checksum validators plus user rules. The rule set
/// is compiled once at construction and shared by every file the copy touches.
#[derive(Debug, Clone)]
pub struct LocalRulesRedacter<'a> {
    rules: Arc<RuleSet>,
    reporter: &'a AppReporter<'a>,
}

impl<'a> LocalRulesRedacter<'a> {
    pub async fn new(
        options: LocalRulesRedacterOptions,
        reporter: &'a AppReporter<'a>,
    ) -> AppResult<Self> {
        let rules = RuleSet::with_user_rules(&options.groups, &options.user_rules)?;
        if rules.is_empty() {
            return Err(LocalRulesError::NoRules.into());
        }
        Ok(Self {
            rules: Arc::new(rules),
            reporter,
        })
    }
}

impl<'a> Redacter for LocalRulesRedacter<'a> {
    async fn redact(&self, input: RedacterDataItem) -> AppResult<Redacted> {
        let (content, replaced) = match input.content {
            RedacterDataItemContent::Value(text) => {
                let (redacted, findings) = self.rules.redact(&text);
                let replaced = Detection::in_text(&text, findings)?.found;
                (RedacterDataItemContent::Value(redacted), replaced)
            }
            RedacterDataItemContent::Table { headers, rows } => {
                let (rows, replaced) = redact_rows(rows, |_, cell| Ok(self.rules.find(cell)))?;
                (RedacterDataItemContent::Table { headers, rows }, replaced)
            }
            RedacterDataItemContent::Image { .. } | RedacterDataItemContent::Pdf { .. } => {
                return Err(AppError::unsupported_content())
            }
        };
        if !replaced.is_empty() {
            self.reporter.report_debug(format!(
                "local-rules: {} finding(s) in {}",
                replaced.len(),
                input.file_ref.relative_path.value()
            ));
        }
        Ok(Redacted::located(
            RedacterDataItem {
                content,
                file_ref: input.file_ref,
            },
            replaced,
        ))
    }

    async fn redact_support(&self, file_ref: &FileSystemRef) -> AppResult<RedactSupport> {
        Ok(text_or_table_support(file_ref))
    }

    async fn detect(&self, input: &RedacterDataItem) -> AppResult<Detections> {
        match &input.content {
            RedacterDataItemContent::Value(text) => Detection::in_text(text, self.rules.find(text)),
            RedacterDataItemContent::Table { rows, .. } => {
                Detection::in_table_cells(rows, |_, cell| Ok::<_, AppError>(self.rules.find(cell)))
            }
            RedacterDataItemContent::Image { .. } | RedacterDataItemContent::Pdf { .. } => {
                Err(AppError::unsupported_content())
            }
        }
    }

    fn redacter_type(&self) -> RedacterType {
        RedacterType::LocalRules
    }

    fn text_mask(&self) -> TextMask {
        TextMask::Token
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::AppError;
    use crate::redacters::test_support::{
        TEST_DOCUMENTS_DIR, TEST_DOCUMENT_SAMPLE_EMAIL, TEST_DOCUMENT_SAMPLE_PHONE,
    };
    use crate::redacters::{DetectionLabel, DetectionLocation};
    use console::Term;

    fn redacter<'a>(reporter: &'a AppReporter<'a>) -> LocalRulesRedacter<'a> {
        let options = LocalRulesRedacterOptions {
            groups: RuleGroup::all(),
            user_rules: Vec::new(),
        };
        futures::executor::block_on(LocalRulesRedacter::new(options, reporter)).unwrap()
    }

    fn text_item(
        name: &str,
        media_type: mime::Mime,
        content: RedacterDataItemContent,
    ) -> RedacterDataItem {
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
    async fn redacts_the_text_fixtures() {
        let term = Term::stdout();
        let reporter = AppReporter::from(&term);
        let redacter = redacter(&reporter);
        for name in [
            "customer-note.txt",
            "customer.json",
            "customer-profile.html",
        ] {
            let text = std::fs::read_to_string(format!("{TEST_DOCUMENTS_DIR}{name}")).unwrap();
            assert!(
                text.contains(TEST_DOCUMENT_SAMPLE_EMAIL),
                "{name} fixture changed"
            );
            let item = text_item(name, mime::TEXT_PLAIN, RedacterDataItemContent::Value(text));
            let redacted = redacter.redact(item).await.unwrap();
            let RedacterDataItemContent::Value(out) = redacted.item.content else {
                panic!("text stays text");
            };
            assert!(!out.contains(TEST_DOCUMENT_SAMPLE_EMAIL), "{name}: {out}");
            assert!(!out.contains(TEST_DOCUMENT_SAMPLE_PHONE), "{name}: {out}");
            assert!(out.contains("[REDACTED]"), "{name}: {out}");
        }
    }

    #[tokio::test]
    async fn json_fixture_stays_valid_json() {
        let term = Term::stdout();
        let reporter = AppReporter::from(&term);
        let text = std::fs::read_to_string(format!("{TEST_DOCUMENTS_DIR}customer.json")).unwrap();
        let item = text_item(
            "customer.json",
            mime::APPLICATION_JSON,
            RedacterDataItemContent::Value(text),
        );
        let redacted = redacter(&reporter).redact(item).await.unwrap();
        let RedacterDataItemContent::Value(out) = redacted.item.content else {
            panic!("text stays text");
        };
        serde_json::from_str::<serde_json::Value>(&out).unwrap();
    }

    #[tokio::test]
    async fn redacts_table_cells_and_keeps_headers() {
        let term = Term::stdout();
        let reporter = AppReporter::from(&term);
        let item = text_item(
            "customers.csv",
            mime::TEXT_CSV,
            RedacterDataItemContent::Table {
                headers: vec!["email".to_string(), "note".to_string()],
                rows: vec![vec![
                    "a@b.io".to_string(),
                    "call +1 555 123 4567".to_string(),
                ]],
            },
        );
        let redacted = redacter(&reporter).redact(item).await.unwrap();
        let RedacterDataItemContent::Table { headers, rows } = redacted.item.content else {
            panic!("table stays table");
        };
        assert_eq!(headers, vec!["email", "note"]);
        assert_eq!(rows, vec![vec!["[REDACTED]", "call [REDACTED]"]]);
    }

    #[tokio::test]
    async fn redacting_text_returns_what_detecting_it_finds() {
        let term = Term::stdout();
        let reporter = AppReporter::from(&term);
        let redacter = redacter(&reporter);
        let item = text_item(
            "note.txt",
            mime::TEXT_PLAIN,
            RedacterDataItemContent::Value("mail a@b.io or call +1 555 123 4567".to_string()),
        );

        let detected = redacter.detect(&item).await.unwrap().found;
        let redacted = redacter.redact(item).await.unwrap();

        assert_eq!(detected.len(), 2, "{detected:?}");
        assert_eq!(redacted.replaced, Some(detected));
        let replaced = redacted.replaced.unwrap();
        assert_eq!(
            replaced[0].location,
            DetectionLocation::Text {
                start: 5,
                end: 11,
                line: 1,
                column: 6,
            }
        );
        assert_eq!(replaced[0].value, "a@b.io");
    }

    #[tokio::test]
    async fn redacting_text_with_nothing_to_redact_returns_an_empty_list() {
        let term = Term::stdout();
        let reporter = AppReporter::from(&term);
        let item = text_item(
            "note.txt",
            mime::TEXT_PLAIN,
            RedacterDataItemContent::Value("nothing to see".to_string()),
        );

        let redacted = redacter(&reporter).redact(item).await.unwrap();

        assert_eq!(redacted.replaced, Some(Vec::new()));
    }

    #[tokio::test]
    async fn detects_text_and_table_cell_findings_where_they_are() {
        let term = Term::stdout();
        let reporter = AppReporter::from(&term);
        let redacter = redacter(&reporter);

        let text = "mail a@b.io";
        let detections = redacter
            .detect(&text_item(
                "note.txt",
                mime::TEXT_PLAIN,
                RedacterDataItemContent::Value(text.to_string()),
            ))
            .await
            .unwrap()
            .found;
        assert_eq!(
            detections,
            vec![Detection {
                label: DetectionLabel::new("email"),
                location: DetectionLocation::Text {
                    start: 5,
                    end: 11,
                    line: 1,
                    column: 6,
                },
                score: None,
                value: "a@b.io".to_string(),
            }]
        );

        let detections = redacter
            .detect(&text_item(
                "customers.csv",
                mime::TEXT_CSV,
                RedacterDataItemContent::Table {
                    headers: vec!["id".to_string(), "contact".to_string()],
                    rows: vec![
                        vec!["1".to_string(), "none".to_string()],
                        vec!["2".to_string(), "mail a@b.io".to_string()],
                    ],
                },
            ))
            .await
            .unwrap()
            .found;
        let locations: Vec<DetectionLocation> = detections
            .iter()
            .map(|detection| detection.location)
            .collect();
        assert_eq!(
            locations,
            vec![DetectionLocation::TableCell {
                row: 2,
                column: 2,
                start: 5,
                end: 11
            }]
        );
    }

    #[tokio::test]
    async fn partially_overlapping_user_rules_redact_and_report_the_union() {
        let term = Term::stdout();
        let reporter = AppReporter::from(&term);
        let options = LocalRulesRedacterOptions {
            groups: BTreeSet::new(),
            user_rules: vec![
                parse_inline_rule("head=abcdef").unwrap(),
                parse_inline_rule("tail=defghi").unwrap(),
            ],
        };
        let redacter = LocalRulesRedacter::new(options, &reporter).await.unwrap();
        let item = text_item(
            "note.txt",
            mime::TEXT_PLAIN,
            RedacterDataItemContent::Value("value abcdefghi end".to_string()),
        );

        let detections = redacter.detect(&item).await.unwrap().found;
        let redacted = redacter.redact(item).await.unwrap();

        let RedacterDataItemContent::Value(out) = redacted.item.content else {
            panic!("text stays text");
        };
        assert_eq!(out, "value [REDACTED] end");
        assert_eq!(detections.len(), 1);
        assert_eq!(detections[0].value, "abcdefghi");
    }

    #[tokio::test]
    async fn supports_text_and_csv_but_not_images_or_pdf() {
        let term = Term::stdout();
        let reporter = AppReporter::from(&term);
        let redacter = redacter(&reporter);
        let file_ref = |media_type: mime::Mime| FileSystemRef {
            relative_path: "f".into(),
            media_type: Some(media_type),
            file_size: None,
        };
        assert_eq!(
            redacter
                .redact_support(&file_ref(mime::TEXT_PLAIN))
                .await
                .unwrap(),
            RedactSupport::Supported
        );
        assert_eq!(
            redacter
                .redact_support(&file_ref(mime::APPLICATION_JSON))
                .await
                .unwrap(),
            RedactSupport::Supported
        );
        assert_eq!(
            redacter
                .redact_support(&file_ref(mime::TEXT_CSV))
                .await
                .unwrap(),
            RedactSupport::Supported
        );
        assert_eq!(
            redacter
                .redact_support(&file_ref(mime::IMAGE_PNG))
                .await
                .unwrap(),
            RedactSupport::Unsupported
        );
        assert_eq!(
            redacter
                .redact_support(&file_ref(mime::APPLICATION_PDF))
                .await
                .unwrap(),
            RedactSupport::Unsupported
        );
        assert_eq!(redacter.redacter_type(), RedacterType::LocalRules);
    }

    #[tokio::test]
    async fn empty_rule_set_is_a_configuration_error() {
        let term = Term::stdout();
        let reporter = AppReporter::from(&term);
        let options = LocalRulesRedacterOptions {
            groups: BTreeSet::new(),
            user_rules: Vec::new(),
        };
        let err = LocalRulesRedacter::new(options, &reporter)
            .await
            .unwrap_err();
        assert!(matches!(err, AppError::RedacterConfigError { .. }), "{err}");
    }
}
