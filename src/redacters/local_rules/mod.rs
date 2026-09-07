mod error;
mod rules;
mod spans;
#[allow(dead_code)] // parsing helpers consumed by the CLI, wired in Task 6
mod user_rules;
mod validators;

pub use error::LocalRulesError;
pub use rules::{RuleGroup, RuleSet};
#[allow(unused_imports)] // consumed by the CLI, wired in Task 6
pub use user_rules::{load_rules_file, parse_inline_rule, UserMatcher, UserRule};

use crate::args::RedacterType;
use crate::errors::AppError;
use crate::file_systems::FileSystemRef;
use crate::redacters::{
    RedactSupport, Redacter, RedacterDataItem, RedacterDataItemContent, Redacters,
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

    fn redact_text(&self, file_ref: &FileSystemRef, text: &str) -> String {
        let (redacted, findings) = self.rules.redact(text);
        if !findings.is_empty() {
            self.reporter.report_debug(format!(
                "local-rules: {} finding(s) in {}",
                findings.len(),
                file_ref.relative_path.value()
            ));
        }
        redacted
    }
}

impl<'a> Redacter for LocalRulesRedacter<'a> {
    async fn redact(&self, input: RedacterDataItem) -> AppResult<RedacterDataItem> {
        let content = match input.content {
            RedacterDataItemContent::Value(text) => {
                RedacterDataItemContent::Value(self.redact_text(&input.file_ref, &text))
            }
            RedacterDataItemContent::Table { headers, rows } => RedacterDataItemContent::Table {
                headers,
                rows: rows
                    .into_iter()
                    .map(|row| {
                        row.iter()
                            .map(|cell| self.redact_text(&input.file_ref, cell))
                            .collect()
                    })
                    .collect(),
            },
            RedacterDataItemContent::Image { .. } | RedacterDataItemContent::Pdf { .. } => {
                return Err(AppError::SystemError {
                    message: "Attempt to redact of unsupported type".to_string(),
                })
            }
        };
        Ok(RedacterDataItem {
            content,
            file_ref: input.file_ref,
        })
    }

    async fn redact_support(&self, file_ref: &FileSystemRef) -> AppResult<RedactSupport> {
        Ok(match file_ref.media_type.as_ref() {
            Some(media_type)
                if Redacters::is_mime_text(media_type) || Redacters::is_mime_table(media_type) =>
            {
                RedactSupport::Supported
            }
            _ => RedactSupport::Unsupported,
        })
    }

    fn redacter_type(&self) -> RedacterType {
        RedacterType::LocalRules
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::redacters::test_support::{
        TEST_DOCUMENTS_DIR, TEST_DOCUMENT_SAMPLE_EMAIL, TEST_DOCUMENT_SAMPLE_PHONE,
    };
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
            let RedacterDataItemContent::Value(out) = redacted.content else {
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
        let RedacterDataItemContent::Value(out) = redacted.content else {
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
        let RedacterDataItemContent::Table { headers, rows } = redacted.content else {
            panic!("table stays table");
        };
        assert_eq!(headers, vec!["email", "note"]);
        assert_eq!(rows, vec![vec!["[REDACTED]", "call [REDACTED]"]]);
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
