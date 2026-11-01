//! The report file `cp` and `scan` write beside their terminal output.

use crate::args::{ReportArgs, ReportFormat};
use crate::commands::sarif::{ArtifactLocator, SarifLog};
use crate::commands::scan_output::ScanCommandResult;
use crate::errors::AppError;
use crate::AppResult;
use console::{Style, Term};
use serde::Serialize;
use std::path::Path;

impl ReportArgs {
    /// Writes `report` to `--report-file` in `--report-format`, `text` being the report for
    /// people, when a file was given, and says where on the terminal.
    pub async fn write(&self, term: &Term, report: &impl Serialize, text: &str) -> AppResult<()> {
        let Some(path) = &self.report_file else {
            return Ok(());
        };
        let (content, format_name) = match self.report_format {
            ReportFormat::Json => (serde_json::to_string_pretty(report)?, "JSON"),
            ReportFormat::Text => (text.to_string(), "text"),
            ReportFormat::Sarif => {
                return Err(AppError::SystemError {
                    message: "This command does not write SARIF reports".to_string(),
                })
            }
        };
        save(term, path, content, format_name).await
    }

    /// Writes the report of `scan` over `source` to `--report-file` in `--report-format`, when a
    /// file was given, and says where on the terminal.
    pub async fn write_scan(
        &self,
        term: &Term,
        result: &ScanCommandResult,
        source: &str,
    ) -> AppResult<()> {
        let Some(path) = &self.report_file else {
            return Ok(());
        };
        let (content, format_name) = match self.report_format {
            ReportFormat::Sarif => {
                let locator = ArtifactLocator::new(source, &std::env::current_dir()?);
                let log = SarifLog::new(&result.sarif_artifacts(), &locator);
                (serde_json::to_string_pretty(&log)?, "SARIF")
            }
            ReportFormat::Json => (serde_json::to_string_pretty(result)?, "JSON"),
            ReportFormat::Text => (result.to_text(), "text"),
        };
        save(term, path, content, format_name).await
    }
}

async fn save(term: &Term, path: &Path, content: String, format_name: &str) -> AppResult<()> {
    tokio::fs::write(path, content).await?;
    term.write_line(
        format!(
            "Results saved to {format_name} file: {}",
            Style::new().bold().apply_to(path.display())
        )
        .as_str(),
    )?;
    Ok(())
}
