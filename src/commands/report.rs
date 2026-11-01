//! The report file `cp` and `scan` write beside their terminal output.

use crate::args::{ReportArgs, ReportFormat};
use crate::commands::sarif::{ArtifactLocator, SarifArtifact, SarifLog};
use crate::AppResult;
use console::{Style, Term};
use serde::Serialize;
use std::path::Path;

impl ReportArgs {
    /// Writes the report of a command run over `source` to `--report-file` in
    /// `--report-format`, when a file was given, and says where on the terminal. `report` is
    /// the JSON report, `text` the report for people, and `sarif_artifacts` the files for the
    /// SARIF report, built only when that format is chosen.
    pub async fn write(
        &self,
        term: &Term,
        report: &impl Serialize,
        text: &str,
        sarif_artifacts: impl FnOnce() -> Vec<SarifArtifact>,
        source: &str,
    ) -> AppResult<()> {
        let Some(path) = &self.report_file else {
            return Ok(());
        };
        let (content, format_name) = match self.report_format {
            ReportFormat::Json => (serde_json::to_string_pretty(report)?, "JSON"),
            ReportFormat::Text => (text.to_string(), "text"),
            ReportFormat::Sarif => {
                let locator = ArtifactLocator::new(source, &std::env::current_dir()?);
                let log = SarifLog::new(&sarif_artifacts(), &locator);
                (serde_json::to_string_pretty(&log)?, "SARIF")
            }
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
