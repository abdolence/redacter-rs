//! The report file `cp` and `scan` write beside their terminal output.

use crate::args::{ReportArgs, ReportFormat};
use crate::AppResult;
use console::{Style, Term};
use serde::Serialize;

/// A command result that can be written to `--report-file`.
pub trait Report: Serialize {
    /// The report for people, without terminal colours.
    fn to_text(&self) -> String;
}

impl ReportArgs {
    /// Writes `report` to `--report-file` in `--report-format`, when a file was given, and
    /// says where on the terminal.
    pub async fn write<R: Report>(&self, term: &Term, report: &R) -> AppResult<()> {
        let Some(path) = &self.report_file else {
            return Ok(());
        };
        let (content, format_name) = match self.report_format {
            ReportFormat::Json => (serde_json::to_string_pretty(report)?, "JSON"),
            ReportFormat::Text => (report.to_text(), "text"),
        };
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
}
