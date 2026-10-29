//! The report file `cp` and `scan` write beside their terminal output.

use crate::args::{ReportArgs, ReportFormat};
use crate::AppResult;
use console::{Style, Term};
use serde::Serialize;

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
