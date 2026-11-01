//! The report file `cp` and `scan` write beside their terminal output.

use crate::args::{ReportArgs, ReportFormat};
use crate::commands::findings::ReportedFile;
use crate::commands::ocsf::{EpochMillis, OcsfEvents};
use crate::commands::sarif::{ArtifactLocator, SarifLog};
use crate::AppResult;
use console::{Style, Term};
use serde::Serialize;
use std::time::SystemTime;

impl ReportArgs {
    /// Writes the report of a command run over `source` to `--report-file` in
    /// `--report-format`, when a file was given, and says where on the terminal. `report` is
    /// the JSON report, `text` the report for people, and `reported_files` the files for the
    /// SARIF and OCSF reports, built only when one of them is chosen.
    pub async fn write(
        &self,
        term: &Term,
        report: &impl Serialize,
        text: &str,
        reported_files: impl FnOnce() -> Vec<ReportedFile>,
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
                let log = SarifLog::new(&reported_files(), &locator);
                (serde_json::to_string_pretty(&log)?, "SARIF")
            }
            ReportFormat::Ocsf => {
                let locator = ArtifactLocator::new(source, &std::env::current_dir()?);
                let events = OcsfEvents::new(
                    &reported_files(),
                    &locator,
                    EpochMillis::try_from(SystemTime::now())?,
                );
                (events.to_ndjson()?, "OCSF")
            }
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
