//! The report file `cp` and `scan` write beside their terminal output.

use crate::args::{ReportArgs, ReportFormat};
use crate::commands::artifact_locator::ArtifactLocator;
use crate::commands::findings::ReportedFile;
use crate::commands::ocsf::{EpochMillis, OcsfEvents};
use crate::commands::sarif::SarifLog;
use crate::redacters::Categories;
use crate::AppResult;
use console::{Style, Term};
use serde::Serialize;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::time::SystemTime;

impl ReportArgs {
    /// Writes the report of a command run over `source` to `--report-file` in
    /// `--report-format`, when a file was given, and says where on the terminal. `report` is
    /// the JSON report, `text` writes the report for people, and `reported_files` gives the
    /// files for the SARIF and OCSF reports, built only when one of them is chosen.
    /// `categories` are the ones `--categories` limited the run to, which the SARIF report
    /// states.
    pub fn write(
        &self,
        term: &Term,
        report: &impl Serialize,
        text: impl FnOnce(&mut dyn Write) -> AppResult<()>,
        reported_files: impl FnOnce() -> Vec<ReportedFile>,
        source: &str,
        categories: Option<&Categories>,
    ) -> AppResult<()> {
        let Some(path) = &self.report_file else {
            return Ok(());
        };
        let mut out = BufWriter::new(File::create(path)?);
        let format_name = match self.report_format {
            ReportFormat::Json => {
                serde_json::to_writer_pretty(&mut out, report)?;
                "JSON"
            }
            ReportFormat::Text => {
                text(&mut out)?;
                "text"
            }
            ReportFormat::Sarif => {
                let locator = ArtifactLocator::new(source, &std::env::current_dir()?);
                let log = SarifLog::try_new(&reported_files(), &locator, categories)?;
                serde_json::to_writer_pretty(&mut out, &log)?;
                "SARIF"
            }
            ReportFormat::Ocsf => {
                let locator = ArtifactLocator::new(source, &std::env::current_dir()?);
                let events = OcsfEvents::try_new(
                    &reported_files(),
                    &locator,
                    EpochMillis::try_from(SystemTime::now())?,
                )?;
                out.write_all(events.to_ndjson()?.as_bytes())?;
                "OCSF"
            }
        };
        out.flush()?;
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
