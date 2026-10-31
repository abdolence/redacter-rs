//! Running the `redacter` binary: the provider specs and one timed `scan` that returns its
//! report.

use crate::datasets::DatasetKind;
use crate::scoring::ScanReport;
use std::fs;
use std::path::Path;
use std::process::Command;
use std::str::FromStr;
use std::time::{Duration, Instant};

/// A redacter or a chain of them joined with `+`, as `redacter -d a -d b` runs them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Spec(Vec<String>);

impl FromStr for Spec {
    type Err = String;

    fn from_str(spec: &str) -> Result<Self, Self::Err> {
        let parts: Vec<String> = spec
            .split('+')
            .map(|part| part.trim().to_string())
            .collect();
        if parts.iter().any(String::is_empty) {
            return Err(format!("provider spec {spec:?} has an empty redacter"));
        }
        Ok(Spec(parts))
    }
}

impl std::fmt::Display for Spec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0.join("+"))
    }
}

impl Spec {
    fn has(&self, redacter: &str) -> bool {
        self.0.iter().any(|part| part == redacter)
    }
}

/// The addons `datasets` and `specs` read: the OCR models and Pdfium for images and PDFs, and
/// the model of each local redacter in a spec.
pub fn required_addons(specs: &[Spec], datasets: &[DatasetKind]) -> Vec<&'static str> {
    let mut addons = Vec::new();
    if datasets.contains(&DatasetKind::Images) {
        addons.extend(["ocr", "pdf"]);
    }
    for (redacter, addon) in [("local-ner", "local-ner"), ("local-gliner", "local-gliner")] {
        if specs.iter().any(|spec| spec.has(redacter)) {
            addons.push(addon);
        }
    }
    addons
}

/// One finished `scan`: how long it took and what it reported.
pub struct ScanRun {
    pub wall: Duration,
    pub report: ScanReport,
}

/// The binary under test, where it keeps its addons and the `scan` options every run gets
/// (`BENCH_DLP_SCAN_ARGS`), which is how a cloud redacter is configured.
pub struct Scanner<'a> {
    pub binary: &'a Path,
    pub addons_dir: &'a Path,
    pub scan_args: Vec<String>,
}

impl Scanner<'_> {
    pub fn install_addons(&self, addons: &[&str]) -> Result<(), String> {
        if addons.is_empty() {
            return Ok(());
        }
        let output = Command::new(self.binary)
            .arg("--addons-dir")
            .arg(self.addons_dir)
            .args(["addons", "install", &addons.join(",")])
            .output()
            .map_err(|e| format!("spawn redacter addons install: {e}"))?;
        if output.status.success() {
            Ok(())
        } else {
            Err(failure_reason(
                output.status.code(),
                &String::from_utf8_lossy(&output.stderr),
            ))
        }
    }

    /// Scans `source` with `spec`. The report is read even when the exit code is 1, since
    /// failed and blocked files are counted in it; without a report the run failed outright.
    pub fn scan(
        &self,
        spec: &Spec,
        source: &Path,
        dataset_args: &[&str],
    ) -> Result<ScanRun, String> {
        let scratch = tempfile::tempdir().map_err(|e| format!("create a report directory: {e}"))?;
        let report_path = scratch.path().join("report.json");
        let started = Instant::now();
        let output = Command::new(self.binary)
            .arg("--addons-dir")
            .arg(self.addons_dir)
            .arg("scan")
            .arg(source)
            .args(spec.0.iter().flat_map(|redacter| ["-d", redacter]))
            .args(&self.scan_args)
            .args(dataset_args)
            .arg("--show-values")
            .arg("--report-file")
            .arg(&report_path)
            .output()
            .map_err(|e| format!("spawn redacter scan: {e}"))?;
        let wall = started.elapsed();
        match fs::read_to_string(&report_path) {
            Ok(json) => Ok(ScanRun {
                wall,
                report: serde_json::from_str(&json)
                    .map_err(|e| format!("unreadable report: {e}"))?,
            }),
            Err(_) => Err(failure_reason(
                output.status.code(),
                &String::from_utf8_lossy(&output.stderr),
            )),
        }
    }
}

/// The `Error:` line of a failed run's output, which is where the binary says what went wrong
/// (the last line when there is none), with the exit code.
fn failure_reason(exit_code: Option<i32>, stderr: &str) -> String {
    let mut lines = stderr
        .lines()
        .rev()
        .map(str::trim)
        .filter(|line| !line.is_empty());
    let message = lines
        .clone()
        .find(|line| line.starts_with("Error:"))
        .or_else(|| lines.next())
        .unwrap_or("no error output");
    let code = exit_code.map_or("no exit code".to_string(), |code| format!("exit {code}"));
    format!("{code}: {}", message.chars().take(200).collect::<String>())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn addons_follow_the_datasets_and_the_local_redacters() {
        let specs: Vec<Spec> = ["local-rules", "local-rules+local-gliner"]
            .map(|spec| spec.parse().unwrap())
            .into();

        assert_eq!(
            required_addons(&specs, &[DatasetKind::Fixtures]),
            ["local-gliner"]
        );
        assert_eq!(
            required_addons(&specs, &[DatasetKind::Images]),
            ["ocr", "pdf", "local-gliner"]
        );
    }

    #[test]
    fn failed_run_is_reported_by_the_error_line_of_its_output() {
        assert_eq!(
            failure_reason(
                Some(1),
                "Redacter v1\nError: no credentials\nDetails: None\n"
            ),
            "exit 1: Error: no credentials"
        );
    }
}
