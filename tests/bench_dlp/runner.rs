//! Running the `redacter` binary: the provider specs and one timed `scan` that returns its
//! report.

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

/// One finished `scan`: how long it took and what it reported.
pub struct ScanRun {
    pub wall: Duration,
    pub report: ScanReport,
}

/// The binary under test and the `scan` options every run gets (`BENCH_DLP_SCAN_ARGS`), which
/// is how a cloud redacter is configured. The binary finds its addons the way it does for a
/// user, in `REDACTER_ADDONS_DIR` or the cache directory.
pub struct Scanner<'a> {
    pub binary: &'a Path,
    pub scan_args: Vec<String>,
}

impl Scanner<'_> {
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
