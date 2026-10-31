//! Running the `redacter` binary: the provider specs, what each needs from the environment,
//! and one timed `scan` that returns its report.

use crate::datasets::DatasetKind;
use crate::scoring::ScanReport;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::process::Command;
use std::str::FromStr;
use std::time::{Duration, Instant};

/// The settings the cloud redacters need, read from `BENCH_DLP_*` variables. A redacter whose
/// setting is missing is reported as not measured rather than failed.
#[derive(Debug, Default, Clone)]
pub struct BenchEnv {
    pub gcp_project: Option<String>,
    pub gcp_region: Option<String>,
    pub aws_region: Option<String>,
    pub aws_guardrail_id: Option<String>,
    pub presidio_analyzer_url: Option<String>,
    pub presidio_image_url: Option<String>,
    pub open_ai_api_key: Option<String>,
}

impl BenchEnv {
    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Self {
        let set = |name: &str| lookup(name).filter(|value| !value.trim().is_empty());
        Self {
            gcp_project: set("BENCH_DLP_GCP_PROJECT"),
            gcp_region: set("BENCH_DLP_GCP_REGION"),
            aws_region: set("BENCH_DLP_AWS_REGION")
                .or_else(|| set("AWS_REGION"))
                .or_else(|| set("AWS_DEFAULT_REGION")),
            aws_guardrail_id: set("BENCH_DLP_AWS_GUARDRAIL_ID"),
            presidio_analyzer_url: set("BENCH_DLP_PRESIDIO_ANALYZER_URL"),
            presidio_image_url: set("BENCH_DLP_PRESIDIO_IMAGE_URL"),
            open_ai_api_key: set("BENCH_DLP_OPENAI_API_KEY"),
        }
    }
}

/// A redacter or a chain of them joined with `+`, as `redacter -d a -d b` runs them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Spec(Vec<String>);

impl FromStr for Spec {
    type Err = String;

    /// `ms-presidio`, the former name of `presidio`, is accepted and reported as `presidio`.
    fn from_str(spec: &str) -> Result<Self, Self::Err> {
        let parts: Vec<String> = spec
            .split('+')
            .map(|part| match part.trim() {
                "ms-presidio" => "presidio".to_string(),
                other => other.to_string(),
            })
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

    /// What this spec needs from `env` and lacks, as one sentence, or `None` when it can run.
    pub fn missing_config(&self, env: &BenchEnv) -> Option<String> {
        let mut missing: Vec<&str> = Vec::new();
        let mut need = |present: &Option<String>, variable: &'static str| {
            if present.is_none() && !missing.contains(&variable) {
                missing.push(variable);
            }
        };
        if self.has("gcp-dlp") || self.has("gcp-vertex-ai") || self.has("gemini-llm") {
            need(&env.gcp_project, "BENCH_DLP_GCP_PROJECT");
        }
        if self.has("aws-comprehend")
            || self.has("aws-bedrock")
            || self.has("aws-bedrock-guardrails")
        {
            need(&env.aws_region, "BENCH_DLP_AWS_REGION");
        }
        if self.has("aws-bedrock-guardrails") {
            need(&env.aws_guardrail_id, "BENCH_DLP_AWS_GUARDRAIL_ID");
        }
        if self.has("presidio") {
            need(
                &env.presidio_analyzer_url,
                "BENCH_DLP_PRESIDIO_ANALYZER_URL",
            );
        }
        if self.has("open-ai-llm") {
            need(&env.open_ai_api_key, "BENCH_DLP_OPENAI_API_KEY");
        }
        (!missing.is_empty()).then(|| format!("set {}", missing.join(" and ")))
    }

    /// The `redacter scan` options that select this spec's redacters and configure them.
    pub fn scan_args(&self, env: &BenchEnv) -> Vec<String> {
        let mut args: Vec<String> = self
            .0
            .iter()
            .flat_map(|part| ["-d".to_string(), part.clone()])
            .collect();
        let mut options: BTreeMap<&str, String> = BTreeMap::new();
        let mut option = |present: bool, name: &'static str, value: &Option<String>| {
            if let (true, Some(value)) = (present, value) {
                options.insert(name, value.clone());
            }
        };
        option(
            self.has("gcp-dlp") || self.has("gcp-vertex-ai") || self.has("gemini-llm"),
            "--gcp-project-id",
            &env.gcp_project,
        );
        option(self.has("gcp-vertex-ai"), "--gcp-region", &env.gcp_region);
        option(
            self.has("aws-comprehend")
                || self.has("aws-bedrock")
                || self.has("aws-bedrock-guardrails"),
            "--aws-region",
            &env.aws_region,
        );
        option(
            self.has("aws-bedrock-guardrails"),
            "--aws-bedrock-guardrail-id",
            &env.aws_guardrail_id,
        );
        option(
            self.has("presidio"),
            "--presidio-text-analyze-url",
            &env.presidio_analyzer_url,
        );
        option(
            self.has("presidio"),
            "--presidio-image-redact-url",
            &env.presidio_image_url,
        );
        option(
            self.has("open-ai-llm"),
            "--open-ai-api-key",
            &env.open_ai_api_key,
        );
        for (name, value) in options {
            args.push(name.to_string());
            args.push(value);
        }
        args
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

/// Median of millisecond durations.
pub fn median(values: &[u128]) -> Option<f64> {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let middle = sorted.len() / 2;
    match sorted.len() {
        0 => None,
        len if len % 2 == 1 => Some(sorted[middle] as f64),
        _ => Some((sorted[middle - 1] as f64 + sorted[middle] as f64) / 2.0),
    }
}

/// One finished `scan`: how long it took and what it reported.
pub struct ScanRun {
    pub wall: Duration,
    pub report: ScanReport,
}

/// The binary under test and where it keeps its addons.
pub struct Scanner<'a> {
    pub binary: &'a Path,
    pub addons_dir: &'a Path,
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
        env: &BenchEnv,
        source: &Path,
        extra_args: &[&str],
    ) -> Result<ScanRun, String> {
        let scratch = tempfile::tempdir().map_err(|e| format!("create a report directory: {e}"))?;
        let report_path = scratch.path().join("report.json");
        let started = Instant::now();
        let output = Command::new(self.binary)
            .arg("--addons-dir")
            .arg(self.addons_dir)
            .arg("scan")
            .arg(source)
            .args(spec.scan_args(env))
            .args(extra_args)
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

/// The last line of a failed run's error output, which is where the binary says what went
/// wrong, with the exit code.
fn failure_reason(exit_code: Option<i32>, stderr: &str) -> String {
    let last_line = stderr
        .lines()
        .rev()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or("no error output");
    let code = exit_code.map_or("no exit code".to_string(), |code| format!("exit {code}"));
    format!(
        "{code}: {}",
        last_line.chars().take(200).collect::<String>()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(text: &str) -> Spec {
        text.parse().unwrap()
    }

    fn env_with(variables: &[(&str, &str)]) -> BenchEnv {
        BenchEnv::from_lookup(|name| {
            variables
                .iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| value.to_string())
        })
    }

    #[test]
    fn median_of_odd_count_is_the_middle_value() {
        assert_eq!(median(&[30, 10, 20]), Some(20.0));
    }

    #[test]
    fn median_of_even_count_averages_the_middle_two() {
        assert_eq!(median(&[10, 20, 30, 40]), Some(25.0));
    }

    #[test]
    fn median_of_empty_slice_is_none() {
        assert_eq!(median(&[]), None);
    }

    #[test]
    fn deprecated_presidio_name_is_accepted_and_reported_under_the_new_one() {
        assert_eq!(
            spec("local-rules+ms-presidio").to_string(),
            "local-rules+presidio"
        );
        assert!("local-rules+".parse::<Spec>().is_err());
    }

    #[test]
    fn local_redacters_need_no_configuration() {
        assert_eq!(
            spec("local-rules+local-gliner").missing_config(&BenchEnv::default()),
            None
        );
    }

    #[test]
    fn cloud_redacter_without_its_setting_is_not_measured() {
        let env = env_with(&[("AWS_REGION", "eu-north-1")]);

        assert_eq!(
            spec("gcp-dlp").missing_config(&env),
            Some("set BENCH_DLP_GCP_PROJECT".to_string())
        );
        assert_eq!(
            spec("aws-bedrock-guardrails").missing_config(&env),
            Some("set BENCH_DLP_AWS_GUARDRAIL_ID".to_string())
        );
        assert_eq!(
            spec("gcp-dlp+gcp-vertex-ai").missing_config(&env),
            Some("set BENCH_DLP_GCP_PROJECT".to_string())
        );
        assert_eq!(spec("aws-comprehend").missing_config(&env), None);
    }

    #[test]
    fn scan_args_select_the_chain_and_pass_each_setting_once() {
        let env = env_with(&[
            ("BENCH_DLP_GCP_PROJECT", "project"),
            ("BENCH_DLP_GCP_REGION", "eu"),
        ]);

        assert_eq!(
            spec("local-rules+gcp-dlp+gcp-vertex-ai").scan_args(&env),
            [
                "-d",
                "local-rules",
                "-d",
                "gcp-dlp",
                "-d",
                "gcp-vertex-ai",
                "--gcp-project-id",
                "project",
                "--gcp-region",
                "eu"
            ]
        );
    }

    #[test]
    fn addons_follow_the_datasets_and_the_local_redacters() {
        let specs = [spec("local-rules"), spec("local-rules+local-gliner")];

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
    fn failure_reason_is_the_last_line_with_the_exit_code() {
        assert_eq!(
            failure_reason(Some(1), "starting\nError: no credentials\n\n"),
            "exit 1: Error: no credentials"
        );
    }
}
