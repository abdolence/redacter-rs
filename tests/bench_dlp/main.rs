//! Benchmark of the redacters of the `redacter` CLI: detection quality per category and time
//! per file, scored from the JSON report of `redacter scan`, over four datasets: the text
//! fixtures, a synthetic secrets corpus, a fixed sample of a public PII dataset, and an image
//! and a PDF form.
//!
//! This is not part of the normal test run: the harness is `#[ignore]`d because it shells out
//! to a release build of the binary and, for cloud redacters, makes real network calls and
//! bills them. Run it explicitly:
//!
//! ```sh
//! cargo build --release
//! cargo test --release --test bench_dlp -- --ignored --nocapture
//! ```
//!
//! The unit tests of the scoring and the tables run under a plain `cargo test`. The settings
//! are `BENCH_DLP_*` environment variables, described in `test-fixtures/bench-dlp/README.md`.

mod categories;
mod datasets;
mod runner;
mod scoring;
mod summary;

use datasets::{Dataset, DatasetKind, DatasetPaths};
use runner::{Scanner, Spec};
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use summary::{DatasetResult, Measurement, Outcome, ProviderResult, Results};

const DEFAULT_PROVIDERS: &str =
    "local-rules,local-ner,local-rules+local-ner,local-gliner,local-rules+local-gliner";
const DEFAULT_DATASETS: &str = "fixtures,secrets,public";

fn comma_separated<T: FromStr<Err = String>>(variable: &str, default: &str) -> Vec<T> {
    let value = std::env::var(variable).unwrap_or_else(|_| default.to_string());
    let items: Vec<T> = value
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(|item| item.parse().unwrap_or_else(|e| panic!("{variable}: {e}")))
        .collect();
    assert!(!items.is_empty(), "{variable} names nothing: {value:?}");
    items
}

/// Scans `dataset` with `spec` and scores the report.
fn measure(scanner: &Scanner, spec: &Spec, dataset: &Dataset) -> Outcome {
    let files = dataset.truth.len();
    println!("  {spec} on {}: {files} files", dataset.kind);
    let run = match scanner.scan(spec, &dataset.dir, dataset.scan_args) {
        Ok(run) => run,
        Err(reason) => return Outcome::Failed { reason },
    };
    let whole_run_ms = run.wall.as_millis();
    Outcome::Measured(Measurement {
        whole_run_ms,
        ms_per_file: whole_run_ms / files as u128,
        files_failed: run.report.files_failed,
        files_blocked: run.report.files_blocked,
        files_unsupported: run.report.files_unsupported,
        tally: run.report.score(&dataset.truth),
    })
}

#[test]
#[ignore]
fn bench_dlp() {
    let specs: Vec<Spec> = comma_separated("BENCH_DLP_PROVIDERS", DEFAULT_PROVIDERS);
    let kinds: Vec<DatasetKind> = comma_separated("BENCH_DLP_DATASETS", DEFAULT_DATASETS);
    let out_root = PathBuf::from(
        std::env::var("BENCH_DLP_OUT").unwrap_or_else(|_| "target/bench-dlp".to_string()),
    );
    fs::create_dir_all(&out_root)
        .unwrap_or_else(|e| panic!("create BENCH_DLP_OUT {}: {e}", out_root.display()));

    let scanner = Scanner {
        binary: Path::new(env!("CARGO_BIN_EXE_redacter")),
        scan_args: std::env::var("BENCH_DLP_SCAN_ARGS")
            .unwrap_or_default()
            .split_whitespace()
            .map(String::from)
            .collect(),
    };

    let paths = DatasetPaths {
        fixtures: &Path::new(env!("CARGO_MANIFEST_DIR")).join("test-fixtures"),
        work: &out_root,
    };
    let mut results = Results {
        datasets: Vec::new(),
    };
    for kind in kinds {
        println!("=== dataset: {kind} ===");
        let dataset = paths
            .load(kind)
            .unwrap_or_else(|e| panic!("load the {kind} dataset: {e}"));
        results.datasets.push(DatasetResult {
            dataset: kind,
            files: dataset.truth.len(),
            targets: dataset.sensitive_count(),
            providers: specs
                .iter()
                .map(|spec| ProviderResult {
                    provider: spec.to_string(),
                    outcome: measure(&scanner, spec, &dataset),
                })
                .collect(),
        });
    }

    let markdown = results.to_markdown();
    println!("\n{markdown}");
    fs::write(out_root.join("summary.md"), &markdown).expect("write summary.md");
    fs::write(
        out_root.join("results.json"),
        serde_json::to_string_pretty(&results).expect("serialise the results"),
    )
    .expect("write results.json");
    println!("written to {}", out_root.display());
}
