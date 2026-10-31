//! Benchmark of the redacters of the `redacter` CLI: detection quality per category and
//! latency, scored from the JSON report of `redacter scan`, over four datasets: the text
//! fixtures, a synthetic secrets corpus, a seeded sample of a public PII dataset, and an image
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
use runner::{median, required_addons, BenchEnv, Scanner, Spec};
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use summary::{DatasetResult, Measurement, Outcome, ProviderResult, Results};

const DEFAULT_PROVIDERS: &str =
    "local-rules,local-ner,local-rules+local-ner,local-gliner,local-rules+local-gliner";
const DEFAULT_DATASETS: &str = "fixtures,secrets";
const DEFAULT_SAMPLE_SIZE: usize = 200;
/// A dataset with more files than this is timed as one run only: a run per file would start the
/// binary, and for a cloud redacter send a request, once for each.
const PER_FILE_TIMING_MAX_FILES: usize = 30;

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
fn measure(scanner: &Scanner, env: &BenchEnv, spec: &Spec, dataset: &Dataset) -> Outcome {
    if let Some(reason) = spec.missing_config(env) {
        return Outcome::NotMeasured { reason };
    }
    let files = dataset.truth.len();
    println!("  {spec} on {}: {files} files", dataset.kind);
    let run = match scanner.scan(spec, env, &dataset.dir, dataset.scan_args) {
        Ok(run) => run,
        Err(reason) => return Outcome::Failed { reason },
    };
    let whole_run_ms = run.wall.as_millis();
    let (files_failed, files_blocked, files_unsupported) = (
        run.report.files_failed,
        run.report.files_blocked,
        run.report.files_unsupported,
    );
    let tally = run.report.score(&dataset.truth);

    let per_file_median_ms = if files <= PER_FILE_TIMING_MAX_FILES {
        let mut timings = Vec::new();
        for name in dataset.truth.keys() {
            match scanner.scan(spec, env, &dataset.dir.join(name), dataset.scan_args) {
                Ok(single) => timings.push(single.wall.as_millis()),
                Err(reason) => {
                    return Outcome::Failed {
                        reason: format!("{name}: {reason}"),
                    }
                }
            }
        }
        median(&timings)
    } else {
        None
    };

    Outcome::Measured(Measurement {
        whole_run_ms,
        per_file_median_ms,
        files_failed,
        files_blocked,
        files_unsupported,
        tally,
    })
}

#[test]
#[ignore]
fn bench_dlp() {
    let specs: Vec<Spec> = comma_separated("BENCH_DLP_PROVIDERS", DEFAULT_PROVIDERS);
    let kinds: Vec<DatasetKind> = comma_separated("BENCH_DLP_DATASETS", DEFAULT_DATASETS);
    let sample_size = std::env::var("BENCH_DLP_DATASET_SAMPLE")
        .map(|value| value.parse().expect("BENCH_DLP_DATASET_SAMPLE is a number"))
        .unwrap_or(DEFAULT_SAMPLE_SIZE);
    let out_root = PathBuf::from(
        std::env::var("BENCH_DLP_OUT").unwrap_or_else(|_| "target/bench-dlp".to_string()),
    );
    fs::create_dir_all(&out_root)
        .unwrap_or_else(|e| panic!("create BENCH_DLP_OUT {}: {e}", out_root.display()));
    let addons_dir = std::env::var("BENCH_DLP_ADDONS_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| out_root.join("addons"));
    let env = BenchEnv::from_lookup(|name| std::env::var(name).ok());

    let scanner = Scanner {
        binary: Path::new(env!("CARGO_BIN_EXE_redacter")),
        addons_dir: &addons_dir,
    };
    scanner
        .install_addons(&required_addons(&specs, &kinds))
        .unwrap_or_else(|e| panic!("install the addons into {}: {e}", addons_dir.display()));

    let paths = DatasetPaths {
        fixtures: &Path::new(env!("CARGO_MANIFEST_DIR")).join("test-fixtures"),
        work: &out_root,
    };
    let mut results = Results {
        datasets: Vec::new(),
    };
    for kind in kinds {
        println!("=== dataset: {kind} ===");
        let loaded = paths.load(kind, sample_size);
        let (files, targets) = loaded.as_ref().map_or((0, 0), |dataset| {
            (dataset.truth.len(), dataset.sensitive_count())
        });
        let providers = specs
            .iter()
            .map(|spec| ProviderResult {
                provider: spec.to_string(),
                outcome: match &loaded {
                    Ok(dataset) => measure(&scanner, &env, spec, dataset),
                    Err(reason) => Outcome::NotMeasured {
                        reason: format!("dataset unavailable: {reason}"),
                    },
                },
            })
            .collect();
        results.datasets.push(DatasetResult {
            dataset: kind,
            files,
            targets,
            providers,
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
