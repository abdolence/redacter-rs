//! Benchmark harness comparing the redacter CLI's providers on latency and
//! detection quality against the fixed corpus under `test-fixtures/documents/`.
//!
//! This is not part of the normal test run: the harness function below is
//! `#[ignore]`d because it shells out to a release build of the binary and,
//! for cloud provider specs, makes real network calls. Run it explicitly:
//!
//! ```sh
//! cargo build --release
//! cargo test --release --test bench_dlp -- --ignored --nocapture
//! ```
//!
//! Configuration is via environment variables (see `README.md` under
//! `test-fixtures/bench-dlp/` for the full description):
//!
//! - `BENCH_DLP_PROVIDERS`: comma-separated provider specs, e.g.
//!   `local-rules,local-ner,local-rules+local-ner,gcp-dlp,gcp-vertex-ai`.
//!   Defaults to `local-rules,local-ner,local-rules+local-ner`.
//! - `BENCH_DLP_GCP_PROJECT`: GCP project id, required only when a `gcp-*`
//!   provider is listed.
//! - `BENCH_DLP_OUT`: output directory for run artifacts and `summary.md`.
//!   Defaults to `target/bench-dlp`. This is scratch output and must never be
//!   committed.
//!
//! The pure scoring/formatting helpers (`score_text`, `median`,
//! `format_table`) have their own unit tests below, which run under a plain
//! `cargo test` (they are not `#[ignore]`d).

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

/// The seven text-only fixtures that make up the corpus, read directly from
/// `test-fixtures/documents/`. This is an explicit list, not a directory
/// listing, so that `test-fixtures/documents/customer-form.pdf` (and anything
/// else added there later) never becomes part of the corpus.
const CORPUS_FILE_NAMES: &[&str] = &[
    "customer-note.txt",
    "customer.json",
    "customer-profile.html",
    "customers.csv",
    "multilingual.txt",
    "false-positives-en.txt",
    "dates-en.txt",
];

const REDACTED_TOKEN: &str = "[REDACTED]";

#[derive(serde::Deserialize, Debug, Clone)]
struct FileTruth {
    pii: Vec<String>,
    #[serde(default)]
    entities: Vec<String>,
    keep: Vec<String>,
}

type Truth = BTreeMap<String, FileTruth>;

/// Per-file scoring result: how many `pii` strings were removed, how many
/// `entities` strings were removed, how many `keep` strings survived, and how
/// many `[REDACTED]` tokens the output contains. Pure function of the
/// redacted text and the truth entry for that file -- no I/O.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct FileScore {
    pii_hits: usize,
    pii_total: usize,
    entities_redacted: usize,
    entities_total: usize,
    keep_survived: usize,
    keep_total: usize,
    redacted_count: usize,
}

impl FileScore {
    fn merge(&mut self, other: &FileScore) {
        self.pii_hits += other.pii_hits;
        self.pii_total += other.pii_total;
        self.entities_redacted += other.entities_redacted;
        self.entities_total += other.entities_total;
        self.keep_survived += other.keep_survived;
        self.keep_total += other.keep_total;
        self.redacted_count += other.redacted_count;
    }
}

/// Scores one redacted file's text against its truth entry. A truth string
/// counts as "gone" (a pii hit, a redacted entity, or a lost keep string)
/// when it is no longer present anywhere in `text` as an exact substring.
/// This is intentionally coarse: it does not check that the *right* span was
/// redacted, only whether the original text survived.
fn score_text(text: &str, truth: &FileTruth) -> FileScore {
    let pii_hits = truth
        .pii
        .iter()
        .filter(|s| !text.contains(s.as_str()))
        .count();
    let entities_redacted = truth
        .entities
        .iter()
        .filter(|s| !text.contains(s.as_str()))
        .count();
    let keep_survived = truth
        .keep
        .iter()
        .filter(|s| text.contains(s.as_str()))
        .count();
    FileScore {
        pii_hits,
        pii_total: truth.pii.len(),
        entities_redacted,
        entities_total: truth.entities.len(),
        keep_survived,
        keep_total: truth.keep.len(),
        redacted_count: text.matches(REDACTED_TOKEN).count(),
    }
}

/// Median of a slice of millisecond durations. Pure, no I/O.
fn median(values: &[u128]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let n = sorted.len();
    let mid = n / 2;
    if n % 2 == 1 {
        Some(sorted[mid] as f64)
    } else {
        Some((sorted[mid - 1] as f64 + sorted[mid] as f64) / 2.0)
    }
}

/// Aggregate result for one provider spec, ready to format into a table row.
struct ProviderScore {
    provider: String,
    whole_median_ms: Option<f64>,
    per_file_median_ms: Option<f64>,
    totals: FileScore,
    missing_files: Vec<String>,
}

fn fmt_ms(v: Option<f64>) -> String {
    match v {
        None => "n/a".to_string(),
        Some(v) => format!("{v:.0} ms"),
    }
}

/// Renders the markdown summary table. Pure function of already-computed
/// scores -- no I/O.
fn format_table(rows: &[ProviderScore]) -> String {
    let mut lines = Vec::new();
    lines.push(
        "| provider | wall time (median of 3) | per-file median | pii hits/total \
         | loc/org redacted/total | keep survived/total | [REDACTED] count |"
            .to_string(),
    );
    lines.push("|---|---|---|---|---|---|---|".to_string());
    for r in rows {
        let note = if r.missing_files.is_empty() {
            String::new()
        } else {
            format!(" (missing: {})", r.missing_files.join(", "))
        };
        lines.push(format!(
            "| {} | {} | {} | {}/{} | {}/{} | {}/{} | {}{} |",
            r.provider,
            fmt_ms(r.whole_median_ms),
            fmt_ms(r.per_file_median_ms),
            r.totals.pii_hits,
            r.totals.pii_total,
            r.totals.entities_redacted,
            r.totals.entities_total,
            r.totals.keep_survived,
            r.totals.keep_total,
            r.totals.redacted_count,
            note,
        ));
    }
    lines.join("\n")
}

/// True when the spec names a cloud provider anywhere (a single spec or a
/// `+`-joined chain), matching run.sh's substring check.
fn is_cloud_spec(spec: &str) -> bool {
    spec.contains("gcp-dlp") || spec.contains("gcp-vertex-ai")
}

/// Reads `run1_dir/<fname>` for every file in `truth` and scores it with
/// [`score_text`]. A missing output file counts its whole truth entry as
/// unscored (matching score.py: added to totals, not to hits).
fn score_provider(
    provider: &str,
    run1_dir: &Path,
    whole_ms: &[u128],
    per_file_ms: &BTreeMap<String, u128>,
    truth: &Truth,
) -> ProviderScore {
    let mut totals = FileScore::default();
    let mut missing_files = Vec::new();

    for (fname, file_truth) in truth {
        let out_path = run1_dir.join(fname);
        if !out_path.exists() {
            missing_files.push(fname.clone());
            totals.pii_total += file_truth.pii.len();
            totals.entities_total += file_truth.entities.len();
            totals.keep_total += file_truth.keep.len();
            continue;
        }
        let bytes = fs::read(&out_path)
            .unwrap_or_else(|e| panic!("read {} for provider {provider}: {e}", out_path.display()));
        let text = String::from_utf8_lossy(&bytes);
        totals.merge(&score_text(&text, file_truth));
    }

    ProviderScore {
        provider: provider.to_string(),
        whole_median_ms: median(whole_ms),
        per_file_median_ms: median(&per_file_ms.values().copied().collect::<Vec<_>>()),
        totals,
        missing_files,
    }
}

/// Runs one `redacter cp` invocation for `spec` copying `src` into `out_dir`.
/// `out_dir` is created first and always passed with a trailing slash --
/// run.sh discovered this is required for the local filesystem to treat the
/// destination as a directory rather than a literal file name.
fn invoke_redacter(spec: &str, src: &Path, out_dir: &Path, gcp_project: Option<&str>) {
    fs::create_dir_all(out_dir)
        .unwrap_or_else(|e| panic!("create output dir {}: {e}", out_dir.display()));

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_redacter"));
    cmd.arg("cp");
    for part in spec.split('+') {
        cmd.arg("-d").arg(part);
        if part == "gcp-dlp" || part == "gcp-vertex-ai" {
            let project = gcp_project
                .unwrap_or_else(|| panic!("provider {part} needs BENCH_DLP_GCP_PROJECT set"));
            cmd.arg("--gcp-project-id").arg(project);
        }
    }
    cmd.arg("--download-models").arg("no");
    cmd.arg(src);

    let mut dest = out_dir.display().to_string();
    if !dest.ends_with('/') {
        dest.push('/');
    }
    cmd.arg(dest);

    let output = cmd
        .output()
        .unwrap_or_else(|e| panic!("spawn redacter for provider {spec}: {e}"));
    if !output.status.success() {
        panic!(
            "redacter cp failed for provider {spec} (src {}): status {}\nstdout:\n{}\nstderr:\n{}",
            src.display(),
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
#[ignore]
fn bench_dlp() {
    let providers_env = std::env::var("BENCH_DLP_PROVIDERS")
        .unwrap_or_else(|_| "local-rules,local-ner,local-rules+local-ner".to_string());
    let specs: Vec<String> = providers_env
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    assert!(
        !specs.is_empty(),
        "BENCH_DLP_PROVIDERS produced no provider specs: {providers_env:?}"
    );

    let gcp_project = std::env::var("BENCH_DLP_GCP_PROJECT").ok();
    for spec in &specs {
        if is_cloud_spec(spec) && gcp_project.is_none() {
            panic!(
                "provider {spec} is a cloud provider; set BENCH_DLP_GCP_PROJECT to run it \
                 (never run cloud providers without an explicit project id)"
            );
        }
    }

    let out_root = PathBuf::from(
        std::env::var("BENCH_DLP_OUT").unwrap_or_else(|_| "target/bench-dlp".to_string()),
    );
    fs::create_dir_all(&out_root)
        .unwrap_or_else(|e| panic!("create BENCH_DLP_OUT {}: {e}", out_root.display()));

    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let documents_dir = manifest_dir.join("test-fixtures").join("documents");
    let truth_path = manifest_dir
        .join("test-fixtures")
        .join("bench-dlp")
        .join("truth.json");
    let truth: Truth = serde_json::from_str(
        &fs::read_to_string(&truth_path)
            .unwrap_or_else(|e| panic!("read {}: {e}", truth_path.display())),
    )
    .unwrap_or_else(|e| panic!("parse {}: {e}", truth_path.display()));

    // Whole-corpus passes need a single directory holding exactly the seven
    // corpus files. Copy them into a fresh temp dir rather than reading
    // test-fixtures/documents/ directly, so the redacter never sees
    // customer-form.pdf or anything else that lives alongside them.
    let corpus_dir = tempfile::tempdir().expect("create temp corpus dir");
    for name in CORPUS_FILE_NAMES {
        let src = documents_dir.join(name);
        let dst = corpus_dir.path().join(name);
        fs::copy(&src, &dst).unwrap_or_else(|e| {
            panic!(
                "copy corpus fixture {} into temp corpus dir: {e}",
                src.display()
            )
        });
    }

    let file_count = CORPUS_FILE_NAMES.len();
    let mut rows = Vec::new();

    for spec in &specs {
        println!("=== provider: {spec} ===");
        let provider_out = out_root.join(spec);
        if provider_out.exists() {
            fs::remove_dir_all(&provider_out).unwrap_or_else(|e| {
                panic!("clear stale output dir {}: {e}", provider_out.display())
            });
        }
        fs::create_dir_all(&provider_out)
            .unwrap_or_else(|e| panic!("create output dir {}: {e}", provider_out.display()));

        let call_budget = 1 + 3 + file_count;
        if is_cloud_spec(spec) {
            println!(
                "cloud provider: this run will make {call_budget} requests against the corpus \
                 (1 warm-up + 3 timed whole-corpus passes + {file_count} per-file passes)."
            );
        } else {
            println!("local provider: {call_budget} passes planned (no network calls).");
        }

        let mut call_count = 0usize;
        let check_budget = |call_count: usize| {
            assert!(
                call_count <= call_budget,
                "provider {spec} exceeded its call budget ({call_budget})"
            );
        };

        // Warm-up pass (untimed, discarded output).
        let warmup_dir = provider_out.join("warmup");
        invoke_redacter(spec, corpus_dir.path(), &warmup_dir, gcp_project.as_deref());
        call_count += 1;
        check_budget(call_count);

        // 3 timed whole-corpus passes.
        let mut whole_ms = Vec::new();
        for i in 1..=3 {
            let run_dir = provider_out.join(format!("run{i}"));
            let t0 = Instant::now();
            invoke_redacter(spec, corpus_dir.path(), &run_dir, gcp_project.as_deref());
            let ms = t0.elapsed().as_millis();
            whole_ms.push(ms);
            call_count += 1;
            check_budget(call_count);
            println!("  run{i}: {ms} ms");
        }

        // Per-file timed passes.
        let mut per_file_ms = BTreeMap::new();
        for name in CORPUS_FILE_NAMES {
            let file_path = corpus_dir.path().join(name);
            let file_out = provider_out.join("perfile").join(name);
            let t0 = Instant::now();
            invoke_redacter(spec, &file_path, &file_out, gcp_project.as_deref());
            let ms = t0.elapsed().as_millis();
            per_file_ms.insert(name.to_string(), ms);
            call_count += 1;
            check_budget(call_count);
            println!("  perfile {name}: {ms} ms");
        }

        println!("provider {spec}: {call_count} / {call_budget} calls made\n");

        let run1_dir = provider_out.join("run1");
        rows.push(score_provider(
            spec,
            &run1_dir,
            &whole_ms,
            &per_file_ms,
            &truth,
        ));
    }

    let table = format_table(&rows);
    println!("{table}");

    let summary_path = out_root.join("summary.md");
    fs::write(&summary_path, format!("{table}\n"))
        .unwrap_or_else(|e| panic!("write {}: {e}", summary_path.display()));
    println!("\nwritten to {}", summary_path.display());
}

#[cfg(test)]
mod unit_tests {
    use super::*;

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
    fn score_text_counts_removed_pii_kept_controls_and_untouched_entities() {
        let truth = FileTruth {
            pii: vec!["Alice Example".to_string(), "alice@example.com".to_string()],
            entities: vec!["Acme Corp".to_string()],
            keep: vec!["Order#1".to_string()],
        };
        // "Alice Example" was redacted, "alice@example.com" was missed,
        // "Acme Corp" (an entity) was left alone, and "Order#1" survived.
        let text = "Contact: [REDACTED], reachable at alice@example.com, works at Acme Corp. \
                     Reference: Order#1.";

        let score = score_text(text, &truth);

        assert_eq!(score.pii_hits, 1);
        assert_eq!(score.pii_total, 2);
        assert_eq!(score.entities_redacted, 0);
        assert_eq!(score.entities_total, 1);
        assert_eq!(score.keep_survived, 1);
        assert_eq!(score.keep_total, 1);
        assert_eq!(score.redacted_count, 1);
    }

    #[test]
    fn format_table_renders_header_and_one_row_per_provider() {
        let rows = vec![ProviderScore {
            provider: "local-rules".to_string(),
            whole_median_ms: Some(18.0),
            per_file_median_ms: Some(17.0),
            totals: FileScore {
                pii_hits: 20,
                pii_total: 41,
                entities_redacted: 0,
                entities_total: 7,
                keep_survived: 25,
                keep_total: 25,
                redacted_count: 23,
            },
            missing_files: Vec::new(),
        }];

        let table = format_table(&rows);

        assert!(table.contains("| provider | wall time (median of 3)"));
        assert!(table.contains("| local-rules | 18 ms | 17 ms | 20/41 | 0/7 | 25/25 | 23 |"));
    }

    #[test]
    fn format_table_notes_missing_files() {
        let rows = vec![ProviderScore {
            provider: "gcp-dlp".to_string(),
            whole_median_ms: None,
            per_file_median_ms: None,
            totals: FileScore::default(),
            missing_files: vec!["customers.csv".to_string()],
        }];

        let table = format_table(&rows);

        assert!(table.contains("n/a"));
        assert!(table.contains("(missing: customers.csv)"));
    }

    #[test]
    fn is_cloud_spec_matches_bare_and_chained_specs() {
        assert!(is_cloud_spec("gcp-dlp"));
        assert!(is_cloud_spec("local-rules+gcp-vertex-ai"));
        assert!(!is_cloud_spec("local-rules+local-ner"));
    }
}
