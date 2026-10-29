//! The `scan` command's results: the JSON report, and the lines the terminal and the text
//! report show: a row per file in the layout of `cp`, the findings of a file under its row,
//! and the closing summary.
//!
//! Everything here is pure: it takes what the scan found and returns what to write, so the
//! layout is unit tested without running a scan.

use crate::args::RedacterType;
use crate::commands::copy_output::{
    format_file_row, format_legend, NotRedactedReason, OutputStyles, RowLayout, RowOutcome,
    SkipReason, GLYPH_ERROR, GLYPH_OK, GLYPH_SKIPPED,
};
use crate::file_systems::FileSystemRef;
use crate::redacters::{Detection, DetectionLabel, DetectionLocation, Score};
use console::{measure_text_width, pad_str, Alignment};
use indicatif::HumanBytes;
use rvstruct::ValueStruct;
use serde::Serialize;
use std::cmp::Ordering;
use std::collections::BTreeMap;

/// The glyph of a scanned file with findings.
const GLYPH_FINDINGS: &str = "!";

/// The glyph of a scanned file without findings from which the redacters named values they
/// could not place in it, so the file may still hold what they meant.
const GLYPH_UNCONFIRMED: &str = "?";

/// What each glyph of a `scan` row means.
pub const SCAN_LEGEND: [(&str, &str); 5] = [
    (GLYPH_OK, "clean"),
    (GLYPH_FINDINGS, "findings"),
    (GLYPH_UNCONFIRMED, "unconfirmed"),
    (GLYPH_SKIPPED, "skipped"),
    (GLYPH_ERROR, "error"),
];

/// How a finding's matched text is shown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueDisplay {
    /// Only the first and last characters of a long value, so a report can be shared
    /// without spreading what it found.
    Masked,
    /// The value as found, for `--show-values`.
    Raw,
}

/// Masks a value without hashing it, since a hash of a phone number or an ID is reversed by
/// trying every candidate. Each character becomes a `*`, so the length shows, except the first
/// and the last of a value longer than 4 characters.
fn mask_value(value: &str) -> String {
    let length = value.chars().count();
    match (value.chars().next(), value.chars().next_back()) {
        (Some(first), Some(last)) if length > 4 => {
            format!("{first}{}{last}", "*".repeat(length - 2))
        }
        _ => "*".repeat(length),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ScanFileStatus {
    Scanned,
    /// No selected redacter can read the file, and no option changes that: its media type is
    /// not read, or its image format is not one OCR reads.
    Unsupported,
    /// The file needs a converter this build or this run lacks, such as OCR or a PDF renderer.
    Blocked,
    /// `--max-size-limit` or `--filename-filter` left the file out.
    Skipped,
    /// The file could not be read, or a redacter or a converter failed on it.
    Failed,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FindingReport {
    redacter: RedacterType,
    label: DetectionLabel,
    score: Option<Score>,
    location: DetectionLocation,
    value: String,
}

impl FindingReport {
    pub fn new(redacter: RedacterType, detection: &Detection, values: ValueDisplay) -> Self {
        Self {
            redacter,
            label: detection.label.clone(),
            score: detection.score,
            location: detection.location,
            value: match values {
                ValueDisplay::Masked => mask_value(&detection.value),
                ValueDisplay::Raw => detection.value.clone(),
            },
        }
    }

    /// The value for a terminal or a text report, see [`escaped`].
    fn escaped_value(&self) -> String {
        escaped(&self.value)
    }

    /// The label for a terminal or a text report, see [`escaped`]. A model chooses a label
    /// while reading the scanned file, so it can hold what a value can.
    fn escaped_label(&self) -> String {
        escaped(&self.label.to_string())
    }
}

/// Text from a scanned file or a model for a terminal or a text report: it decides what the
/// text holds, and a newline would break a row while an escape sequence would reach the
/// terminal. Both quotes stay as they are, since they are common in names.
fn escaped(text: &str) -> String {
    text.escape_debug()
        .to_string()
        .replace("\\'", "'")
        .replace("\\\"", "\"")
}

/// One file of the scan. Built only through its constructors, which keep `status`, `reason`
/// and `findings` consistent with one another.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ScanFileReport {
    path: String,
    mime_type: Option<String>,
    /// Shown in the file's row and summed in the summary; not part of the JSON report.
    #[serde(skip)]
    size: Option<usize>,
    status: ScanFileStatus,
    reason: Option<String>,
    findings: Vec<FindingReport>,
    /// How many values the redacters named in the file without being able to place them in it,
    /// so a file without findings may still hold what they meant. Only the count is kept, since
    /// a value is the sensitive data itself.
    unlocated_values: usize,
}

impl ScanFileReport {
    /// A scanned file, its findings in the order a reader meets them; findings at the same
    /// place keep the order they are given in.
    pub fn scanned(
        file_ref: &FileSystemRef,
        mut findings: Vec<FindingReport>,
        unlocated_values: usize,
    ) -> Self {
        findings.sort_by(|a, b| {
            a.location
                .partial_cmp(&b.location)
                .unwrap_or(Ordering::Equal)
        });
        Self {
            unlocated_values,
            ..Self::new(file_ref, ScanFileStatus::Scanned, None, findings)
        }
    }

    pub fn not_scanned(file_ref: &FileSystemRef, reason: SkipReason) -> Self {
        let status = match reason {
            SkipReason::OverSizeLimit | SkipReason::NameFilter => ScanFileStatus::Skipped,
            SkipReason::NotRedacted(
                NotRedactedReason::TypeNotSupported
                | NotRedactedReason::NoRedacterApplied
                | NotRedactedReason::OcrImageFormatNotSupported,
            ) => ScanFileStatus::Unsupported,
            SkipReason::NotRedacted(
                NotRedactedReason::PdfRendererUnavailable | NotRedactedReason::OcrUnavailable,
            ) => ScanFileStatus::Blocked,
        };
        Self::new(
            file_ref,
            status,
            Some(reason.detail().to_string()),
            Vec::new(),
        )
    }

    pub fn failed(file_ref: &FileSystemRef, message: String) -> Self {
        Self::new(file_ref, ScanFileStatus::Failed, Some(message), Vec::new())
    }

    /// Whether the file was read, has no findings, and has values the redacters named but could
    /// not locate. Such a file is not known to be clean, and a model's hallucination looks the
    /// same, so it is neither a finding nor clean.
    fn is_unconfirmed(&self) -> bool {
        self.status == ScanFileStatus::Scanned
            && self.findings.is_empty()
            && self.unlocated_values > 0
    }

    /// What the file's row shows: `!` and the number of findings, `?` for a file without findings
    /// whose values could not be located, `✓` for a clean file, `↷` for a file left out or that
    /// no redacter reads, and `✗` for one that failed or is blocked, since both make the scan
    /// incomplete.
    pub fn row<'a>(&self, styles: &'a OutputStyles) -> RowOutcome<'a> {
        let reason = self.reason.clone().unwrap_or_default();
        match self.status {
            ScanFileStatus::Scanned => {
                let (glyph, style) = if !self.findings.is_empty() {
                    (GLYPH_FINDINGS, &styles.found)
                } else if self.is_unconfirmed() {
                    (GLYPH_UNCONFIRMED, &styles.unconfirmed)
                } else {
                    (GLYPH_OK, &styles.ok)
                };
                let mut counts = Vec::new();
                if !self.findings.is_empty() {
                    counts.push(counted(self.findings.len(), "finding"));
                }
                if self.unlocated_values > 0 {
                    counts.push(format!(
                        "{} not located",
                        counted(self.unlocated_values, "value")
                    ));
                }
                RowOutcome {
                    glyph,
                    style,
                    detail: counts.join(", "),
                }
            }
            ScanFileStatus::Unsupported | ScanFileStatus::Skipped => RowOutcome {
                glyph: GLYPH_SKIPPED,
                style: &styles.skipped,
                detail: reason,
            },
            ScanFileStatus::Blocked | ScanFileStatus::Failed => RowOutcome {
                glyph: GLYPH_ERROR,
                style: &styles.error,
                detail: reason,
            },
        }
    }

    fn new(
        file_ref: &FileSystemRef,
        status: ScanFileStatus,
        reason: Option<String>,
        findings: Vec<FindingReport>,
    ) -> Self {
        Self {
            path: file_ref.relative_path.value().clone(),
            mime_type: file_ref.media_type.as_ref().map(ToString::to_string),
            size: file_ref.file_size,
            status,
            reason,
            findings,
            unlocated_values: 0,
        }
    }
}

fn counted(count: usize, noun: &str) -> String {
    if count == 1 {
        format!("1 {noun}")
    } else {
        format!("{count} {noun}s")
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
#[serde(transparent)]
pub struct FindingsByLabel(BTreeMap<DetectionLabel, usize>);

/// What `scan` reports. Fields are only ever added, so a consumer of the JSON report can rely
/// on every one here.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct ScanCommandResult {
    pub files_scanned: usize,
    pub files_with_findings: usize,
    pub files_unconfirmed: usize,
    pub files_skipped: usize,
    pub files_unsupported: usize,
    pub files_blocked: usize,
    pub files_failed: usize,
    pub findings: usize,
    pub findings_by_label: FindingsByLabel,
    pub files: Vec<ScanFileReport>,
}

impl ScanCommandResult {
    /// Adds one file and counts it.
    pub fn push(&mut self, file: ScanFileReport) {
        match file.status {
            ScanFileStatus::Scanned => {
                self.files_scanned += 1;
                if !file.findings.is_empty() {
                    self.files_with_findings += 1;
                } else if file.is_unconfirmed() {
                    self.files_unconfirmed += 1;
                }
            }
            ScanFileStatus::Unsupported => self.files_unsupported += 1,
            ScanFileStatus::Blocked => self.files_blocked += 1,
            ScanFileStatus::Skipped => self.files_skipped += 1,
            ScanFileStatus::Failed => self.files_failed += 1,
        }
        self.findings += file.findings.len();
        for finding in &file.findings {
            *self
                .findings_by_label
                .0
                .entry(finding.label.clone())
                .or_default() += 1;
        }
        self.files.push(file);
    }

    /// The process exit code: 1 when a file that should have been scanned was not, because it
    /// failed or because the run lacks a converter it needs; 2 when something was found,
    /// or with `fail_on_unlocated` when a file is unconfirmed, unless `no_fail`; 0 otherwise.
    pub fn exit_code(&self, no_fail: bool, fail_on_unlocated: bool) -> i32 {
        let found = self.findings > 0 || (fail_on_unlocated && self.files_unconfirmed > 0);
        if self.files_failed > 0 || self.files_blocked > 0 {
            1
        } else if found && !no_fail {
            2
        } else {
            0
        }
    }
}

impl ScanCommandResult {
    /// The report for people, without terminal colours: the legend, each file's row with its
    /// findings under it, and the summary. Rows are as wide as the longest path needs, so no
    /// path is shortened.
    pub fn to_text(&self) -> String {
        let styles = OutputStyles::plain();
        let layout = RowLayout::for_report(
            self.files
                .iter()
                .map(|file| measure_text_width(&file.path))
                .max()
                .unwrap_or(0),
        );
        let mut lines = vec![format_legend(&SCAN_LEGEND, &styles)];
        for file in &self.files {
            lines.extend(format_file_lines(file, layout, &styles));
        }
        lines.push(String::new());
        lines.push(format_scan_summary(self, &styles));
        lines.join("\n") + "\n"
    }
}

/// The lines of one file: its row in `layout`, then its findings.
pub fn format_file_lines(
    file: &ScanFileReport,
    layout: RowLayout,
    styles: &OutputStyles,
) -> Vec<String> {
    let mut lines = vec![format_file_row(
        layout,
        &file.path,
        file.mime_type.as_deref().unwrap_or_default(),
        &HumanBytes(file.size.unwrap_or(0) as u64).to_string(),
        &file.row(styles),
        styles,
    )];
    lines.extend(format_finding_lines(file, styles));
    lines
}

/// The findings of a file, a line each, indented under its row and in columns as wide as the
/// widest value of the file: the location, the label, the redacter, the score, and the value.
/// The score column is left out when no finding of the file has a score.
fn format_finding_lines(file: &ScanFileReport, styles: &OutputStyles) -> Vec<String> {
    fn widest(findings: &[FindingReport], cell: impl Fn(&FindingReport) -> String) -> usize {
        findings
            .iter()
            .map(|finding| measure_text_width(&cell(finding)))
            .max()
            .unwrap_or(0)
    }
    let score = |finding: &FindingReport| {
        finding
            .score
            .map(|score| score.to_string())
            .unwrap_or_default()
    };
    let location_width = widest(&file.findings, |finding| finding.location.to_string());
    let label_width = widest(&file.findings, FindingReport::escaped_label);
    let redacter_width = widest(&file.findings, |finding| finding.redacter.to_string());
    let score_width = widest(&file.findings, score);
    let pad = |cell: String, width| pad_str(&cell, width, Alignment::Left, None).into_owned();
    file.findings
        .iter()
        .map(|finding| {
            let mut line = format!(
                "    {}  {}  {}  ",
                pad(finding.location.to_string(), location_width),
                styles
                    .label
                    .apply_to(pad(finding.escaped_label(), label_width)),
                styles
                    .dimmed
                    .apply_to(pad(finding.redacter.to_string(), redacter_width)),
            );
            if score_width > 0 {
                line.push_str(&format!(
                    "{}  ",
                    styles.dimmed.apply_to(pad(score(finding), score_width))
                ));
            }
            line.push_str(&styles.found.apply_to(finding.escaped_value()).to_string());
            line
        })
        .collect()
}

/// The closing summary: the file counts, the findings by label, and the size of the files.
pub fn format_scan_summary(result: &ScanCommandResult, styles: &OutputStyles) -> String {
    let total = result.files_scanned
        + result.files_skipped
        + result.files_unsupported
        + result.files_blocked
        + result.files_failed;
    let mut line = format!(
        "{} {}: {} with findings, {} clean, {} skipped, {} unsupported",
        styles.highlighted.apply_to(total),
        if total == 1 { "file" } else { "files" },
        styles.found.apply_to(result.files_with_findings),
        styles
            .highlighted
            .apply_to(result.files_scanned - result.files_with_findings - result.files_unconfirmed),
        styles.highlighted.apply_to(result.files_skipped),
        styles.highlighted.apply_to(result.files_unsupported),
    );
    if result.files_unconfirmed > 0 {
        line.push_str(&format!(
            ", {} unconfirmed",
            styles.unconfirmed.apply_to(result.files_unconfirmed)
        ));
    }
    if result.files_blocked > 0 {
        line.push_str(&format!(
            ", {} blocked",
            styles.error.apply_to(result.files_blocked)
        ));
    }
    if result.files_failed > 0 {
        line.push_str(&format!(
            ", {} failed",
            styles.error.apply_to(result.files_failed)
        ));
    }
    line.push_str(&format!(
        ". {} {}",
        styles.found.apply_to(result.findings),
        if result.findings == 1 {
            "finding"
        } else {
            "findings"
        }
    ));
    if !result.findings_by_label.0.is_empty() {
        let by_label: Vec<String> = result
            .findings_by_label
            .0
            .iter()
            .map(|(label, count)| {
                format!(
                    "{} {count}",
                    styles.label.apply_to(escaped(&label.to_string()))
                )
            })
            .collect();
        line.push_str(&format!(": {}", by_label.join(", ")));
    }
    let total_size: usize = result.files.iter().filter_map(|file| file.size).sum();
    line.push_str(&format!(
        ". Total size: {}",
        styles.highlighted.apply_to(HumanBytes(total_size as u64))
    ));
    line
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file_ref(path: &str, media_type: mime::Mime) -> FileSystemRef {
        FileSystemRef {
            relative_path: path.into(),
            media_type: Some(media_type),
            file_size: None,
        }
    }

    fn finding(
        label: &str,
        location: DetectionLocation,
        score: Option<f32>,
        value: &str,
    ) -> FindingReport {
        FindingReport {
            redacter: RedacterType::LocalRules,
            label: DetectionLabel::new(label),
            score: score.map(|score| Score::try_from(score).unwrap()),
            location,
            value: mask_value(value),
        }
    }

    fn sample_result() -> ScanCommandResult {
        let mut result = ScanCommandResult::default();
        result.push(ScanFileReport::scanned(
            &file_ref("customer-note.txt", mime::TEXT_PLAIN),
            vec![finding(
                "email",
                DetectionLocation::Text {
                    start: 40,
                    end: 62,
                    line: 3,
                    column: 15,
                },
                None,
                "john.smith@example.com",
            )],
            0,
        ));
        result.push(ScanFileReport::scanned(
            &file_ref("customers.csv", mime::TEXT_CSV),
            vec![finding(
                "person",
                DetectionLocation::TableCell {
                    row: 1,
                    column: 2,
                    start: 0,
                    end: 4,
                },
                Some(0.75),
                "Akira",
            )],
            1,
        ));
        result.push(ScanFileReport::scanned(
            &file_ref("form.pdf", mime::APPLICATION_PDF),
            vec![finding(
                "email",
                DetectionLocation::Image {
                    page: Some(1),
                    x1: 310.2,
                    y1: 193.0,
                    x2: 621.6,
                    y2: 218.0,
                },
                None,
                "j@b.io",
            )],
            0,
        ));
        result.push(ScanFileReport::scanned(
            &file_ref("clean.txt", mime::TEXT_PLAIN),
            Vec::new(),
            0,
        ));
        result.push(ScanFileReport::not_scanned(
            &file_ref("scan.png", mime::IMAGE_PNG),
            SkipReason::NotRedacted(NotRedactedReason::OcrUnavailable),
        ));
        result.push(ScanFileReport::not_scanned(
            &file_ref("archive.bin", mime::APPLICATION_OCTET_STREAM),
            SkipReason::NotRedacted(NotRedactedReason::TypeNotSupported),
        ));
        result.push(ScanFileReport::not_scanned(
            &file_ref("huge.txt", mime::TEXT_PLAIN),
            SkipReason::OverSizeLimit,
        ));
        result
    }

    #[test]
    fn a_long_value_keeps_only_its_first_and_last_characters() {
        assert_eq!(
            mask_value("john.smith@example.com"),
            "j********************m"
        );
        assert_eq!(mask_value("S3cret!"), "S*****!");
        assert_eq!(mask_value("Jürgen"), "J****n");
        assert_eq!(mask_value("Akira"), "A***a");
        assert_eq!(mask_value("1234"), "****");
        assert_eq!(mask_value("ab"), "**");
    }

    fn email_detection() -> Detection {
        Detection {
            label: DetectionLabel::new("email"),
            location: DetectionLocation::Text {
                start: 5,
                end: 27,
                line: 1,
                column: 6,
            },
            score: None,
            value: "john.smith@example.com".to_string(),
        }
    }

    #[test]
    fn raw_values_are_reported_only_when_asked_for_in_both_report_formats() {
        for (values, shown, hidden) in [
            (
                ValueDisplay::Masked,
                "j********************m",
                "john.smith@example.com",
            ),
            (
                ValueDisplay::Raw,
                "john.smith@example.com",
                "j********************m",
            ),
        ] {
            let mut result = ScanCommandResult::default();
            result.push(ScanFileReport::scanned(
                &file_ref("note.txt", mime::TEXT_PLAIN),
                vec![FindingReport::new(
                    RedacterType::LocalRules,
                    &email_detection(),
                    values,
                )],
                0,
            ));
            let json = serde_json::to_string(&result).unwrap();
            for report in [result.to_text(), json] {
                assert!(report.contains(shown), "{values:?}: {report}");
                assert!(!report.contains(hidden), "{values:?}: {report}");
            }
        }
    }

    #[test]
    fn a_value_is_escaped_in_the_terminal_rows_and_the_text_report_but_not_in_the_json_report() {
        let value = "-----BEGIN KEY-----\nabc\n\u{1b}[31m O'Brien";
        let detection = Detection {
            value: value.to_string(),
            ..email_detection()
        };
        let mut result = ScanCommandResult::default();
        result.push(ScanFileReport::scanned(
            &file_ref("key.pem", mime::TEXT_PLAIN),
            vec![FindingReport::new(
                RedacterType::LocalRules,
                &detection,
                ValueDisplay::Raw,
            )],
            0,
        ));

        let rows = format_file_lines(
            &result.files[0],
            RowLayout::for_terminal(100),
            &OutputStyles::plain(),
        );
        assert_eq!(
            rows[1..],
            ["    1:6  email  local-rules  -----BEGIN KEY-----\\nabc\\n\\u{1b}[31m O'Brien"]
        );
        let text = result.to_text();
        assert!(!text.contains(['\u{1b}', '\r']));
        assert_eq!(text.lines().count(), 5, "{text}");
        let json: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&result).unwrap()).unwrap();
        assert_eq!(json["files"][0]["findings"][0]["value"], value);
    }

    #[test]
    fn a_label_is_escaped_in_the_rows_the_text_report_and_the_summary() {
        let label = "ssn\n\u{1b}[31mred";
        let mut result = ScanCommandResult::default();
        result.push(ScanFileReport::scanned(
            &file_ref("note.txt", mime::TEXT_PLAIN),
            vec![finding(
                label,
                DetectionLocation::Text {
                    start: 0,
                    end: 6,
                    line: 1,
                    column: 1,
                },
                None,
                "a@b.io",
            )],
            0,
        ));

        let rows = format_file_lines(
            &result.files[0],
            RowLayout::for_terminal(100),
            &OutputStyles::plain(),
        );
        assert_eq!(
            rows[1..],
            ["    1:1  ssn\\n\\u{1b}[31mred  local-rules  a****o"]
        );
        let text = result.to_text();
        assert!(!text.contains('\u{1b}'), "{text}");
        assert_eq!(text.lines().count(), 5, "{text}");
        let summary = format_scan_summary(&result, &OutputStyles::plain());
        assert!(summary.contains("ssn\\n\\u{1b}[31mred 1"), "{summary}");
        let json: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&result).unwrap()).unwrap();
        assert_eq!(json["files"][0]["findings"][0]["label"], label);
    }

    #[test]
    fn the_text_report_pads_the_path_to_the_longest_path_and_no_further() {
        let mut result = ScanCommandResult::default();
        for name in ["customer-note.txt", "a.txt"] {
            result.push(ScanFileReport::scanned(
                &file_ref(name, mime::TEXT_PLAIN),
                Vec::new(),
                0,
            ));
        }

        let text = result.to_text();

        let rows: Vec<&str> = text.lines().skip(1).take(2).collect();
        assert_eq!(
            rows,
            [
                format!("✓ {:<17} {:<24} 0 B", "customer-note.txt", "text/plain"),
                format!("✓ {:<17} {:<24} 0 B", "a.txt", "text/plain"),
            ]
        );
    }

    #[test]
    fn the_json_report_counts_files_and_places_each_finding() {
        // Read back from the written text: a value built in memory would widen every `f32`.
        let written = serde_json::to_string(&sample_result()).unwrap();
        let json: serde_json::Value = serde_json::from_str(&written).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "files_scanned": 4,
                "files_with_findings": 3,
                "files_unconfirmed": 0,
                "files_skipped": 1,
                "files_unsupported": 1,
                "files_blocked": 1,
                "files_failed": 0,
                "findings": 3,
                "findings_by_label": {"email": 2, "person": 1},
                "files": [
                    {"path": "customer-note.txt", "mime_type": "text/plain", "status": "scanned",
                     "reason": null, "unlocated_values": 0, "findings": [
                        {"redacter": "local-rules", "label": "email", "score": null,
                         "location": {"kind": "text", "start": 40, "end": 62, "line": 3, "column": 15},
                         "value": "j********************m"}]},
                    {"path": "customers.csv", "mime_type": "text/csv", "status": "scanned",
                     "reason": null, "unlocated_values": 1, "findings": [
                        {"redacter": "local-rules", "label": "person", "score": 0.75,
                         "location": {"kind": "table_cell", "row": 1, "column": 2, "start": 0, "end": 4},
                         "value": "A***a"}]},
                    {"path": "form.pdf", "mime_type": "application/pdf", "status": "scanned",
                     "reason": null, "unlocated_values": 0, "findings": [
                        {"redacter": "local-rules", "label": "email", "score": null,
                         "location": {"kind": "image", "page": 1, "x1": 310.2, "y1": 193.0,
                                      "x2": 621.6, "y2": 218.0},
                         "value": "j****o"}]},
                    {"path": "clean.txt", "mime_type": "text/plain", "status": "scanned",
                     "reason": null, "unlocated_values": 0, "findings": []},
                    {"path": "scan.png", "mime_type": "image/png", "status": "blocked",
                     "reason": "ocr unavailable", "unlocated_values": 0, "findings": []},
                    {"path": "archive.bin", "mime_type": "application/octet-stream",
                     "status": "unsupported", "reason": "type not supported", "unlocated_values": 0, "findings": []},
                    {"path": "huge.txt", "mime_type": "text/plain", "status": "skipped",
                     "reason": "over size limit", "unlocated_values": 0, "findings": []}
                ]
            })
        );
    }

    #[test]
    fn the_text_report_lists_each_file_with_its_findings_under_it() {
        let long_path = format!("{}/notes.txt", "nested".repeat(20));
        let mut result = sample_result();
        result.push(ScanFileReport::not_scanned(
            &file_ref(&long_path, mime::TEXT_PLAIN),
            SkipReason::NameFilter,
        ));
        let email_at = |line, start| {
            finding(
                "email",
                DetectionLocation::Text {
                    start,
                    end: start + 6,
                    line,
                    column: 1,
                },
                None,
                if line == 1 { "a@b.io" } else { "c@d.io" },
            )
        };
        result.push(ScanFileReport::scanned(
            &file_ref("two.txt", mime::TEXT_PLAIN),
            vec![email_at(1, 0), email_at(2, 7)],
            0,
        ));
        result.push(ScanFileReport::scanned(
            &file_ref("reformatted.txt", mime::TEXT_PLAIN),
            Vec::new(),
            2,
        ));
        result.push(ScanFileReport::failed(
            &file_ref("broken.txt", mime::TEXT_PLAIN),
            "provider unavailable".to_string(),
        ));

        let text = result.to_text();

        let lines: Vec<String> = text
            .lines()
            .map(|line| line.split_whitespace().collect::<Vec<&str>>().join(" "))
            .collect();
        assert_eq!(
            lines,
            [
                "✓ clean ! findings ? unconfirmed ↷ skipped ✗ error",
                "! customer-note.txt text/plain 0 B 1 finding",
                "3:15 email local-rules j********************m",
                "! customers.csv text/csv 0 B 1 finding, 1 value not located",
                "1:2 person local-rules 0.75 A***a",
                "! form.pdf application/pdf 0 B 1 finding",
                "1:[310,193,622,218] email local-rules j****o",
                "✓ clean.txt text/plain 0 B",
                "✗ scan.png image/png 0 B ocr unavailable",
                "↷ archive.bin application/octet-stream 0 B type not supported",
                "↷ huge.txt text/plain 0 B over size limit",
                &format!("↷ {long_path} text/plain 0 B name filter"),
                "! two.txt text/plain 0 B 2 findings",
                "1:1 email local-rules a****o",
                "2:1 email local-rules c****o",
                "? reformatted.txt text/plain 0 B 2 values not located",
                "✗ broken.txt text/plain 0 B provider unavailable",
                "",
                "11 files: 4 with findings, 1 clean, 2 skipped, 1 unsupported, 1 unconfirmed, \
                 1 blocked, 1 failed. 5 findings: email 4, person 1. Total size: 0 B",
            ]
        );
        assert!(
            !text.contains('\u{1b}'),
            "the text report has no ANSI escapes"
        );
    }

    #[test]
    fn the_findings_of_a_file_are_aligned_and_an_unscored_one_has_an_empty_score() {
        let text_at = |line, column| DetectionLocation::Text {
            start: 0,
            end: 1,
            line,
            column,
        };
        let found =
            |redacter, label: &str, location, score: Option<f32>, value: &str| FindingReport {
                redacter,
                label: DetectionLabel::new(label),
                score: score.map(|score| Score::try_from(score).unwrap()),
                location,
                value: value.to_string(),
            };
        let styles = OutputStyles::plain();
        let mixed = ScanFileReport::scanned(
            &file_ref("customer-note.txt", mime::TEXT_PLAIN),
            vec![
                found(
                    RedacterType::LocalNer,
                    "per",
                    text_at(3, 11),
                    Some(1.0),
                    "J***h",
                ),
                found(
                    RedacterType::LocalRules,
                    "email",
                    text_at(4, 8),
                    None,
                    "j***m",
                ),
                found(
                    RedacterType::LocalRules,
                    "payment-card",
                    text_at(11, 34),
                    None,
                    "4***1",
                ),
            ],
            0,
        );
        let unscored = ScanFileReport::scanned(
            &file_ref("notes.txt", mime::TEXT_PLAIN),
            vec![found(
                RedacterType::LocalRules,
                "email",
                text_at(1, 10),
                None,
                "j***m",
            )],
            0,
        );

        assert_eq!(
            format_file_lines(&mixed, RowLayout::for_terminal(100), &styles)[1..],
            [
                "    3:11   per           local-ner    1.00  J***h",
                "    4:8    email         local-rules        j***m",
                "    11:34  payment-card  local-rules        4***1",
            ]
        );
        assert_eq!(
            format_file_lines(&unscored, RowLayout::for_terminal(100), &styles)[1..],
            ["    1:10  email  local-rules  j***m"]
        );
    }

    #[test]
    fn the_findings_of_a_file_are_listed_in_reading_order() {
        let at = |line, column, start| DetectionLocation::Text {
            start,
            end: start + 6,
            line,
            column,
        };
        let file = ScanFileReport::scanned(
            &file_ref("note.txt", mime::TEXT_PLAIN),
            vec![
                finding("email", at(2, 1, 20), None, "c@d.io"),
                finding("email", at(1, 5, 4), None, "a@b.io"),
                finding("person", at(1, 5, 4), Some(0.9), "a@b.io"),
            ],
            0,
        );
        let order: Vec<(&DetectionLocation, &DetectionLabel)> = file
            .findings
            .iter()
            .map(|finding| (&finding.location, &finding.label))
            .collect();
        assert_eq!(
            order,
            vec![
                (&at(1, 5, 4), &DetectionLabel::new("email")),
                (&at(1, 5, 4), &DetectionLabel::new("person")),
                (&at(2, 1, 20), &DetectionLabel::new("email")),
            ]
        );
    }

    #[test]
    fn the_exit_code_is_1_for_an_incomplete_scan_2_for_findings_and_0_otherwise() {
        let note = file_ref("note.txt", mime::TEXT_PLAIN);
        let found = || {
            ScanFileReport::scanned(
                &note,
                vec![finding(
                    "email",
                    DetectionLocation::Text {
                        start: 0,
                        end: 6,
                        line: 1,
                        column: 1,
                    },
                    None,
                    "a@b.io",
                )],
                0,
            )
        };
        let unconfirmed = || ScanFileReport::scanned(&note, Vec::new(), 1);
        let not_read = |name, media_type, reason| {
            ScanFileReport::not_scanned(
                &file_ref(name, media_type),
                SkipReason::NotRedacted(reason),
            )
        };
        let blocked = || {
            not_read(
                "scan.png",
                mime::IMAGE_PNG,
                NotRedactedReason::OcrUnavailable,
            )
        };
        // Exit codes by default, with `no_fail`, with `fail_on_unlocated`, and with both.
        for (files, expected) in [
            (vec![], [0, 0, 0, 0]),
            (vec![found()], [2, 0, 2, 0]),
            (vec![unconfirmed()], [0, 0, 2, 0]),
            (vec![found(), unconfirmed()], [2, 0, 2, 0]),
            (
                vec![
                    found(),
                    ScanFileReport::failed(&note, "unavailable".to_string()),
                ],
                [1, 1, 1, 1],
            ),
            (vec![found(), blocked()], [1, 1, 1, 1]),
            (vec![unconfirmed(), blocked()], [1, 1, 1, 1]),
            (
                vec![not_read(
                    "blob.bin",
                    mime::APPLICATION_OCTET_STREAM,
                    NotRedactedReason::TypeNotSupported,
                )],
                [0, 0, 0, 0],
            ),
            (
                vec![not_read(
                    "logo.svg",
                    mime::IMAGE_SVG,
                    NotRedactedReason::OcrImageFormatNotSupported,
                )],
                [0, 0, 0, 0],
            ),
        ] {
            let mut result = ScanCommandResult::default();
            for file in files {
                result.push(file);
            }
            let exit_codes = [(false, false), (true, false), (false, true), (true, true)]
                .map(|(no_fail, fail_on_unlocated)| result.exit_code(no_fail, fail_on_unlocated));
            assert_eq!(exit_codes, expected, "{result:?}");
        }
    }
}
