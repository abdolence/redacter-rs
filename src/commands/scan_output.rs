//! The `scan` command's results: the JSON report, and the lines the terminal and the text
//! report show for each finding, each file that was not scanned, and the closing summary.
//!
//! Everything here is pure: it takes what the scan found and returns what to write, so the
//! layout is unit tested without running a scan.

use crate::args::RedacterType;
use crate::commands::copy_output::{NotRedactedReason, SkipReason};
use crate::commands::report::Report;
use crate::file_systems::FileSystemRef;
use crate::redacters::{DetectionLabel, DetectionLocation, ScannedDetection, Score, TextPosition};
use console::Style;
use rvstruct::ValueStruct;
use serde::Serialize;
use std::collections::BTreeMap;

/// How a finding's matched text is shown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueDisplay {
    /// Only the first and last characters of a long value, so a report can be shared
    /// without spreading what it found.
    Masked,
    /// The value as found, for `--show-values`.
    Raw,
}

/// Masks a value without hashing it: a hash of a phone number or an ID is reversed by trying
/// every candidate.
fn mask_value(value: &str) -> String {
    let mut chars = value.chars();
    match (chars.next(), chars.next_back()) {
        (Some(first), Some(last)) if value.chars().count() >= 6 => format!("{first}***{last}"),
        _ => "***".to_string(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ScanFileStatus {
    Scanned,
    /// No selected redacter reads the file's media type.
    Unsupported,
    /// The file needs a converter this build or this run lacks, such as OCR or a PDF renderer.
    Blocked,
    /// `--max-size-limit` or `--filename-filter` left the file out.
    Skipped,
    /// A redacter or a converter failed on the file.
    Failed,
}

/// A detection's location as a report shows it. Byte offsets count from 0; lines, columns,
/// table rows and columns, and PDF pages count from 1, as editors and spreadsheets do.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ReportLocation {
    Text {
        start: usize,
        end: usize,
        #[serde(flatten)]
        position: Option<TextPosition>,
    },
    TableCell {
        row: usize,
        column: usize,
        start: usize,
        end: usize,
    },
    Image {
        page: Option<usize>,
        x1: f32,
        y1: f32,
        x2: f32,
        y2: f32,
    },
}

impl ReportLocation {
    fn new(location: DetectionLocation, position: Option<TextPosition>) -> Self {
        match location {
            DetectionLocation::Text { start, end } => ReportLocation::Text {
                start,
                end,
                position,
            },
            DetectionLocation::TableCell {
                row,
                column,
                start,
                end,
            } => ReportLocation::TableCell {
                row: row + 1,
                column: column + 1,
                start,
                end,
            },
            DetectionLocation::Image {
                page,
                x1,
                y1,
                x2,
                y2,
            } => ReportLocation::Image {
                page: page.map(|page| page + 1),
                x1,
                y1,
                x2,
                y2,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FindingReport {
    redacter: RedacterType,
    label: DetectionLabel,
    score: Option<Score>,
    location: ReportLocation,
    value: String,
}

impl FindingReport {
    pub fn new(scanned: &ScannedDetection, values: ValueDisplay) -> Self {
        let detection = scanned.detection();
        Self {
            redacter: scanned.redacter(),
            label: detection.label.clone(),
            score: detection.score,
            location: ReportLocation::new(detection.location, scanned.position()),
            value: match values {
                ValueDisplay::Masked => mask_value(&detection.value),
                ValueDisplay::Raw => detection.value.clone(),
            },
        }
    }
}

/// One file of the scan. Built only through its constructors, which keep `status`, `reason`
/// and `findings` consistent with one another.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ScanFileReport {
    path: String,
    mime_type: Option<String>,
    status: ScanFileStatus,
    reason: Option<String>,
    findings: Vec<FindingReport>,
}

impl ScanFileReport {
    pub fn scanned(file_ref: &FileSystemRef, findings: Vec<FindingReport>) -> Self {
        Self::new(file_ref, ScanFileStatus::Scanned, None, findings)
    }

    pub fn not_scanned(file_ref: &FileSystemRef, reason: SkipReason) -> Self {
        let status = match reason {
            SkipReason::OverSizeLimit | SkipReason::NameFilter => ScanFileStatus::Skipped,
            SkipReason::NotRedacted(NotRedactedReason::TypeNotSupported) => {
                ScanFileStatus::Unsupported
            }
            SkipReason::NotRedacted(_) => ScanFileStatus::Blocked,
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

    fn new(
        file_ref: &FileSystemRef,
        status: ScanFileStatus,
        reason: Option<String>,
        findings: Vec<FindingReport>,
    ) -> Self {
        Self {
            path: file_ref.relative_path.value().clone(),
            mime_type: file_ref.media_type.as_ref().map(ToString::to_string),
            status,
            reason,
            findings,
        }
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
    /// unless `no_fail`; 0 otherwise.
    pub fn exit_code(&self, no_fail: bool) -> i32 {
        if self.files_failed > 0 || self.files_blocked > 0 {
            1
        } else if self.findings > 0 && !no_fail {
            2
        } else {
            0
        }
    }
}

impl Report for ScanCommandResult {
    fn to_text(&self) -> String {
        let styles = ScanOutputStyles::plain();
        let mut text = String::new();
        for file in &self.files {
            for line in format_file_lines(file, &styles) {
                text.push_str(&line);
                text.push('\n');
            }
        }
        if !text.is_empty() {
            text.push('\n');
        }
        text.push_str(&format_scan_summary(self, &styles));
        text.push('\n');
        text
    }
}

/// Styles used by the `scan` output, kept together so the text report renders the same
/// lines without any styling at all.
#[derive(Debug, Clone)]
pub struct ScanOutputStyles {
    pub location: Style,
    pub label: Style,
    pub value: Style,
    pub dimmed: Style,
    pub highlighted: Style,
    pub found: Style,
    pub skipped: Style,
    pub error: Style,
}

impl ScanOutputStyles {
    pub fn new() -> Self {
        Self {
            location: Style::new().bold().white(),
            label: Style::new().yellow(),
            value: Style::new().red(),
            dimmed: Style::new().dim(),
            highlighted: Style::new().bold().white(),
            found: Style::new().bold().red(),
            skipped: Style::new().dim().yellow(),
            error: Style::new().red(),
        }
    }

    pub fn plain() -> Self {
        Self {
            location: Style::new(),
            label: Style::new(),
            value: Style::new(),
            dimmed: Style::new(),
            highlighted: Style::new(),
            found: Style::new(),
            skipped: Style::new(),
            error: Style::new(),
        }
    }
}

impl Default for ScanOutputStyles {
    fn default() -> Self {
        Self::new()
    }
}

/// The line that opens a scan: what is scanned, by what, and which converters this run has.
pub fn format_scan_header(
    source: &str,
    redacters: &[RedacterType],
    pdf_rendering: bool,
    ocr: bool,
    styles: &ScanOutputStyles,
) -> String {
    let yes_no = |enabled: bool| {
        if enabled {
            styles.highlighted.apply_to("yes")
        } else {
            styles.dimmed.apply_to("no")
        }
    };
    format!(
        "Scanning {} with {}. PDF rendering: {} · OCR: {}",
        styles.highlighted.apply_to(source),
        styles.label.apply_to(
            redacters
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<String>>()
                .join(", ")
        ),
        yes_no(pdf_rendering),
        yes_no(ocr),
    )
}

/// Where a finding is, grep-style: `path:line:column` for text, `path:row:column` for a
/// table cell, `path:[x1,y1,x2,y2]` for an image and `path:page:[x1,y1,x2,y2]` for a PDF page.
fn format_location(path: &str, location: &ReportLocation) -> String {
    match location {
        ReportLocation::Text {
            position: Some(position),
            ..
        } => format!("{path}:{}:{}", position.line, position.column),
        ReportLocation::Text {
            start,
            position: None,
            ..
        } => format!("{path}:byte {start}"),
        ReportLocation::TableCell { row, column, .. } => format!("{path}:{row}:{column}"),
        ReportLocation::Image {
            page,
            x1,
            y1,
            x2,
            y2,
        } => {
            let image_box = format!(
                "[{},{},{},{}]",
                x1.round(),
                y1.round(),
                x2.round(),
                y2.round()
            );
            match page {
                Some(page) => format!("{path}:{page}:{image_box}"),
                None => format!("{path}:{image_box}"),
            }
        }
    }
}

/// The lines for one file: a line per finding, or one line saying why it was not scanned.
/// A scanned file without findings has none, as with grep.
pub fn format_file_lines(file: &ScanFileReport, styles: &ScanOutputStyles) -> Vec<String> {
    let (status, status_style) = match file.status {
        ScanFileStatus::Scanned => {
            return file
                .findings
                .iter()
                .map(|finding| format_finding_line(&file.path, finding, styles))
                .collect()
        }
        ScanFileStatus::Unsupported => ("unsupported", &styles.skipped),
        ScanFileStatus::Blocked => ("blocked", &styles.skipped),
        ScanFileStatus::Skipped => ("skipped", &styles.skipped),
        ScanFileStatus::Failed => ("failed", &styles.error),
    };
    vec![format!(
        "{}  {}",
        styles.location.apply_to(&file.path),
        status_style.apply_to(match &file.reason {
            Some(reason) => format!("{status}: {reason}"),
            None => status.to_string(),
        })
    )]
}

fn format_finding_line(path: &str, finding: &FindingReport, styles: &ScanOutputStyles) -> String {
    format!(
        "{}  {}  {}  {}  {}",
        styles
            .location
            .apply_to(format_location(path, &finding.location)),
        styles.label.apply_to(&finding.label),
        styles.dimmed.apply_to(finding.redacter),
        styles.dimmed.apply_to(
            finding
                .score
                .map_or_else(|| "-".to_string(), |score| score.to_string())
        ),
        styles.value.apply_to(&finding.value),
    )
}

/// The closing summary: the file counts, then the findings by label.
pub fn format_scan_summary(result: &ScanCommandResult, styles: &ScanOutputStyles) -> String {
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
            .apply_to(result.files_scanned - result.files_with_findings),
        styles.highlighted.apply_to(result.files_skipped),
        styles.highlighted.apply_to(result.files_unsupported),
    );
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
            .map(|(label, count)| format!("{} {count}", styles.label.apply_to(label)))
            .collect();
        line.push_str(&format!(": {}", by_label.join(", ")));
    }
    line.push('.');
    line
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::redacters::Detection;

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
            location: ReportLocation::new(
                location,
                Some(TextPosition {
                    line: 3,
                    column: 15,
                }),
            ),
            value: mask_value(value),
        }
    }

    fn sample_result() -> ScanCommandResult {
        let mut result = ScanCommandResult::default();
        result.push(ScanFileReport::scanned(
            &file_ref("customer-note.txt", mime::TEXT_PLAIN),
            vec![finding(
                "email",
                DetectionLocation::Text { start: 40, end: 62 },
                None,
                "john.smith@example.com",
            )],
        ));
        result.push(ScanFileReport::scanned(
            &file_ref("customers.csv", mime::TEXT_CSV),
            vec![finding(
                "person",
                DetectionLocation::TableCell {
                    row: 0,
                    column: 1,
                    start: 0,
                    end: 4,
                },
                Some(0.75),
                "Akira",
            )],
        ));
        result.push(ScanFileReport::scanned(
            &file_ref("form.pdf", mime::APPLICATION_PDF),
            vec![finding(
                "email",
                DetectionLocation::Image {
                    page: Some(0),
                    x1: 310.2,
                    y1: 193.0,
                    x2: 621.6,
                    y2: 218.0,
                },
                None,
                "j@b.io",
            )],
        ));
        result.push(ScanFileReport::scanned(
            &file_ref("clean.txt", mime::TEXT_PLAIN),
            Vec::new(),
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
        assert_eq!(mask_value("john.smith@example.com"), "j***m");
        assert_eq!(mask_value("Jürgen"), "J***n");
        assert_eq!(mask_value("Akira"), "***");
        assert_eq!(mask_value(""), "***");
    }

    #[test]
    fn raw_values_are_reported_only_when_asked_for() {
        let text = "mail john.smith@example.com";
        let scanned = |values| {
            let detection = Detection {
                label: DetectionLabel::new("email"),
                location: DetectionLocation::Text { start: 5, end: 27 },
                score: None,
                value: text[5..27].to_string(),
            };
            FindingReport::new(
                &ScannedDetection::without_position(RedacterType::LocalRules, detection),
                values,
            )
            .value
        };
        assert_eq!(scanned(ValueDisplay::Masked), "j***m");
        assert_eq!(scanned(ValueDisplay::Raw), "john.smith@example.com");
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
                "files_skipped": 1,
                "files_unsupported": 1,
                "files_blocked": 1,
                "files_failed": 0,
                "findings": 3,
                "findings_by_label": {"email": 2, "person": 1},
                "files": [
                    {"path": "customer-note.txt", "mime_type": "text/plain", "status": "scanned",
                     "reason": null, "findings": [
                        {"redacter": "local-rules", "label": "email", "score": null,
                         "location": {"kind": "text", "start": 40, "end": 62, "line": 3, "column": 15},
                         "value": "j***m"}]},
                    {"path": "customers.csv", "mime_type": "text/csv", "status": "scanned",
                     "reason": null, "findings": [
                        {"redacter": "local-rules", "label": "person", "score": 0.75,
                         "location": {"kind": "table_cell", "row": 1, "column": 2, "start": 0, "end": 4},
                         "value": "***"}]},
                    {"path": "form.pdf", "mime_type": "application/pdf", "status": "scanned",
                     "reason": null, "findings": [
                        {"redacter": "local-rules", "label": "email", "score": null,
                         "location": {"kind": "image", "page": 1, "x1": 310.2, "y1": 193.0,
                                      "x2": 621.6, "y2": 218.0},
                         "value": "j***o"}]},
                    {"path": "clean.txt", "mime_type": "text/plain", "status": "scanned",
                     "reason": null, "findings": []},
                    {"path": "scan.png", "mime_type": "image/png", "status": "blocked",
                     "reason": "ocr unavailable", "findings": []},
                    {"path": "archive.bin", "mime_type": "application/octet-stream",
                     "status": "unsupported", "reason": "type not supported", "findings": []},
                    {"path": "huge.txt", "mime_type": "text/plain", "status": "skipped",
                     "reason": "over size limit", "findings": []}
                ]
            })
        );
    }

    #[test]
    fn the_text_report_has_a_grep_like_line_per_finding_and_per_unscanned_file() {
        let text = sample_result().to_text();
        assert_eq!(
            text,
            "customer-note.txt:3:15  email  local-rules  -  j***m\n\
             customers.csv:1:2  person  local-rules  0.75  ***\n\
             form.pdf:1:[310,193,622,218]  email  local-rules  -  j***o\n\
             scan.png  blocked: ocr unavailable\n\
             archive.bin  unsupported: type not supported\n\
             huge.txt  skipped: over size limit\n\
             \n\
             7 files: 3 with findings, 1 clean, 1 skipped, 1 unsupported, 1 blocked. 3 findings: email 2, person 1.\n"
        );
        assert!(
            !text.contains('\u{1b}'),
            "the text report has no ANSI escapes"
        );
    }

    #[test]
    fn the_exit_code_says_whether_anything_was_found_or_a_file_failed() {
        let clean = ScanCommandResult::default();
        assert_eq!(clean.exit_code(false), 0);

        let mut found = ScanCommandResult::default();
        found.push(ScanFileReport::scanned(
            &file_ref("note.txt", mime::TEXT_PLAIN),
            vec![finding(
                "email",
                DetectionLocation::Text { start: 0, end: 6 },
                None,
                "a@b.io",
            )],
        ));
        assert_eq!(found.exit_code(false), 2);
        assert_eq!(found.exit_code(true), 0);

        found.push(ScanFileReport::failed(
            &file_ref("broken.txt", mime::TEXT_PLAIN),
            "provider unavailable".to_string(),
        ));
        assert_eq!(found.exit_code(false), 1);
        assert_eq!(found.exit_code(true), 1);
    }

    #[test]
    fn a_blocked_file_makes_the_scan_incomplete_and_an_unsupported_one_does_not() {
        let scan_of = |reason| {
            let mut result = ScanCommandResult::default();
            result.push(ScanFileReport::not_scanned(
                &file_ref("scan.png", mime::IMAGE_PNG),
                SkipReason::NotRedacted(reason),
            ));
            result
        };

        let blocked = scan_of(NotRedactedReason::OcrUnavailable);
        assert_eq!((blocked.files_blocked, blocked.files_unsupported), (1, 0));
        assert_eq!(blocked.exit_code(false), 1);
        assert_eq!(blocked.exit_code(true), 1);

        let unsupported = scan_of(NotRedactedReason::TypeNotSupported);
        assert_eq!(
            (unsupported.files_blocked, unsupported.files_unsupported),
            (0, 1)
        );
        assert_eq!(unsupported.exit_code(false), 0);
    }
}
