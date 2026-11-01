//! Rendering of the `cp` command's terminal output: the capabilities line, the
//! legend, one row per file and the closing summary. `scan` prints its rows, legend and
//! capabilities line with the same functions.
//!
//! Everything here is pure: it takes what happened to a file and returns the
//! line to print, so the layout is unit tested without running a copy.

use crate::addon_store::Addon;
use crate::args::RedacterType;
use crate::file_converters::FileConverters;
use crate::redacters::{Categories, RedactionBlocked, RedactionConversion};
use console::{pad_str, Alignment, Style};

/// Why a file was copied without being redacted, or skipped instead of redacted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotRedactedReason {
    /// No configured redacter supports the file's media type.
    TypeNotSupported,
    /// The redaction pipeline ran no redacter for the file, so its content is left
    /// as it is.
    NoRedacterApplied,
    /// The file is a PDF and the PDF renderer is not available in this build.
    PdfRendererUnavailable,
    /// Redaction needs the OCR engine and it is not available in this build.
    OcrUnavailable,
    /// The OCR engine cannot read the image format of the file.
    OcrImageFormatNotSupported,
}

impl From<RedactionBlocked> for NotRedactedReason {
    fn from(blocked: RedactionBlocked) -> Self {
        match blocked {
            RedactionBlocked::PdfRendererUnavailable => NotRedactedReason::PdfRendererUnavailable,
            RedactionBlocked::OcrUnavailable => NotRedactedReason::OcrUnavailable,
            RedactionBlocked::OcrImageFormatNotSupported => {
                NotRedactedReason::OcrImageFormatNotSupported
            }
            RedactionBlocked::NotText => NotRedactedReason::TypeNotSupported,
        }
    }
}

/// Why a file was not copied at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
    /// The file is larger than `--max-size-limit`.
    OverSizeLimit,
    /// The file name does not match `--filename-filter`.
    NameFilter,
    /// Redaction did not apply and unsupported copies are not allowed.
    NotRedacted(NotRedactedReason),
}

/// What happened to a single file, as the row for it is printed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CopyFileOutcome {
    /// Redacted by the listed redacters and copied.
    Redacted {
        redacters: Vec<RedacterType>,
        conversion: Option<RedactionConversion>,
    },
    /// Copied unchanged because no redacter was configured.
    Copied,
    /// Copied unchanged although a redacter was configured, because
    /// `--allow-unsupported-copies` is set.
    CopiedUnredacted { reason: NotRedactedReason },
    /// Not copied at all.
    Skipped { reason: SkipReason },
    /// Redaction failed, so the file was not copied.
    Error { message: String },
}

/// The glyph that carries the outcome in the first column of a row.
pub(crate) const GLYPH_OK: &str = "✓";
const GLYPH_COPIED: &str = "→";
pub(crate) const GLYPH_SKIPPED: &str = "↷";
pub(crate) const GLYPH_ERROR: &str = "✗";

/// What each glyph of a `cp` row means.
pub const COPY_LEGEND: [(&str, &str); 4] = [
    (GLYPH_OK, "redacted"),
    (GLYPH_COPIED, "copied as is"),
    (GLYPH_SKIPPED, "skipped"),
    (GLYPH_ERROR, "error"),
];

const MEDIA_TYPE_WIDTH: usize = 24;
const SIZE_WIDTH: usize = 10;
const MIN_FILENAME_WIDTH: usize = 12;
const MIN_DETAIL_WIDTH: usize = 20;
const MAX_DETAIL_WIDTH: usize = 40;
const FALLBACK_TERMINAL_WIDTH: usize = 80;
/// The glyph and its trailing space, plus one space between each pair of columns, and the
/// media type and size columns.
const ROW_FIXED_WIDTH: usize = 2 + 3 + MEDIA_TYPE_WIDTH + SIZE_WIDTH;

/// Styles of the `cp` and `scan` output, kept together so a text report renders the same
/// lines without any styling at all.
#[derive(Debug, Clone)]
pub struct OutputStyles {
    pub filename: Style,
    pub media_type: Style,
    pub size: Style,
    pub ok: Style,
    pub copied: Style,
    pub skipped: Style,
    pub error: Style,
    pub dimmed: Style,
    pub highlighted: Style,
    /// The glyph of a file with findings, the finding counts and the values found.
    pub found: Style,
    /// The label of a finding.
    pub label: Style,
    /// The glyph of a file whose only results are values the redacters could not locate.
    pub unconfirmed: Style,
}

impl OutputStyles {
    /// The closing part of a status line that states the categories the run was limited to.
    pub fn categories_suffix(&self, categories: &Categories) -> String {
        format!(" · Categories: {}", self.highlighted.apply_to(categories))
    }

    pub fn new() -> Self {
        Self {
            filename: Style::new().bold().white(),
            media_type: Style::new(),
            size: Style::new().bold(),
            ok: Style::new().green(),
            copied: Style::new(),
            skipped: Style::new().dim().yellow(),
            error: Style::new().red(),
            dimmed: Style::new().dim(),
            highlighted: Style::new().bold().white(),
            found: Style::new().bold().red(),
            label: Style::new().yellow(),
            unconfirmed: Style::new().cyan(),
        }
    }

    pub fn plain() -> Self {
        Self {
            filename: Style::new(),
            media_type: Style::new(),
            size: Style::new(),
            ok: Style::new(),
            copied: Style::new(),
            skipped: Style::new(),
            error: Style::new(),
            dimmed: Style::new(),
            highlighted: Style::new(),
            found: Style::new(),
            label: Style::new(),
            unconfirmed: Style::new(),
        }
    }
}

impl Default for OutputStyles {
    fn default() -> Self {
        Self::new()
    }
}

/// What the redacters and converters available for this run can do, as shown in
/// the header of the command.
#[derive(Debug, Clone)]
pub struct Capabilities {
    pub redacters: Vec<RedacterType>,
    pub pdf_rendering: Capability,
    pub ocr: Capability,
    pub sampling_size: Option<usize>,
    pub categories: Option<Categories>,
}

impl Capabilities {
    /// The capabilities of a run with `redacters`, the converters this build bound, and
    /// `--sampling-size`. A converter that is off names the addon that turns it on, in a build
    /// that has the converter.
    pub fn new(
        redacters: Vec<RedacterType>,
        file_converters: &FileConverters<'_>,
        sampling_size: Option<usize>,
        categories: Option<Categories>,
    ) -> Self {
        Self {
            redacters,
            pdf_rendering: match file_converters.pdf_image_converter {
                Some(_) => Capability::Yes,
                #[cfg(feature = "pdf-render")]
                None => Capability::No(Addon::Pdf.manifest().map(|_| Addon::Pdf)),
                #[cfg(not(feature = "pdf-render"))]
                None => Capability::No(None),
            },
            ocr: match file_converters.ocr {
                Some(_) => Capability::Yes,
                #[cfg(feature = "ocr")]
                None => Capability::No(Some(Addon::Ocr)),
                #[cfg(not(feature = "ocr"))]
                None => Capability::No(None),
            },
            sampling_size,
            categories,
        }
    }
}

/// A converter on the capabilities line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Capability {
    Yes,
    /// Off, with the addon that turns it on in a build that offers one.
    No(Option<Addon>),
}

/// The counts and the size shown in the closing summary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CopySummary {
    pub redacted: usize,
    pub copied_as_is: usize,
    pub skipped: usize,
    pub errors: usize,
    pub total_size: u64,
}

/// What a row shows of a file's outcome: the glyph, the style of the glyph and the detail, and
/// the detail itself.
pub struct RowOutcome<'a> {
    pub glyph: &'static str,
    pub style: &'a Style,
    pub detail: String,
}

impl CopyFileOutcome {
    pub fn row<'a>(&self, styles: &'a OutputStyles) -> RowOutcome<'a> {
        let (glyph, style) = match self {
            CopyFileOutcome::Redacted { .. } => (GLYPH_OK, &styles.ok),
            CopyFileOutcome::Copied | CopyFileOutcome::CopiedUnredacted { .. } => {
                (GLYPH_COPIED, &styles.copied)
            }
            CopyFileOutcome::Skipped { .. } => (GLYPH_SKIPPED, &styles.skipped),
            CopyFileOutcome::Error { .. } => (GLYPH_ERROR, &styles.error),
        };
        RowOutcome {
            glyph,
            style,
            detail: format_outcome_detail(self),
        }
    }
}

impl NotRedactedReason {
    /// Whether the file is not read because this run lacks a converter for it, which a user
    /// can fix by installing an addon, as opposed to a file no selected redacter reads at all.
    pub(crate) fn needs_converter(self) -> bool {
        matches!(
            self,
            NotRedactedReason::PdfRendererUnavailable | NotRedactedReason::OcrUnavailable
        )
    }

    pub(crate) fn detail(&self) -> &'static str {
        match self {
            NotRedactedReason::TypeNotSupported => "type not supported",
            NotRedactedReason::NoRedacterApplied => "no redacter applied",
            NotRedactedReason::PdfRendererUnavailable => "pdf renderer unavailable",
            NotRedactedReason::OcrUnavailable => "ocr unavailable",
            NotRedactedReason::OcrImageFormatNotSupported => "ocr: image format not supported",
        }
    }
}

impl SkipReason {
    pub(crate) fn detail(&self) -> &'static str {
        match self {
            SkipReason::OverSizeLimit => "over size limit",
            SkipReason::NameFilter => "name filter",
            SkipReason::NotRedacted(reason) => reason.detail(),
        }
    }
}

/// The detail column of a row: what the glyph does not already say.
pub fn format_outcome_detail(outcome: &CopyFileOutcome) -> String {
    match outcome {
        CopyFileOutcome::Redacted {
            redacters,
            conversion,
        } => {
            let mut parts: Vec<String> = redacters.iter().map(|r| r.to_string()).collect();
            match conversion {
                Some(RedactionConversion::PdfToImages { pages, ocr }) => {
                    parts.push(format!(
                        "pdf → {pages} {}",
                        if *pages == 1 { "image" } else { "images" }
                    ));
                    if *ocr {
                        parts.push("ocr".to_string());
                    }
                }
                Some(RedactionConversion::Ocr) => parts.push("ocr".to_string()),
                None => {}
            }
            parts.join(", ")
        }
        CopyFileOutcome::Copied => String::new(),
        CopyFileOutcome::CopiedUnredacted { reason } => reason.detail().to_string(),
        CopyFileOutcome::Skipped { reason } => reason.detail().to_string(),
        CopyFileOutcome::Error { message } => message.clone(),
    }
}

/// The widths of the four columns of a row, derived from the terminal width.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RowLayout {
    filename: usize,
    media_type: usize,
    size: usize,
    detail: usize,
}

impl RowLayout {
    /// The layout for rows written to a file rather than a terminal: no file name up to
    /// `filename_width` columns is shortened, and the detail column is at its widest.
    pub(crate) fn for_report(filename_width: usize) -> Self {
        Self {
            filename: filename_width.max(MIN_FILENAME_WIDTH),
            media_type: MEDIA_TYPE_WIDTH,
            size: SIZE_WIDTH,
            detail: MAX_DETAIL_WIDTH,
        }
    }

    /// The layout for rows written to a terminal `terminal_width` columns wide, 80 when that is
    /// unknown (0): the file name takes what the other columns leave.
    pub(crate) fn for_terminal(terminal_width: usize) -> Self {
        let terminal_width = if terminal_width == 0 {
            FALLBACK_TERMINAL_WIDTH
        } else {
            terminal_width
        };
        let available = terminal_width.saturating_sub(ROW_FIXED_WIDTH);
        let mut detail = (available / 3).clamp(MIN_DETAIL_WIDTH, MAX_DETAIL_WIDTH);
        if available.saturating_sub(detail) < MIN_FILENAME_WIDTH {
            detail = available.saturating_sub(MIN_FILENAME_WIDTH);
        }
        let filename = available.saturating_sub(detail).max(MIN_FILENAME_WIDTH);
        RowLayout {
            filename,
            media_type: MEDIA_TYPE_WIDTH,
            size: SIZE_WIDTH,
            detail,
        }
    }
}

/// Truncates `s` to at most `width` display columns, keeping a prefix and a
/// (slightly longer) suffix joined by a single `…`, so the most identifying
/// part of a path (its file name) tends to survive. Widths are measured with
/// `console::measure_text_width` so multi-byte and wide characters are
/// accounted for rather than counted as one byte each.
fn truncate_middle(s: &str, width: usize) -> String {
    if width == 0 {
        return String::new();
    }
    if console::measure_text_width(s) <= width {
        return s.to_string();
    }
    if width == 1 {
        return "…".to_string();
    }
    let budget = width - 1; // reserve one column for the ellipsis
    let head_budget = budget / 2;
    let tail_budget = budget - head_budget;

    let chars: Vec<char> = s.chars().collect();

    let mut head = String::new();
    let mut used = 0;
    for &c in &chars {
        let w = console::measure_text_width(&c.to_string());
        if used + w > head_budget {
            break;
        }
        head.push(c);
        used += w;
    }

    let mut tail = String::new();
    used = 0;
    for &c in chars.iter().rev() {
        let w = console::measure_text_width(&c.to_string());
        if used + w > tail_budget {
            break;
        }
        tail.insert(0, c);
        used += w;
    }

    format!("{head}…{tail}")
}

/// Truncates `s` to at most `width` display columns, keeping its beginning,
/// which is what matters for a media type or an error message.
fn truncate_end(s: &str, width: usize) -> String {
    if width == 0 {
        return String::new();
    }
    if console::measure_text_width(s) <= width {
        return s.to_string();
    }
    if width == 1 {
        return "…".to_string();
    }
    let budget = width - 1;
    let mut head = String::new();
    let mut used = 0;
    for c in s.chars() {
        let w = console::measure_text_width(&c.to_string());
        if used + w > budget {
            break;
        }
        head.push(c);
        used += w;
    }
    format!("{head}…")
}

/// Fits `s` into exactly `width` display columns: pads short values on the
/// right, and middle-truncates values that overflow.
fn fit_column(s: &str, width: usize) -> String {
    if console::measure_text_width(s) <= width {
        pad_str(s, width, Alignment::Left, None).to_string()
    } else {
        truncate_middle(s, width)
    }
}

/// Same as [`fit_column`], keeping the beginning of an overflowing value.
fn fit_column_head(s: &str, width: usize) -> String {
    if console::measure_text_width(s) <= width {
        pad_str(s, width, Alignment::Left, None).to_string()
    } else {
        truncate_end(s, width)
    }
}

/// The legend printed once, before the first row: each glyph and what it means.
pub fn format_legend(entries: &[(&str, &str)], styles: &OutputStyles) -> String {
    styles
        .dimmed
        .apply_to(
            entries
                .iter()
                .map(|(glyph, meaning)| format!("{glyph} {meaning}"))
                .collect::<Vec<String>>()
                .join("   "),
        )
        .to_string()
}

/// Text from a scanned file or a model, or a name in the source, for a terminal or a text
/// report: whoever wrote it decides what it holds. Control characters are written out, since a
/// newline would break a row and an escape sequence would reach the terminal, and so are the
/// bidirectional controls, which reorder the text around them. Everything else is left as it is,
/// as names are full of backslashes, quotes and joined emoji.
pub(crate) fn escaped(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());
    for character in text.chars() {
        match character {
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            _ if character.is_control() || is_bidirectional_control(character) => {
                escaped.extend(character.escape_unicode());
            }
            _ => escaped.push(character),
        }
    }
    escaped
}

fn is_bidirectional_control(character: char) -> bool {
    matches!(
        character,
        '\u{200e}' | '\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}'
    )
}

/// One row of the table, written when a file's processing finishes.
pub fn format_file_row(
    layout: RowLayout,
    filename: &str,
    media_type: &str,
    size: &str,
    outcome: &RowOutcome,
    styles: &OutputStyles,
) -> String {
    let outcome_style = outcome.style;
    let detail = truncate_end(&escaped(&outcome.detail), layout.detail);

    let mut row = format!(
        "{} {} {} ",
        outcome_style.apply_to(outcome.glyph),
        styles
            .filename
            .apply_to(fit_column(&escaped(filename), layout.filename)),
        styles
            .media_type
            .apply_to(fit_column_head(media_type, layout.media_type)),
    );
    if detail.is_empty() {
        row.push_str(&styles.size.apply_to(size).to_string());
    } else {
        row.push_str(
            &styles
                .size
                .apply_to(fit_column(size, layout.size))
                .to_string(),
        );
        row.push(' ');
        row.push_str(&outcome_style.apply_to(detail).to_string());
    }
    row.trim_end().to_string()
}

/// The header line describing what this run can do.
pub fn format_capabilities_line(capabilities: &Capabilities, styles: &OutputStyles) -> String {
    let redacters = if capabilities.redacters.is_empty() {
        styles.dimmed.apply_to("no".to_string())
    } else {
        styles.ok.apply_to(
            capabilities
                .redacters
                .iter()
                .map(|r| r.to_string())
                .collect::<Vec<String>>()
                .join(", "),
        )
    };
    let yes_no = |capability: Capability| match capability {
        Capability::Yes => styles.ok.apply_to("yes".to_string()),
        Capability::No(None) => styles.dimmed.apply_to("no".to_string()),
        Capability::No(Some(addon)) => styles
            .dimmed
            .apply_to(format!("no (redacter addons install {addon})")),
    };
    let sampling = match capabilities.sampling_size {
        Some(size) => styles.highlighted.apply_to(format!("{size} bytes")),
        None => styles.dimmed.apply_to("off".to_string()),
    };
    let mut line = format!(
        "Redacters: {} · PDF rendering: {} · OCR: {} · Sampling: {}",
        redacters,
        yes_no(capabilities.pdf_rendering),
        yes_no(capabilities.ocr),
        sampling
    );
    if let Some(categories) = &capabilities.categories {
        line.push_str(&styles.categories_suffix(categories));
    }
    line
}

/// The closing summary, replacing the per-file step lines with one count line.
pub fn format_summary_line(
    summary: &CopySummary,
    categories: Option<&Categories>,
    styles: &OutputStyles,
) -> String {
    let total = summary.redacted + summary.copied_as_is + summary.skipped + summary.errors;
    let mut line = format!(
        "{} {}: {} redacted, {} copied as is, {} skipped",
        styles.highlighted.apply_to(total),
        if total == 1 { "file" } else { "files" },
        styles.highlighted.apply_to(summary.redacted),
        styles.highlighted.apply_to(summary.copied_as_is),
        styles.highlighted.apply_to(summary.skipped),
    );
    if summary.errors > 0 {
        line.push_str(
            format!(
                ", {} {}",
                styles.error.apply_to(summary.errors),
                if summary.errors == 1 {
                    "error"
                } else {
                    "errors"
                }
            )
            .as_str(),
        );
    }
    line.push_str(
        format!(
            ". Total size: {}",
            styles
                .highlighted
                .apply_to(indicatif::HumanBytes(summary.total_size))
        )
        .as_str(),
    );
    if let Some(categories) = categories {
        line.push_str(&styles.categories_suffix(categories));
    }
    line
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(
        width: usize,
        filename: &str,
        media_type: &str,
        size: &str,
        outcome: &CopyFileOutcome,
    ) -> String {
        let styles = OutputStyles::plain();
        format_file_row(
            RowLayout::for_terminal(width),
            filename,
            media_type,
            size,
            &outcome.row(&styles),
            &styles,
        )
    }

    #[test]
    fn a_filename_is_escaped_in_the_row() {
        let outcome = CopyFileOutcome::Copied;

        let line = row(100, "a\n\u{1b}[31m b.txt", "text/plain", "1 B", &outcome);

        assert!(line.contains("a\\n\\u{1b}[31m b.txt "), "{line}");
        assert!(!line.contains(['\n', '\u{1b}']));
    }

    #[test]
    fn only_what_changes_terminal_output_is_escaped() {
        let escaped_text = escaped("a\tb\rc\n\u{1b}\u{7f}\u{85}\u{202e}\u{200f}\u{2066}");
        assert_eq!(
            escaped_text,
            "a\\tb\\rc\\n\\u{1b}\\u{7f}\\u{85}\\u{202e}\\u{200f}\\u{2066}"
        );

        let unchanged = "sub\\file.txt it's \"q\" no\u{a0}break \u{1f469}\u{200d}\u{1f4bb} \u{e9}";
        assert_eq!(escaped(unchanged), unchanged);
    }

    #[test]
    fn a_detail_is_escaped_in_the_row() {
        let outcome = CopyFileOutcome::Error {
            message: "read a\n\u{1b}[31m b".to_string(),
        };

        let line = row(100, "b.txt", "text/plain", "1 B", &outcome);

        assert!(line.contains("read a\\n\\u{1b}[31m b"), "{line}");
        assert!(!line.contains(['\n', '\u{1b}']));
    }

    fn redacted(
        redacters: &[RedacterType],
        conversion: Option<RedactionConversion>,
    ) -> CopyFileOutcome {
        CopyFileOutcome::Redacted {
            redacters: redacters.to_vec(),
            conversion,
        }
    }

    #[test]
    fn outcome_detail_renders_every_variant() {
        assert_eq!(
            format_outcome_detail(&redacted(&[RedacterType::GcpDlp], None)),
            "gcp-dlp"
        );
        assert_eq!(
            format_outcome_detail(&redacted(
                &[RedacterType::GcpDlp, RedacterType::AwsComprehend],
                None
            )),
            "gcp-dlp, aws-comprehend"
        );
        assert_eq!(
            format_outcome_detail(&redacted(
                &[RedacterType::GcpDlp],
                Some(RedactionConversion::PdfToImages {
                    pages: 3,
                    ocr: false
                })
            )),
            "gcp-dlp, pdf → 3 images"
        );
        assert_eq!(
            format_outcome_detail(&redacted(
                &[RedacterType::GcpDlp],
                Some(RedactionConversion::PdfToImages {
                    pages: 1,
                    ocr: false
                })
            )),
            "gcp-dlp, pdf → 1 image"
        );
        assert_eq!(
            format_outcome_detail(&redacted(
                &[RedacterType::GcpDlp],
                Some(RedactionConversion::PdfToImages {
                    pages: 2,
                    ocr: true
                })
            )),
            "gcp-dlp, pdf → 2 images, ocr"
        );
        assert_eq!(
            format_outcome_detail(&redacted(
                &[RedacterType::GcpDlp],
                Some(RedactionConversion::Ocr)
            )),
            "gcp-dlp, ocr"
        );
        assert_eq!(format_outcome_detail(&CopyFileOutcome::Copied), "");
        assert_eq!(
            format_outcome_detail(&CopyFileOutcome::CopiedUnredacted {
                reason: NotRedactedReason::TypeNotSupported
            }),
            "type not supported"
        );
        for (reason, expected) in [
            (SkipReason::OverSizeLimit, "over size limit"),
            (SkipReason::NameFilter, "name filter"),
            (
                SkipReason::NotRedacted(NotRedactedReason::TypeNotSupported),
                "type not supported",
            ),
            (
                SkipReason::NotRedacted(NotRedactedReason::NoRedacterApplied),
                "no redacter applied",
            ),
            (
                SkipReason::NotRedacted(NotRedactedReason::PdfRendererUnavailable),
                "pdf renderer unavailable",
            ),
            (
                SkipReason::NotRedacted(NotRedactedReason::OcrUnavailable),
                "ocr unavailable",
            ),
            (
                SkipReason::NotRedacted(NotRedactedReason::OcrImageFormatNotSupported),
                "ocr: image format not supported",
            ),
        ] {
            assert_eq!(
                format_outcome_detail(&CopyFileOutcome::Skipped { reason }),
                expected
            );
        }
        assert_eq!(
            format_outcome_detail(&CopyFileOutcome::Error {
                message: "invalid JSON at line 1".to_string()
            }),
            "invalid JSON at line 1"
        );
    }

    #[test]
    fn row_starts_with_the_glyph_of_its_outcome() {
        let cases = [
            (redacted(&[RedacterType::GcpDlp], None), '✓'),
            (CopyFileOutcome::Copied, '→'),
            (
                CopyFileOutcome::CopiedUnredacted {
                    reason: NotRedactedReason::TypeNotSupported,
                },
                '→',
            ),
            (
                CopyFileOutcome::Skipped {
                    reason: SkipReason::NameFilter,
                },
                '↷',
            ),
            (
                CopyFileOutcome::Error {
                    message: "boom".to_string(),
                },
                '✗',
            ),
        ];
        for (outcome, glyph) in cases {
            let line = row(100, "documents/a.txt", "text/plain", "1.00 KiB", &outcome);
            assert_eq!(
                line.chars().next(),
                Some(glyph),
                "row was: {line:?} for {outcome:?}"
            );
        }
    }

    #[test]
    fn rows_fit_the_terminal_width_and_keep_the_file_name_tail() {
        let filename = "documents/very-long-nested-directory-path/customer-profile.html";
        let outcome = redacted(
            &[RedacterType::GcpDlp, RedacterType::AwsComprehend],
            Some(RedactionConversion::PdfToImages {
                pages: 12,
                ocr: true,
            }),
        );
        for width in [80, 100, 150] {
            let line = row(
                width,
                filename,
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                "12.34 KiB",
                &outcome,
            );
            assert!(
                console::measure_text_width(&line) <= width,
                "row at {width} columns was {} wide: {line:?}",
                console::measure_text_width(&line)
            );
            assert_eq!(line, line.trim_end(), "row has trailing spaces: {line:?}");
            assert!(
                line.contains("file.html"),
                "row at {width} lost the file name tail: {line:?}"
            );
        }
        // A wide terminal keeps the whole path rather than only its tail.
        let wide_line = row(
            150,
            filename,
            "text/html",
            "12.34 KiB",
            &CopyFileOutcome::Copied,
        );
        assert!(wide_line.contains(filename), "row was: {wide_line:?}");
    }

    #[test]
    fn a_row_without_a_detail_has_no_trailing_padding() {
        let line = row(
            100,
            "a.txt",
            "text/plain",
            "1.00 KiB",
            &CopyFileOutcome::Copied,
        );
        assert!(line.ends_with("1.00 KiB"), "row was: {line:?}");
    }

    #[test]
    fn narrow_rows_still_render_every_column() {
        let line = row(
            80,
            "documents/customer-note.txt",
            "text/plain",
            "1.00 KiB",
            &CopyFileOutcome::Skipped {
                reason: SkipReason::OverSizeLimit,
            },
        );
        assert!(line.contains("text/plain"), "row was: {line:?}");
        assert!(line.contains("1.00 KiB"), "row was: {line:?}");
        assert!(line.contains("over size limit"), "row was: {line:?}");
    }

    #[test]
    fn legend_names_every_glyph_once() {
        let legend = format_legend(&COPY_LEGEND, &OutputStyles::plain());
        assert_eq!(legend, "✓ redacted   → copied as is   ↷ skipped   ✗ error");
    }

    #[test]
    fn capabilities_line_shows_what_is_on_and_off() {
        let styles = OutputStyles::plain();
        assert_eq!(
            format_capabilities_line(
                &Capabilities {
                    redacters: vec![RedacterType::GcpDlp],
                    pdf_rendering: Capability::Yes,
                    ocr: Capability::No(None),
                    sampling_size: None,
                    categories: None,
                },
                &styles
            ),
            "Redacters: gcp-dlp · PDF rendering: yes · OCR: no · Sampling: off"
        );
        assert_eq!(
            format_capabilities_line(
                &Capabilities {
                    redacters: vec![],
                    pdf_rendering: Capability::No(None),
                    ocr: Capability::Yes,
                    sampling_size: Some(1024),
                    categories: None,
                },
                &styles
            ),
            "Redacters: no · PDF rendering: no · OCR: yes · Sampling: 1024 bytes"
        );
        assert_eq!(
            format_capabilities_line(
                &Capabilities {
                    redacters: vec![RedacterType::GcpDlp, RedacterType::AwsComprehend],
                    pdf_rendering: Capability::Yes,
                    ocr: Capability::Yes,
                    sampling_size: None,
                    categories: None,
                },
                &styles
            ),
            "Redacters: gcp-dlp, aws-comprehend · PDF rendering: yes · OCR: yes · Sampling: off"
        );
    }

    #[test]
    fn capabilities_line_names_the_subcommand_that_installs_a_missing_addon() {
        assert_eq!(
            format_capabilities_line(
                &Capabilities {
                    redacters: vec![RedacterType::LocalRules],
                    pdf_rendering: Capability::No(Some(Addon::Pdf)),
                    ocr: Capability::No(Some(Addon::Ocr)),
                    sampling_size: None,
                    categories: None,
                },
                &OutputStyles::plain()
            ),
            "Redacters: local-rules · PDF rendering: no (redacter addons install pdf) · \
             OCR: no (redacter addons install ocr) · Sampling: off"
        );
    }

    #[test]
    fn summary_line_counts_files_and_only_mentions_errors_when_there_are_any() {
        let styles = OutputStyles::plain();
        assert_eq!(
            format_summary_line(
                &CopySummary {
                    redacted: 3,
                    copied_as_is: 1,
                    skipped: 1,
                    errors: 1,
                    total_size: 6 * 1024,
                },
                None,
                &styles
            ),
            "6 files: 3 redacted, 1 copied as is, 1 skipped, 1 error. Total size: 6.00 KiB"
        );
        assert_eq!(
            format_summary_line(
                &CopySummary {
                    redacted: 5,
                    copied_as_is: 0,
                    skipped: 1,
                    errors: 0,
                    total_size: 0,
                },
                None,
                &styles
            ),
            "6 files: 5 redacted, 0 copied as is, 1 skipped. Total size: 0 B"
        );
        assert_eq!(
            format_summary_line(
                &CopySummary {
                    redacted: 0,
                    copied_as_is: 1,
                    skipped: 0,
                    errors: 0,
                    total_size: 12,
                },
                None,
                &styles
            ),
            "1 file: 0 redacted, 1 copied as is, 0 skipped. Total size: 12 B"
        );
        assert_eq!(
            format_summary_line(
                &CopySummary {
                    redacted: 0,
                    copied_as_is: 0,
                    skipped: 0,
                    errors: 2,
                    total_size: 0,
                },
                None,
                &styles
            ),
            "2 files: 0 redacted, 0 copied as is, 0 skipped, 2 errors. Total size: 0 B"
        );
    }

    #[test]
    fn truncate_middle_measures_multibyte_characters_by_display_width() {
        let source = "documents/café-résumé-naïve-longer-file-name.txt";
        // Every character in this path is 1 display column wide (no CJK),
        // so display width differs from byte length but not from char count.
        assert!(source.len() > source.chars().count());

        let truncated = truncate_middle(source, 20);
        assert_eq!(console::measure_text_width(&truncated), 20);
        assert!(truncated.contains('…'));

        let wide = "文档/非常长的中文文件名称示例说明.txt";
        let truncated_wide = truncate_middle(wide, 20);
        assert!(console::measure_text_width(&truncated_wide) <= 20);
        assert!(truncated_wide.contains('…'));
    }

    #[test]
    fn fit_column_pads_short_values_without_truncating() {
        let result = fit_column("a.txt", 20);
        assert!(!result.contains('…'), "result was: {result:?}");
        assert_eq!(console::measure_text_width(&result), 20);
        assert!(result.starts_with("a.txt"));
    }

    #[test]
    fn media_type_column_keeps_the_beginning_when_it_overflows() {
        let media_type = "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
        let fitted = fit_column_head(media_type, MEDIA_TYPE_WIDTH);
        assert_eq!(console::measure_text_width(&fitted), MEDIA_TYPE_WIDTH);
        assert!(fitted.starts_with("application/vnd."), "was: {fitted:?}");
        assert!(fitted.ends_with('…'), "was: {fitted:?}");
    }

    #[test]
    fn terminal_layout_falls_back_to_80_columns_when_the_width_is_unknown() {
        assert_eq!(RowLayout::for_terminal(0), RowLayout::for_terminal(80));
    }
}
