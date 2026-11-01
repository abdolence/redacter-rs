//! What a provider found in an item, and where: the contract `scan` reports from. A
//! detection never changes the item; redaction is a separate path.

use crate::errors::AppError;
use crate::redacters::text_spans::{Finding, RuleName};
use crate::AppResult;
use serde::Serialize;
use std::cell::Cell;
use std::collections::HashSet;
use std::fmt::Display;
use std::ops::Range;
use std::str::FromStr;

/// The provider's own name for the kind of data it found: `email` from local-rules,
/// `EMAIL_ADDRESS` from GCP DLP, `per` from local-ner, `person` from local-gliner. Names are
/// not mapped across providers.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
pub struct DetectionLabel(String);

impl DetectionLabel {
    pub fn new(label: impl Into<String>) -> Self {
        Self(label.into())
    }

    /// The label of data a redacter replaced or boxed without saying what kind it is.
    pub fn unclassified() -> Self {
        Self::new("sensitive_data")
    }
}

impl From<RuleName> for DetectionLabel {
    fn from(rule: RuleName) -> Self {
        Self::new(rule.as_str())
    }
}

impl Display for DetectionLabel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A provider's confidence in a detection, from 0 to 1 inclusive.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct Score(f32);

impl Score {
    /// A score fixed in the source. Evaluated in a `const` item, an out-of-range value fails
    /// the build.
    pub const fn from_static(value: f32) -> Self {
        assert!(value >= 0.0 && value <= 1.0, "a score is between 0 and 1");
        Self(value)
    }
}

impl TryFrom<f32> for Score {
    type Error = AppError;

    fn try_from(value: f32) -> Result<Self, Self::Error> {
        if (0.0..=1.0).contains(&value) {
            Ok(Self(value))
        } else {
            Err(AppError::InvalidScore {
                value: value.to_string(),
            })
        }
    }
}

impl FromStr for Score {
    type Err = AppError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let value: f32 = s.trim().parse().map_err(|_| AppError::InvalidScore {
            value: s.to_string(),
        })?;
        Self::try_from(value)
    }
}

impl Display for Score {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:.2}", self.0)
    }
}

/// One piece of sensitive data a provider found in an item.
#[derive(Debug, Clone, PartialEq)]
pub struct Detection {
    pub label: DetectionLabel,
    pub location: DetectionLocation,
    /// `None` for providers that give no confidence, such as regex rules and LLMs.
    pub score: Option<Score>,
    /// The matched text. It is the sensitive value itself, so a report masks it unless the
    /// user asks for raw values.
    pub value: String,
}

/// What a provider found in an item.
#[derive(Debug, Default)]
pub struct Detections {
    pub found: Vec<Detection>,
    /// How many values the provider named without being able to place them in the item, such
    /// as a value a model reformatted. Only the count is kept, since the values are the
    /// sensitive data. A file with unlocated values may hold findings that `found` lacks.
    pub unlocated: usize,
}

impl From<Vec<Detection>> for Detections {
    fn from(found: Vec<Detection>) -> Self {
        Self {
            found,
            unlocated: 0,
        }
    }
}

/// Where a detection sits, as a report shows it. Byte offsets count from 0; lines, columns,
/// table rows and columns, and PDF pages count from 1, as editors and spreadsheets do.
///
/// Locations are ordered the way a reader meets them: texts by offset, then cells by row and
/// column, then image boxes by page, top to bottom and left to right, then whole pages, then
/// the whole file. The fields are declared in that order for the derived `PartialOrd`.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DetectionLocation {
    /// A half-open byte range of the item's text on character boundaries, and the line and
    /// character column its start is on.
    Text {
        start: usize,
        end: usize,
        line: usize,
        column: usize,
    },
    /// A half-open byte range of one cell. `row` counts the data rows, the header row excluded.
    TableCell {
        row: usize,
        column: usize,
        start: usize,
        end: usize,
    },
    /// A box in pixels of the image as the file holds it, or of PDF page `page` as rendered.
    /// `x1 <= x2` and `y1 <= y2`.
    Image {
        page: Option<usize>,
        y1: f32,
        x1: f32,
        y2: f32,
        x2: f32,
    },
    /// PDF page `page`, when where on it is not known.
    Page { page: usize },
    /// The file as a whole, when where in it is not known.
    File,
}

impl DetectionLocation {
    /// The byte range inside the text or the table cell the location is in; `None` for the
    /// kinds that do not point into a text.
    pub(crate) fn byte_range(&self) -> Option<Range<usize>> {
        match *self {
            Self::Text { start, end, .. } | Self::TableCell { start, end, .. } => Some(start..end),
            Self::Image { .. } | Self::Page { .. } | Self::File => None,
        }
    }
}

/// How a report shows a location: `line:column` in a text, `row:column` of a table cell,
/// `[x1,y1,x2,y2]` in whole pixels of an image, after `page:` on a PDF page, `page N` for a
/// whole page and `file` for the whole file.
impl Display for DetectionLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Text { line, column, .. } => write!(f, "{line}:{column}"),
            Self::TableCell { row, column, .. } => write!(f, "{row}:{column}"),
            Self::Image {
                page,
                x1,
                y1,
                x2,
                y2,
            } => {
                if let Some(page) = page {
                    write!(f, "{page}:")?;
                }
                write!(
                    f,
                    "[{},{},{},{}]",
                    x1.round(),
                    y1.round(),
                    x2.round(),
                    y2.round()
                )
            }
            Self::Page { page } => write!(f, "page {page}"),
            Self::File => f.write_str("file"),
        }
    }
}

/// The text that makes a finding ignored when it stands on a line the finding covers, or in the
/// table cell of the finding, outside every finding. It is a substring, so it sits in whatever
/// comment syntax the file uses.
pub const ALLOW_MARKER: &str = "redacter:allow";

/// Whether [`ALLOW_MARKER`] occurs in `text[window]` clear of every range of `findings`. An
/// occurrence that overlaps a finding is part of a value, so it cannot be used to hide that value
/// or any other finding on the line.
fn has_marker_clear_of(text: &str, window: Range<usize>, findings: &[Range<usize>]) -> bool {
    text[window.clone()]
        .match_indices(ALLOW_MARKER)
        .any(|(at, marker)| {
            let start = window.start + at;
            let end = start + marker.len();
            findings
                .iter()
                .all(|finding| end <= finding.start || start >= finding.end)
        })
}

/// Whether [`ALLOW_MARKER`] occurs in `text` clear of every range of `findings`.
pub fn has_marker_outside(text: &str, findings: &[Range<usize>]) -> bool {
    has_marker_clear_of(text, 0..text.len(), findings)
}

/// A text with the byte offset of each line start, so many detections are placed in it without
/// rescanning it for each one.
pub struct TextLines<'a> {
    text: &'a str,
    line_starts: Vec<usize>,
    /// The offset and column of the last detection. Detections come mostly in order, so counting
    /// from it keeps a long line, such as minified JSON, from being counted again for each one.
    last_column: Cell<(usize, usize)>,
}

impl<'a> TextLines<'a> {
    pub fn new(text: &'a str) -> Self {
        Self {
            text,
            line_starts: std::iter::once(0)
                .chain(text.match_indices('\n').map(|(newline, _)| newline + 1))
                .collect(),
            last_column: Cell::new((0, 1)),
        }
    }

    /// Whether [`ALLOW_MARKER`] stands, clear of every range of `findings`, on a line that
    /// `range` covers: from the line it starts on to the line it ends on.
    pub fn has_marker_beside(&self, range: Range<usize>, findings: &[Range<usize>]) -> bool {
        let first_line = self.line_of(range.start);
        let last_line = self.line_of(range.end.saturating_sub(1).max(range.start));
        let to = self
            .line_starts
            .get(last_line + 1)
            .copied()
            .unwrap_or(self.text.len());
        has_marker_clear_of(self.text, self.line_starts[first_line]..to, findings)
    }

    /// The 0-based index of the line `offset` is on.
    fn line_of(&self, offset: usize) -> usize {
        // The first line starts at 0, so at least one line start is not after `offset`.
        self.line_starts.partition_point(|&start| start <= offset) - 1
    }

    fn column_of(&self, line_start: usize, offset: usize) -> usize {
        let (from, column) = match self.last_column.get() {
            (last, column) if (line_start..=offset).contains(&last) => (last, column),
            _ => (line_start, 1),
        };
        let column = column + self.text[from..offset].chars().count();
        self.last_column.set((offset, column));
        column
    }

    /// The detection of `range`, with its value copied out of the text. An error when the range
    /// is not inside the text on character boundaries: a report would otherwise place it where
    /// it is not.
    pub fn detection(
        &self,
        range: Range<usize>,
        label: DetectionLabel,
        score: Option<Score>,
    ) -> AppResult<Detection> {
        let value = value_at(self.text, &range)?;
        let line_index = self.line_of(range.start);
        let line = line_index + 1;
        let line_start = self.line_starts[line_index];
        Ok(Detection {
            label,
            location: DetectionLocation::Text {
                start: range.start,
                end: range.end,
                line,
                column: self.column_of(line_start, range.start),
            },
            score,
            value,
        })
    }
}

/// The text of `range` in `text`, or an error when the range is not inside it on character
/// boundaries.
fn value_at(text: &str, range: &Range<usize>) -> AppResult<String> {
    text.get(range.clone())
        .map(str::to_string)
        .ok_or(AppError::DetectionOutsideContent {
            start: range.start,
            end: range.end,
        })
}

impl Detection {
    /// The detection of `range` in `cell`, at 0-based `row` and `column` as the caller indexes
    /// its rows; the location stores them as a report shows them. An error when the range is
    /// not inside the cell on character boundaries.
    pub fn in_table_cell(
        row: usize,
        column: usize,
        cell: &str,
        range: Range<usize>,
        label: DetectionLabel,
        score: Option<Score>,
    ) -> AppResult<Self> {
        let value = value_at(cell, &range)?;
        Ok(Self {
            label,
            location: DetectionLocation::TableCell {
                row: row + 1,
                column: column + 1,
                start: range.start,
                end: range.end,
            },
            score,
            value,
        })
    }

    /// The detections of `findings` in `text`, for providers that report ranges of the text.
    pub fn in_text(text: &str, findings: Vec<Finding>) -> AppResult<Detections> {
        let lines = TextLines::new(text);
        findings
            .into_iter()
            .map(|finding| {
                lines.detection(
                    finding.start..finding.end,
                    finding.rule.into(),
                    finding.score,
                )
            })
            .collect::<AppResult<Vec<Self>>>()
            .map(Detections::from)
    }

    /// The findings of `find_in_cell(column, cell)` on every cell of `rows`, placed in their
    /// cell, for providers that read a table one cell at a time.
    pub fn in_table_cells<E: Into<AppError>>(
        rows: &[Vec<String>],
        mut find_in_cell: impl FnMut(usize, &str) -> Result<Vec<Finding>, E>,
    ) -> AppResult<Detections> {
        let mut detections = Vec::new();
        for (row_index, row) in rows.iter().enumerate() {
            for (column, cell) in row.iter().enumerate() {
                for finding in find_in_cell(column, cell).map_err(Into::into)? {
                    detections.push(Self::in_table_cell(
                        row_index,
                        column,
                        cell,
                        finding.start..finding.end,
                        finding.rule.into(),
                        finding.score,
                    )?);
                }
            }
        }
        Ok(detections.into())
    }

    /// Each value's occurrences in `text` that do not overlap each other, for providers that
    /// name the values they found but not where. Occurrences are found ignoring case, since a model
    /// may not copy a value letter for letter, but accents are not folded (`Müller` does not
    /// match `Muller`). They are reported wherever they occur, inside longer words too:
    /// `John` is also reported inside `Johnson`, because text in scripts without spaces and
    /// inflected words have no word boundary around a value, and a missed value is worse than
    /// an extra one. An occurrence inside a longer one is dropped, so a name and its first
    /// word are reported once; partial overlaps are all kept. When two values have the same
    /// span, the first one's label is kept. An empty value or a repeated `(value, label)`
    /// pair adds nothing. Sorted by position. A value with no occurrence is counted in
    /// [`Detections::unlocated`].
    pub fn locate_values(text: &str, values: &[(String, DetectionLabel)]) -> AppResult<Detections> {
        let mut seen = HashSet::new();
        let mut occurrences = Vec::new();
        let mut unlocated = 0;
        let folded_text = FoldedText::new(text);
        for (value, label) in values {
            if value.is_empty() || !seen.insert((value, label)) {
                continue;
            }
            let ranges = folded_text.occurrences_of(value);
            if ranges.is_empty() {
                unlocated += 1;
            }
            occurrences.extend(ranges.into_iter().map(|range| (range, label)));
        }
        let lines = TextLines::new(text);
        let found = without_contained(occurrences)
            .into_iter()
            .map(|(range, label)| lines.detection(range, label.clone(), None))
            .collect::<AppResult<_>>()?;
        Ok(Detections { found, unlocated })
    }
}

/// A text case-folded one character at a time, with the byte offsets that map a position in
/// the folded copy back to the original.
struct FoldedText {
    folded: String,
    /// The byte index in `folded` of every character, then the length of `folded`.
    folded_starts: Vec<usize>,
    /// The byte index in `text` of the same characters, then the length of `text`.
    original_starts: Vec<usize>,
}

impl FoldedText {
    fn new(text: &str) -> Self {
        let mut folded = String::with_capacity(text.len());
        let mut folded_starts = Vec::with_capacity(text.len() + 1);
        let mut original_starts = Vec::with_capacity(text.len() + 1);
        for (index, character) in text.char_indices() {
            original_starts.push(index);
            folded_starts.push(folded.len());
            folded.push(fold_character(character));
        }
        original_starts.push(text.len());
        folded_starts.push(folded.len());
        Self {
            folded,
            folded_starts,
            original_starts,
        }
    }

    /// The byte ranges of the original text where `value` occurs ignoring case, not counting an
    /// occurrence that starts inside the previous one.
    fn occurrences_of(&self, value: &str) -> Vec<Range<usize>> {
        let folded_value: String = value.chars().map(fold_character).collect();
        self.folded
            .match_indices(&folded_value)
            .filter_map(|(start, matched)| {
                let first = self.folded_starts.binary_search(&start).ok()?;
                let end = self
                    .folded_starts
                    .binary_search(&(start + matched.len()))
                    .ok()?;
                Some(self.original_starts[first]..self.original_starts[end])
            })
            .collect()
    }
}

/// The character that `character` and its other-case forms share: the lowercase of its
/// uppercase when that is a single character, which maps the Greek final `ς` and `Σ` to `σ`
/// and the long `ſ` to `s`. A character whose uppercase is longer keeps its own lowercase, so
/// `ß` and `SS` stay different, and the first character of a longer lowercase is taken, which
/// turns `İ` into `i`.
fn fold_character(character: char) -> char {
    let mut uppercase = character.to_uppercase();
    let base = match (uppercase.next(), uppercase.next()) {
        (Some(single), None) => single,
        _ => character,
    };
    base.to_lowercase().next().unwrap_or(base)
}

/// `occurrences` sorted by position, without those that lie inside a longer one. Occurrences
/// that only overlap are all kept: two values can share words, and dropping either would leave
/// part of the sensitive data unreported.
fn without_contained(
    mut occurrences: Vec<(Range<usize>, &DetectionLabel)>,
) -> Vec<(Range<usize>, &DetectionLabel)> {
    occurrences.sort_by(|(a, _), (b, _)| a.start.cmp(&b.start).then(b.end.cmp(&a.end)));
    let mut furthest_end = 0;
    occurrences.retain(|(range, _)| {
        let contained = range.end <= furthest_end;
        furthest_end = furthest_end.max(range.end);
        !contained
    });
    occurrences
}

/// Converts the character offsets that text analysis services report into byte offsets of
/// the same text. Comprehend and Presidio count characters, while a detection's range counts
/// bytes.
pub(crate) struct CharOffsets {
    /// The byte index of every character, then the length of the text.
    byte_starts: Vec<usize>,
}

impl CharOffsets {
    pub(crate) fn new(text: &str) -> Self {
        Self {
            byte_starts: text
                .char_indices()
                .map(|(index, _)| index)
                .chain([text.len()])
                .collect(),
        }
    }

    /// The byte range of the characters `start..end`, or `None` when it reaches past the text.
    pub(crate) fn byte_range(&self, start: usize, end: usize) -> Option<Range<usize>> {
        if start > end {
            return None;
        }
        Some(*self.byte_starts.get(start)?..*self.byte_starts.get(end)?)
    }
}

/// `text` with every text range of `detections` replaced by one `X` per character. Ranges
/// may overlap or arrive in any order.
pub(crate) fn mask_detections(text: &str, detections: &[Detection]) -> String {
    let mut ranges: Vec<(usize, usize)> = detections
        .iter()
        .filter_map(|detection| match detection.location {
            DetectionLocation::Text { start, end, .. } => Some((start, end)),
            _ => None,
        })
        .collect();
    ranges.sort_unstable();

    let mut masked = String::with_capacity(text.len());
    let mut cursor = 0;
    for (start, end) in ranges {
        let start = start.max(cursor);
        if end <= start {
            continue;
        }
        masked.push_str(&text[cursor..start]);
        masked.extend(text[start..end].chars().map(|_| 'X'));
        cursor = end;
    }
    masked.push_str(&text[cursor..]);
    masked
}

#[cfg(test)]
mod tests {
    use super::*;

    fn score(value: f32) -> Score {
        Score::try_from(value).unwrap()
    }

    fn email() -> DetectionLabel {
        DetectionLabel::new("email")
    }

    /// The text range and the value of each detection, in order.
    fn located(detections: &[Detection]) -> Vec<(Range<usize>, &str)> {
        detections
            .iter()
            .map(|detection| match detection.location {
                DetectionLocation::Text { start, end, .. } => {
                    (start..end, detection.value.as_str())
                }
                other => panic!("a text detection is expected, got {other:?}"),
            })
            .collect()
    }

    #[test]
    fn a_score_is_a_probability() {
        for valid in [0.0_f32, 0.5, 1.0] {
            assert!(Score::try_from(valid).is_ok(), "{valid}");
        }
        assert_eq!("0.8".parse::<Score>().unwrap(), score(0.8));
        for invalid in [-0.01_f32, 1.01, f32::NAN, f32::INFINITY] {
            assert!(
                matches!(Score::try_from(invalid), Err(AppError::InvalidScore { .. })),
                "{invalid}"
            );
        }
        for invalid in ["high", "", "1.5", "-1"] {
            assert!(
                matches!(invalid.parse::<Score>(), Err(AppError::InvalidScore { .. })),
                "{invalid:?}"
            );
        }
    }

    #[test]
    fn columns_are_right_whatever_order_the_detections_come_in() {
        let text = "ü@x.io, 😀 b@y.io\nc@z.io";
        let lines = TextLines::new(text);
        let columns = |needles: [&str; 4]| -> Vec<usize> {
            needles
                .map(|needle| {
                    let start = text.find(needle).unwrap();
                    match lines
                        .detection(start..start + needle.len(), email(), None)
                        .unwrap()
                        .location
                    {
                        DetectionLocation::Text { column, .. } => column,
                        other => panic!("{other:?}"),
                    }
                })
                .to_vec()
        };

        assert_eq!(
            columns(["ü@x.io", "b@y.io", "c@z.io", "x.io"]),
            [1, 11, 1, 3]
        );
        assert_eq!(
            columns(["c@z.io", "b@y.io", "x.io", "ü@x.io"]),
            [1, 11, 3, 1]
        );
    }

    #[test]
    fn a_text_detection_is_placed_by_line_and_character_column() {
        let text = "Grüße\nÜber a@b.io";
        let lines = TextLines::new(text);
        let start = text.find("a@b.io").unwrap();

        assert_eq!(
            lines
                .detection(start..start + 6, email(), Some(score(0.9)))
                .unwrap(),
            Detection {
                label: email(),
                location: DetectionLocation::Text {
                    start,
                    end: start + 6,
                    line: 2,
                    column: 6,
                },
                score: Some(score(0.9)),
                value: "a@b.io".to_string(),
            }
        );
        assert_eq!(
            lines.detection(0..2, email(), None).unwrap().location,
            DetectionLocation::Text {
                start: 0,
                end: 2,
                line: 1,
                column: 1,
            }
        );
        for (outside, why) in [
            (start..text.len() + 1, "past the end"),
            (2..3, "inside `ü`"),
        ] {
            assert!(
                matches!(
                    lines.detection(outside, email(), None),
                    Err(AppError::DetectionOutsideContent { .. })
                ),
                "{why}"
            );
        }
    }

    #[test]
    fn a_table_cell_finding_names_its_row_and_column() {
        let detection =
            Detection::in_table_cell(2, 1, "mail a@b.io", 5..11, email(), Some(score(0.9)))
                .unwrap();
        assert_eq!(
            detection,
            Detection {
                label: email(),
                location: DetectionLocation::TableCell {
                    row: 3,
                    column: 2,
                    start: 5,
                    end: 11
                },
                score: Some(score(0.9)),
                value: "a@b.io".to_string(),
            }
        );
    }

    #[test]
    fn located_values_cover_every_occurrence_in_text_order() {
        let text = "Jörg wrote to Ana, then Jörg left";
        let person = DetectionLabel::new("PERSON");
        let detections = Detection::locate_values(
            text,
            &[
                ("Ana".to_string(), person.clone()),
                ("Jörg".to_string(), person.clone()),
            ],
        )
        .unwrap()
        .found;
        let second = text.rfind("Jörg").unwrap();
        assert_eq!(
            located(&detections),
            vec![
                (0..5, "Jörg"),
                (15..18, "Ana"),
                (second..second + 5, "Jörg")
            ]
        );
        assert!(detections
            .iter()
            .all(|detection| detection.label == person && detection.score.is_none()));
    }

    #[test]
    fn a_value_inside_a_longer_reported_value_is_not_reported_again() {
        let text = "John Michael Smith met John";
        let person = DetectionLabel::new("PERSON");
        let detections = Detection::locate_values(
            text,
            &[
                ("John".to_string(), person.clone()),
                ("John Michael Smith".to_string(), person),
            ],
        )
        .unwrap()
        .found;
        assert_eq!(
            located(&detections),
            vec![(0..18, "John Michael Smith"), (23..27, "John")]
        );
    }

    fn values_found(text: &str, value: &str) -> Vec<String> {
        Detection::locate_values(text, &[(value.to_string(), DetectionLabel::new("PERSON"))])
            .unwrap()
            .found
            .into_iter()
            .map(|detection| detection.value)
            .collect()
    }

    #[test]
    fn a_value_is_located_wherever_it_occurs_ignoring_case_and_not_accents() {
        for (text, value, found) in [
            ("Johnson called John", "John", &["John", "John"][..]),
            ("ticket 4242 and 42", "42", &["42", "42", "42"]),
            (
                "Herr Müller und Müllers Adresse",
                "Müller",
                &["Müller", "Müller"],
            ),
            ("田中太郎さんに連絡してください", "田中太郎", &["田中太郎"]),
            ("電話は09012345678です", "09012345678", &["09012345678"]),
            ("Ask Ana.", "Zoe", &[]),
            ("john and John", "john", &["john", "John"]),
            ("Ask JOHN SMITH.", "john smith", &["JOHN SMITH"]),
            ("Ο ΟΔΥΣΣΕΥΣ ήρθε", "Οδυσσευς", &["ΟΔΥΣΣΕΥΣ"]),
            ("Ο ΓΙΑΝΝΗΣ ήρθε", "Γιαννης", &["ΓΙΑΝΝΗΣ"]),
            ("Ο Γιαννης ήρθε", "ΓΙΑΝΝΗΣ", &["Γιαννης"]),
            ("Ma\u{17f}on", "Mason", &["Ma\u{17f}on"]),
            ("STRASSE", "Straße", &[]),
            ("Flights to İstanbul", "istanbul", &["İstanbul"]),
            ("Flights to ISTANBUL", "İstanbul", &["ISTANBUL"]),
            ("Herr Muller", "Müller", &[]),
            ("Herr Müller", "Muller", &[]),
        ] {
            assert_eq!(values_found(text, value), found, "{value:?} in {text:?}");
        }
    }

    #[test]
    fn the_range_of_a_case_insensitive_match_is_in_bytes_of_the_original_text() {
        let place = DetectionLabel::new("LOCATION");
        let text = "Kelvin K, then İstanbul";
        let detections = Detection::locate_values(text, &[("istanbul".to_string(), place)])
            .unwrap()
            .found;
        let start = text.find('İ').unwrap();
        assert_eq!(located(&detections), vec![(start..text.len(), "İstanbul")]);
    }

    #[test]
    fn partially_overlapping_values_are_both_reported() {
        let text = "Ana Lima Street 5";
        let detections = Detection::locate_values(
            text,
            &[
                ("Ana Lima".to_string(), DetectionLabel::new("PERSON")),
                ("Lima Street 5".to_string(), DetectionLabel::new("ADDRESS")),
            ],
        )
        .unwrap()
        .found;
        assert_eq!(
            located(&detections),
            vec![(0..8, "Ana Lima"), (4..17, "Lima Street 5")]
        );
    }

    #[test]
    fn located_values_ignore_empty_and_repeated_values() {
        let detections = Detection::locate_values(
            "a@b.io",
            &[
                (String::new(), email()),
                ("a@b.io".to_string(), email()),
                ("a@b.io".to_string(), email()),
            ],
        )
        .unwrap()
        .found;
        assert_eq!(detections.len(), 1);
    }

    #[test]
    fn locations_sort_in_the_order_a_reader_meets_them() {
        let text = |start| DetectionLocation::Text {
            start,
            end: start + 1,
            line: 1,
            column: start + 1,
        };
        let cell = |row, column| DetectionLocation::TableCell {
            row,
            column,
            start: 0,
            end: 1,
        };
        let image = |page, x1, y1| DetectionLocation::Image {
            page,
            x1,
            y1,
            x2: x1 + 10.0,
            y2: y1 + 10.0,
        };
        let in_order = vec![
            text(0),
            text(4),
            cell(1, 2),
            cell(2, 1),
            image(None, 90.0, 90.0),
            image(Some(1), 50.0, 10.0),
            image(Some(1), 5.0, 20.0),
            image(Some(1), 40.0, 20.0),
            image(Some(2), 0.0, 0.0),
        ];
        let mut sorted = in_order.clone();
        sorted.reverse();

        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

        assert_eq!(sorted, in_order);
    }

    #[test]
    fn character_offsets_become_byte_ranges_and_none_past_the_text() {
        let offsets = CharOffsets::new("Grüße, Zoë");
        assert_eq!(offsets.byte_range(0, 5), Some(0..7));
        assert_eq!(offsets.byte_range(7, 10), Some(9..13));
        assert_eq!(offsets.byte_range(5, 5), Some(7..7));
        for (start, end) in [(0, 11), (11, 12), (3, 2)] {
            assert_eq!(offsets.byte_range(start, end), None, "{start}..{end}");
        }
    }

    #[test]
    fn values_with_no_occurrence_are_counted_once_and_not_named() {
        let person = DetectionLabel::new("PERSON");

        let detections = Detection::locate_values(
            "Ask Ana.",
            &[
                ("Zoe".to_string(), person.clone()),
                ("Zoe".to_string(), person.clone()),
                ("Max".to_string(), person.clone()),
                ("Ana".to_string(), person),
            ],
        )
        .unwrap();

        assert_eq!(located(&detections.found), vec![(4..7, "Ana")]);
        assert_eq!(detections.unlocated, 2);
    }

    #[test]
    fn masking_replaces_each_character_of_a_detected_range_on_multibyte_text() {
        let text = "Grüße, Zoë Keller: zoe@example.com";
        let lines = TextLines::new(text);
        let detection = |needle: &str| {
            let start = text.find(needle).unwrap();
            lines
                .detection(
                    start..start + needle.len(),
                    DetectionLabel::new("PII"),
                    None,
                )
                .unwrap()
        };

        let masked = mask_detections(
            text,
            &[
                detection("zoe@example.com"),
                detection("Zoë Keller"),
                detection("Keller: zoe"),
            ],
        );

        assert_eq!(masked, "Grüße, XXXXXXXXXXXXXXXXXXXXXXXXXXX");
    }

    #[test]
    fn a_page_and_the_whole_file_serialise_by_kind() {
        let page = serde_json::to_value(DetectionLocation::Page { page: 2 }).unwrap();
        let file = serde_json::to_value(DetectionLocation::File).unwrap();

        assert_eq!(page, serde_json::json!({"kind": "page", "page": 2}));
        assert_eq!(file, serde_json::json!({"kind": "file"}));
    }
}
