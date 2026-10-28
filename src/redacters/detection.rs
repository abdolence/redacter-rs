//! What a provider found in an item, and where: the contract `scan` reports from. A
//! detection never changes the item; redaction is a separate path.

use crate::errors::AppError;
use crate::redacters::text_spans::{Finding, RuleName};
use serde::Serialize;
use std::collections::HashSet;
use std::fmt::Display;
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

    // Read by the model-backed redacters, which compare a score with their f32 floor.
    #[cfg_attr(
        not(any(feature = "local-ner", feature = "local-gliner")),
        allow(dead_code)
    )]
    pub fn value(self) -> f32 {
        self.0
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

/// Where a detection sits in the item the provider was given. Indexes count from 0.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DetectionLocation {
    /// A half-open byte range of the item's text, on character boundaries.
    Text { start: usize, end: usize },
    /// A half-open byte range of one table cell. `row` indexes the data rows, the header row
    /// excluded, and `column` the cells of that row.
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
        x1: f32,
        y1: f32,
        x2: f32,
        y2: f32,
    },
}

impl Detection {
    /// A finding of a local provider on `text`, the text its offsets index into.
    pub fn in_text(text: &str, finding: Finding) -> Self {
        Self {
            value: text[finding.start..finding.end].to_string(),
            location: DetectionLocation::Text {
                start: finding.start,
                end: finding.end,
            },
            label: finding.rule.into(),
            score: finding.score,
        }
    }

    /// A finding of a local provider on one table cell, whose text its offsets index into.
    pub fn in_table_cell(row: usize, column: usize, cell: &str, finding: Finding) -> Self {
        Self {
            value: cell[finding.start..finding.end].to_string(),
            location: DetectionLocation::TableCell {
                row,
                column,
                start: finding.start,
                end: finding.end,
            },
            label: finding.rule.into(),
            score: finding.score,
        }
    }

    /// The findings of `find_in_cell(column, cell)` on every cell of `rows`, placed in their
    /// cell, for providers that read a table one cell at a time.
    // Only the model-backed redacters read a table cell by cell.
    #[cfg_attr(
        not(any(feature = "local-ner", feature = "local-gliner")),
        allow(dead_code)
    )]
    pub fn in_table_cells<E>(
        rows: &[Vec<String>],
        mut find_in_cell: impl FnMut(usize, &str) -> Result<Vec<Finding>, E>,
    ) -> Result<Vec<Self>, E> {
        let mut detections = Vec::new();
        for (row_index, row) in rows.iter().enumerate() {
            for (column, cell) in row.iter().enumerate() {
                for finding in find_in_cell(column, cell)? {
                    detections.push(Self::in_table_cell(row_index, column, cell, finding));
                }
            }
        }
        Ok(detections)
    }

    /// Every occurrence of every value in `text`, for providers that name the values they
    /// found but not where. Occurrences are found ignoring case, since a model may not copy a
    /// value letter for letter, and wherever they occur, inside longer words too: `John` is
    /// also reported inside `Johnson`, because text in scripts without spaces and inflected
    /// words have no word boundary around a value, and a missed value is worse than an extra
    /// one. An occurrence inside a longer one is dropped, so a name and its first word are
    /// reported once; partial overlaps are all kept. When two values have the same span, the
    /// first one's label is kept. An empty value or a repeated `(value, label)` pair adds
    /// nothing. Sorted by position.
    pub fn locate_values(text: &str, values: &[(String, DetectionLabel)]) -> Vec<Self> {
        let mut seen = HashSet::new();
        let mut findings = Vec::new();
        let mut unmatched = 0_usize;
        let folded_text = FoldedText::new(text);
        for (value, label) in values {
            if value.is_empty() || !seen.insert((value, label)) {
                continue;
            }
            let ranges = folded_text.occurrences_of(value);
            if ranges.is_empty() {
                unmatched += 1;
            }
            let rule = RuleName::new(&label.to_string());
            findings.extend(ranges.into_iter().map(|(start, end)| Finding {
                start,
                end,
                rule: rule.clone(),
                score: None,
            }));
        }
        if unmatched > 0 {
            // Counted, never named: the values are the sensitive data.
            tracing::warn!(
                unmatched,
                "values named by the provider were not found in the text"
            );
        }
        without_contained(findings)
            .into_iter()
            .map(|finding| Self::in_text(text, finding))
            .collect()
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

    /// The byte ranges of the original text where `value` occurs ignoring case.
    fn occurrences_of(&self, value: &str) -> Vec<(usize, usize)> {
        let folded_value: String = value.chars().map(fold_character).collect();
        self.folded
            .match_indices(&folded_value)
            .filter_map(|(start, matched)| {
                let first = self.folded_starts.binary_search(&start).ok()?;
                let end = self
                    .folded_starts
                    .binary_search(&(start + matched.len()))
                    .ok()?;
                Some((self.original_starts[first], self.original_starts[end]))
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

/// `findings` sorted by position, without those that lie inside a longer one. Findings that
/// only overlap are all kept: two values can share words, and dropping either
/// would leave part of the sensitive data unreported.
fn without_contained(mut findings: Vec<Finding>) -> Vec<Finding> {
    findings.sort_by(|a, b| a.start.cmp(&b.start).then(b.end.cmp(&a.end)));
    let mut furthest_end = 0;
    findings.retain(|finding| {
        let contained = finding.end <= furthest_end;
        furthest_end = furthest_end.max(finding.end);
        !contained
    });
    findings
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
    pub(crate) fn byte_range(&self, start: usize, end: usize) -> Option<(usize, usize)> {
        if start > end {
            return None;
        }
        Some((*self.byte_starts.get(start)?, *self.byte_starts.get(end)?))
    }
}

/// `text` with every text range of `detections` replaced by one `X` per character. Ranges
/// may overlap or arrive in any order.
pub(crate) fn mask_detections(text: &str, detections: &[Detection]) -> String {
    let mut ranges: Vec<(usize, usize)> = detections
        .iter()
        .filter_map(|detection| match detection.location {
            DetectionLocation::Text { start, end } => Some((start, end)),
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

    fn email_finding(start: usize, end: usize) -> Finding {
        Finding {
            start,
            end,
            rule: RuleName::new("email"),
            score: None,
        }
    }

    #[test]
    fn a_score_is_a_probability() {
        for valid in [0.0_f32, 0.5, 1.0] {
            assert_eq!(Score::try_from(valid).unwrap().value(), valid);
        }
        for invalid in [-0.01_f32, 1.01, f32::NAN, f32::INFINITY] {
            assert!(
                matches!(Score::try_from(invalid), Err(AppError::InvalidScore { .. })),
                "{invalid}"
            );
        }
    }

    #[test]
    fn a_score_parses_from_the_command_line_and_rejects_what_is_not_a_probability() {
        assert_eq!("0.8".parse::<Score>().unwrap(), score(0.8));
        for invalid in ["high", "", "1.5", "-1"] {
            assert!(
                matches!(invalid.parse::<Score>(), Err(AppError::InvalidScore { .. })),
                "{invalid:?}"
            );
        }
    }

    #[test]
    fn a_text_finding_carries_its_matched_value_and_byte_range() {
        let text = "Grüße an a@b.io";
        let start = text.find("a@b.io").unwrap();
        let detection = Detection::in_text(text, email_finding(start, start + 6));
        assert_eq!(
            detection,
            Detection {
                label: email(),
                location: DetectionLocation::Text {
                    start,
                    end: start + 6
                },
                score: None,
                value: "a@b.io".to_string(),
            }
        );
    }

    #[test]
    fn a_table_cell_finding_names_its_row_and_column() {
        let mut finding = email_finding(5, 11);
        finding.score = Some(score(0.9));
        let detection = Detection::in_table_cell(2, 1, "mail a@b.io", finding);
        assert_eq!(
            detection,
            Detection {
                label: email(),
                location: DetectionLocation::TableCell {
                    row: 2,
                    column: 1,
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
        );
        let located: Vec<(DetectionLocation, &str)> = detections
            .iter()
            .map(|detection| (detection.location, detection.value.as_str()))
            .collect();
        let second = text.rfind("Jörg").unwrap();
        assert_eq!(
            located,
            vec![
                (DetectionLocation::Text { start: 0, end: 5 }, "Jörg"),
                (DetectionLocation::Text { start: 15, end: 18 }, "Ana"),
                (
                    DetectionLocation::Text {
                        start: second,
                        end: second + 5
                    },
                    "Jörg"
                ),
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
        );
        let located: Vec<(DetectionLocation, &str)> = detections
            .iter()
            .map(|detection| (detection.location, detection.value.as_str()))
            .collect();
        assert_eq!(
            located,
            vec![
                (
                    DetectionLocation::Text { start: 0, end: 18 },
                    "John Michael Smith"
                ),
                (DetectionLocation::Text { start: 23, end: 27 }, "John"),
            ]
        );
    }

    fn values_found(text: &str, value: &str) -> Vec<String> {
        Detection::locate_values(text, &[(value.to_string(), DetectionLabel::new("PERSON"))])
            .into_iter()
            .map(|detection| detection.value)
            .collect()
    }

    #[test]
    fn a_value_is_reported_inside_longer_words_too() {
        assert_eq!(
            values_found("Johnson called John", "John"),
            ["John", "John"]
        );
        assert_eq!(values_found("ticket 4242 and 42", "42"), ["42", "42", "42"]);
        assert_eq!(
            values_found("Herr Müller und Müllers Adresse", "Müller"),
            ["Müller", "Müller"]
        );
    }

    #[test]
    fn greek_final_sigma_and_capitals_match_each_other() {
        assert_eq!(values_found("Ο ΟΔΥΣΣΕΥΣ ήρθε", "Οδυσσευς"), ["ΟΔΥΣΣΕΥΣ"]);
        assert_eq!(values_found("Ο ΓΙΑΝΝΗΣ ήρθε", "Γιαννης"), ["ΓΙΑΝΝΗΣ"]);
        assert_eq!(values_found("Ο Γιαννης ήρθε", "ΓΙΑΝΝΗΣ"), ["Γιαννης"]);
    }

    #[test]
    fn the_long_s_matches_s_and_sharp_s_stays_distinct_from_ss() {
        assert_eq!(values_found("Ma\u{17f}on", "Mason"), ["Ma\u{17f}on"]);
        assert!(values_found("STRASSE", "Straße").is_empty());
    }

    #[test]
    fn a_value_without_an_exact_match_is_located_ignoring_case() {
        let person = DetectionLabel::new("PERSON");
        let detections =
            Detection::locate_values("Ask JOHN SMITH.", &[("john smith".to_string(), person)]);
        assert_eq!(detections.len(), 1);
        assert_eq!(
            detections[0].location,
            DetectionLocation::Text { start: 4, end: 14 }
        );
        assert_eq!(detections[0].value, "JOHN SMITH");
    }

    #[test]
    fn every_occurrence_is_found_whatever_its_case() {
        let person = DetectionLabel::new("PERSON");
        let detections = Detection::locate_values("john and John", &[("john".to_string(), person)]);
        let values: Vec<&str> = detections.iter().map(|d| d.value.as_str()).collect();
        assert_eq!(values, ["john", "John"]);
    }

    #[test]
    fn a_dotted_capital_i_matches_the_letter_i() {
        let place = DetectionLabel::new("LOCATION");
        let values_found = |text: &str, value: &str| -> Vec<String> {
            Detection::locate_values(text, &[(value.to_string(), place.clone())])
                .into_iter()
                .map(|detection| detection.value)
                .collect()
        };
        assert_eq!(
            values_found("Flights to İstanbul", "istanbul"),
            ["İstanbul"]
        );
        assert_eq!(
            values_found("Flights to ISTANBUL", "İstanbul"),
            ["ISTANBUL"]
        );
    }

    #[test]
    fn the_range_of_a_case_insensitive_match_is_in_bytes_of_the_original_text() {
        let place = DetectionLabel::new("LOCATION");
        let text = "Kelvin K, then İstanbul";
        let detections = Detection::locate_values(text, &[("istanbul".to_string(), place)]);
        let start = text.find('İ').unwrap();
        assert_eq!(
            detections[0].location,
            DetectionLocation::Text {
                start,
                end: text.len()
            }
        );
    }

    #[test]
    fn a_value_in_text_without_word_breaks_is_found() {
        assert_eq!(
            values_found("田中太郎さんに連絡してください", "田中太郎"),
            ["田中太郎"]
        );
        assert_eq!(
            values_found("電話は09012345678です", "09012345678"),
            ["09012345678"]
        );
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
        );
        let located: Vec<(DetectionLocation, &str)> = detections
            .iter()
            .map(|detection| (detection.location, detection.value.as_str()))
            .collect();
        assert_eq!(
            located,
            vec![
                (DetectionLocation::Text { start: 0, end: 8 }, "Ana Lima"),
                (
                    DetectionLocation::Text { start: 4, end: 17 },
                    "Lima Street 5"
                ),
            ]
        );
    }

    #[test]
    fn a_value_that_is_nowhere_in_the_text_is_not_reported() {
        let person = DetectionLabel::new("PERSON");
        assert!(Detection::locate_values("Ask Ana.", &[("Zoe".to_string(), person)]).is_empty());
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
        );
        assert_eq!(detections.len(), 1);
    }

    #[test]
    fn masking_replaces_each_character_of_a_detected_range_on_multibyte_text() {
        let text = "Grüße, Zoë Keller: zoe@example.com";
        let detection = |needle: &str, len_in_bytes: usize| {
            let start = text.find(needle).unwrap();
            Detection {
                label: DetectionLabel::new("PII"),
                location: DetectionLocation::Text {
                    start,
                    end: start + len_in_bytes,
                },
                score: None,
                value: needle.to_string(),
            }
        };

        let masked = mask_detections(
            text,
            &[
                detection("zoe@example.com", 15),
                detection("Zoë Keller", "Zoë Keller".len()),
                detection("Keller: zoe", "Keller: zoe".len()),
            ],
        );

        assert_eq!(masked, "Grüße, XXXXXXXXXXXXXXXXXXXXXXXXXXX");
    }
}
