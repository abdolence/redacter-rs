//! What a provider found in an item, and where: the contract `scan` reports from. A
//! detection never changes the item; redaction is a separate path.

use crate::errors::AppError;
use crate::redacters::text_spans::{Finding, RuleName};
use crate::redacters::RedacterDataItemContent;
use serde::Serialize;
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

impl DetectionLocation {
    /// Whether the location points into `content`: a text range on character boundaries of
    /// a text item, an existing cell's range for a table, an ordered box for an image.
    pub fn points_into(&self, content: &RedacterDataItemContent) -> bool {
        match (*self, content) {
            (DetectionLocation::Text { start, end }, RedacterDataItemContent::Value(text)) => {
                text.get(start..end).is_some()
            }
            (
                DetectionLocation::TableCell {
                    row,
                    column,
                    start,
                    end,
                },
                RedacterDataItemContent::Table { rows, .. },
            ) => rows
                .get(row)
                .and_then(|cells| cells.get(column))
                .is_some_and(|cell| cell.get(start..end).is_some()),
            (
                DetectionLocation::Image { x1, y1, x2, y2, .. },
                RedacterDataItemContent::Image { .. },
            ) => x1 <= x2 && y1 <= y2,
            _ => false,
        }
    }
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

    /// Every occurrence of every value in `text`, for providers that name the values they
    /// found but not where: each value is matched literally, as a whole string, and an empty
    /// value or a repeated `(value, label)` pair adds nothing. Sorted by position.
    // TODO: drop the allow once the Bedrock Guardrails and LLM detectors call this; they name
    // values without offsets.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn locate_values(text: &str, values: &[(String, DetectionLabel)]) -> Vec<Self> {
        let mut located = std::collections::BTreeSet::new();
        let mut detections: Vec<(usize, Self)> = Vec::new();
        for (value, label) in values {
            if value.is_empty() || !located.insert((value, label)) {
                continue;
            }
            for (start, matched) in text.match_indices(value.as_str()) {
                detections.push((
                    start,
                    Self {
                        label: label.clone(),
                        location: DetectionLocation::Text {
                            start,
                            end: start + matched.len(),
                        },
                        score: None,
                        value: matched.to_string(),
                    },
                ));
            }
        }
        detections.sort_by_key(|(start, _)| *start);
        detections
            .into_iter()
            .map(|(_, detection)| detection)
            .collect()
    }

    /// Whether the detection is kept under `--min-score`: an unscored one always is.
    pub fn meets_min_score(&self, min_score: Score) -> bool {
        self.score.is_none_or(|score| score >= min_score)
    }
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
    fn a_location_must_point_into_the_content_it_was_found_in() {
        let text = RedacterDataItemContent::Value("Grüße a@b.io".to_string());
        let table = RedacterDataItemContent::Table {
            headers: vec!["email".to_string()],
            rows: vec![vec!["a@b.io".to_string()]],
        };
        let image = RedacterDataItemContent::Image {
            mime_type: mime::IMAGE_PNG,
            data: bytes::Bytes::new(),
        };
        let text_range = |start, end| DetectionLocation::Text { start, end };
        let cell = |row, column, end| DetectionLocation::TableCell {
            row,
            column,
            start: 0,
            end,
        };
        let image_box = |x1, x2| DetectionLocation::Image {
            page: None,
            x1,
            y1: 0.0,
            x2,
            y2: 1.0,
        };

        assert!(text_range(8, 14).points_into(&text));
        assert!(!text_range(8, 15).points_into(&text), "past the end");
        assert!(!text_range(3, 7).points_into(&text), "inside `ü`");
        assert!(!text_range(10, 9).points_into(&text), "reversed");
        assert!(cell(0, 0, 6).points_into(&table));
        assert!(!cell(1, 0, 6).points_into(&table), "no such row");
        assert!(!cell(0, 1, 6).points_into(&table), "no such column");
        assert!(!cell(0, 0, 7).points_into(&table), "past the cell");
        assert!(image_box(1.0, 2.0).points_into(&image));
        assert!(!image_box(2.0, 1.0).points_into(&image), "reversed");
        assert!(
            !text_range(0, 1).points_into(&image),
            "a text range on an image"
        );
        assert!(!image_box(1.0, 2.0).points_into(&text), "a box on a text");
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
    fn the_min_score_drops_weaker_scored_detections_and_keeps_unscored_ones() {
        let detection = |score: Option<Score>| Detection {
            label: email(),
            location: DetectionLocation::Text { start: 0, end: 1 },
            score,
            value: "a".to_string(),
        };
        let floor = score(0.7);
        assert!(detection(None).meets_min_score(floor));
        assert!(detection(Some(score(0.7))).meets_min_score(floor));
        assert!(!detection(Some(score(0.69))).meets_min_score(floor));
    }
}
