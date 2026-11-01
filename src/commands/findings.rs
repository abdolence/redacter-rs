//! What a command found in the files of a run, at whatever precision it knows, as the report
//! writers take it. A writer does not know the command that produced it.

use crate::args::RedacterType;
use crate::commands::scan_output::mask_value;
use crate::redacters::{Category, DetectionLabel, DetectionLocation, Score};

/// A file of a run and what the command found in it.
#[derive(Debug, Clone, PartialEq)]
pub struct ReportedFile {
    /// The path as the source's file system names it, relative to the source.
    pub path: String,
    pub outcome: FileOutcome,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FileOutcome {
    /// The file was read. A clean file has no findings.
    Findings(Vec<ReportedFinding>),
    /// The file was read and has no findings, but the redacters named values they could not
    /// place in it, so it is not known to be clean.
    Unconfirmed { unlocated_values: usize },
    /// The file could not be read or processed.
    Failed { reason: String },
    /// The file needs a converter the run lacks.
    Blocked { reason: String },
    /// No selected redacter reads the file.
    Unsupported { reason: String },
}

/// One piece of sensitive data in a file. A report holds its value masked, whatever the
/// command shows.
#[derive(Debug, Clone, PartialEq)]
pub struct ReportedFinding {
    pub(super) redacter: RedacterType,
    pub(super) label: DetectionLabel,
    pub(super) score: Option<Score>,
    pub(super) location: DetectionLocation,
    /// `place` is the location in the terms of a report that cannot hold all of it; `None` when
    /// only the file is known to hold the value.
    pub(super) place: Option<Place>,
    pub(super) masked_value: String,
}

impl ReportedFinding {
    /// `value` may be the value as found: it is masked here.
    pub fn new(
        redacter: RedacterType,
        label: DetectionLabel,
        score: Option<Score>,
        location: DetectionLocation,
        place: Option<Place>,
        value: &str,
    ) -> Self {
        Self {
            redacter,
            label,
            score,
            location,
            place,
            masked_value: mask_value(value),
        }
    }

    /// Whether the finding is a credential: a password, a key or a token.
    pub(super) fn is_secret(&self) -> bool {
        self.label.category(self.redacter) == Category::Credential
    }
}

/// Where in a file something is, at the precision a command knows. Line, column, row and page
/// numbers count from 1.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Place {
    /// A range of a text. The column counts characters, and `end_column` is the one after the
    /// last character of the range.
    Text {
        line: usize,
        column: usize,
        end_line: usize,
        end_column: usize,
    },
    /// A cell of a table, the header row not counted. The line of a cell in its file is not
    /// known.
    TableCell { row: usize, column: usize },
    /// An image, or a page of a PDF, and the box on it when the position is known.
    Image {
        page: Option<usize>,
        area: Option<PixelBox>,
    },
}

/// A box in pixels, `x1 <= x2` and `y1 <= y2`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PixelBox {
    pub x1: f32,
    pub y1: f32,
    pub x2: f32,
    pub y2: f32,
}

impl Place {
    /// The place of a detection; `value` is the text it matched, which gives the end of a
    /// range that spans lines. `None` for a location that names the file as a whole.
    pub fn new(location: DetectionLocation, value: &str) -> Option<Self> {
        Some(match location {
            DetectionLocation::Text { line, column, .. } => {
                let (end_line, end_column) = match value.rsplit_once('\n') {
                    Some((before, last_line)) => (
                        line + before.matches('\n').count() + 1,
                        last_line.chars().count() + 1,
                    ),
                    None => (line, column + value.chars().count()),
                };
                Self::Text {
                    line,
                    column,
                    end_line,
                    end_column,
                }
            }
            DetectionLocation::TableCell { row, column, .. } => Self::TableCell { row, column },
            DetectionLocation::Image {
                page,
                x1,
                y1,
                x2,
                y2,
            } => Self::Image {
                page,
                area: Some(PixelBox { x1, y1, x2, y2 }),
            },
            DetectionLocation::Page { page } => Self::Image {
                page: Some(page),
                area: None,
            },
            DetectionLocation::File => return None,
        })
    }
}
