//! The block form of the allow marker: every finding inside a region that
//! [`ALLOW_BEGIN_MARKER`] and [`ALLOW_END_MARKER`] enclose is ignored, as one covered by the
//! inline [`crate::redacters::ALLOW_MARKER`].
//!
//! A region runs from the start of the begin line to the end of the end line, so the marker
//! lines belong to it. A finding is ignored only when it lies entirely inside one region. A
//! begin that no end follows ignores nothing, since a forgotten end marker would otherwise hide
//! the rest of the file.

use crate::redacters::text_pieces::TextPosition;
use crate::redacters::{has_block_marker_outside, ALLOW_BEGIN_MARKER, ALLOW_END_MARKER};
use serde::Serialize;
use std::fmt::{Display, Formatter};
use std::ops::Range;

/// What is wrong with the block markers of a file. None of them hides a finding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AllowMarkerProblem {
    /// A begin with no end after it.
    UnclosedBegin,
    /// An end with no begin before it.
    StrayEnd,
    /// A begin inside a region that is already open. The region keeps its first begin.
    NestedBegin,
}

/// A block marker that does not pair up, and the 1-based line it is on. It never holds a value
/// of the file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct AllowMarkerWarning {
    pub problem: AllowMarkerProblem,
    pub line: usize,
}

impl Display for AllowMarkerWarning {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let line = self.line;
        match self.problem {
            AllowMarkerProblem::UnclosedBegin => write!(
                f,
                "line {line}: {ALLOW_BEGIN_MARKER} has no {ALLOW_END_MARKER} after it, so it ignores nothing"
            ),
            AllowMarkerProblem::StrayEnd => write!(
                f,
                "line {line}: {ALLOW_END_MARKER} has no {ALLOW_BEGIN_MARKER} before it"
            ),
            AllowMarkerProblem::NestedBegin => write!(
                f,
                "line {line}: {ALLOW_BEGIN_MARKER} inside a region that is already open, the region keeps its first begin"
            ),
        }
    }
}

struct OpenRegion {
    /// The file offset of the start of the begin line.
    start: usize,
    line: usize,
}

/// The regions of one text, read window by window in file order. Every offset is an offset in
/// the file, not in a window.
#[derive(Default)]
pub(crate) struct AllowRegions {
    /// The file offset up to which lines were read for markers; the start of a line. Windows
    /// overlap, and a line is read once.
    read_to: usize,
    open: Option<OpenRegion>,
    /// The closed regions that end after the last window started, in file order and apart.
    closed: Vec<Range<usize>>,
}

impl AllowRegions {
    /// Reads the markers on the whole lines of a window, `whole_lines` being their byte range in
    /// `text`, and returns what does not pair up. A window starts at `start` in the file.
    ///
    /// A marker inside a range of `findings`, the findings of the window as byte ranges in
    /// `text`, is part of a value and does not count.
    pub fn read_window(
        &mut self,
        text: &str,
        start: TextPosition,
        whole_lines: Range<usize>,
        findings: &[Range<usize>],
    ) -> Vec<AllowMarkerWarning> {
        let skipped = self
            .closed
            .partition_point(|region| region.end <= start.offset);
        self.closed.drain(..skipped);

        let from = whole_lines
            .start
            .max(self.read_to.saturating_sub(start.offset));
        if from >= whole_lines.end {
            return Vec::new();
        }
        let mut warnings = Vec::new();
        let first_line = start.lines_before + 1 + text[..from].matches('\n').count();
        let mut line_start = from;
        for (line, line_text) in
            (first_line..).zip(text[from..whole_lines.end].split_inclusive('\n'))
        {
            let line_range = line_start..line_start + line_text.len();
            if has_block_marker_outside(text, line_range.clone(), ALLOW_BEGIN_MARKER, findings) {
                if self.open.is_some() {
                    warnings.push(AllowMarkerWarning {
                        problem: AllowMarkerProblem::NestedBegin,
                        line,
                    });
                } else {
                    self.open = Some(OpenRegion {
                        start: start.offset + line_start,
                        line,
                    });
                }
            }
            if has_block_marker_outside(text, line_range.clone(), ALLOW_END_MARKER, findings) {
                match self.open.take() {
                    Some(open) => self.closed.push(open.start..start.offset + line_range.end),
                    None => warnings.push(AllowMarkerWarning {
                        problem: AllowMarkerProblem::StrayEnd,
                        line,
                    }),
                }
            }
            line_start = line_range.end;
        }
        self.read_to = self.read_to.max(start.offset + whole_lines.end);
        warnings
    }

    /// Whether `range`, in the file, lies entirely inside one closed region.
    pub fn covers(&self, range: Range<usize>) -> bool {
        let regions_before = self
            .closed
            .partition_point(|region| region.start <= range.start);
        regions_before
            .checked_sub(1)
            .is_some_and(|last| range.end <= self.closed[last].end)
    }

    /// The warning for the begin that no end closed, once the whole text was read.
    pub fn finish(self) -> Option<AllowMarkerWarning> {
        self.open.map(|open| AllowMarkerWarning {
            problem: AllowMarkerProblem::UnclosedBegin,
            line: open.line,
        })
    }
}
