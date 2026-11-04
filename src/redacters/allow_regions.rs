//! The block form of the allow marker: every finding between the lines of
//! [`ALLOW_BEGIN_MARKER`] and [`ALLOW_END_MARKER`] is ignored, as one covered by the inline
//! [`crate::redacters::ALLOW_MARKER`].
//!
//! A region holds the lines strictly between the begin line and the end line, so a finding on a
//! marker line is not ignored unless the inline marker ignores it. A finding is ignored only when
//! it lies entirely inside one region. A begin that no end follows ignores nothing, since a
//! forgotten end marker would otherwise hide the rest of the file.
//!
//! A text read in pieces reads the markers that start in a piece with the window of that piece.
//! A value no longer than the context of a window that includes a byte of the piece lies whole
//! inside that window, so a marker that starts in the piece is judged against every value it can
//! be inside. A marker in the context after the piece is left to the window of the next piece.
//! A finding is therefore ignored only by a region whose end lies in its own piece or before.

use crate::redacters::{markers_outside, AllowMarker, ALLOW_BEGIN_MARKER, ALLOW_END_MARKER};
use serde::Serialize;
use std::fmt::{Display, Formatter};
use std::ops::{Range, RangeInclusive};

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

/// Where in the text of a window the markers are read.
pub(crate) struct ReadSpans {
    /// The 1-based line number in the file of the first line of the window's text.
    pub first_line: usize,
    /// The bytes of the window's text that hold whole lines, in which the inline marker counts.
    pub whole_lines: Range<usize>,
    /// The bytes of the window's text that are the piece of the window, in which the block
    /// markers count.
    pub piece: Range<usize>,
}

impl ReadSpans {
    /// The spans of a text read whole.
    pub fn whole(text: &str) -> Self {
        Self {
            first_line: 1,
            whole_lines: 0..text.len(),
            piece: 0..text.len(),
        }
    }
}

/// The regions of one text, read window by window in file order. Every line is the 1-based
/// number of a line in the file, not in a window.
#[derive(Default)]
pub(crate) struct AllowRegions {
    /// The line of the begin that no end closed yet.
    open: Option<usize>,
    /// The closed regions as the lines of their begin and their end, in file order and apart.
    /// The regions that end before the last window started are dropped.
    closed: Vec<Range<usize>>,
}

impl AllowRegions {
    /// Reads the block markers that start in `spans.piece` of `text`, and returns what does not
    /// pair up. Windows are read in file order, each piece once.
    ///
    /// A marker inside a range of `findings`, the findings of the window as byte ranges in
    /// `text`, is part of a value and does not count.
    pub fn read_window(
        &mut self,
        text: &str,
        spans: &ReadSpans,
        findings: &[Range<usize>],
    ) -> Vec<AllowMarkerWarning> {
        let dropped = self
            .closed
            .partition_point(|region| region.end <= spans.first_line);
        self.closed.drain(..dropped);

        let mut warnings = Vec::new();
        let (mut line, mut counted_to) = (spans.first_line, 0);
        for (at, marker) in markers_outside(text, spans.piece.start..text.len(), findings)
            .take_while(|(at, _)| *at < spans.piece.end)
        {
            line += text[counted_to..at].matches('\n').count();
            counted_to = at;
            match (marker, self.open) {
                (AllowMarker::Inline, _) => {}
                (AllowMarker::Begin, None) => self.open = Some(line),
                (AllowMarker::Begin, Some(_)) => warnings.push(AllowMarkerWarning {
                    problem: AllowMarkerProblem::NestedBegin,
                    line,
                }),
                (AllowMarker::End, Some(begin)) => {
                    self.open = None;
                    self.closed.push(begin..line);
                }
                (AllowMarker::End, None) => warnings.push(AllowMarkerWarning {
                    problem: AllowMarkerProblem::StrayEnd,
                    line,
                }),
            }
        }
        warnings
    }

    /// Whether the lines `lines`, in the file, lie entirely inside one closed region.
    pub fn covers(&self, lines: RangeInclusive<usize>) -> bool {
        let regions_before = self
            .closed
            .partition_point(|region| region.start < *lines.start());
        regions_before
            .checked_sub(1)
            .is_some_and(|last| *lines.end() < self.closed[last].end)
    }

    /// The warning for the begin that no end closed, once the whole text was read.
    pub fn finish(self) -> Option<AllowMarkerWarning> {
        self.open.map(|line| AllowMarkerWarning {
            problem: AllowMarkerProblem::UnclosedBegin,
            line,
        })
    }
}
