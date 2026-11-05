//! The block form of the allow marker: every finding between the lines of
//! [`ALLOW_BEGIN_MARKER`] and [`ALLOW_END_MARKER`] is ignored, as one covered by the inline
//! [`crate::redacters::ALLOW_MARKER`].
//!
//! A region holds the lines strictly between the begin line and the end line, so a finding on a
//! marker line is not ignored unless the inline marker ignores it. A finding is ignored only when
//! it lies entirely inside one region. A begin that no end follows ignores nothing, since a
//! forgotten end marker would otherwise hide the rest of the file.
//!
//! A text read in pieces reads each marker once, in the last window that holds it: the markers
//! from the start of a window's text to where the next window starts. A marker is judged against
//! the findings that the window reports and the earlier windows reported, never against one a
//! window leaves to an earlier window: a window that starts inside a value need not find what a
//! whole read finds there.
//!
//! A marker so read lies at least two contexts before the end of its window, so a value of up to
//! twice the context that holds it ends inside the window, and one that starts before the window
//! lies whole in an earlier window. Either way the marker is inside a finding that one of those
//! windows reports. A value longer than twice the context is found by no window, as for any
//! finding, so a marker inside it counts.
//!
//! A finding is ignored only by a region whose end was read before the finding's window was
//! judged, so one whose end marker lies in the last context of its piece is reported.

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
    /// Where the window's text starts in the file, in bytes.
    pub offset: usize,
    /// The bytes of the window's text that hold whole lines, in which the inline marker counts.
    pub whole_lines: Range<usize>,
    /// The end of the bytes of the window's text in which the block markers count: where the next
    /// window starts, or the end of the text for the last window. They count from the start of
    /// the text.
    pub markers_end: usize,
}

impl ReadSpans {
    /// The spans of a text read whole.
    pub fn whole(text: &str) -> Self {
        Self {
            first_line: 1,
            offset: 0,
            whole_lines: 0..text.len(),
            markers_end: text.len(),
        }
    }
}

/// A region between two block markers, as the 1-based lines of the markers.
#[derive(Debug, Clone, Copy)]
struct Region {
    begin: usize,
    end: usize,
}

/// The regions of one text, read window by window in file order. Every line is the 1-based
/// number of a line in the file, not in a window.
#[derive(Default)]
pub(crate) struct AllowRegions {
    /// The line of the begin that no end closed yet.
    open: Option<usize>,
    /// The closed regions, in file order and apart. The regions that end before the last window
    /// started are dropped.
    closed: Vec<Region>,
    /// The findings that the windows before reported and that reach past the start of the next
    /// window, as byte ranges in the file.
    carried: Vec<Range<usize>>,
}

impl AllowRegions {
    /// Reads the block markers that start in `text[..spans.markers_end]`, and returns what does
    /// not pair up. Windows are read in file order, each marker once.
    ///
    /// A marker inside a range of `findings`, the findings the window reports as byte ranges in
    /// `text`, or inside a finding of an earlier window that reaches into `text`, is part of a
    /// value and does not count.
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

        let judged_against: Vec<Range<usize>> = findings
            .iter()
            .cloned()
            .chain(self.carried.iter().map(|carried| {
                carried.start.saturating_sub(spans.offset)..carried.end.saturating_sub(spans.offset)
            }))
            .collect();
        let mut warnings = Vec::new();
        let (mut line, mut counted_to) = (spans.first_line, 0);
        for (at, marker) in markers_outside(text, 0..text.len(), &judged_against)
            .take_while(|(at, _)| *at < spans.markers_end)
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
                    self.closed.push(Region { begin, end: line });
                }
                (AllowMarker::End, None) => warnings.push(AllowMarkerWarning {
                    problem: AllowMarkerProblem::StrayEnd,
                    line,
                }),
            }
        }

        let next_start = spans.offset + spans.markers_end;
        self.carried.retain(|carried| carried.end > next_start);
        self.carried.extend(
            findings
                .iter()
                .map(|finding| finding.start + spans.offset..finding.end + spans.offset)
                .filter(|finding| finding.end > next_start),
        );
        warnings
    }

    /// Whether the lines `lines`, in the file, lie entirely inside one closed region.
    pub fn covers(&self, lines: RangeInclusive<usize>) -> bool {
        let regions_before = self
            .closed
            .partition_point(|region| region.begin < *lines.start());
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
