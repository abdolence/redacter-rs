//! Redacts a text read in pieces, so `cp` holds a few windows of it and not the file.
//!
//! Each window is read by the chain of redacters in turn, every step reading the window as the
//! steps before it left it. A step only detects: the values it finds are placed in the window, and
//! the masks are made here, each with the look of the redacter that found the value. Ranges from
//! every window are then merged in the file, and the text is written once nothing a later window
//! finds can change it.

use crate::args::RedacterType;
use crate::errors::AppError;
use crate::file_systems::FileSystemRef;
use crate::redacters::text_changes::{OutputPosition, TextChange, TextChanges};
use crate::redacters::{
    Categories, Detection, DetectionLabel, DetectionLocation, PieceLayout, Redacter,
    RedacterDataItem, RedacterDataItemContent, Redacters, Score, TextLines, TextMask,
    TextPieceLimits, TextPieces, TextPosition, TextWindow,
};
use crate::AppResult;
use bytes::Bytes;
use futures::Stream;
use std::ops::Range;
use tokio::sync::mpsc;

/// How many chunks of written text wait for the upload to take them.
const WRITTEN_CHUNKS_IN_FLIGHT: usize = 4;

/// The size of the chunks the redacted text goes to the upload in. Chunks of one size are given
/// back to the allocator and taken again whole, where a text of a window's size, different for
/// every window, would leave freed memory of every size behind.
const WRITTEN_CHUNK_BYTES: usize = 256 * 1024;

/// A value one step of the chain found, placed in a text.
#[derive(Debug, Clone)]
struct StepFinding {
    range: Range<usize>,
    /// The place of the redacter in the chain.
    step: usize,
    mask: TextMask,
    label: DetectionLabel,
    score: Option<Score>,
}

/// Findings that overlap, as one range to mask.
#[derive(Debug, PartialEq, Eq)]
struct MergedRange {
    range: Range<usize>,
    /// The finding whose mask the range takes: the latest step among the findings that make it,
    /// and among those the one that starts first and reaches furthest.
    representative: usize,
}

/// The findings united where they overlap, in order of position. Findings that only touch stay
/// apart, as a mask stands for one value.
fn merge(findings: &[StepFinding]) -> Vec<MergedRange> {
    let mut order: Vec<usize> = Vec::with_capacity(findings.len());
    order.extend((0..findings.len()).filter(|&index| !findings[index].range.is_empty()));
    order.sort_by(|&a, &b| {
        let (a_range, b_range) = (&findings[a].range, &findings[b].range);
        a_range
            .start
            .cmp(&b_range.start)
            .then(b_range.end.cmp(&a_range.end))
            .then(a.cmp(&b))
    });
    let mut merged: Vec<MergedRange> = Vec::with_capacity(order.len());
    for index in order {
        let finding = &findings[index];
        match merged.last_mut() {
            Some(last) if finding.range.start < last.range.end => {
                last.range.end = last.range.end.max(finding.range.end);
                if finding.step > findings[last.representative].step {
                    last.representative = index;
                }
            }
            _ => merged.push(MergedRange {
                range: finding.range.clone(),
                representative: index,
            }),
        }
    }
    merged
}

/// One redacter of the chain with what it reads of a window.
struct ChainStep<'a> {
    redacter: &'a Redacters<'a>,
    redacter_type: RedacterType,
    mask: TextMask,
    /// How much of the window it reads on each side of the piece.
    reach: usize,
    max_request_bytes: usize,
}

/// The chain of redacters that read the windows of one file.
pub(crate) struct PieceRedaction<'a> {
    steps: Vec<ChainStep<'a>>,
    categories: Option<&'a Categories>,
    file_ref: FileSystemRef,
    recording: bool,
}

/// What the chain found in one window, in the file.
struct WindowFindings {
    text: String,
    start: TextPosition,
    next_start: Option<usize>,
    findings: Vec<StepFinding>,
}

impl<'a> PieceRedaction<'a> {
    /// The chain of `redacters`, each of which reads text in pieces. With `recording`, the
    /// redactions are placed in the file for the report.
    pub fn new(
        redacters: &[&'a Redacters<'a>],
        categories: Option<&'a Categories>,
        file_ref: &FileSystemRef,
        recording: bool,
    ) -> AppResult<Self> {
        let limits = redacters
            .iter()
            .map(|redacter| {
                redacter
                    .text_piece_limits()
                    .ok_or_else(|| AppError::SystemError {
                        message: format!(
                            "{} does not read text in pieces",
                            redacter.redacter_type()
                        ),
                    })
            })
            .collect::<AppResult<Vec<TextPieceLimits>>>()?;
        let steps = redacters
            .iter()
            .zip(&limits)
            .zip(PieceLayout::chain_reaches(&limits))
            .map(|((redacter, limits), reach)| ChainStep {
                redacter,
                redacter_type: redacter.redacter_type(),
                mask: redacter.text_mask(),
                reach,
                max_request_bytes: limits.max_request_bytes,
            })
            .collect();
        Ok(Self {
            steps,
            categories,
            file_ref: file_ref.clone(),
            recording,
        })
    }

    /// The redacters of the chain, in order.
    pub fn redacters(&self) -> Vec<RedacterType> {
        self.steps.iter().map(|step| step.redacter_type).collect()
    }

    /// Runs the chain on `window`: every step reads the window as the steps before it left it,
    /// and finds values in what it reads. A window depends on no other, so it can be read at any
    /// time before it is merged into the file.
    ///
    /// The findings are those the window answers for: the ones that start in its piece, and the
    /// ones that start before it and run into it, which only this window may have seen whole.
    async fn find_in_window(&self, window: TextWindow) -> AppResult<WindowFindings> {
        let read = window.text.as_str();
        let window_start = window.start.offset;
        let piece = window.piece.start - window_start..window.piece.end - window_start;
        let unchanged = TextChanges::new(Vec::new());
        let mut found: Vec<StepFinding> = Vec::new();
        let mut masked: Option<(String, TextChanges)> = None;

        for (index, step) in self.steps.iter().enumerate() {
            let (text, changes) = match &masked {
                Some((text, changes)) => (text.as_str(), changes),
                None => (read, &unchanged),
            };
            // The first step reads the window as it was read, which the layout cut to its reach.
            let reach = if index == 0 { usize::MAX } else { step.reach };
            let cut = step_window(read, text, changes, &piece, reach, step.max_request_bytes)?;
            let item = RedacterDataItem {
                content: RedacterDataItemContent::Value(text[cut.clone()].to_string()),
                file_ref: self.file_ref.clone(),
            };
            let mut detections = step.redacter.detect(&item).await?;
            // A redacter that narrows itself finds nothing outside the categories already.
            if let Some(categories) = self
                .categories
                .filter(|_| !step.redacter_type.narrows_itself())
            {
                detections.retain_categories(step.redacter_type, categories);
            }
            if !detections.unlocated.is_empty() {
                return Err(AppError::UnlocatedValues {
                    redacter: step.redacter_type,
                    count: detections.unlocated.len(),
                });
            }
            found.reserve(detections.found.len());
            for detection in detections.found {
                let DetectionLocation::Text { start, end, .. } = detection.location else {
                    continue;
                };
                found.push(StepFinding {
                    range: changes.to_input(cut.start + start..cut.start + end),
                    step: index,
                    mask: step.mask,
                    label: detection.label,
                    score: detection.score,
                });
            }
            if index + 1 < self.steps.len() {
                masked = Some(mask_text(read, &found));
            }
        }

        found.retain(|finding| {
            piece.contains(&finding.range.start)
                || (finding.range.start < piece.start && finding.range.end > piece.start)
        });
        for finding in &mut found {
            finding.range = finding.range.start + window_start..finding.range.end + window_start;
        }
        Ok(WindowFindings {
            start: window.start,
            next_start: window.next_start,
            text: window.text,
            findings: found,
        })
    }
}

/// `text` with the findings merged and masked, and the changes that made it.
fn mask_text(text: &str, findings: &[StepFinding]) -> (String, TextChanges) {
    let merged = merge(findings);
    let mut masked = String::with_capacity(text.len());
    let mut changes = Vec::with_capacity(merged.len());
    let mut cursor = 0;
    for range in &merged {
        masked.push_str(&text[cursor..range.range.start]);
        let representative = &findings[range.representative];
        let start = masked.len();
        masked.push_str(
            &representative
                .mask
                .replacement(&representative.label, &text[range.range.clone()]),
        );
        changes.push(TextChange {
            input: range.range.clone(),
            output: start..masked.len(),
        });
        cursor = range.range.end;
    }
    masked.push_str(&text[cursor..]);
    (masked, TextChanges::new(changes))
}

/// Text collected into chunks of [`WRITTEN_CHUNK_BYTES`].
#[derive(Default)]
struct WrittenChunks {
    done: Vec<Bytes>,
    current: String,
}

impl WrittenChunks {
    fn push(&mut self, mut text: &str) {
        while !text.is_empty() {
            if self.current.capacity() == 0 {
                self.current = String::with_capacity(WRITTEN_CHUNK_BYTES);
            }
            let mut at = text.len().min(WRITTEN_CHUNK_BYTES - self.current.len());
            while !text.is_char_boundary(at) {
                at -= 1;
            }
            self.current.push_str(&text[..at]);
            text = &text[at..];
            if !text.is_empty() {
                self.flush();
            }
        }
    }

    fn flush(&mut self) {
        if !self.current.is_empty() {
            self.done
                .push(Bytes::from(std::mem::take(&mut self.current)));
        }
    }

    fn finish(mut self) -> Vec<Bytes> {
        self.flush();
        self.done
    }
}

/// The text of a file between what has been written and what a later window can still change,
/// with the findings of the windows read so far.
struct PendingMasks {
    /// The file from the first byte not written to the end of the last window read.
    tail: String,
    tail_start: TextPosition,
    findings: Vec<StepFinding>,
    redacters: Vec<RedacterType>,
    recording: bool,
    /// What each redacter redacted, by its place in the chain.
    redactions: Vec<Vec<(RedacterType, Detection)>>,
}

impl PendingMasks {
    fn new(redacters: Vec<RedacterType>, recording: bool) -> Self {
        Self {
            tail: String::new(),
            tail_start: TextPosition::default(),
            findings: Vec::new(),
            redactions: redacters.iter().map(|_| Vec::new()).collect(),
            redacters,
            recording,
        }
    }

    /// Merges what `window` found into the file, and returns the text now final: masked, and
    /// ending where a later window may still add a finding, or at the end of the file after
    /// the last window.
    ///
    /// A window finds nothing that starts before its own start, so nothing before the start of
    /// the next window changes, except through a range that reaches past it. The text from the
    /// start of the first such range is held back, whole.
    fn take(&mut self, window: WindowFindings) -> AppResult<Vec<Bytes>> {
        let base = self.tail_start.offset;
        let tail_end = base + self.tail.len();
        let skip = tail_end.saturating_sub(window.start.offset);
        if let Some(new_text) = window.text.get(skip..) {
            self.tail.push_str(new_text);
        }
        self.findings.extend(window.findings);

        let merged = merge(&self.findings);
        let hold = window.next_start;
        let settled =
            merged.partition_point(|range| hold.is_none_or(|hold| range.range.end <= hold));
        let tail_end = base + self.tail.len();
        let limit = match (merged.get(settled), hold) {
            (Some(next), Some(hold)) => hold.min(next.range.start),
            (Some(next), None) => next.range.start,
            (None, hold) => hold.unwrap_or(tail_end),
        };

        let mut text = WrittenChunks::default();
        let mut cursor = base;
        for range in &merged[..settled] {
            text.push(self.tail_slice(cursor..range.range.start)?);
            let representative = &self.findings[range.representative];
            text.push(
                &representative
                    .mask
                    .replacement(&representative.label, self.tail_slice(range.range.clone())?),
            );
            cursor = range.range.end;
        }
        text.push(self.tail_slice(cursor..limit)?);

        if self.recording {
            self.record(limit)?;
        }
        self.findings
            .retain(|finding| !finding.range.is_empty() && finding.range.start >= limit);
        self.tail_start = self.tail_start.after(&self.tail[..limit - base]);
        self.tail.drain(..limit - base);
        Ok(text.finish())
    }

    fn tail_slice(&self, range: Range<usize>) -> AppResult<&str> {
        let base = self.tail_start.offset;
        range
            .start
            .checked_sub(base)
            .zip(range.end.checked_sub(base))
            .and_then(|(start, end)| self.tail.get(start..end))
            .ok_or(AppError::DetectionOutsideContent {
                start: range.start,
                end: range.end,
            })
    }

    /// Places what each redacter masked before `limit`, as its findings that overlap united.
    fn record(&mut self, limit: usize) -> AppResult<()> {
        let base = self.tail_start.offset;
        let lines = TextLines::new(&self.tail);
        for (step, redactions) in self.redactions.iter_mut().enumerate() {
            let mut own: Vec<(usize, &StepFinding)> = self
                .findings
                .iter()
                .enumerate()
                .filter(|(_, finding)| {
                    finding.step == step && !finding.range.is_empty() && finding.range.start < limit
                })
                .collect();
            own.sort_by(|(a_index, a), (b_index, b)| {
                a.range
                    .start
                    .cmp(&b.range.start)
                    .then(b.range.end.cmp(&a.range.end))
                    .then(a_index.cmp(b_index))
            });
            let mut united: Option<(Range<usize>, &StepFinding, usize)> = None;
            let mut place = |range: Range<usize>, label_from: &StepFinding| -> AppResult<()> {
                let mut detection = lines.detection(
                    range.start - base..range.end - base,
                    label_from.label.clone(),
                    label_from.score,
                )?;
                detection.location = detection.location.in_file(self.tail_start);
                redactions.push((self.redacters[step], detection));
                Ok(())
            };
            for (_, finding) in own {
                united = match united.take() {
                    Some((range, label_from, longest)) if finding.range.start < range.end => {
                        let length = finding.range.len();
                        let (label_from, longest) = if length > longest {
                            (finding, length)
                        } else {
                            (label_from, longest)
                        };
                        Some((
                            range.start..range.end.max(finding.range.end),
                            label_from,
                            longest,
                        ))
                    }
                    Some((range, label_from, _)) => {
                        place(range, label_from)?;
                        Some((finding.range.clone(), finding, finding.range.len()))
                    }
                    None => Some((finding.range.clone(), finding, finding.range.len())),
                };
            }
            if let Some((range, label_from, _)) = united {
                place(range, label_from)?;
            }
        }
        Ok(())
    }

    fn into_redactions(self) -> Vec<(RedacterType, Detection)> {
        self.redactions.into_iter().flatten().collect()
    }
}

/// The part of a file the stream hands the upload, and the redactions it lists, once every window
/// is read. Stops as soon as the upload no longer takes the stream.
///
/// When a window cannot be redacted, the stream ends with the error, so that the destination does
/// not keep the part of the file it was given.
pub(crate) async fn redact_windows<S: Stream<Item = AppResult<Bytes>> + Unpin>(
    redaction: PieceRedaction<'_>,
    mut pieces: TextPieces<S>,
    first: TextWindow,
    written: mpsc::Sender<AppResult<Bytes>>,
) -> AppResult<Vec<(RedacterType, Detection)>> {
    let mut pending = PendingMasks::new(redaction.redacters(), redaction.recording);
    let outcome: AppResult<()> = async {
        let mut next = Some(first);
        while let Some(window) = next {
            let chunks = pending.take(redaction.find_in_window(window).await?)?;
            for chunk in chunks {
                if written.send(Ok(chunk)).await.is_err() {
                    return Ok(());
                }
            }
            next = pieces.next_window().await?;
        }
        Ok(())
    }
    .await;
    match outcome {
        Ok(()) => Ok(pending.into_redactions()),
        Err(error) => {
            // The receiver may be gone already, and then there is nobody to tell.
            let _ = written
                .send(Err(AppError::SystemError {
                    message: error.to_string(),
                }))
                .await;
            Err(error)
        }
    }
}

/// The channel the redacted windows go through, and the stream of what comes out of it.
pub(crate) fn written_windows() -> (
    mpsc::Sender<AppResult<Bytes>>,
    Box<dyn Stream<Item = AppResult<Bytes>> + Send + Sync + Unpin + 'static>,
) {
    let (sender, receiver) = mpsc::channel(WRITTEN_CHUNKS_IN_FLIGHT);
    let stream = futures::stream::unfold(receiver, |mut receiver| async move {
        receiver.recv().await.map(|item| (item, receiver))
    });
    (sender, Box::new(Box::pin(stream)))
}

/// The part of `masked` that a step reading `reach` bytes of `read` on each side of `piece` is
/// given, where `masked` is `read` with the changes of the steps before it.
///
/// The part never cuts a mask: a cut that falls inside one leaves the mask out, which loses
/// nothing since a mask holds nothing to detect. It is never longer than `max_request_bytes`;
/// when the masks make it so, the context shrinks first.
fn step_window(
    read: &str,
    masked: &str,
    changes: &TextChanges,
    piece: &Range<usize>,
    reach: usize,
    max_request_bytes: usize,
) -> AppResult<Range<usize>> {
    let from = char_start_at_or_before(read, piece.start.saturating_sub(reach));
    let to = char_start_at_or_after(read, piece.end.saturating_add(reach).min(read.len()));
    let after_mask = |position: usize| match changes.in_output(position) {
        OutputPosition::At(position) => position,
        OutputPosition::Inside(mask) => mask.end,
    };
    let before_mask = |position: usize| match changes.in_output(position) {
        OutputPosition::At(position) => position,
        OutputPosition::Inside(mask) => mask.start,
    };
    let (mut start, mut end) = (after_mask(from), before_mask(to));
    let (piece_start, piece_end) = (after_mask(piece.start), before_mask(piece.end));

    let excess = (end - start).saturating_sub(max_request_bytes);
    if excess > 0 {
        let before = piece_start.saturating_sub(start);
        let after = end.saturating_sub(piece_end);
        let mut trim_before = before.min(excess.div_ceil(2));
        let trim_after = after.min(excess - trim_before);
        trim_before = before.min(excess - trim_after);
        if trim_before + trim_after < excess {
            return Err(AppError::TextPieceMaskedTooLong {
                piece_bytes: piece_end.saturating_sub(piece_start),
                max_request_bytes,
            });
        }
        start = outside_mask_after(masked, changes, start + trim_before);
        end = outside_mask_before(masked, changes, end - trim_after);
    }
    Ok(start..end.max(start))
}

/// `position`, or the character start before it, as a character start of `text` clear of the
/// masks of `changes`: a position inside a mask moves to its start.
fn outside_mask_before(text: &str, changes: &TextChanges, position: usize) -> usize {
    let position = char_start_at_or_before(text, position);
    changes
        .enclosing_output(position)
        .map_or(position, |mask| mask.start)
}

/// `position`, or the character start after it, as a character start of `text` clear of the
/// masks of `changes`: a position inside a mask moves to its end.
fn outside_mask_after(text: &str, changes: &TextChanges, position: usize) -> usize {
    let position = char_start_at_or_after(text, position);
    changes
        .enclosing_output(position)
        .map_or(position, |mask| mask.end)
}

fn char_start_at_or_before(text: &str, position: usize) -> usize {
    let mut position = position.min(text.len());
    while !text.is_char_boundary(position) {
        position -= 1;
    }
    position
}

fn char_start_at_or_after(text: &str, position: usize) -> usize {
    let mut position = position.min(text.len());
    while !text.is_char_boundary(position) {
        position += 1;
    }
    position
}

#[cfg(test)]
mod tests {
    use super::*;

    fn finding(range: Range<usize>, step: usize, mask: TextMask, label: &str) -> StepFinding {
        StepFinding {
            range,
            step,
            mask,
            label: DetectionLabel::new(label),
            score: None,
        }
    }

    #[test]
    fn findings_that_overlap_are_one_range_and_findings_that_touch_are_two() {
        let findings = [
            finding(10..14, 0, TextMask::Token, "b"),
            finding(0..5, 0, TextMask::Token, "a"),
            finding(4..8, 0, TextMask::Token, "c"),
            finding(14..16, 0, TextMask::Token, "d"),
            finding(3..4, 0, TextMask::Token, "inner"),
        ];

        let merged = merge(&findings);

        assert_eq!(
            merged,
            vec![
                MergedRange {
                    range: 0..8,
                    representative: 1
                },
                MergedRange {
                    range: 10..14,
                    representative: 0
                },
                MergedRange {
                    range: 14..16,
                    representative: 3
                },
            ]
        );
    }

    #[test]
    fn a_merged_range_takes_the_mask_of_the_latest_step_and_the_first_of_equals() {
        let findings = [
            finding(0..6, 0, TextMask::Token, "early"),
            finding(4..9, 1, TextMask::EveryCharacter, "late"),
            finding(4..12, 1, TextMask::EveryCharacter, "late-longer"),
            finding(2..3, 0, TextMask::Token, "inner"),
        ];

        let merged = merge(&findings);

        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].range, 0..12);
        assert_eq!(
            findings[merged[0].representative].label.to_string(),
            "late-longer"
        );
    }

    #[test]
    fn empty_findings_make_no_range() {
        let findings = [finding(5..5, 0, TextMask::Token, "empty")];

        assert!(merge(&findings).is_empty());
    }

    /// `read` with the ranges replaced by `[REDACTED]`, and the changes that did it.
    fn masked(read: &str, ranges: &[Range<usize>]) -> (String, TextChanges) {
        let mut out = String::new();
        let mut changes = Vec::new();
        let mut cursor = 0;
        for range in ranges {
            out.push_str(&read[cursor..range.start]);
            let start = out.len();
            out.push_str("[REDACTED]");
            changes.push(TextChange {
                input: range.clone(),
                output: start..out.len(),
            });
            cursor = range.end;
        }
        out.push_str(&read[cursor..]);
        (out, TextChanges::new(changes))
    }

    #[test]
    fn a_step_reads_its_reach_around_the_piece_in_the_masked_text() {
        let read = "0123456789".repeat(10);
        let (masked, changes) = masked(&read, &[5..7, 60..62]);

        // The piece is 30..50; a reach of 20 would start at 10 and end at 70 in the read text.
        let window = step_window(&read, &masked, &changes, &(30..50), 20, 1000).unwrap();

        // 10 is after the first mask and 70 after the second, which each lengthen the text by 8.
        assert_eq!(window, 10 + 8..70 + 16);
    }

    #[test]
    fn a_cut_inside_a_mask_leaves_the_mask_out() {
        let read = "0123456789".repeat(10);
        let (masked, changes) = masked(&read, &[8..14, 66..72]);

        // The reach of 20 around 30..50 cuts at 10 and 70, inside both masks.
        let window = step_window(&read, &masked, &changes, &(30..50), 20, 1000).unwrap();

        // It starts after the first mask (14 in the read text) and ends before the second (66).
        assert_eq!(window, 14 + 4..66 + 4);
        assert!(!masked[window.clone()].contains('['));
    }

    #[test]
    fn a_cut_falls_on_the_start_of_a_character() {
        let read = "é".repeat(50);
        let (masked, changes) = masked(&read, &[]);

        let window = step_window(&read, &masked, &changes, &(40..60), 21, 1000).unwrap();

        assert!(masked.is_char_boundary(window.start) && masked.is_char_boundary(window.end));
        assert!(window.start <= 40 - 21 && window.end >= 60 + 21);
    }

    #[test]
    fn masks_that_lengthen_the_text_shrink_the_context_and_not_the_piece() {
        let read = "ab".repeat(100);
        let masks: Vec<Range<usize>> = (0..100).map(|index| index * 2..index * 2 + 1).collect();
        let (masked, changes) = masked(&read, &masks);

        // Nine bytes added per mask: the piece 80..120 is 40 bytes of read text and 40 + 9 * 20
        // of masked text.
        let window = step_window(&read, &masked, &changes, &(80..120), 40, 260).unwrap();

        assert!(window.len() <= 260, "{}", window.len());
        let piece_start = 80 + 9 * 40;
        let piece_end = 120 + 9 * 60;
        assert!(window.start <= piece_start && window.end >= piece_end);
        assert!(window.len() >= 220, "context is kept where it fits");
    }

    #[test]
    fn a_piece_that_masks_make_longer_than_the_limit_fails() {
        let read = "ab".repeat(100);
        let masks: Vec<Range<usize>> = (0..100).map(|index| index * 2..index * 2 + 1).collect();
        let (masked, changes) = masked(&read, &masks);

        let result = step_window(&read, &masked, &changes, &(80..120), 40, 100);

        assert!(matches!(
            result,
            Err(AppError::TextPieceMaskedTooLong {
                max_request_bytes: 100,
                ..
            })
        ));
    }

    fn text_of(chunks: AppResult<Vec<Bytes>>) -> String {
        String::from_utf8(chunks.unwrap().concat()).unwrap()
    }

    #[test]
    fn written_text_is_cut_into_chunks_of_one_size_on_character_starts() {
        let mut chunks = WrittenChunks::default();
        let text = "é日本語".repeat(WRITTEN_CHUNK_BYTES);

        chunks.push(&text);
        chunks.push("tail");
        let chunks = chunks.finish();

        assert!(chunks.len() > 8);
        for chunk in &chunks[..chunks.len() - 1] {
            assert!(chunk.len() > WRITTEN_CHUNK_BYTES - 4 && chunk.len() <= WRITTEN_CHUNK_BYTES);
        }
        let joined = String::from_utf8(chunks.concat()).unwrap();
        assert_eq!(joined, format!("{text}tail"));
    }

    fn window(
        text: &str,
        start: TextPosition,
        next_start: Option<usize>,
        findings: Vec<StepFinding>,
    ) -> WindowFindings {
        WindowFindings {
            text: text.to_string(),
            start,
            next_start,
            findings,
        }
    }

    fn at(offset: usize, lines_before: usize, columns_before: usize) -> TextPosition {
        TextPosition {
            offset,
            lines_before,
            columns_before,
        }
    }

    #[test]
    fn a_value_is_masked_with_the_look_of_the_redacter_that_found_it() {
        let mut pending = PendingMasks::new(vec![RedacterType::LocalRules], false);
        let text = "mail ana@x.io, id 12345, key=abc.";
        let findings = vec![
            finding(5..13, 0, TextMask::Token, "email"),
            finding(18..23, 0, TextMask::EveryCharacter, "id"),
            finding(29..32, 0, TextMask::Label, "SECRET"),
        ];

        let written = text_of(pending.take(window(text, at(0, 0, 0), None, findings)));

        assert_eq!(written, "mail [REDACTED], id XXXXX, key={SECRET}.");
    }

    #[test]
    fn a_later_step_masks_the_range_it_shares_with_an_earlier_one_over_the_original_value() {
        let mut pending = PendingMasks::new(vec![RedacterType::LocalRules; 2], false);
        let text = "id=abcdef;";
        let findings = vec![
            finding(3..9, 0, TextMask::Token, "early"),
            finding(5..9, 1, TextMask::EveryCharacter, "late"),
        ];

        let written = text_of(pending.take(window(text, at(0, 0, 0), None, findings)));

        assert_eq!(written, "id=XXXXXX;");
    }

    #[test]
    fn text_is_written_up_to_where_the_next_window_starts() {
        let mut pending = PendingMasks::new(vec![RedacterType::LocalRules], false);
        let first = window(
            "0123456789",
            at(0, 0, 0),
            Some(6),
            vec![finding(1..3, 0, TextMask::Token, "early")],
        );

        let written = text_of(pending.take(first));

        assert_eq!(written, "0[REDACTED]345");
        let second = window("6789abcdef", at(6, 0, 6), None, vec![]);
        assert_eq!(text_of(pending.take(second)), "6789abcdef");
    }

    #[test]
    fn a_range_that_reaches_past_the_next_window_start_is_held_back_and_united_with_what_that_window_finds(
    ) {
        let mut pending = PendingMasks::new(vec![RedacterType::LocalRules], false);
        // The first window sees the value as 4..8 and stops there, the second as 4..11.
        let first = window(
            "0123456789ab",
            at(0, 0, 0),
            Some(3),
            vec![finding(4..8, 0, TextMask::Token, "short")],
        );
        let second = window(
            "3456789abcdef",
            at(3, 0, 3),
            None,
            vec![finding(4..11, 0, TextMask::EveryCharacter, "long")],
        );

        let held = text_of(pending.take(first));
        let rest = text_of(pending.take(second));

        assert_eq!(held, "012");
        assert_eq!(rest, "3XXXXXXXbcdef");
    }

    #[test]
    fn redactions_are_placed_by_line_and_column_whichever_window_ends_them() {
        let mut pending = PendingMasks::new(vec![RedacterType::LocalRules], true);
        let text = "one\ntwo ana@x.io\nthree\n";
        // The first window ends inside the second line, which the value is on.
        let first = window(
            &text[..12],
            at(0, 0, 0),
            Some(10),
            vec![finding(8..12, 0, TextMask::Token, "email")],
        );
        let second = window(
            &text[10..],
            at(10, 1, 6),
            None,
            vec![finding(8..16, 0, TextMask::Token, "email")],
        );

        pending.take(first).unwrap();
        pending.take(second).unwrap();

        let redactions = pending.into_redactions();
        assert_eq!(redactions.len(), 1);
        let (redacter, detection) = &redactions[0];
        assert_eq!(*redacter, RedacterType::LocalRules);
        assert_eq!(detection.value, "ana@x.io");
        assert_eq!(
            detection.location,
            DetectionLocation::Text {
                start: 8,
                end: 16,
                line: 2,
                column: 5
            }
        );
    }

    #[test]
    fn a_redaction_is_listed_under_the_label_of_the_longest_finding_it_unites() {
        let mut pending = PendingMasks::new(vec![RedacterType::LocalRules], true);
        let findings = vec![
            finding(2..6, 0, TextMask::Token, "short"),
            finding(4..12, 0, TextMask::Token, "long"),
            finding(4..12, 0, TextMask::Token, "same-length"),
        ];

        pending
            .take(window("0123456789abcdef", at(0, 0, 0), None, findings))
            .unwrap();

        let redactions = pending.into_redactions();
        assert_eq!(redactions.len(), 1);
        assert_eq!(redactions[0].1.label.to_string(), "long");
        assert_eq!(redactions[0].1.value, "23456789ab");
    }

    #[test]
    fn redactions_are_listed_by_the_step_that_made_them_whichever_window_found_them() {
        let steps = vec![RedacterType::LocalRules, RedacterType::LocalCredentialMl];
        let mut pending = PendingMasks::new(steps, true);
        let first = window(
            "aa bb cc dd",
            at(0, 0, 0),
            Some(6),
            vec![
                finding(0..2, 1, TextMask::Token, "late-first"),
                finding(3..5, 0, TextMask::Token, "early-first"),
            ],
        );
        let second = window(
            "cc dd",
            at(6, 0, 6),
            None,
            vec![
                finding(6..8, 1, TextMask::Token, "late-second"),
                finding(9..11, 0, TextMask::Token, "early-second"),
            ],
        );

        pending.take(first).unwrap();
        pending.take(second).unwrap();

        let labels: Vec<_> = pending
            .into_redactions()
            .into_iter()
            .map(|(redacter, detection)| (redacter, detection.label.to_string()))
            .collect();
        assert_eq!(
            labels,
            vec![
                (RedacterType::LocalRules, "early-first".to_string()),
                (RedacterType::LocalRules, "early-second".to_string()),
                (RedacterType::LocalCredentialMl, "late-first".to_string()),
                (RedacterType::LocalCredentialMl, "late-second".to_string()),
            ]
        );
    }
}
