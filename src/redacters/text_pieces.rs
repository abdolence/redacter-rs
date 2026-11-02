//! Reads a long text in pieces, so a redacter never holds the whole file.
//!
//! A piece is a run of bytes that ends at a line start when it can. Pieces follow one another
//! with no gap and no overlap, and where they fall depends on the text alone. A window is a
//! piece with `context_bytes` of the text on each side, the text one `detect` call is given. A
//! window is never longer than the limit it was laid out for, so a line longer than a window is
//! cut inside, at a character boundary.

use crate::errors::AppError;
use crate::redacters::{ContentStream, TextPieceLimits};
use crate::AppResult;
use bytes::Bytes;
use futures::{stream, Stream, StreamExt, TryStreamExt};
use std::future::Future;
use std::num::NonZeroUsize;
use std::ops::Range;

/// The most bytes a window can hold beyond the context it was asked for on one side, because a
/// cut moves out to the start of a character.
pub(crate) const CUT_ROUNDING_BYTES: usize = 3;

/// A place in a text read in pieces: the bytes, the lines and the characters of its line before
/// it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct TextPosition {
    pub offset: usize,
    pub lines_before: usize,
    /// The characters between the start of the line the position is on and the position.
    pub columns_before: usize,
}

impl TextPosition {
    /// The position `bytes` end at, when they start at this one.
    pub fn after(self, bytes: &[u8]) -> Self {
        match bytes.iter().rposition(|&byte| byte == b'\n') {
            Some(last_newline) => Self {
                offset: self.offset + bytes.len(),
                lines_before: self.lines_before + newlines(bytes),
                columns_before: characters(&bytes[last_newline + 1..]),
            },
            None => Self {
                offset: self.offset + bytes.len(),
                lines_before: self.lines_before,
                columns_before: self.columns_before + characters(bytes),
            },
        }
    }
}

/// How a text is cut into pieces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PieceLayout {
    piece_bytes: usize,
    context_bytes: usize,
    /// How many windows are read at a time.
    concurrency: NonZeroUsize,
}

impl PieceLayout {
    /// Pieces of at most `piece_bytes`, and `context_bytes` of the text on each side of a piece,
    /// read one window at a time.
    pub fn new(piece_bytes: usize, context_bytes: usize) -> Self {
        Self {
            piece_bytes,
            context_bytes,
            concurrency: NonZeroUsize::MIN,
        }
    }

    /// The same layout with up to `concurrency` windows read at a time.
    pub fn reading(self, concurrency: NonZeroUsize) -> Self {
        Self {
            concurrency,
            ..self
        }
    }

    /// The layout whose windows fit every one of `limits`, or `None` for no limits. Every
    /// redacter reads the same window, so it carries the widest context asked for, and a window
    /// is never longer than the smallest request limit. Fails when that leaves no room for a
    /// piece. Windows are read as many at a time as the redacter that takes the fewest.
    pub fn fitting(limits: &[TextPieceLimits]) -> AppResult<Option<Self>> {
        let (Some(context_bytes), Some(window_bytes), Some(concurrency)) = (
            limits.iter().map(|limit| limit.context_bytes).max(),
            limits.iter().map(|limit| limit.max_request_bytes).min(),
            limits.iter().map(|limit| limit.concurrent_requests).min(),
        ) else {
            return Ok(None);
        };
        let piece_bytes = window_bytes
            .checked_sub(2 * (context_bytes + CUT_ROUNDING_BYTES))
            .filter(|&piece_bytes| piece_bytes > 0)
            .ok_or(AppError::TextPieceLimitsTooTight {
                max_request_bytes: window_bytes,
                context_bytes,
            })?;
        Ok(Some(
            Self::new(piece_bytes, context_bytes).reading(concurrency),
        ))
    }

    /// The text each step of a chain reads on each side of a piece: the contexts of the step and
    /// of every step after it, since a step is right only where the text it reads was made right
    /// by the steps before it.
    pub fn chain_reaches(limits: &[TextPieceLimits]) -> Vec<usize> {
        let mut reaches: Vec<usize> = limits
            .iter()
            .rev()
            .scan(0usize, |reach, limit| {
                *reach = reach.saturating_add(limit.context_bytes);
                Some(*reach)
            })
            .collect();
        reaches.reverse();
        reaches
    }

    /// The layout for redacters that run one after another on each window, the first on the
    /// window as read: it carries the context the first step reaches, and the piece is the
    /// largest that leaves every step a window within its request limit. Fails when that leaves
    /// no room for a piece. Windows are read as many at a time as the step that takes the fewest.
    pub fn chained(limits: &[TextPieceLimits]) -> AppResult<Option<Self>> {
        let reaches = Self::chain_reaches(limits);
        let (Some(&first_reach), Some(concurrency)) = (
            reaches.first(),
            limits.iter().map(|limit| limit.concurrent_requests).min(),
        ) else {
            return Ok(None);
        };
        let mut piece_bytes = usize::MAX;
        for (limit, reach) in limits.iter().zip(reaches) {
            piece_bytes = limit
                .max_request_bytes
                .checked_sub(2 * (reach + CUT_ROUNDING_BYTES))
                .filter(|&room| room > 0)
                .ok_or(AppError::TextPieceLimitsTooTight {
                    max_request_bytes: limit.max_request_bytes,
                    context_bytes: reach,
                })?
                .min(piece_bytes);
        }
        Ok(Some(
            Self::new(piece_bytes, first_reach).reading(concurrency),
        ))
    }
}

/// A piece of the text with its context.
#[derive(Debug)]
pub(crate) struct TextWindow {
    pub text: String,
    /// Where `text` starts in the file.
    pub start: TextPosition,
    /// The byte range of the file this window's piece covers.
    pub piece: Range<usize>,
    /// Whether `text` reaches the end of the file.
    pub reaches_end: bool,
    /// Where the window after this one starts, or `None` after the last. A later window can
    /// change nothing before it.
    pub next_start: Option<usize>,
}

impl TextWindow {
    /// Whether `text` is the whole file.
    pub fn is_whole_text(&self) -> bool {
        self.start.offset == 0 && self.reaches_end
    }

    pub fn end(&self) -> usize {
        self.start.offset + self.text.len()
    }

    /// The bytes of `text` that hold only whole lines of the file: a window that starts or ends
    /// inside a line leaves that line's part out.
    pub fn whole_lines(&self) -> Range<usize> {
        let from = if self.start.columns_before == 0 {
            0
        } else {
            self.text
                .find('\n')
                .map_or(self.text.len(), |newline| newline + 1)
        };
        let to = if self.reaches_end || self.text.ends_with('\n') {
            self.text.len()
        } else {
            self.text.rfind('\n').map_or(0, |newline| newline + 1)
        };
        from..to.max(from)
    }
}

/// The windows of a text read from a stream, one at a time. It holds the bytes from the context
/// of the next window to the end of the window it is reading.
pub(crate) struct TextPieces<S> {
    input: S,
    layout: PieceLayout,
    buffer: Vec<u8>,
    buffer_start: TextPosition,
    input_ended: bool,
    next_piece_start: usize,
    finished: bool,
    /// Set when a NUL byte makes the text not text: how many bytes at the start are checked for
    /// one as they are read.
    nul_probe_bytes: Option<usize>,
}

impl<S: Stream<Item = AppResult<Bytes>> + Unpin> TextPieces<S> {
    pub fn new(input: S, layout: PieceLayout) -> Self {
        Self {
            input,
            layout,
            buffer: Vec::new(),
            buffer_start: TextPosition::default(),
            input_ended: false,
            next_piece_start: 0,
            finished: false,
            nul_probe_bytes: None,
        }
    }

    /// The same pieces, where a window whose text holds a NUL byte fails with
    /// [`AppError::TextHasNul`] as one that is not UTF-8 fails with [`AppError::TextNotUtf8`].
    ///
    /// A NUL byte in the first `probe_bytes` of the text fails as soon as it is read, so that a
    /// binary file is not read to the end of its first window. The probe reaches no further than
    /// the first window surely does, the context of its piece.
    pub fn rejecting_nul(self, probe_bytes: usize) -> Self {
        Self {
            nul_probe_bytes: Some(probe_bytes),
            ..self
        }
    }

    /// The whole input again, from its first byte: what was read, then what was not. Fails once
    /// a window was taken, since the bytes before it are dropped; a window that failed to be cut
    /// took none.
    pub fn into_unread(self) -> AppResult<ContentStream>
    where
        S: Send + Sync + 'static,
    {
        if self.buffer_start.offset != 0 {
            return Err(AppError::SystemError {
                message: "The start of the text was dropped already".to_string(),
            });
        }
        let read = stream::iter([Ok(Bytes::from(self.buffer))]);
        Ok(if self.input_ended {
            Box::new(read)
        } else {
            Box::new(read.chain(self.input))
        })
    }

    /// The next window, or `None` after the last. A text with no bytes has one empty window.
    /// Fails when the window is not valid UTF-8, naming the byte of the file where it is not.
    pub async fn next_window(&mut self) -> AppResult<Option<TextWindow>> {
        if self.finished {
            return Ok(None);
        }
        let context = self.layout.context_bytes;
        let piece_start = self.next_piece_start;
        let (piece_end, last_piece) = self.piece_end(piece_start).await?;
        let window_start = self.char_start_at_or_before(piece_start.saturating_sub(context));
        let window_end = if last_piece {
            piece_end
        } else {
            self.char_start_at_or_after(piece_end + context).await?
        };
        let reaches_end = last_piece || self.is_end_of_text(window_end).await?;
        let keep_from = self.char_start_at_or_before(piece_end.saturating_sub(context));

        let bytes = &self.buffer
            [window_start - self.buffer_start.offset..window_end - self.buffer_start.offset];
        if self.nul_probe_bytes.is_some() {
            if let Some(nul) = bytes.iter().position(|&byte| byte == 0) {
                return Err(AppError::TextHasNul {
                    offset: window_start + nul,
                });
            }
        }
        let text = String::from_utf8(bytes.to_vec()).map_err(|error| AppError::TextNotUtf8 {
            offset: window_start + error.utf8_error().valid_up_to(),
        })?;
        let window = TextWindow {
            text,
            start: self.position_of(window_start),
            piece: piece_start..piece_end,
            reaches_end,
            next_start: (!last_piece).then_some(keep_from),
        };

        self.next_piece_start = piece_end;
        self.finished = last_piece;
        self.drop_before(keep_from);
        Ok(Some(window))
    }

    /// What `read` gives for each window, in the order of the windows, with up to the layout's
    /// `concurrency` windows read at a time. `first` is a window already taken from `self`, which
    /// comes first.
    ///
    /// Windows are cut one after another, so memory is that of `concurrency` windows and the
    /// text between them. The stream ends after the error of a window that cannot be cut; an
    /// error from `read` does not end it, so a caller stops at the first error.
    pub fn read_ahead<T, Read, Pending>(
        self,
        first: Option<TextWindow>,
        mut read: Read,
    ) -> impl Stream<Item = AppResult<T>>
    where
        Read: FnMut(TextWindow) -> Pending,
        Pending: Future<Output = AppResult<T>>,
    {
        let concurrency = self.layout.concurrency;
        stream::unfold(Some((self, first)), |state| async move {
            let (mut pieces, first) = state?;
            let next = match first {
                Some(window) => Ok(Some(window)),
                None => pieces.next_window().await,
            };
            match next {
                Ok(Some(window)) => Some((Ok(window), Some((pieces, None)))),
                Ok(None) => None,
                Err(error) => Some((Err(error), None)),
            }
        })
        .map(move |window| {
            let pending = window.map(&mut read);
            async move { pending?.await }
        })
        .buffered(concurrency.get())
    }

    /// Where the piece that starts at `piece_start` ends, and whether it is the last. A piece
    /// ends at the last line start it can, and inside a line when none is in reach.
    async fn piece_end(&mut self, piece_start: usize) -> AppResult<(usize, bool)> {
        let target = piece_start + self.layout.piece_bytes;
        if self.is_end_of_text(target).await? {
            return Ok((self.buffer_end(), true));
        }
        let from = piece_start - self.buffer_start.offset;
        let to = target - self.buffer_start.offset;
        if let Some(newline) = self.buffer[from..to]
            .iter()
            .rposition(|&byte| byte == b'\n')
        {
            return Ok((piece_start + newline + 1, false));
        }
        let cut = self.char_start_at_or_before(target);
        if cut > piece_start {
            return Ok((cut, false));
        }
        // A piece shorter than a character still takes one whole character.
        Ok((self.char_start_at_or_after(piece_start + 1).await?, false))
    }

    /// Looks back at most three bytes, so text that is not UTF-8 cannot stretch a window.
    fn char_start_at_or_before(&self, position: usize) -> usize {
        let mut start = position.clamp(self.buffer_start.offset, self.buffer_end());
        for _ in 0..CUT_ROUNDING_BYTES {
            let continues = self
                .buffer
                .get(start - self.buffer_start.offset)
                .is_some_and(|&byte| is_continuation(byte));
            if !continues || start == self.buffer_start.offset {
                break;
            }
            start -= 1;
        }
        start
    }

    /// Reads ahead as far as it needs, and looks forward at most three bytes.
    async fn char_start_at_or_after(&mut self, position: usize) -> AppResult<usize> {
        let mut start = position;
        for _ in 0..CUT_ROUNDING_BYTES {
            if self.is_end_of_text(start).await? {
                return Ok(self.buffer_end());
            }
            if !is_continuation(self.buffer[start - self.buffer_start.offset]) {
                return Ok(start);
            }
            start += 1;
        }
        Ok(start)
    }

    /// Reads until a byte at `position` exists, or the input ends.
    async fn is_end_of_text(&mut self, position: usize) -> AppResult<bool> {
        while position >= self.buffer_end() {
            if !self.read_more().await? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn buffer_end(&self) -> usize {
        self.buffer_start.offset + self.buffer.len()
    }

    async fn read_more(&mut self) -> AppResult<bool> {
        while !self.input_ended {
            match self.input.try_next().await? {
                Some(chunk) if chunk.is_empty() => {}
                Some(chunk) => {
                    self.buffer.extend_from_slice(&chunk);
                    self.check_probe(chunk.len())?;
                    return Ok(true);
                }
                None => self.input_ended = true,
            }
        }
        Ok(false)
    }

    /// Fails on a NUL byte in the part of the last `chunk_len` bytes read that the probe covers.
    fn check_probe(&self, chunk_len: usize) -> AppResult<()> {
        let Some(probe_bytes) = self.nul_probe_bytes else {
            return Ok(());
        };
        let probe_end = probe_bytes.min(self.layout.context_bytes);
        let chunk_start = self.buffer_end() - chunk_len;
        let probed = probe_end.saturating_sub(chunk_start).min(chunk_len);
        let chunk = &self.buffer[chunk_start - self.buffer_start.offset..][..probed];
        match chunk.iter().position(|&byte| byte == 0) {
            Some(nul) => Err(AppError::TextHasNul {
                offset: chunk_start + nul,
            }),
            None => Ok(()),
        }
    }

    fn position_of(&self, offset: usize) -> TextPosition {
        self.buffer_start
            .after(&self.buffer[..offset - self.buffer_start.offset])
    }

    fn drop_before(&mut self, offset: usize) {
        let local = offset - self.buffer_start.offset;
        self.buffer_start = self.position_of(offset);
        self.buffer.drain(..local);
    }
}

fn is_continuation(byte: u8) -> bool {
    byte & 0b1100_0000 == 0b1000_0000
}

fn newlines(bytes: &[u8]) -> usize {
    bytes.iter().filter(|&&byte| byte == b'\n').count()
}

fn characters(bytes: &[u8]) -> usize {
    bytes.iter().filter(|&&byte| !is_continuation(byte)).count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::time::Duration;

    /// `text` as a stream of chunks of `chunk_size` bytes.
    fn chunks(text: &[u8], chunk_size: usize) -> impl Stream<Item = AppResult<Bytes>> + Unpin {
        stream::iter(
            text.chunks(chunk_size)
                .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
                .collect::<Vec<_>>(),
        )
    }

    async fn windows(text: &[u8], chunk_size: usize, layout: PieceLayout) -> Vec<TextWindow> {
        let mut pieces = TextPieces::new(chunks(text, chunk_size), layout);
        let mut windows = Vec::new();
        while let Some(window) = pieces.next_window().await.unwrap() {
            windows.push(window);
        }
        windows
    }

    #[tokio::test]
    async fn a_piece_ends_at_the_last_line_start_in_reach_and_its_window_adds_exact_context() {
        let text = "aaa\nbbb\nccc\nddd\neee\nfff\nggg\n";
        let found = windows(text.as_bytes(), 1000, PieceLayout::new(10, 5)).await;

        let shapes: Vec<_> = found
            .iter()
            .map(|window| {
                (
                    window.piece.clone(),
                    window.text.as_str(),
                    window.start,
                    window.reaches_end,
                )
            })
            .collect();
        let start = |offset, lines_before, columns_before| TextPosition {
            offset,
            lines_before,
            columns_before,
        };
        assert_eq!(
            shapes,
            vec![
                (0..8, "aaa\nbbb\nccc\nd", start(0, 0, 0), false),
                (8..16, "\nbbb\nccc\nddd\neee\nf", start(3, 0, 3), false),
                (16..24, "\nddd\neee\nfff\nggg\n", start(11, 2, 3), true),
                (24..28, "\nfff\nggg\n", start(19, 4, 3), true),
            ]
        );
    }

    #[tokio::test]
    async fn a_window_says_where_the_next_one_starts() {
        let text = "aaa\nbbb\nccc\nddd\neee\nfff\nggg\n日本語 and a very long line without end";
        let found = windows(text.as_bytes(), 3, PieceLayout::new(10, 5)).await;

        let next_starts: Vec<_> = found.iter().map(|window| window.next_start).collect();
        let expected: Vec<_> = found
            .iter()
            .skip(1)
            .map(|window| Some(window.start.offset))
            .chain([None])
            .collect();
        assert!(found.len() > 4);
        assert_eq!(next_starts, expected);
    }

    #[tokio::test]
    async fn a_text_that_fits_in_one_window_is_one_whole_window() {
        for text in ["one\ntwo\n", ""] {
            let found = windows(text.as_bytes(), 3, PieceLayout::new(1000, 100)).await;

            assert_eq!(found.len(), 1, "{text:?}");
            assert!(found[0].is_whole_text());
            assert_eq!(found[0].text, text);
        }
    }

    #[tokio::test]
    async fn pieces_tile_the_text_whatever_the_chunks_cuts_and_line_ends() {
        let texts = [
            "alpha\nbeta\ngamma\ndelta\nepsilon",
            "crlf one\r\ncrlf two\r\ncrlf three\r\n",
            "é\né\nüüü\nλ\n日本語\n\n\n😀 x\n",
            "one very long line with no line end at all, longer than any piece",
            "short\none very long line that is longer than a piece\nshort\n",
            "日本語 Σίσυφος 😀 on one line with no end that falls on a character",
        ];
        for text in texts {
            for layout in [
                PieceLayout::new(1, 0),
                PieceLayout::new(7, 3),
                PieceLayout::new(16, 20),
            ] {
                let reference = windows(text.as_bytes(), usize::MAX, layout).await;
                for chunk_size in [1, 2, 5] {
                    let chunked = windows(text.as_bytes(), chunk_size, layout).await;
                    assert_eq!(
                        chunked
                            .iter()
                            .map(|window| (&window.text, window.start, &window.piece))
                            .collect::<Vec<_>>(),
                        reference
                            .iter()
                            .map(|window| (&window.text, window.start, &window.piece))
                            .collect::<Vec<_>>(),
                        "{text:?} in chunks of {chunk_size}"
                    );
                }
                let mut next_piece = 0;
                for found in &reference {
                    assert_eq!(
                        found.piece.start, next_piece,
                        "{text:?}: no gap, no overlap"
                    );
                    next_piece = found.piece.end;
                    assert!(found.start.offset <= found.piece.start);
                    assert_eq!(
                        &text[found.start.offset..found.end()],
                        found.text,
                        "{text:?}: the window is the text it starts at"
                    );
                    let before = &text[..found.start.offset];
                    assert_eq!(found.start.lines_before, before.matches('\n').count());
                    assert_eq!(
                        found.start.columns_before,
                        before.rsplit('\n').next().unwrap().chars().count()
                    );
                }
                assert_eq!(
                    next_piece,
                    text.len(),
                    "{text:?}: the pieces cover the text"
                );
            }
        }
    }

    #[tokio::test]
    async fn no_window_is_longer_than_the_request_limit() {
        let layout = PieceLayout::fitting(&[limits(400, 100)]).unwrap().unwrap();
        let texts = [
            "0123456789\n".repeat(200),
            "é7\nλλλλ\n日本語日本語\n😀\n".repeat(60),
            "日本語日本語日本語".repeat(200),
            format!("{}\n{}\n", "a".repeat(1500), "b".repeat(30)),
        ];
        for text in &texts {
            for found in windows(text.as_bytes(), 7, layout).await {
                assert!(found.text.len() <= 400, "{} bytes", found.text.len());
            }
        }
    }

    #[tokio::test]
    async fn a_line_longer_than_a_window_is_cut_inside_and_never_held_whole() {
        let text = format!("short\n{}\nshort\n", "x".repeat(10_000));
        let layout = PieceLayout::fitting(&[limits(400, 100)]).unwrap().unwrap();
        let mut pieces = TextPieces::new(chunks(text.as_bytes(), 64), layout);

        let mut windows = 0;
        while pieces.next_window().await.unwrap().is_some() {
            windows += 1;
            assert!(
                pieces.buffer.len() <= 400 + 2 * 64,
                "{}",
                pieces.buffer.len()
            );
        }
        assert!(windows > 30, "{windows} windows");
    }

    #[test]
    fn limits_that_leave_no_room_for_a_piece_are_refused() {
        let too_tight = [limits(1000, 100), limits(500, 247)];

        assert!(matches!(
            PieceLayout::fitting(&too_tight),
            Err(AppError::TextPieceLimitsTooTight {
                max_request_bytes: 500,
                context_bytes: 247
            })
        ));
        assert!(PieceLayout::fitting(&[]).unwrap().is_none());
    }

    fn limits(max_request_bytes: usize, context_bytes: usize) -> TextPieceLimits {
        TextPieceLimits {
            max_request_bytes,
            context_bytes,
            ..TextPieceLimits::LOCAL
        }
    }

    #[test]
    fn a_step_reaches_the_contexts_of_itself_and_of_every_step_after_it() {
        let chain = [limits(9000, 100), limits(9000, 30), limits(9000, 7)];

        assert_eq!(PieceLayout::chain_reaches(&chain), vec![137, 37, 7]);
        assert!(PieceLayout::chain_reaches(&[]).is_empty());
    }

    #[test]
    fn a_chained_layout_carries_the_first_reach_and_fits_the_window_of_every_step() {
        let layout = PieceLayout::chained(&[limits(1000, 100), limits(500, 50)])
            .unwrap()
            .unwrap();

        // The second step is the narrower: 500 less its own reach of 50 and the rounding, twice.
        assert_eq!(
            layout,
            PieceLayout::new(500 - 2 * (50 + 3), 150)
                .reading(TextPieceLimits::LOCAL.concurrent_requests)
        );
    }

    fn taking(concurrent_requests: usize) -> TextPieceLimits {
        TextPieceLimits {
            concurrent_requests: NonZeroUsize::new(concurrent_requests).unwrap(),
            ..limits(9000, 100)
        }
    }

    #[test]
    fn windows_are_read_as_many_at_a_time_as_the_redacter_that_takes_the_fewest() {
        let chain = [taking(4), taking(2), taking(3)];
        let fewest = NonZeroUsize::new(2).unwrap();

        let fitting = PieceLayout::fitting(&chain).unwrap().unwrap();
        let chained = PieceLayout::chained(&chain).unwrap().unwrap();

        assert_eq!((fitting.concurrency, chained.concurrency), (fewest, fewest));
    }

    #[test]
    fn a_chain_whose_first_reach_leaves_a_later_step_no_room_is_refused() {
        assert!(matches!(
            PieceLayout::chained(&[limits(1000, 100), limits(240, 120)]),
            Err(AppError::TextPieceLimitsTooTight {
                max_request_bytes: 240,
                context_bytes: 120
            })
        ));
    }

    #[tokio::test]
    async fn invalid_utf8_names_the_byte_of_the_file_where_it_is() {
        let text = b"fine line\nfine line\nbad \xff byte\nfine line\n";
        let mut pieces = TextPieces::new(chunks(text, 4), PieceLayout::new(10, 0));

        let mut error = None;
        while error.is_none() {
            match pieces.next_window().await {
                Ok(Some(_)) => {}
                Ok(None) => break,
                Err(found) => error = Some(found),
            }
        }

        assert!(
            matches!(error, Some(AppError::TextNotUtf8 { offset: 24 })),
            "{error:?}"
        );
    }

    /// Reads the eight windows of a text with `concurrency` at a time, the later windows
    /// answering sooner than the earlier ones. Returns where the pieces of the answers start and
    /// the most windows that were being read at once.
    async fn read_in_reverse_speed(first: bool, concurrency: usize) -> (Vec<usize>, usize) {
        let text = "aaaa\n".repeat(16);
        let layout = PieceLayout::new(10, 0).reading(NonZeroUsize::new(concurrency).unwrap());
        let mut pieces = TextPieces::new(chunks(text.as_bytes(), 7), layout);
        let first = if first {
            pieces.next_window().await.unwrap()
        } else {
            None
        };
        let (reading, most_reading) = (Cell::new(0usize), Cell::new(0usize));
        let starts = pieces
            .read_ahead(first, |window| {
                reading.set(reading.get() + 1);
                most_reading.set(most_reading.get().max(reading.get()));
                let (reading, start) = (&reading, window.piece.start);
                async move {
                    tokio::time::sleep(Duration::from_millis(100 - start as u64)).await;
                    reading.set(reading.get() - 1);
                    Ok(start)
                }
            })
            .try_collect()
            .await
            .unwrap();
        (starts, most_reading.get())
    }

    #[tokio::test(start_paused = true)]
    async fn windows_are_read_as_many_at_a_time_as_asked_and_come_out_in_order() {
        let in_order: Vec<usize> = (0..8).map(|piece| piece * 10).collect();
        for (concurrency, most_reading) in [(1, 1), (3, 3), (20, 8)] {
            let (starts, most) = read_in_reverse_speed(false, concurrency).await;

            assert_eq!(starts, in_order, "concurrency {concurrency}");
            assert_eq!(most, most_reading, "concurrency {concurrency}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_window_taken_beforehand_comes_first() {
        let (starts, _) = read_in_reverse_speed(true, 2).await;

        assert_eq!(starts, (0..8).map(|piece| piece * 10).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn reading_ends_at_the_first_window_that_cannot_be_read() {
        let text = b"fine line\nfine line\nbad \xff byte\nfine line\nfine line\n";
        let layout = PieceLayout::new(10, 0).reading(NonZeroUsize::new(3).unwrap());
        let pieces = TextPieces::new(chunks(text, 4), layout);

        let results: Vec<_> = pieces
            .read_ahead(None, |window| async move { Ok(window.piece) })
            .collect()
            .await;

        assert!(results[..2].iter().all(Result::is_ok));
        assert!(matches!(
            results[2],
            Err(AppError::TextNotUtf8 { offset: 24 })
        ));
        assert_eq!(results.len(), 3);
    }

    #[tokio::test]
    async fn a_window_with_a_nul_byte_fails_only_when_asked_to_and_names_the_byte() {
        let text = b"line one\nline two\nline\0three\nline four\n";
        let layout = PieceLayout::new(40, 0);
        let mut accepting = TextPieces::new(chunks(text, 3), layout);
        assert!(accepting.next_window().await.unwrap().is_some());

        let mut rejecting = TextPieces::new(chunks(text, 3), layout).rejecting_nul(0);
        assert!(matches!(
            rejecting.next_window().await,
            Err(AppError::TextHasNul { offset: 22 })
        ));
    }

    #[tokio::test]
    async fn a_first_window_that_cannot_be_cut_gives_the_whole_input_back() {
        let layout = PieceLayout::new(40, 10);
        let long = "line of text\n".repeat(30);
        let cases = [
            [b"not \xff text\n".as_slice(), long.as_bytes()].concat(),
            [b"not \0 text\n".as_slice(), long.as_bytes()].concat(),
            b"Caf\xe9".to_vec(),
            b"\0".to_vec(),
        ];
        for text in cases {
            for chunk_size in [1, 5, 1000] {
                let mut pieces =
                    TextPieces::new(chunks(&text, chunk_size), layout).rejecting_nul(0);
                let error = pieces.next_window().await.expect_err("not text");
                assert!(error.is_not_text(), "{error:?}");

                let unread: Vec<Bytes> = pieces.into_unread().unwrap().try_collect().await.unwrap();

                assert_eq!(unread.concat(), text, "chunks of {chunk_size}");
            }
        }
    }
}
