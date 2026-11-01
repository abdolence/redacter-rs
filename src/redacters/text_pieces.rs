//! Reads a long text in pieces of whole lines, so a redacter never holds the whole file.
//!
//! A piece is a run of whole lines. Pieces follow one another with no gap and no overlap, and
//! where they fall depends on the text alone. A window is a piece with whole lines of context on
//! each side, the text one `detect` call is given. Lines end at `\n`, so a text with no `\n` is
//! one piece however long it is.

use crate::errors::AppError;
use crate::redacters::TextPieceLimits;
use crate::AppResult;
use bytes::Bytes;
use futures::{Stream, TryStreamExt};
use std::ops::Range;

/// A place in a text read in pieces: the bytes and the lines that come before it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TextPosition {
    pub offset: usize,
    pub lines_before: usize,
}

/// How a text is cut into pieces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PieceLayout {
    piece_bytes: usize,
    context_bytes: usize,
}

impl PieceLayout {
    /// Pieces of at least `piece_bytes` and windows with at least `context_bytes` on each side.
    pub fn new(piece_bytes: usize, context_bytes: usize) -> Self {
        Self {
            piece_bytes: piece_bytes.max(1),
            context_bytes,
        }
    }

    /// The layout whose windows fit every one of `limits`, or `None` for no limits. Every
    /// redacter reads the same window, so it carries the widest context asked for.
    pub fn fitting(limits: impl IntoIterator<Item = TextPieceLimits>) -> Option<Self> {
        let limits: Vec<TextPieceLimits> = limits.into_iter().collect();
        let context_bytes = limits.iter().map(|limit| limit.context_bytes).max()?;
        let window_bytes = limits.iter().map(|limit| limit.max_request_bytes).min()?;
        Some(Self::new(
            window_bytes.saturating_sub(2 * context_bytes),
            context_bytes,
        ))
    }
}

/// A piece of the text with its context.
#[derive(Debug, PartialEq, Eq)]
pub struct TextWindow {
    pub text: String,
    /// Where `text` starts in the file, at the start of a line.
    pub start: TextPosition,
    /// The byte range of the file this window's piece covers.
    pub piece: Range<usize>,
    /// Whether `text` reaches the end of the file.
    pub reaches_end: bool,
}

impl TextWindow {
    /// Whether `text` is the whole file.
    pub fn is_whole_text(&self) -> bool {
        self.start.offset == 0 && self.reaches_end
    }
}

/// The windows of a text read from a stream, one at a time. It holds the bytes from the context
/// of the next window to the end of the window it is reading.
pub struct TextPieces<S> {
    input: S,
    layout: PieceLayout,
    /// The bytes read and kept, from `buffer_start`, which is the start of a line.
    buffer: Vec<u8>,
    buffer_start: TextPosition,
    input_ended: bool,
    /// Where the next piece starts, at the start of a line.
    next_piece_start: usize,
    finished: bool,
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
        }
    }

    /// The next window, or `None` after the last. A text with no bytes has one empty window.
    /// Fails when the window is not valid UTF-8, naming the byte of the file where it is not.
    pub async fn next_window(&mut self) -> AppResult<Option<TextWindow>> {
        if self.finished {
            return Ok(None);
        }
        let context = self.layout.context_bytes.max(1);
        let piece_start = self.next_piece_start;
        let piece_end = self
            .line_start_at_or_after(piece_start + self.layout.piece_bytes)
            .await?;
        let last_piece = self.is_end_of_text(piece_end).await?;
        let window_end = if last_piece {
            piece_end
        } else {
            self.line_start_at_or_after(piece_end + context).await?
        };
        let reaches_end = last_piece || self.is_end_of_text(window_end).await?;
        let window_start = self.line_start_at_or_before(piece_start.saturating_sub(context));

        let bytes = &self.buffer
            [window_start - self.buffer_start.offset..window_end - self.buffer_start.offset];
        let text = String::from_utf8(bytes.to_vec()).map_err(|error| AppError::TextNotUtf8 {
            offset: window_start + error.utf8_error().valid_up_to(),
        })?;
        let window = TextWindow {
            text,
            start: TextPosition {
                offset: window_start,
                lines_before: self.buffer_start.lines_before
                    + newlines(&self.buffer[..window_start - self.buffer_start.offset]),
            },
            piece: piece_start..piece_end,
            reaches_end,
        };

        self.next_piece_start = piece_end;
        self.finished = last_piece;
        let keep_from = self.line_start_at_or_before(piece_end.saturating_sub(context));
        self.drop_before(keep_from);
        Ok(Some(window))
    }

    /// The first start of a line at or after `position`, or the end of the text when the text
    /// ends first. Reads as far as it needs.
    async fn line_start_at_or_after(&mut self, position: usize) -> AppResult<usize> {
        if position <= self.buffer_start.offset {
            return Ok(self.buffer_start.offset);
        }
        // A line starts at `position` when the byte before it is a newline.
        let mut search_from = position - 1;
        loop {
            let local = search_from - self.buffer_start.offset;
            if local < self.buffer.len() {
                if let Some(newline) = self.buffer[local..].iter().position(|&byte| byte == b'\n') {
                    return Ok(self.buffer_start.offset + local + newline + 1);
                }
                search_from = self.buffer_end();
            }
            if !self.read_more().await? {
                return Ok(self.buffer_end());
            }
        }
    }

    /// The last start of a line at or before `position` among the bytes kept.
    fn line_start_at_or_before(&self, position: usize) -> usize {
        let local = position
            .saturating_sub(self.buffer_start.offset)
            .min(self.buffer.len());
        self.buffer[..local]
            .iter()
            .rposition(|&byte| byte == b'\n')
            .map_or(self.buffer_start.offset, |newline| {
                self.buffer_start.offset + newline + 1
            })
    }

    /// Whether `position` is the end of the text. Reads until a byte at or after `position`
    /// exists, or the input ends.
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

    /// Appends the next chunk of the input to the buffer. `false` when the input has ended.
    async fn read_more(&mut self) -> AppResult<bool> {
        while !self.input_ended {
            match self.input.try_next().await? {
                Some(chunk) if chunk.is_empty() => {}
                Some(chunk) => {
                    self.buffer.extend_from_slice(&chunk);
                    return Ok(true);
                }
                None => self.input_ended = true,
            }
        }
        Ok(false)
    }

    /// Forgets the bytes before `offset`, which is the start of a line.
    fn drop_before(&mut self, offset: usize) {
        let local = offset - self.buffer_start.offset;
        self.buffer_start.lines_before += newlines(&self.buffer[..local]);
        self.buffer_start.offset = offset;
        self.buffer.drain(..local);
    }
}

fn newlines(bytes: &[u8]) -> usize {
    bytes.iter().filter(|&&byte| byte == b'\n').count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;

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

    fn window(text: &str, offset: usize, lines_before: usize, piece: Range<usize>) -> TextWindow {
        TextWindow {
            text: text.to_string(),
            start: TextPosition {
                offset,
                lines_before,
            },
            piece,
            reaches_end: false,
        }
    }

    #[tokio::test]
    async fn a_window_is_a_piece_of_whole_lines_with_whole_lines_of_context() {
        let text = "aaa\nbbb\nccc\nddd\neee\nfff\nggg\n";
        let found = windows(text.as_bytes(), 1000, PieceLayout::new(6, 5)).await;

        let reaches_end = |window: TextWindow| TextWindow {
            reaches_end: true,
            ..window
        };
        assert_eq!(
            found,
            vec![
                window("aaa\nbbb\nccc\nddd\n", 0, 0, 0..8),
                window("aaa\nbbb\nccc\nddd\neee\nfff\n", 0, 0, 8..16),
                reaches_end(window("ccc\nddd\neee\nfff\nggg\n", 8, 2, 16..24)),
                reaches_end(window("eee\nfff\nggg\n", 16, 4, 24..28)),
            ]
        );
    }

    #[tokio::test]
    async fn a_text_that_fits_in_one_window_is_one_whole_window() {
        let found = windows(b"one\ntwo\n", 3, PieceLayout::new(1000, 100)).await;

        assert_eq!(found.len(), 1);
        assert!(found[0].is_whole_text());
        assert_eq!(found[0].text, "one\ntwo\n");
    }

    #[tokio::test]
    async fn a_text_without_bytes_is_one_empty_whole_window() {
        let found = windows(b"", 8, PieceLayout::new(10, 10)).await;

        assert_eq!(found.len(), 1);
        assert!(found[0].is_whole_text());
        assert_eq!(found[0].text, "");
    }

    #[tokio::test]
    async fn pieces_tile_the_text_whatever_the_chunks_and_the_line_ends() {
        let texts = [
            "alpha\nbeta\ngamma\ndelta\nepsilon",
            "crlf one\r\ncrlf two\r\ncrlf three\r\n",
            "é\né\nüüü\nλ\n日本語\n\n\n😀 x\n",
            "one very long line with no line end at all, longer than any piece",
            "short\none very long line that is longer than a piece\nshort\n",
            "ends on a newline exactly\n",
        ];
        for text in texts {
            for layout in [
                PieceLayout::new(1, 0),
                PieceLayout::new(7, 3),
                PieceLayout::new(16, 20),
            ] {
                let reference = windows(text.as_bytes(), usize::MAX, layout).await;
                for chunk_size in [1, 2, 5] {
                    assert_eq!(
                        windows(text.as_bytes(), chunk_size, layout).await,
                        reference,
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
                    assert!(
                        found.piece.start == 0 || text.as_bytes()[found.piece.start - 1] == b'\n',
                        "{text:?}: a piece starts at the start of a line"
                    );
                    assert!(found.start.offset <= found.piece.start);
                    assert_eq!(
                        &text[found.start.offset..found.start.offset + found.text.len()],
                        found.text
                    );
                    assert_eq!(
                        found.start.lines_before,
                        text[..found.start.offset].matches('\n').count()
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
    async fn a_window_holds_at_least_the_context_and_one_line_on_each_side() {
        let text = "a\nb\nc\nd\ne\nf\ng\nh\ni\nj\n";
        let found = windows(text.as_bytes(), 1000, PieceLayout::new(4, 3)).await;

        let middle = &found[1];
        assert_eq!(middle.piece, 4..8);
        assert_eq!(
            middle.text, "a\nb\nc\nd\ne\nf\n",
            "3 bytes of context, rounded out to lines"
        );
        let no_context = windows(text.as_bytes(), 1000, PieceLayout::new(4, 0)).await;
        assert_eq!(
            no_context[1].text, "b\nc\nd\ne\n",
            "one line when the context is none"
        );
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
}
