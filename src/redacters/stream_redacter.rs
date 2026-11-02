use crate::args::RedacterType;
use crate::common_types::TextImageCoords;
use crate::errors::AppError;
use crate::file_converters::ocr::Ocr;
use crate::file_converters::pdf::{PdfInfo, PdfPageInfo, PdfToImage};
use crate::file_converters::FileConverters;
use crate::file_systems::FileSystemRef;
use crate::redacters::piece_redaction::{redact_windows, written_windows, PieceRedaction};
use crate::redacters::table_pieces::{
    redact_batches, table_csv, TableBatch, TableBatches, TableRedaction, TABLE_BATCH_BYTES,
};
use crate::redacters::{
    has_marker_outside, redact_rgba_image_at_coords, words_to_redact, Categories, Detection,
    DetectionLocation, PieceLayout, RedactSupport, Redacter, RedacterBaseOptions, RedacterDataItem,
    RedacterDataItemContent, Redacters, StreamRedactions, TextLines, TextPieceLimits, TextPieces,
    TextPosition, TextWindow,
};
use crate::AppResult;
use futures::future::LocalBoxFuture;
use futures::{FutureExt, Stream, StreamExt, TryStreamExt};
use image::ImageFormat;
use mime::Mime;
use rvstruct::ValueStruct;
use std::collections::HashMap;
use std::ops::Range;

/// The conversion the content went through before it was redacted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedactionConversion {
    /// The PDF was rendered to `pages` images, optionally read back with OCR.
    PdfToImages { pages: usize, ocr: bool },
    /// The image was read with the OCR engine.
    Ocr,
}

/// Why the redaction pipeline did not redact the content.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedactionBlocked {
    /// The file is a PDF and no PDF renderer is available in this build.
    PdfRendererUnavailable,
    /// Redaction needs the OCR engine and it is not available in this build.
    OcrUnavailable,
    /// The OCR engine cannot read the image format of the file.
    OcrImageFormatNotSupported,
    /// No redacter supports the type of the file, which was to be read as text, and its content
    /// is not text.
    NotText,
}

/// What the pipeline actually did to a file, so the caller can report it once the
/// transfer has finished rather than the pipeline announcing steps as it goes.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RedactionSummary {
    /// The redacters that redacted the content, in the order they ran.
    pub applied: Vec<RedacterType>,
    /// The conversion applied before redaction, if any.
    pub conversion: Option<RedactionConversion>,
    /// Why nothing was redacted, when `applied` is empty.
    pub blocked: Option<RedactionBlocked>,
}

pub struct RedactStreamResult<'f> {
    /// How many redaction steps ran. This still counts a step that turned out to be a
    /// no-op (see `summary`), because it decides whether the file is uploaded.
    pub number_of_redactions: usize,
    pub summary: RedactionSummary,
    pub output: RedactedOutput<'f>,
}

/// Receives what the redacters replaced in a file, placed in it, a window of a text or a batch
/// of a table at a time, in the order the windows and batches are read. Within one call the
/// redactions are in plan order. A file read whole gives one call.
///
/// An error stops the redaction of the file, and `redact_stream` or [`RedactedOutput::drive`]
/// returns it as it is.
pub type RecordRedactions<'r> = dyn FnMut(Vec<(RedacterType, Detection)>) -> AppResult<()> + 'r;

/// How a file is redacted, once its redactions are all handed over.
type RedactionRun<'f> = LocalBoxFuture<'f, AppResult<()>>;

/// The redacted bytes of a file and the run that hands over what was redacted in them.
///
/// A text read in pieces is redacted as its stream is read, so the stream stops when nobody
/// polls the run and the run completes only once the stream has ended. That is why neither
/// half can be taken alone: [`RedactedOutput::drive`] polls both.
pub struct RedactedOutput<'f> {
    stream: ContentStream,
    /// An error means the file could not be redacted; the stream then ends with an error as
    /// well, so that no destination keeps a partial copy.
    run: RedactionRun<'f>,
}

impl<'f> RedactStreamResult<'f> {
    /// The result of a file redacted part by part as its stream is read.
    fn in_parts(
        redacters: Vec<RedacterType>,
        stream: ContentStream,
        run: RedactionRun<'f>,
    ) -> Self {
        Self {
            number_of_redactions: redacters.len(),
            summary: RedactionSummary {
                applied: redacters,
                ..RedactionSummary::default()
            },
            output: RedactedOutput { stream, run },
        }
    }
}

impl<'f> RedactedOutput<'f> {
    /// A file whose redactions were all handed over before the stream is read.
    fn known(stream: ContentStream) -> Self {
        Self {
            stream,
            run: futures::future::ready(Ok(())).boxed_local(),
        }
    }

    /// Hands the stream to `consume`, which reads it to the end, and returns what `consume`
    /// returned beside how the redaction ended.
    pub async fn drive<T>(
        self,
        consume: impl AsyncFnOnce(ContentStream) -> T,
    ) -> (T, AppResult<()>) {
        futures::join!(consume(self.stream), self.run)
    }
}

/// The bytes of a file, in the order they were read.
pub type ContentStream =
    Box<dyn Stream<Item = AppResult<bytes::Bytes>> + Send + Sync + Unpin + 'static>;

/// How much of a file is read before a NUL byte in it settles that it is not text, so that a
/// large binary file is not read whole just to be skipped.
const NOT_TEXT_PROBE_SIZE: usize = 8 * 1024;

/// What a read of a file found, or what one window or batch of it found.
#[derive(Default)]
pub struct DetectStreamResult {
    /// What each redacter of the plan found, with the redacter that found it, in plan order and
    /// in the order the redacter reported.
    pub detections: Vec<(RedacterType, Detection)>,
    /// How many values the redacters named without being able to place them in the file, see
    /// [`crate::redacters::Detections::unlocated`].
    pub unlocated: usize,
    /// The detections moved out of `detections` because a line they cover, or their cell, carries
    /// [`crate::redacters::ALLOW_MARKER`] outside them, kept so the caller can count them.
    pub ignored: Vec<(RedacterType, Detection)>,
}

impl DetectStreamResult {
    /// Moves the detections that `content` marks as ignored from `detections` to `ignored`.
    /// Only text and table cells can carry the marker: the words OCR reads from an image or a
    /// PDF page have no author-controlled place for it.
    ///
    /// `whole_lines` is the part of a text that holds whole lines of the file, `None` for all of
    /// it. A marker outside it does not count.
    fn set_aside_ignored(
        &mut self,
        content: &RedacterDataItemContent,
        whole_lines: Option<Range<usize>>,
    ) {
        let marked: Vec<bool> = match content {
            RedacterDataItemContent::Value(text) => {
                let lines = match whole_lines {
                    Some(span) => TextLines::new(text).with_marker_span(span),
                    None => TextLines::new(text),
                };
                let findings: Vec<Range<usize>> = self
                    .detections
                    .iter()
                    .filter_map(|(_, detection)| match detection.location {
                        DetectionLocation::Text { start, end, .. } => Some(start..end),
                        _ => None,
                    })
                    .collect();
                self.detections
                    .iter()
                    .map(|(_, detection)| match detection.location {
                        DetectionLocation::Text { start, end, .. } => {
                            lines.has_marker_beside(start..end, &findings)
                        }
                        _ => false,
                    })
                    .collect()
            }
            RedacterDataItemContent::Table { rows, .. } => {
                let mut findings_by_cell: HashMap<(usize, usize), Vec<Range<usize>>> =
                    HashMap::new();
                for (_, detection) in &self.detections {
                    if let DetectionLocation::TableCell {
                        row,
                        column,
                        start,
                        end,
                    } = detection.location
                    {
                        findings_by_cell
                            .entry((row, column))
                            .or_default()
                            .push(start..end);
                    }
                }
                self.detections
                    .iter()
                    .map(|(_, detection)| match detection.location {
                        DetectionLocation::TableCell { row, column, .. } => rows
                            .get(row.saturating_sub(1))
                            .and_then(|cells| cells.get(column.saturating_sub(1)))
                            .is_some_and(|cell| {
                                has_marker_outside(cell, &findings_by_cell[&(row, column)])
                            }),
                        _ => false,
                    })
                    .collect()
            }
            RedacterDataItemContent::Image { .. } | RedacterDataItemContent::Pdf { .. } => return,
        };
        for (detection, is_marked) in std::mem::take(&mut self.detections).into_iter().zip(marked) {
            if is_marked {
                self.ignored.push(detection);
            } else {
                self.detections.push(detection);
            }
        }
    }

    /// What the window that starts at `window_start` of the file reports, placed in the file,
    /// given that `piece` is the byte range of the file the window owns and the window before
    /// it ended at `previous_window_end`.
    ///
    /// A detection is reported by the window of the piece it starts in. One that starts before
    /// the piece is reported here only when it ends past the end of the window before: that
    /// window could not see it whole, and this is the first window that does.
    fn owned_by(
        self,
        piece: &Range<usize>,
        window_start: TextPosition,
        previous_window_end: usize,
    ) -> Self {
        let owned = |(redacter, mut detection): (RedacterType, Detection)| {
            detection.location = detection.location.in_file(window_start);
            let reported = detection.location.byte_range().is_none_or(|range| {
                piece.contains(&range.start)
                    || (range.start < piece.start && range.end > previous_window_end)
            });
            reported.then_some((redacter, detection))
        };
        Self {
            detections: self.detections.into_iter().filter_map(owned).collect(),
            ignored: self.ignored.into_iter().filter_map(owned).collect(),
            unlocated: self.unlocated,
        }
    }

    fn absorb(&mut self, other: Self) {
        self.detections.extend(other.detections);
        self.ignored.extend(other.ignored);
        self.unlocated += other.unlocated;
    }
}

/// The result of running one OCR conversion step.
enum OcrStepResult {
    /// The image was redacted; the records place what each redacter replaced on the image.
    Redacted(Vec<(RedacterType, Detection)>),
    ImageFormatNotSupported,
}

pub struct StreamRedacter<'a> {
    redacter_base_options: &'a RedacterBaseOptions,
    file_converters: &'a FileConverters<'a>,
}

/// How the content of a file is read: whole, or in parts when every redacter of the plan gives
/// the same findings on the parts as on the whole. A file that fits in one part is read whole
/// all the same.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PlannedRead {
    Whole,
    /// A text, cut in pieces by the layout, see [`Redacter::text_piece_limits`].
    TextPieces(PieceLayout),
    /// A table, cut in batches of rows of about `batch_bytes` of cells, see
    /// [`RedacterType::reads_tables_by_cell`].
    TableBatches {
        batch_bytes: usize,
    },
}

pub struct StreamRedactPlan<'a> {
    pub apply_pdf_image_converter: bool,
    pub apply_ocr: bool,
    pub leave_data_table_as_text: bool,
    /// No redacter supports the type of the file, and `--unsupported-as-text` asks for it to be
    /// read as text.
    pub read_unsupported_as_text: bool,
    pub supported_redacters: Vec<&'a Redacters<'a>>,
    pub(crate) read: PlannedRead,
    /// Set when `supported_redacters` is empty because a PDF or image needs a conversion
    /// engine (pdfium, OCR) that this build does not have, rather than because none of the
    /// configured redacters is willing to work on this file. `redact_stream` is never called
    /// in that case, so the caller reads this directly to report the real reason instead of
    /// defaulting to "type not supported".
    pub blocked: Option<RedactionBlocked>,
}

impl<'a> StreamRedacter<'a> {
    pub fn new(
        redacter_base_options: &'a RedacterBaseOptions,
        file_converters: &'a FileConverters<'a>,
    ) -> Self {
        Self {
            redacter_base_options,
            file_converters,
        }
    }

    pub async fn create_redact_plan(
        &'a self,
        redacters: &'a [Redacters<'a>],
        file_ref: &FileSystemRef,
    ) -> AppResult<StreamRedactPlan<'a>> {
        self.create_plan(
            redacters,
            file_ref,
            async |redacter, file_ref| redacter.redact_support(file_ref).await,
            PieceLayout::chained,
        )
        .await
    }

    /// The plan `scan` runs: the same decisions as redaction, asked of `detect_support`.
    pub async fn create_detect_plan(
        &'a self,
        redacters: &'a [Redacters<'a>],
        file_ref: &FileSystemRef,
    ) -> AppResult<StreamRedactPlan<'a>> {
        self.create_plan(
            redacters,
            file_ref,
            async |redacter, file_ref| redacter.detect_support(file_ref).await,
            PieceLayout::fitting,
        )
        .await
    }

    async fn create_plan(
        &'a self,
        redacters: &'a [Redacters<'a>],
        file_ref: &FileSystemRef,
        supports: impl AsyncFn(&Redacters<'a>, &FileSystemRef) -> AppResult<RedactSupport>,
        piece_layout: fn(&[TextPieceLimits]) -> AppResult<Option<PieceLayout>>,
    ) -> AppResult<StreamRedactPlan<'a>> {
        let mut stream_redact_plan = StreamRedactPlan {
            apply_pdf_image_converter: false,
            apply_ocr: false,
            leave_data_table_as_text: false,
            read_unsupported_as_text: false,
            // Supports natively
            supported_redacters: Self::supporting(redacters, file_ref, None, &supports).await?,
            read: PlannedRead::Whole,
            blocked: None,
        };

        if stream_redact_plan.supported_redacters.is_empty() {
            // Supports with conversion
            match &file_ref.media_type {
                Some(file_ref_media) if Redacters::is_mime_table(file_ref_media) => {
                    stream_redact_plan.supported_redacters =
                        Self::supporting(redacters, file_ref, Some(mime::TEXT_PLAIN), &supports)
                            .await?;
                    stream_redact_plan.leave_data_table_as_text =
                        !stream_redact_plan.supported_redacters.is_empty();
                }
                Some(file_ref_media) if Redacters::is_mime_pdf(file_ref_media) => {
                    if self.file_converters.pdf_image_converter.is_some() {
                        stream_redact_plan.supported_redacters =
                            Self::supporting(redacters, file_ref, Some(mime::IMAGE_PNG), &supports)
                                .await?;

                        if !stream_redact_plan.supported_redacters.is_empty() {
                            stream_redact_plan.apply_pdf_image_converter = true;
                        } else if self.file_converters.ocr.is_some() {
                            stream_redact_plan.supported_redacters = Self::supporting(
                                redacters,
                                file_ref,
                                Some(mime::TEXT_PLAIN),
                                &supports,
                            )
                            .await?;
                            if !stream_redact_plan.supported_redacters.is_empty() {
                                stream_redact_plan.apply_pdf_image_converter = true;
                                stream_redact_plan.apply_ocr = true;
                            }
                        } else {
                            // A renderer is available but no redacter can read the rendered
                            // page directly, and there is no OCR engine to fall back to.
                            stream_redact_plan.blocked = Some(RedactionBlocked::OcrUnavailable);
                        }
                    } else {
                        stream_redact_plan.blocked = Some(RedactionBlocked::PdfRendererUnavailable);
                    }
                }
                Some(file_ref_media) if Redacters::is_mime_image(file_ref_media) => {
                    if self.file_converters.ocr.is_some() {
                        stream_redact_plan.supported_redacters = Self::supporting(
                            redacters,
                            file_ref,
                            Some(mime::TEXT_PLAIN),
                            &supports,
                        )
                        .await?;
                        if !stream_redact_plan.supported_redacters.is_empty() {
                            stream_redact_plan.apply_ocr = true;
                        }
                    } else {
                        stream_redact_plan.blocked = Some(RedactionBlocked::OcrUnavailable);
                    }
                }
                _ if self.redacter_base_options.unsupported_as_text => {
                    stream_redact_plan.supported_redacters =
                        Self::supporting(redacters, file_ref, Some(mime::TEXT_PLAIN), &supports)
                            .await?;
                    stream_redact_plan.read_unsupported_as_text =
                        !stream_redact_plan.supported_redacters.is_empty();
                }
                _ => {}
            }
        }

        // Sampling cuts the text to a prefix, so a sampled file is read whole.
        let reads_pieces = (stream_redact_plan.read_unsupported_as_text
            || file_ref
                .media_type
                .as_ref()
                .is_some_and(Redacters::is_mime_text))
            && self.redacter_base_options.sampling_size.is_none();
        if reads_pieces {
            let limits: Option<Vec<TextPieceLimits>> = stream_redact_plan
                .supported_redacters
                .iter()
                .map(|redacter| redacter.text_piece_limits())
                .collect();
            // Contexts that fill a request leave no room for a piece. The text is then read
            // whole, as when a redacter has no limits. `cp` then fails at the provider's limit,
            // while `scan` sends it in parts that fit a request.
            let layout = limits.and_then(|limits| match piece_layout(&limits) {
                Ok(layout) => layout,
                Err(error) => {
                    tracing::debug!(%error, "the text is read whole");
                    None
                }
            });
            if let Some(layout) = layout {
                stream_redact_plan.read = PlannedRead::TextPieces(layout);
            }
        }

        let reads_cells = !stream_redact_plan.supported_redacters.is_empty()
            && stream_redact_plan
                .supported_redacters
                .iter()
                .all(|redacter| redacter.redacter_type().reads_tables_by_cell());
        if reads_cells
            && !stream_redact_plan.leave_data_table_as_text
            && file_ref
                .media_type
                .as_ref()
                .is_some_and(Redacters::is_mime_table)
        {
            stream_redact_plan.read = PlannedRead::TableBatches {
                batch_bytes: TABLE_BATCH_BYTES,
            };
        }

        Ok(stream_redact_plan)
    }

    /// The redacters that `supports` says read the file, as `media_type` when one is given.
    async fn supporting(
        redacters: &'a [Redacters<'a>],
        file_ref: &FileSystemRef,
        media_type: Option<Mime>,
        supports: &impl AsyncFn(&Redacters<'a>, &FileSystemRef) -> AppResult<RedactSupport>,
    ) -> AppResult<Vec<&'a Redacters<'a>>> {
        let file_ref = FileSystemRef {
            media_type: media_type.or_else(|| file_ref.media_type.clone()),
            ..file_ref.clone()
        };
        let mut supported = Vec::new();
        for redacter in redacters {
            if supports(redacter, &file_ref).await? == RedactSupport::Supported {
                supported.push(redacter);
            }
        }
        Ok(supported)
    }

    pub async fn redact_stream<
        S: Stream<Item = AppResult<bytes::Bytes>> + Send + Unpin + Sync + 'static,
    >(
        &'a self,
        input: S,
        redact_plan: StreamRedactPlan<'a>,
        file_ref: &FileSystemRef,
        record: &'a mut RecordRedactions<'a>,
    ) -> AppResult<RedactStreamResult<'a>> {
        let item = match redact_plan.read {
            PlannedRead::TableBatches { batch_bytes } => {
                let mut batches =
                    TableBatches::new(input, self.redacter_base_options, batch_bytes).await?;
                let first = batches.next_batch().await?;
                if !first.is_whole_table() {
                    return self.redact_table_in_batches(
                        batches,
                        first,
                        redact_plan,
                        file_ref,
                        record,
                    );
                }
                Ok(first.into_item(file_ref))
            }
            PlannedRead::TextPieces(layout) => {
                let unsupported = redact_plan.read_unsupported_as_text;
                let mut pieces = Self::text_pieces(input, layout, unsupported);
                match pieces.next_window().await {
                    Ok(Some(first)) if first.is_whole_text() => {
                        Ok(self.text_redact_item(first.text, file_ref))
                    }
                    Ok(Some(first)) => {
                        return self.redact_text_in_pieces(
                            pieces,
                            first,
                            redact_plan,
                            file_ref,
                            record,
                        );
                    }
                    Ok(None) => {
                        return Err(AppError::SystemError {
                            message: "A text has no first window".to_string(),
                        });
                    }
                    Err(error) if unsupported && error.is_not_text() => Err(pieces.into_unread()?),
                    Err(error) => return Err(error),
                }
            }
            PlannedRead::Whole => {
                self.stream_to_redact_item(
                    self.redacter_base_options,
                    input,
                    file_ref,
                    &redact_plan,
                )
                .await?
            }
        };
        let mut redacted = match item {
            Ok(item) => item,
            Err(data) => {
                return Ok(RedactStreamResult {
                    number_of_redactions: 0,
                    summary: RedactionSummary {
                        blocked: Some(RedactionBlocked::NotText),
                        ..RedactionSummary::default()
                    },
                    output: RedactedOutput::known(data),
                });
            }
        };
        let mut number_of_redactions = 0;
        let mut summary = RedactionSummary::default();
        let mut recorder = StreamRedactions::new(self.redacter_base_options.list_redactions);

        if redact_plan.apply_ocr {
            // The whole chain of text-only redacters runs once here, on the words the OCR
            // pass produced, instead of once per redacter re-rendering and re-OCRing the
            // page each time: `local-ner` needs to see `local-rules`'s `[REDACTED]` tokens,
            // not OCR text that was never redacted by anything but the first redacter.
            match (
                &self.file_converters.pdf_image_converter,
                &self.file_converters.ocr,
            ) {
                (_, None) => {
                    summary.blocked = Some(RedactionBlocked::OcrUnavailable);
                }
                (None, Some(_)) if redact_plan.apply_pdf_image_converter => {
                    summary.blocked = Some(RedactionBlocked::PdfRendererUnavailable);
                }
                (pdf_to_image, Some(ocr)) => {
                    let redacters = &redact_plan.supported_redacters;
                    if redact_plan.apply_pdf_image_converter {
                        let pdf_to_image = pdf_to_image
                            .as_ref()
                            .expect("checked by the match arm above");
                        let (item, conversion) = self
                            .redact_pdf_with_images_converter(
                                file_ref,
                                redacted,
                                redacters,
                                pdf_to_image.as_ref(),
                                Some(ocr.as_ref()),
                                &mut recorder,
                            )
                            .await?;
                        redacted = item;
                        number_of_redactions += redacters.len();
                        summary
                            .applied
                            .extend(redacters.iter().map(|r| r.redacter_type()));
                        summary.conversion = conversion.or(summary.conversion);
                    } else {
                        let (item, step) = self
                            .redact_with_ocr_converter(
                                file_ref,
                                redacted,
                                redacters,
                                ocr.as_ref(),
                                None,
                            )
                            .await?;
                        redacted = item;
                        match step {
                            OcrStepResult::Redacted(records) => {
                                recorder.extend(records);
                                number_of_redactions += redacters.len();
                                summary
                                    .applied
                                    .extend(redacters.iter().map(|r| r.redacter_type()));
                                summary.conversion = Some(RedactionConversion::Ocr);
                            }
                            OcrStepResult::ImageFormatNotSupported => {
                                summary.blocked =
                                    Some(RedactionBlocked::OcrImageFormatNotSupported);
                            }
                        }
                    }
                }
            }
        } else {
            for redacter in redact_plan.supported_redacters.iter() {
                if redact_plan.apply_pdf_image_converter {
                    match &self.file_converters.pdf_image_converter {
                        Some(pdf_to_image) => {
                            let (item, conversion) = self
                                .redact_pdf_with_images_converter(
                                    file_ref,
                                    redacted,
                                    std::slice::from_ref(redacter),
                                    pdf_to_image.as_ref(),
                                    None,
                                    &mut recorder,
                                )
                                .await?;
                            redacted = item;
                            number_of_redactions += 1;
                            summary.applied.push(redacter.redacter_type());
                            summary.conversion = conversion.or(summary.conversion);
                        }
                        None => {
                            summary.blocked = Some(RedactionBlocked::PdfRendererUnavailable);
                        }
                    }
                } else {
                    tracing::debug!(
                        redacter = %redacter.redacter_type(),
                        file = %file_ref.relative_path.value(),
                        "redacting"
                    );
                    redacted = recorder
                        .redact(
                            redacter,
                            None,
                            redacted,
                            self.redacter_base_options.categories.as_ref(),
                        )
                        .await?;
                    number_of_redactions += 1;
                    summary.applied.push(redacter.redacter_type());
                }
            }
        }

        let output_stream = match redacted.content {
            RedacterDataItemContent::Value(content) => {
                let bytes = bytes::Bytes::from(content.into_bytes());
                Box::new(futures::stream::iter(vec![Ok(bytes)]))
            }
            RedacterDataItemContent::Image { data, .. } => {
                Box::new(futures::stream::iter(vec![Ok(data)]))
            }
            RedacterDataItemContent::Pdf { data } => {
                Box::new(futures::stream::iter(vec![Ok(data)]))
            }
            RedacterDataItemContent::Table { headers, rows } => {
                let bytes = table_csv(self.redacter_base_options, headers, rows, true).await?;
                Box::new(futures::stream::iter(vec![Ok(bytes)]))
            }
        };

        record(recorder.into_records())?;
        Ok(RedactStreamResult {
            number_of_redactions,
            summary,
            output: RedactedOutput::known(output_stream),
        })
    }

    /// The result of redacting a text that does not fit in one window, whose first window is
    /// `first`. The text is redacted as the stream is read, see [`RedactedOutput`].
    fn redact_text_in_pieces<S: Stream<Item = AppResult<bytes::Bytes>> + Send + Unpin + 'static>(
        &'a self,
        pieces: TextPieces<S>,
        first: TextWindow,
        redact_plan: StreamRedactPlan<'a>,
        file_ref: &FileSystemRef,
        record: &'a mut RecordRedactions<'a>,
    ) -> AppResult<RedactStreamResult<'a>> {
        let redaction = PieceRedaction::new(
            &redact_plan.supported_redacters,
            self.redacter_base_options.categories.as_ref(),
            file_ref,
            self.redacter_base_options.list_redactions,
        )?;
        tracing::debug!(
            file = %file_ref.relative_path.value(),
            redacters = redaction.redacters().len(),
            read = ?redact_plan.read,
            "redacting in pieces"
        );
        let (written, stream) = written_windows();
        Ok(RedactStreamResult::in_parts(
            redaction.redacters(),
            stream,
            redact_windows(redaction, pieces, first, written, record).boxed_local(),
        ))
    }

    /// The result of redacting a table that does not fit in one batch, whose first batch is
    /// `first`. The table is redacted as the stream is read, see [`RedactedOutput`].
    fn redact_table_in_batches<
        S: Stream<Item = AppResult<bytes::Bytes>> + Send + Unpin + 'static,
    >(
        &'a self,
        batches: TableBatches<S>,
        first: TableBatch,
        redact_plan: StreamRedactPlan<'a>,
        file_ref: &FileSystemRef,
        record: &'a mut RecordRedactions<'a>,
    ) -> AppResult<RedactStreamResult<'a>> {
        let redaction = TableRedaction::new(
            &redact_plan.supported_redacters,
            self.redacter_base_options,
            file_ref,
        );
        let redacters = redact_plan
            .supported_redacters
            .iter()
            .map(|redacter| redacter.redacter_type())
            .collect();
        tracing::debug!(
            file = %file_ref.relative_path.value(),
            read = ?redact_plan.read,
            "redacting a table in batches"
        );
        let (written, stream) = written_windows();
        Ok(RedactStreamResult::in_parts(
            redacters,
            stream,
            redact_batches(redaction, batches, first, written, record).boxed_local(),
        ))
    }

    /// What the redacters of `plan` find in the file, read as `redact_stream` reads it. Every
    /// redacter reads the original content: unlike redaction, nothing is chained.
    ///
    /// `found` is called with what each window of a text, or batch of a table, finds as it is
    /// placed in the file, so the findings of a large file are never held together; any other
    /// file is read whole and `found` is called once. The findings of a window are in plan
    /// order, and the windows are in file order.
    ///
    /// Returns why the file could not be read as planned, if it could not: OCR cannot decode
    /// its image format, or a file read as text is not text. The plan was not blocked; the step
    /// was, so the caller reports the file as unsupported. `found` is not called then.
    pub async fn detect_stream<
        S: Stream<Item = AppResult<bytes::Bytes>> + Send + Unpin + Sync + 'static,
    >(
        &'a self,
        input: S,
        plan: &StreamRedactPlan<'a>,
        file_ref: &FileSystemRef,
        found: &mut dyn FnMut(DetectStreamResult) -> AppResult<()>,
    ) -> AppResult<Option<RedactionBlocked>> {
        let redacters = &plan.supported_redacters;
        let categories = self.redacter_base_options.categories.as_ref();
        match plan.read {
            PlannedRead::TextPieces(layout) => {
                let unsupported = plan.read_unsupported_as_text;
                return Self::detect_text(
                    input,
                    layout,
                    unsupported,
                    redacters,
                    categories,
                    file_ref,
                    found,
                )
                .await;
            }
            PlannedRead::TableBatches { batch_bytes } => {
                let batches =
                    TableBatches::new(input, self.redacter_base_options, batch_bytes).await?;
                return Self::detect_table(batches, redacters, categories, file_ref, found).await;
            }
            PlannedRead::Whole => {}
        }
        let Ok(item) = self
            .stream_to_redact_item(self.redacter_base_options, input, file_ref, plan)
            .await?
        else {
            return Ok(Some(RedactionBlocked::NotText));
        };
        let mut result = DetectStreamResult::default();
        match &item.content {
            RedacterDataItemContent::Pdf { data } if plan.apply_pdf_image_converter => {
                let converter = self
                    .file_converters
                    .pdf_image_converter
                    .as_deref()
                    .ok_or_else(|| AppError::SystemError {
                        message: "No PDF renderer is available to detect in a PDF".to_string(),
                    })?;
                for (index, page_info) in converter
                    .convert_to_images(data.clone())?
                    .pages
                    .into_iter()
                    .enumerate()
                {
                    let page = Some(index + 1);
                    if plan.apply_ocr {
                        result.absorb(
                            Self::detect_with_ocr(
                                redacters,
                                &page_info.page_as_images,
                                page,
                                self.ocr()?,
                                file_ref,
                                categories,
                            )
                            .await?,
                        );
                    } else {
                        let mut png = std::io::Cursor::new(Vec::new());
                        page_info
                            .page_as_images
                            .write_to(&mut png, ImageFormat::Png)?;
                        let page_item = RedacterDataItem {
                            content: RedacterDataItemContent::Image {
                                mime_type: mime::IMAGE_PNG,
                                data: png.into_inner().into(),
                            },
                            file_ref: file_ref.clone(),
                        };
                        result.absorb(
                            Self::detect_all(redacters, &page_item, page, categories).await?,
                        );
                    }
                }
            }
            RedacterDataItemContent::Image { mime_type, data } if plan.apply_ocr => {
                match ImageFormat::from_mime_type(mime_type) {
                    Some(format) => {
                        let image = image::load_from_memory_with_format(data, format)?;
                        result = Self::detect_with_ocr(
                            redacters,
                            &image,
                            None,
                            self.ocr()?,
                            file_ref,
                            categories,
                        )
                        .await?;
                    }
                    None => return Ok(Some(RedactionBlocked::OcrImageFormatNotSupported)),
                }
            }
            _ => result = Self::detect_all(redacters, &item, None, categories).await?,
        }
        result.set_aside_ignored(&item.content, None);
        found(result)?;
        Ok(None)
    }

    /// The pieces of the text `input` holds. A file read as text only on its content is not text
    /// where a NUL byte is, which binary formats carry and text does not.
    fn text_pieces<S: Stream<Item = AppResult<bytes::Bytes>> + Unpin>(
        input: S,
        layout: PieceLayout,
        unsupported_as_text: bool,
    ) -> TextPieces<S> {
        let pieces = TextPieces::new(input, layout);
        if unsupported_as_text {
            pieces.rejecting_nul(NOT_TEXT_PROBE_SIZE)
        } else {
            pieces
        }
    }

    /// What `redacters` find in the text `input` holds, read in pieces of `layout`. A text that
    /// fits in one window is read whole, as `detect_stream` reads any other file.
    ///
    /// A file read as text only on its content, `unsupported_as_text`, reports nothing but
    /// [`RedactionBlocked::NotText`] when its first window is not text, as `cp` leaves it as it
    /// is. A later window that is not text fails the scan, as it fails the copy.
    ///
    /// A finding is reported by the window of the piece it starts in, see `owned_by`.
    async fn detect_text<S: Stream<Item = AppResult<bytes::Bytes>> + Unpin>(
        input: S,
        layout: PieceLayout,
        unsupported_as_text: bool,
        redacters: &[&Redacters<'a>],
        categories: Option<&Categories>,
        file_ref: &FileSystemRef,
        found: &mut dyn FnMut(DetectStreamResult) -> AppResult<()>,
    ) -> AppResult<Option<RedactionBlocked>> {
        let mut pieces = Self::text_pieces(input, layout, unsupported_as_text);
        let mut previous_window_end = 0;
        let mut windows = 0;
        loop {
            let window = match pieces.next_window().await {
                Ok(Some(window)) => window,
                Ok(None) => break,
                Err(error) if unsupported_as_text && windows == 0 && error.is_not_text() => {
                    return Ok(Some(RedactionBlocked::NotText));
                }
                Err(error) => return Err(error),
            };
            let (is_whole, window_end) = (window.is_whole_text(), window.end());
            let whole_lines = window.whole_lines();
            let (piece, start) = (window.piece, window.start);
            let item = RedacterDataItem {
                content: RedacterDataItemContent::Value(window.text),
                file_ref: file_ref.clone(),
            };
            let mut detected = Self::detect_all(redacters, &item, None, categories).await?;
            detected.set_aside_ignored(&item.content, Some(whole_lines));
            if is_whole {
                found(detected)?;
                return Ok(None);
            }
            found(detected.owned_by(&piece, start, previous_window_end))?;
            previous_window_end = window_end;
            windows += 1;
        }
        tracing::debug!(
            file = %file_ref.relative_path.value(),
            windows,
            redacters = redacters.len(),
            ?layout,
            "read in pieces"
        );
        Ok(None)
    }

    /// What `redacters` find in the table `batches` read, a batch at a time. A marker in a cell
    /// is read within its batch, since a cell is in one.
    async fn detect_table<S: Stream<Item = AppResult<bytes::Bytes>> + Send + Unpin>(
        mut batches: TableBatches<S>,
        redacters: &[&Redacters<'a>],
        categories: Option<&Categories>,
        file_ref: &FileSystemRef,
        found: &mut dyn FnMut(DetectStreamResult) -> AppResult<()>,
    ) -> AppResult<Option<RedactionBlocked>> {
        tracing::debug!(file = %file_ref.relative_path.value(), "reading a table in batches");
        let mut number_of_batches = 0usize;
        loop {
            let batch = batches.next_batch().await?;
            number_of_batches += 1;
            let (rows_before, reaches_end) = (batch.rows_before, batch.reaches_end);
            let item = batch.into_item(file_ref);
            let mut detected = Self::detect_all(redacters, &item, None, categories).await?;
            detected.set_aside_ignored(&item.content, None);
            for (_, detection) in detected.detections.iter_mut().chain(&mut detected.ignored) {
                detection.location = detection.location.after_rows(rows_before);
            }
            found(detected)?;
            if reaches_end {
                tracing::debug!(
                    file = %file_ref.relative_path.value(),
                    batches = number_of_batches,
                    "read a table in batches"
                );
                return Ok(None);
            }
        }
    }

    fn ocr(&self) -> AppResult<&dyn Ocr> {
        self.file_converters
            .ocr
            .as_deref()
            .ok_or_else(|| AppError::SystemError {
                message: "No OCR engine is available to detect in an image".to_string(),
            })
    }

    /// What each of `redacters` finds in `item` within `categories`, every image location placed
    /// on `page`. A redacter that narrows its own request finds nothing outside them already;
    /// for the others this is what limits the findings.
    async fn detect_all(
        redacters: &[&Redacters<'a>],
        item: &RedacterDataItem,
        page: Option<usize>,
        categories: Option<&Categories>,
    ) -> AppResult<DetectStreamResult> {
        let mut result = DetectStreamResult::default();
        for redacter in redacters {
            tracing::debug!(
                redacter = %redacter.redacter_type(),
                file = %item.file_ref.relative_path.value(),
                "detecting"
            );
            let mut detected = redacter.detect(item).await?;
            if let Some(categories) = categories {
                detected.retain_categories(redacter.redacter_type(), categories);
            }
            result.unlocated += detected.unlocated.len();
            for mut detection in detected.found {
                if let DetectionLocation::Image {
                    page: ref mut image_page,
                    ..
                } = detection.location
                {
                    *image_page = page;
                }
                result
                    .detections
                    .push((redacter.redacter_type(), detection));
            }
        }
        Ok(result)
    }

    /// What `redacters` find in the text OCR reads from `image`, each detection boxed by the
    /// words it covers. A detection that covers no word, only the spaces between them, has no
    /// box and is dropped.
    async fn detect_with_ocr(
        redacters: &[&Redacters<'a>],
        image: &image::DynamicImage,
        page: Option<usize>,
        ocr: &dyn Ocr,
        file_ref: &FileSystemRef,
        categories: Option<&Categories>,
    ) -> AppResult<DetectStreamResult> {
        let ocr_text = OcrText::from(ocr.image_to_text(image.clone())?.as_slice());
        let item = RedacterDataItem {
            content: RedacterDataItemContent::Value(ocr_text.text.clone()),
            file_ref: file_ref.clone(),
        };
        let detected = Self::detect_all(redacters, &item, page, categories).await?;
        let mut result = DetectStreamResult {
            unlocated: detected.unlocated,
            ..DetectStreamResult::default()
        };
        for (redacter, detection) in detected.detections {
            let DetectionLocation::Text { start, end, .. } = detection.location else {
                continue;
            };
            match ocr_text.image_location(start, end, page) {
                Some(location) => result.detections.push((
                    redacter,
                    Detection {
                        location,
                        ..detection
                    },
                )),
                None => tracing::debug!(
                    %redacter,
                    start,
                    end,
                    "dropping a detection that covers no OCR word"
                ),
            }
        }
        Ok(result)
    }

    /// The item `input` is read as under the plan. With `--unsupported-as-text` the content
    /// is text when it is valid UTF-8 without a NUL byte, which binary formats carry and text
    /// does not; otherwise the content comes back as `Err`, for a caller that leaves the file as
    /// it is. A NUL byte near the start of the file settles that, before the rest is read.
    async fn stream_to_redact_item<
        S: Stream<Item = AppResult<bytes::Bytes>> + Send + Unpin + Sync + 'static,
    >(
        &'a self,
        redacter_base_options: &RedacterBaseOptions,
        input: S,
        file_ref: &FileSystemRef,
        redact_plan: &StreamRedactPlan<'a>,
    ) -> AppResult<Result<RedacterDataItem, ContentStream>> {
        if redact_plan.read_unsupported_as_text {
            let mut input = input;
            let mut head = Vec::new();
            let mut head_size = 0;
            let mut has_nul = false;
            while head_size < NOT_TEXT_PROBE_SIZE && !has_nul {
                let Some(chunk) = input.try_next().await? else {
                    break;
                };
                head_size += chunk.len();
                has_nul = chunk.contains(&0);
                head.push(chunk);
            }
            if has_nul {
                return Ok(Err(Box::new(
                    futures::stream::iter(head.into_iter().map(Ok)).chain(input),
                )));
            }
            head.extend(input.try_collect::<Vec<_>>().await?);
            let bytes = match String::from_utf8(head.concat()) {
                Ok(text) if !text.contains('\0') => {
                    return Ok(Ok(self.text_redact_item(text, file_ref)));
                }
                Ok(text) => text.into_bytes(),
                Err(error) => error.into_bytes(),
            };
            return Ok(Err(Box::new(futures::stream::iter(vec![Ok(
                bytes::Bytes::from(bytes),
            )]))));
        }
        let item = match file_ref.media_type {
            Some(ref mime)
                if Redacters::is_mime_text(mime)
                    || (Redacters::is_mime_table(mime) && redact_plan.leave_data_table_as_text) =>
            {
                self.stream_to_text_redact_item(input, file_ref).await
            }
            Some(ref mime) if Redacters::is_mime_image(mime) => {
                self.stream_to_image_redact_item(input, file_ref, mime.clone())
                    .await
            }
            Some(ref mime) if Redacters::is_mime_table(mime) => {
                self.stream_to_table_redact_item(redacter_base_options, input, file_ref)
                    .await
            }
            Some(ref mime) if Redacters::is_mime_pdf(mime) => {
                self.stream_to_pdf_redact_item(input, file_ref).await
            }
            Some(ref mime) => Err(AppError::SystemError {
                message: format!("Media type {mime} is not supported for redaction"),
            }),
            None => Err(AppError::SystemError {
                message: "Media type is not provided to redact".to_string(),
            }),
        };
        item.map(Ok)
    }

    async fn stream_to_text_redact_item<
        S: Stream<Item = AppResult<bytes::Bytes>> + Send + Unpin + Sync + 'static,
    >(
        &self,
        input: S,
        file_ref: &FileSystemRef,
    ) -> AppResult<RedacterDataItem> {
        // A character takes at most four bytes, so the first `sampling_size` characters lie in
        // the first four times as many bytes and the rest of the file need not be read.
        let byte_limit = self
            .redacter_base_options
            .sampling_size
            .map(|sampling_size| sampling_size.saturating_mul(4));
        let mut input = input;
        let mut bytes = Vec::new();
        while byte_limit.is_none_or(|limit| bytes.len() < limit) {
            let Some(chunk) = input.try_next().await? else {
                break;
            };
            bytes.extend_from_slice(&chunk);
        }
        let cut_short = byte_limit.is_some_and(|limit| bytes.len() >= limit);
        if let Some(limit) = byte_limit {
            bytes.truncate(limit);
        }
        let content = String::from_utf8(bytes)
            .or_else(|error| {
                // The cut may fall inside a character that the file goes on with.
                let utf8_error = error.utf8_error();
                if cut_short && utf8_error.error_len().is_none() {
                    let mut bytes = error.into_bytes();
                    bytes.truncate(utf8_error.valid_up_to());
                    String::from_utf8(bytes)
                } else {
                    Err(error)
                }
            })
            .map_err(|error| AppError::TextNotUtf8 {
                offset: error.utf8_error().valid_up_to(),
            })?;
        Ok(self.text_redact_item(content, file_ref))
    }

    fn text_redact_item(
        &self,
        whole_content: String,
        file_ref: &FileSystemRef,
    ) -> RedacterDataItem {
        let content = if let Some(sampling_size) = self.redacter_base_options.sampling_size {
            let sampling_size = std::cmp::min(sampling_size, whole_content.len());
            whole_content
                .chars()
                .take(sampling_size)
                .collect::<String>()
        } else {
            whole_content
        };
        RedacterDataItem {
            content: RedacterDataItemContent::Value(content),
            file_ref: file_ref.clone(),
        }
    }

    async fn stream_to_table_redact_item<
        S: Stream<Item = AppResult<bytes::Bytes>> + Send + Unpin + Sync + 'static,
    >(
        &self,
        redacter_base_options: &RedacterBaseOptions,
        input: S,
        file_ref: &FileSystemRef,
    ) -> AppResult<RedacterDataItem> {
        let mut batches = TableBatches::new(input, redacter_base_options, usize::MAX).await?;
        Ok(batches.next_batch().await?.into_item(file_ref))
    }

    async fn stream_to_image_redact_item<
        S: Stream<Item = AppResult<bytes::Bytes>> + Send + Unpin + Sync + 'static,
    >(
        &'a self,
        input: S,
        file_ref: &FileSystemRef,
        mime: mime::Mime,
    ) -> AppResult<RedacterDataItem> {
        let all_chunks: Vec<bytes::Bytes> = input.try_collect().await?;
        let all_bytes = all_chunks.concat();
        Ok(RedacterDataItem {
            content: RedacterDataItemContent::Image {
                mime_type: mime.clone(),
                data: all_bytes.into(),
            },
            file_ref: file_ref.clone(),
        })
    }

    async fn stream_to_pdf_redact_item<
        S: Stream<Item = AppResult<bytes::Bytes>> + Send + Unpin + Sync + 'static,
    >(
        &'a self,
        input: S,
        file_ref: &FileSystemRef,
    ) -> AppResult<RedacterDataItem> {
        let all_chunks: Vec<bytes::Bytes> = input.try_collect().await?;
        let all_bytes = all_chunks.concat();
        Ok(RedacterDataItem {
            content: RedacterDataItemContent::Pdf {
                data: all_bytes.into(),
            },
            file_ref: file_ref.clone(),
        })
    }

    async fn redact_pdf_with_images_converter(
        &'a self,
        file_ref: &FileSystemRef,
        redacted: RedacterDataItem,
        redacters: &[&Redacters<'a>],
        converter: &dyn PdfToImage,
        ocr: Option<&dyn Ocr>,
        recorder: &mut StreamRedactions,
    ) -> Result<(RedacterDataItem, Option<RedactionConversion>), AppError> {
        match redacted.content {
            RedacterDataItemContent::Pdf { data } => {
                tracing::debug!(
                    redacters = redacters.len(),
                    file = %file_ref.relative_path.value(),
                    "redacting the PDF as images"
                );
                let pdf_info = converter.convert_to_images(data)?;
                let pages = pdf_info.pages.len();
                tracing::debug!(pages, "converting the PDF pages to images");
                let mut redacted_pages = Vec::with_capacity(pdf_info.pages.len());
                for (index, page) in pdf_info.pages.into_iter().enumerate() {
                    let page_number = index + 1;
                    let mut png_image_bytes = std::io::Cursor::new(Vec::new());
                    page.page_as_images
                        .write_to(&mut png_image_bytes, ImageFormat::Png)?;
                    let image_to_redact = RedacterDataItem {
                        content: RedacterDataItemContent::Image {
                            mime_type: mime::IMAGE_PNG,
                            data: png_image_bytes.into_inner().into(),
                        },
                        file_ref: file_ref.clone(),
                    };
                    let redacted_image = if let Some(ocr_engine) = ocr {
                        let (item, step) = self
                            .redact_with_ocr_converter(
                                file_ref,
                                image_to_redact,
                                redacters,
                                ocr_engine,
                                Some(page_number),
                            )
                            .await?;
                        if let OcrStepResult::Redacted(records) = step {
                            recorder.extend(records);
                        }
                        item
                    } else {
                        let mut item = image_to_redact;
                        for redacter in redacters {
                            item = recorder
                                .redact(
                                    redacter,
                                    Some(page_number),
                                    item,
                                    self.redacter_base_options.categories.as_ref(),
                                )
                                .await?;
                        }
                        item
                    };
                    if let RedacterDataItemContent::Image { data, .. } = redacted_image.content {
                        redacted_pages.push(PdfPageInfo {
                            page_as_images: image::load_from_memory_with_format(
                                &data,
                                ImageFormat::Png,
                            )?,
                            ..page
                        });
                    }
                }
                let redacted_pdf_info = PdfInfo {
                    pages: redacted_pages,
                };
                let redact_pdf_as_images = converter.images_to_pdf(redacted_pdf_info)?;
                Ok((
                    RedacterDataItem {
                        content: RedacterDataItemContent::Pdf {
                            data: redact_pdf_as_images,
                        },
                        file_ref: file_ref.clone(),
                    },
                    Some(RedactionConversion::PdfToImages {
                        pages,
                        ocr: ocr.is_some(),
                    }),
                ))
            }
            _ => Ok((redacted, None)),
        }
    }

    /// Runs the whole chain of text-only `redacters` once on the words an OCR pass found in
    /// the image, in order (so a later redacter sees an earlier one's `[REDACTED]` tokens),
    /// then boxes exactly the words the chain changed.
    ///
    /// Words are joined by line (see [`OcrText`]) rather than all with a single space, so
    /// keyword-window rules see the same line breaks they would see reading the file as
    /// text. Which words changed is decided by aligning the original OCR words against
    /// the final redacted text (see [`words_to_redact`]), not by checking whether a word still
    /// appears anywhere in a document-wide set: that would miss a word that is also part of an
    /// unredacted span elsewhere on the page, and would box every occurrence of a repeated
    /// word even when only one of them was redacted.
    async fn redact_with_ocr_converter(
        &'a self,
        file_ref: &FileSystemRef,
        redacted: RedacterDataItem,
        redacters: &[&Redacters<'a>],
        ocr: &dyn Ocr,
        page: Option<usize>,
    ) -> Result<(RedacterDataItem, OcrStepResult), AppError> {
        match &redacted.content {
            RedacterDataItemContent::Image { data, mime_type } => {
                match ImageFormat::from_mime_type(mime_type) {
                    Some(image_format) => {
                        tracing::debug!(
                            redacters = redacters.len(),
                            file = %file_ref.relative_path.value(),
                            "redacting the image with the OCR engine"
                        );
                        let image = image::load_from_memory_with_format(data, image_format)?;
                        let text_coords = ocr.image_to_text(image.clone())?;
                        let ocr_text = OcrText::from(text_coords.as_slice());
                        let original_words: Vec<String> = ocr_text
                            .words
                            .iter()
                            .map(|word| ocr_text.text[word.span.clone()].to_string())
                            .collect();

                        let mut redacted_item = RedacterDataItem {
                            content: RedacterDataItemContent::Value(ocr_text.text.clone()),
                            file_ref: file_ref.clone(),
                        };
                        let mut steps =
                            StreamRedactions::new(self.redacter_base_options.list_redactions);
                        for redacter in redacters {
                            redacted_item = steps
                                .redact(
                                    redacter,
                                    page,
                                    redacted_item,
                                    self.redacter_base_options.categories.as_ref(),
                                )
                                .await?;
                        }

                        match redacted_item.content {
                            RedacterDataItemContent::Value(content) => {
                                let to_redact = words_to_redact(&original_words, &content);
                                let records = redacters
                                    .last()
                                    .filter(|_| self.redacter_base_options.list_redactions)
                                    .map_or_else(Vec::new, |last| {
                                        ocr_text.drawn_records(
                                            steps.into_records(),
                                            &to_redact,
                                            page,
                                            last.redacter_type(),
                                        )
                                    });
                                let mut redacted_image = image.to_rgb8();
                                for (word, redact) in ocr_text.words.iter().zip(&to_redact) {
                                    if *redact {
                                        redact_rgba_image_at_coords(
                                            &mut redacted_image,
                                            &vec![word.coords.clone()],
                                            0.10,
                                        );
                                    }
                                }
                                let mut output = std::io::Cursor::new(Vec::new());
                                redacted_image.write_to(&mut output, image_format)?;
                                Ok((
                                    RedacterDataItem {
                                        file_ref: file_ref.clone(),
                                        content: RedacterDataItemContent::Image {
                                            mime_type: mime_type.clone(),
                                            data: output.into_inner().into(),
                                        },
                                    },
                                    OcrStepResult::Redacted(records),
                                ))
                            }
                            _ => Err(AppError::SystemError {
                                message: "Redacted text is not returned as text".to_string(),
                            }),
                        }
                    }
                    None => {
                        tracing::debug!(
                            media_type = %mime_type,
                            file = %file_ref.relative_path.value(),
                            "skipping OCR because the image format is not supported"
                        );
                        Ok((redacted, OcrStepResult::ImageFormatNotSupported))
                    }
                }
            }
            _ => Ok((redacted, OcrStepResult::Redacted(Vec::new()))),
        }
    }
}

/// One OCR line (`TextImageCoords::line`) with its text-bearing words and vertical extent.
struct LineSpan<'a> {
    words: Vec<&'a TextImageCoords>,
    /// Lowest `y1` (highest on the page) of the line's words.
    top: f32,
    /// Highest `y2` (lowest on the page) of the line's words.
    bottom: f32,
    /// Lowest `x1` of the line's words, used to order lines within the same row.
    left: f32,
}

/// The text an OCR pass read, and every word of it with its place in that text, so a redacted
/// text or a detection can be mapped back onto the words' boxes.
pub(crate) struct OcrText {
    /// The words in the order they appear in `text`; words without recognised text are dropped.
    pub words: Vec<OcrWord>,
    pub text: String,
}

/// One OCR word and the half-open byte range it fills in [`OcrText::text`].
pub(crate) struct OcrWord {
    pub coords: TextImageCoords,
    pub span: std::ops::Range<usize>,
}

/// Builds the text sent to the redacters from OCR words.
///
/// Words are joined by `" "` within a line and lines are joined by `"\n"` (using each word's
/// [`TextImageCoords::line`]), so keyword-window rules see the same line breaks they would see
/// reading the file as text. [`words_to_redact`] tokenises on any whitespace, so the newlines
/// cost nothing there.
///
/// Lines are ordered by row, not by the order the OCR engine emitted them in: a multi-column
/// layout such as a form (a column of labels, a column of values) is read by line detection as
/// one column's lines top to bottom and then the next, not row by row — which would otherwise
/// put a label like "Passport no.:" a whole page-column away from its value in the text a
/// keyword-window rule sees, even though on the page they sit right next to each other. A row
/// is a maximal run of lines whose vertical extents overlap once lines are considered in
/// top-to-bottom order (so a label and its value, which usually differ from each other by only
/// a pixel or two of font-metric noise, are still recognised as the same row even when that
/// noise makes one line's top edge a pixel above the other's); within a row, lines are ordered
/// left to right, which is what actually keeps a label ahead of its value — the row grouping
/// alone does not order them, and cannot rely on either line's exact top edge to do it. This is
/// a no-op for ordinary single-column text, where rows never span more than one line and are
/// already emitted top to bottom. Coordinates without recognised text are dropped.
impl From<&[TextImageCoords]> for OcrText {
    fn from(text_coords: &[TextImageCoords]) -> Self {
        let mut lines: std::collections::BTreeMap<usize, Vec<&TextImageCoords>> =
            std::collections::BTreeMap::new();
        for coord in text_coords {
            if coord.text.is_some() {
                lines.entry(coord.line).or_default().push(coord);
            }
        }

        let mut spans: Vec<LineSpan> = lines
            .into_values()
            .map(|words| LineSpan {
                top: words.iter().map(|w| w.y1).fold(f32::INFINITY, f32::min),
                bottom: words.iter().map(|w| w.y2).fold(f32::NEG_INFINITY, f32::max),
                left: words.iter().map(|w| w.x1).fold(f32::INFINITY, f32::min),
                words,
            })
            .collect();
        spans.sort_by(|a, b| a.top.total_cmp(&b.top));

        let mut rows: Vec<Vec<LineSpan>> = vec![];
        for span in spans {
            let overlaps_current_row = rows.last().is_some_and(|row: &Vec<LineSpan>| {
                let row_top = row.iter().map(|l| l.top).fold(f32::INFINITY, f32::min);
                let row_bottom = row
                    .iter()
                    .map(|l| l.bottom)
                    .fold(f32::NEG_INFINITY, f32::max);
                span.top < row_bottom && span.bottom > row_top
            });
            if overlaps_current_row {
                rows.last_mut().expect("checked above").push(span);
            } else {
                rows.push(vec![span]);
            }
        }
        for row in &mut rows {
            row.sort_by(|a, b| a.left.total_cmp(&b.left));
        }

        let mut words = Vec::with_capacity(text_coords.len());
        let mut text = String::new();
        for (row_index, row) in rows.iter().enumerate() {
            if row_index > 0 {
                text.push('\n');
            }
            for (line_index, line) in row.iter().enumerate() {
                if line_index > 0 {
                    text.push(' ');
                }
                for (word_index, coord) in line.words.iter().enumerate() {
                    if word_index > 0 {
                        text.push(' ');
                    }
                    let start = text.len();
                    // `words` only holds coordinates whose `text` is `Some`, checked when built.
                    text.push_str(coord.text.as_deref().unwrap_or_default());
                    words.push(OcrWord {
                        coords: (*coord).clone(),
                        span: start..text.len(),
                    });
                }
            }
        }

        OcrText { words, text }
    }
}

impl OcrText {
    /// The box around every word that overlaps the half-open byte range `start..end` of the
    /// text, or `None` when the range covers no word.
    fn image_location(
        &self,
        start: usize,
        end: usize,
        page: Option<usize>,
    ) -> Option<DetectionLocation> {
        Self::box_around(
            self.words
                .iter()
                .filter(|word| word.span.start < end && start < word.span.end),
            page,
        )
    }

    /// The box around `words`, or `None` when there are none.
    pub(super) fn box_around<'w>(
        words: impl Iterator<Item = &'w OcrWord>,
        page: Option<usize>,
    ) -> Option<DetectionLocation> {
        words
            .map(|word| &word.coords)
            .fold(None, |covered, coords| {
                Some(match covered {
                    None => (coords.x1, coords.y1, coords.x2, coords.y2),
                    Some((x1, y1, x2, y2)) => (
                        coords.x1.min(x1),
                        coords.y1.min(y1),
                        coords.x2.max(x2),
                        coords.y2.max(y2),
                    ),
                })
            })
            .map(|(x1, y1, x2, y2)| DetectionLocation::Image {
                page,
                x1,
                y1,
                x2,
                y2,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::redacters::{
        parse_inline_rule, Category, DetectionLabel, LocalRulesRedacter, LocalRulesRedacterOptions,
        PresidioRedacter, PresidioRedacterOptions, RuleGroup, UserRule,
    };
    use crate::reporter::AppReporter;
    use std::collections::BTreeSet;

    /// What a read found, gathered from the calls of `detect_stream`'s callback, and why the
    /// file was blocked if it was.
    #[derive(Default)]
    struct Collected {
        found: DetectStreamResult,
        blocked: Option<RedactionBlocked>,
    }

    impl<'a> StreamRedacter<'a> {
        async fn detect_stream_collected<
            S: Stream<Item = AppResult<bytes::Bytes>> + Send + Unpin + Sync + 'static,
        >(
            &'a self,
            input: S,
            plan: &StreamRedactPlan<'a>,
            file_ref: &FileSystemRef,
        ) -> AppResult<Collected> {
            let mut collected = Collected::default();
            let blocked = self
                .detect_stream(input, plan, file_ref, &mut |found| {
                    collected.found.absorb(found);
                    Ok(())
                })
                .await?;
            collected.blocked = blocked;
            Ok(collected)
        }
    }

    fn coord(text: &str, line: usize) -> TextImageCoords {
        coord_at(text, line, 0.0)
    }

    fn coord_at(text: &str, line: usize, y1: f32) -> TextImageCoords {
        coord_at_xy(text, line, 0.0, y1)
    }

    fn coord_at_xy(text: &str, line: usize, x1: f32, y1: f32) -> TextImageCoords {
        TextImageCoords {
            x1,
            y1,
            x2: x1 + 1.0,
            y2: y1 + 1.0,
            text: Some(text.to_string()),
            line,
        }
    }

    /// Each word as it reads in the OCR text, which must match its own recognised text.
    fn words_of(ocr: &OcrText) -> Vec<&str> {
        ocr.words
            .iter()
            .map(|word| {
                let in_text = &ocr.text[word.span.clone()];
                assert_eq!(Some(in_text), word.coords.text.as_deref());
                in_text
            })
            .collect()
    }

    #[test]
    fn words_on_the_same_line_are_joined_by_a_space() {
        let coords = vec![coord("hello", 0), coord("world", 0)];
        let ocr = OcrText::from(coords.as_slice());
        assert_eq!(words_of(&ocr), vec!["hello", "world"]);
        assert_eq!(ocr.text, "hello world");
    }

    #[test]
    fn a_new_line_number_starts_a_new_line_in_the_text() {
        let coords = vec![
            coord_at("first", 0, 0.0),
            coord_at("second", 1, 1.0),
            coord_at("third", 1, 1.0),
        ];
        let ocr = OcrText::from(coords.as_slice());
        assert_eq!(words_of(&ocr), vec!["first", "second", "third"]);
        assert_eq!(ocr.text, "first\nsecond third");
    }

    #[test]
    fn coordinates_without_text_are_skipped() {
        let mut untexted = coord("ignored", 0);
        untexted.text = None;
        let coords = vec![coord("kept", 0), untexted];
        let ocr = OcrText::from(coords.as_slice());
        assert_eq!(words_of(&ocr), vec!["kept"]);
        assert_eq!(ocr.text, "kept");
    }

    /// Reproduces the two-column form layout that motivated row grouping: the OCR engine
    /// emits an entire label column's lines (by ascending line index) before the value
    /// column's, even though on the page each label sits right next to its value. `line`'s
    /// emission order alone would put "Passport" a whole column away from "X1234567" in the
    /// text a keyword-window rule sees; grouping overlapping lines into a row and ordering
    /// left to right within it interleaves them back into the row a person reads, and does so
    /// even for a label/value pair whose top edges differ by a pixel or two (here 473 vs 474,
    /// values lifted straight from the fixture that exposed this) — real font-metric noise
    /// that a plain "sort by top edge" would get backwards.
    fn word_at_xy(text: &str, line: usize, x1: f32, y1: f32) -> TextImageCoords {
        // A realistic word height (OCR word boxes are tens of pixels tall, not 1), so two
        // lines whose top edges differ by a pixel of font-metric noise still have genuinely
        // overlapping vertical extents, the way real OCR output does.
        TextImageCoords {
            x1,
            y1,
            x2: x1 + 1.0,
            y2: y1 + 15.0,
            text: Some(text.to_string()),
            line,
        }
    }

    #[test]
    fn lines_sharing_a_row_are_ordered_left_to_right_despite_near_tied_vertical_noise() {
        let coords = vec![
            word_at_xy("Full", 0, 50.0, 100.0),
            word_at_xy("name:", 0, 100.0, 100.0),
            word_at_xy("Passport", 1, 51.0, 473.0),
            word_at_xy("John", 2, 300.0, 100.0),
            word_at_xy("X1234567", 3, 311.0, 474.0),
        ];
        let ocr = OcrText::from(coords.as_slice());
        assert_eq!(
            words_of(&ocr),
            vec!["Full", "name:", "John", "Passport", "X1234567"]
        );
        assert_eq!(ocr.text, "Full name: John\nPassport X1234567");
    }

    fn reporter() -> &'static AppReporter<'static> {
        // Leaked so the reporter, and the terminal it borrows, outlive the redacters built
        // from it in a short-lived test process.
        let term: &'static console::Term = Box::leak(Box::new(console::Term::stdout()));
        Box::leak(Box::new(AppReporter::from(term)))
    }

    fn png_file_ref(name: &str) -> FileSystemRef {
        FileSystemRef {
            relative_path: name.into(),
            media_type: Some(mime::IMAGE_PNG),
            file_size: None,
        }
    }

    fn pdf_file_ref(name: &str) -> FileSystemRef {
        FileSystemRef {
            relative_path: name.into(),
            media_type: Some(mime::APPLICATION_PDF),
            file_size: None,
        }
    }

    fn base_options() -> RedacterBaseOptions {
        RedacterBaseOptions {
            allow_unsupported_copies: false,
            unsupported_as_text: false,
            csv_headers_disable: false,
            csv_delimiter: None,
            sampling_size: None,
            limit_dlp_requests: None,
            categories: None,
            list_redactions: true,
        }
    }

    async fn local_rules(
        groups: BTreeSet<RuleGroup>,
        user_rules: Vec<UserRule>,
    ) -> LocalRulesRedacter<'static> {
        LocalRulesRedacter::new(LocalRulesRedacterOptions { groups, user_rules }, reporter())
            .await
            .expect("valid rule set")
    }

    /// A redacter that supports text/csv (so it can join the OCR/PDF-image plan branches)
    /// but never actually matches anything: enough for tests that only check plan shape.
    async fn any_local_rules() -> LocalRulesRedacter<'static> {
        local_rules(RuleGroup::all(), vec![]).await
    }

    /// Renders as many blank pages as it is told; the OCR fake decides what they say.
    struct FakePdf {
        pages: usize,
    }

    #[cfg(feature = "pdf-render")]
    const NO_POINTS: pdfium_render::prelude::PdfPoints = pdfium_render::prelude::PdfPoints::ZERO;
    #[cfg(not(feature = "pdf-render"))]
    const NO_POINTS: f32 = 0.0;

    impl PdfToImage for FakePdf {
        fn convert_to_images(&self, _pdf_bytes: bytes::Bytes) -> AppResult<PdfInfo> {
            Ok(PdfInfo {
                pages: (0..self.pages)
                    .map(|_| PdfPageInfo {
                        height: NO_POINTS,
                        width: NO_POINTS,
                        page_as_images: image::DynamicImage::new_rgb8(4, 4),
                    })
                    .collect(),
            })
        }

        fn images_to_pdf(&self, _pdf_info: PdfInfo) -> AppResult<bytes::Bytes> {
            unreachable!("planning and detection never write a PDF")
        }
    }

    #[tokio::test]
    async fn a_pdf_is_blocked_on_no_renderer_when_none_is_available() {
        let redacter = any_local_rules().await;
        let redacters = vec![Redacters::LocalRules(redacter)];
        let converters = FileConverters::new();
        let options = base_options();
        let stream_redacter = StreamRedacter::new(&options, &converters);

        let plan = stream_redacter
            .create_redact_plan(&redacters, &pdf_file_ref("form.pdf"))
            .await
            .expect("plan builds");

        assert!(plan.supported_redacters.is_empty());
        assert_eq!(plan.blocked, Some(RedactionBlocked::PdfRendererUnavailable));
    }

    #[tokio::test]
    async fn a_pdf_is_blocked_on_no_ocr_when_no_redacter_reads_the_rendered_page_directly() {
        // `local-rules` only ever supports text/csv, never PNG, so with a renderer present
        // but no OCR engine there is nothing left that can read a PDF page.
        let redacter = any_local_rules().await;
        let redacters = vec![Redacters::LocalRules(redacter)];
        let mut converters = FileConverters::new();
        converters.pdf_image_converter = Some(Box::new(FakePdf { pages: 0 }));
        let options = base_options();
        let stream_redacter = StreamRedacter::new(&options, &converters);

        let plan = stream_redacter
            .create_redact_plan(&redacters, &pdf_file_ref("form.pdf"))
            .await
            .expect("plan builds");

        assert!(plan.supported_redacters.is_empty());
        assert_eq!(plan.blocked, Some(RedactionBlocked::OcrUnavailable));
    }

    #[tokio::test]
    async fn an_image_is_blocked_on_no_ocr_when_none_is_available() {
        let redacter = any_local_rules().await;
        let redacters = vec![Redacters::LocalRules(redacter)];
        let converters = FileConverters::new();
        let options = base_options();
        let stream_redacter = StreamRedacter::new(&options, &converters);

        let plan = stream_redacter
            .create_redact_plan(&redacters, &png_file_ref("scan.png"))
            .await
            .expect("plan builds");

        assert!(plan.supported_redacters.is_empty());
        assert_eq!(plan.blocked, Some(RedactionBlocked::OcrUnavailable));
    }

    #[tokio::test]
    async fn a_pdf_with_a_renderer_and_ocr_and_a_text_redacter_is_not_blocked() {
        let redacter = any_local_rules().await;
        let redacters = vec![Redacters::LocalRules(redacter)];
        let mut converters = FileConverters::new();
        converters.pdf_image_converter = Some(Box::new(FakePdf { pages: 0 }));
        converters.ocr = Some(Box::new(FakeOcr { coords: vec![] }));
        let options = base_options();
        let stream_redacter = StreamRedacter::new(&options, &converters);

        let plan = stream_redacter
            .create_redact_plan(&redacters, &pdf_file_ref("form.pdf"))
            .await
            .expect("plan builds");

        assert!(!plan.supported_redacters.is_empty());
        assert!(plan.apply_pdf_image_converter);
        assert!(plan.apply_ocr);
        assert_eq!(plan.blocked, None);
    }

    struct FakeOcr {
        coords: Vec<TextImageCoords>,
    }

    impl Ocr for FakeOcr {
        fn image_to_text(&self, _image: image::DynamicImage) -> AppResult<Vec<TextImageCoords>> {
            Ok(self.coords.clone())
        }
    }

    fn white_png(width: u32, height: u32) -> bytes::Bytes {
        let image = image::RgbImage::from_pixel(width, height, image::Rgb([255, 255, 255]));
        let mut out = std::io::Cursor::new(Vec::new());
        image.write_to(&mut out, ImageFormat::Png).expect("encodes");
        out.into_inner().into()
    }

    fn is_black_at(data: &[u8], x: u32, y: u32) -> bool {
        let image = image::load_from_memory_with_format(data, ImageFormat::Png)
            .expect("decodes")
            .to_rgb8();
        *image.get_pixel(x, y) == image::Rgb([0, 0, 0])
    }

    fn word_at(text: &str, line: usize, x: f32) -> TextImageCoords {
        TextImageCoords {
            x1: x,
            y1: 0.0,
            x2: x + 1.0,
            y2: 1.0,
            text: Some(text.to_string()),
            line,
        }
    }

    /// Reproduces the defect a document-wide word set has: a word that is part of a PII span
    /// (here, only when it precedes the digits) but also appears unredacted elsewhere on the
    /// page. The custom rule matches only "SSN 123-45-6789" as one span, replacing both OCR
    /// words with a single `[REDACTED]` token; the second, unrelated "SSN" on another line is
    /// left alone. A document-wide word set would see "SSN" survive in the final text (from
    /// the second occurrence) and refuse to box either occurrence, including the one that was
    /// actually redacted.
    #[tokio::test]
    async fn only_the_occurrence_that_was_actually_redacted_is_boxed() {
        let rule = parse_inline_rule(r"ssn=SSN \d{3}-\d{2}-\d{4}").expect("valid inline rule");
        let redacter = local_rules(std::collections::BTreeSet::new(), vec![rule]).await;
        let redacters = [Redacters::LocalRules(redacter)];
        let redacter_refs: Vec<&Redacters> = redacters.iter().collect();

        let ocr = FakeOcr {
            coords: vec![
                word_at("SSN", 0, 0.0),
                word_at("123-45-6789", 0, 1.0),
                word_at("SSN", 1, 2.0),
                word_at("Form", 1, 3.0),
            ],
        };

        let options = base_options();
        let converters = FileConverters::new();
        let stream_redacter = StreamRedacter::new(&options, &converters);
        let file_ref = png_file_ref("form.png");
        let redacted_item = RedacterDataItem {
            content: RedacterDataItemContent::Image {
                mime_type: mime::IMAGE_PNG,
                data: white_png(4, 1),
            },
            file_ref: file_ref.clone(),
        };

        let (result, step) = stream_redacter
            .redact_with_ocr_converter(&file_ref, redacted_item, &redacter_refs, &ocr, None)
            .await
            .expect("redacts");
        assert!(matches!(step, OcrStepResult::Redacted(_)));

        let data = match result.content {
            RedacterDataItemContent::Image { data, .. } => data,
            other => panic!("expected an image, got {other:?}"),
        };
        assert!(is_black_at(&data, 0, 0), "the redacted \"SSN\" is boxed");
        assert!(is_black_at(&data, 1, 0), "the redacted digits are boxed");
        assert!(
            !is_black_at(&data, 2, 0),
            "the untouched second \"SSN\" is not boxed"
        );
        assert!(!is_black_at(&data, 3, 0), "\"Form\" is not boxed");
    }

    fn word(text: &str, line: usize, x1: f32, y1: f32, x2: f32, y2: f32) -> TextImageCoords {
        TextImageCoords {
            x1,
            y1,
            x2,
            y2,
            text: Some(text.to_string()),
            line,
        }
    }

    /// The email and phone rows of the form fixture, as OCR reads them: each label on a line
    /// of its own beside its value.
    fn form_words() -> Vec<TextImageCoords> {
        vec![
            word("Email:", 0, 50.0, 193.0, 125.0, 218.0),
            word("john.smith@example.com", 1, 310.0, 193.0, 622.0, 218.0),
            word("Phone:", 2, 50.0, 263.0, 131.0, 288.0),
            word("+1", 3, 310.0, 263.0, 338.0, 288.0),
            word("(555)", 3, 345.0, 262.0, 415.0, 290.0),
            word("123-4567", 3, 425.0, 263.0, 540.0, 288.0),
        ]
    }

    async fn email_and_phone_rules() -> Redacters<'static> {
        Redacters::LocalRules(
            local_rules([RuleGroup::Email, RuleGroup::Phone].into(), vec![]).await,
        )
    }

    async fn email_rules() -> Redacters<'static> {
        Redacters::LocalRules(local_rules([RuleGroup::Email].into(), vec![]).await)
    }

    fn file_ref(name: &str, media_type: mime::Mime) -> FileSystemRef {
        FileSystemRef {
            relative_path: name.into(),
            media_type: Some(media_type),
            file_size: None,
        }
    }

    async fn detect(
        converters: &FileConverters<'_>,
        redacters: &[Redacters<'static>],
        file_ref: &FileSystemRef,
        data: bytes::Bytes,
    ) -> AppResult<Collected> {
        detect_with(&base_options(), converters, redacters, file_ref, data).await
    }

    async fn detect_with(
        options: &RedacterBaseOptions,
        converters: &FileConverters<'_>,
        redacters: &[Redacters<'static>],
        file_ref: &FileSystemRef,
        data: bytes::Bytes,
    ) -> AppResult<Collected> {
        let stream_redacter = StreamRedacter::new(options, converters);
        let plan = stream_redacter
            .create_detect_plan(redacters, file_ref)
            .await?;
        assert!(
            !plan.supported_redacters.is_empty(),
            "nothing can read {file_ref:?}"
        );
        stream_redacter
            .detect_stream_collected(futures::stream::iter(vec![Ok(data)]), &plan, file_ref)
            .await
    }

    #[tokio::test]
    async fn detection_reports_only_the_findings_in_the_chosen_categories() {
        let converters = FileConverters::new();
        let redacters = vec![Redacters::LocalRules(any_local_rules().await)];
        let text = bytes::Bytes::from("DB_PASSWORD=hunter2-Secret\nADMIN_EMAIL=ops@example.com\n");
        let file = file_ref("app.env", mime::TEXT_PLAIN);

        let everything = detect(&converters, &redacters, &file, text.clone())
            .await
            .unwrap();
        let mut options = base_options();
        options.categories = Categories::limiting([Category::Secrets]);
        let credentials = detect_with(&options, &converters, &redacters, &file, text)
            .await
            .unwrap();

        let values = |result: &Collected| {
            result
                .found
                .detections
                .iter()
                .map(|(_, detection)| detection.value.clone())
                .collect::<Vec<_>>()
        };
        assert_eq!(values(&everything), ["hunter2-Secret", "ops@example.com"]);
        assert_eq!(values(&credentials), ["hunter2-Secret"]);
    }

    fn located(result: &Collected) -> Vec<(DetectionLocation, &str)> {
        result
            .found
            .detections
            .iter()
            .map(|(_, detection)| (detection.location, detection.value.as_str()))
            .collect()
    }

    #[tokio::test]
    async fn an_ocr_detection_is_boxed_by_the_words_it_covers() {
        let mut converters = FileConverters::new();
        converters.ocr = Some(Box::new(FakeOcr {
            coords: form_words(),
        }));
        let redacters = vec![email_and_phone_rules().await];

        let result = detect(
            &converters,
            &redacters,
            &png_file_ref("form.png"),
            white_png(8, 8),
        )
        .await
        .unwrap();

        assert_eq!(
            located(&result),
            vec![
                (
                    DetectionLocation::Image {
                        page: None,
                        x1: 310.0,
                        y1: 193.0,
                        x2: 622.0,
                        y2: 218.0
                    },
                    "john.smith@example.com"
                ),
                (
                    DetectionLocation::Image {
                        page: None,
                        x1: 310.0,
                        y1: 262.0,
                        x2: 540.0,
                        y2: 290.0
                    },
                    "+1 (555) 123-4567"
                ),
            ]
        );
        assert!(result
            .found
            .detections
            .iter()
            .all(|(redacter, _)| *redacter == RedacterType::LocalRules));
    }

    #[tokio::test]
    async fn an_image_format_ocr_cannot_decode_is_blocked_and_reports_nothing() {
        let mut converters = FileConverters::new();
        converters.ocr = Some(Box::new(FakeOcr {
            coords: form_words(),
        }));
        let redacters = vec![email_and_phone_rules().await];

        for media_type in [mime::IMAGE_SVG, "image/heic".parse().unwrap()] {
            let result = detect(
                &converters,
                &redacters,
                &file_ref("picture", media_type.clone()),
                white_png(8, 8),
            )
            .await
            .unwrap();
            assert_eq!(
                result.blocked,
                Some(RedactionBlocked::OcrImageFormatNotSupported),
                "{media_type}"
            );
            assert!(result.found.detections.is_empty(), "{media_type}");
        }
    }

    #[tokio::test]
    async fn every_page_of_a_pdf_is_read_and_named() {
        let mut converters = FileConverters::new();
        converters.ocr = Some(Box::new(FakeOcr {
            coords: form_words(),
        }));
        converters.pdf_image_converter = Some(Box::new(FakePdf { pages: 2 }));
        let redacters = vec![email_rules().await];

        let result = detect(
            &converters,
            &redacters,
            &pdf_file_ref("form.pdf"),
            bytes::Bytes::from_static(b"%PDF"),
        )
        .await
        .unwrap();

        let pages: Vec<Option<usize>> = result
            .found
            .detections
            .iter()
            .map(|(_, detection)| match detection.location {
                DetectionLocation::Image { page, .. } => page,
                other => panic!("a PDF page yields image locations, got {other:?}"),
            })
            .collect();
        assert_eq!(pages, vec![Some(1), Some(2)]);
    }

    /// Redaction chains redacters, so the second one sees the first one's `[REDACTED]`;
    /// detection must not, or a value two redacters both recognise is reported once.
    #[tokio::test]
    async fn every_redacter_reads_the_original_content() {
        let converters = FileConverters::new();
        let contact_rule = parse_inline_rule("contact=a@b\\.io").unwrap();
        let redacters = vec![
            email_rules().await,
            Redacters::LocalRules(local_rules(BTreeSet::new(), vec![contact_rule]).await),
        ];

        let result = detect(
            &converters,
            &redacters,
            &file_ref("note.txt", mime::TEXT_PLAIN),
            bytes::Bytes::from_static(b"mail a@b.io"),
        )
        .await
        .unwrap();

        let labels: Vec<&DetectionLabel> = result
            .found
            .detections
            .iter()
            .map(|(_, detection)| &detection.label)
            .collect();
        assert_eq!(
            labels,
            vec![
                &DetectionLabel::new("email"),
                &DetectionLabel::new("contact")
            ]
        );
    }

    #[tokio::test]
    async fn a_csv_file_is_read_as_a_table_of_cells() {
        let converters = FileConverters::new();
        let redacters = vec![email_rules().await];

        let result = detect(
            &converters,
            &redacters,
            &file_ref("customers.csv", mime::TEXT_CSV),
            bytes::Bytes::from_static(b"id,contact\n1,none\n2,a@b.io\n"),
        )
        .await
        .unwrap();

        assert_eq!(
            located(&result),
            vec![(
                DetectionLocation::TableCell {
                    row: 2,
                    column: 2,
                    start: 0,
                    end: 6
                },
                "a@b.io"
            )]
        );
    }

    /// What redacting a file writes and lists, read together as `cp` does.
    struct Redaction {
        number_of_redactions: usize,
        summary: RedactionSummary,
        written: AppResult<Vec<bytes::Bytes>>,
        listed: AppResult<Vec<(RedacterType, Detection)>>,
    }

    async fn redact_and_list(
        stream_redacter: &StreamRedacter<'_>,
        input: impl Stream<Item = AppResult<bytes::Bytes>> + Send + Sync + Unpin + 'static,
        plan: StreamRedactPlan<'_>,
        file_ref: &FileSystemRef,
    ) -> AppResult<Redaction> {
        let mut listed = Vec::new();
        let mut record = |records| {
            listed.extend(records);
            Ok(())
        };
        let RedactStreamResult {
            number_of_redactions,
            summary,
            output,
        } = stream_redacter
            .redact_stream(input, plan, file_ref, &mut record)
            .await?;
        let (written, run) = output
            .drive(async |stream| stream.try_collect().await)
            .await;
        Ok(Redaction {
            number_of_redactions,
            summary,
            written,
            listed: run.map(|()| listed),
        })
    }

    async fn redact(
        converters: &FileConverters<'_>,
        redacters: &[Redacters<'static>],
        file_ref: &FileSystemRef,
        data: &[u8],
    ) -> Vec<(RedacterType, Detection)> {
        let options = base_options();
        let stream_redacter = StreamRedacter::new(&options, converters);
        let plan = stream_redacter
            .create_redact_plan(redacters, file_ref)
            .await
            .unwrap();
        redact_and_list(&stream_redacter, one_chunk(data), plan, file_ref)
            .await
            .unwrap()
            .listed
            .unwrap()
    }

    fn redaction_places(
        redactions: &[(RedacterType, Detection)],
    ) -> Vec<(RedacterType, DetectionLocation, &str)> {
        redactions
            .iter()
            .map(|(redacter, detection)| (*redacter, detection.location, detection.value.as_str()))
            .collect()
    }

    #[tokio::test]
    async fn a_redacted_csv_file_reports_the_cells() {
        let redacters = vec![email_rules().await];

        let redactions = redact(
            &FileConverters::new(),
            &redacters,
            &file_ref("customers.csv", mime::TEXT_CSV),
            b"id,contact\n1,none\n2,a@b.io\n",
        )
        .await;

        assert_eq!(
            redaction_places(&redactions),
            vec![(
                RedacterType::LocalRules,
                DetectionLocation::TableCell {
                    row: 2,
                    column: 2,
                    start: 0,
                    end: 6
                },
                "a@b.io"
            )]
        );
    }

    #[tokio::test]
    async fn a_copy_that_lists_no_redactions_records_nothing_and_redacts_the_same() {
        let redacters = vec![email_rules().await];
        let file = file_ref("note.txt", mime::TEXT_PLAIN);
        let converters = FileConverters::new();
        let mut options = base_options();
        options.list_redactions = false;
        let stream_redacter = StreamRedacter::new(&options, &converters);
        let plan = stream_redacter
            .create_redact_plan(&redacters, &file)
            .await
            .unwrap();

        let result = redact_and_list(
            &stream_redacter,
            one_chunk(b"Mail ana@x.io today"),
            plan,
            &file,
        )
        .await
        .unwrap();

        assert_eq!(result.number_of_redactions, 1);
        assert!(result.listed.unwrap().is_empty());
        assert_eq!(result.written.unwrap().concat(), b"Mail [REDACTED] today");
    }

    async fn ocr_redaction(page: Option<usize>) -> Vec<(RedacterType, Detection)> {
        let redacters = [email_and_phone_rules().await];
        let redacter_refs: Vec<&Redacters> = redacters.iter().collect();
        let ocr = FakeOcr {
            coords: form_words(),
        };
        let options = base_options();
        let converters = FileConverters::new();
        let stream_redacter = StreamRedacter::new(&options, &converters);
        let file_ref = png_file_ref("form.png");
        let item = RedacterDataItem {
            content: RedacterDataItemContent::Image {
                mime_type: mime::IMAGE_PNG,
                data: white_png(8, 8),
            },
            file_ref: file_ref.clone(),
        };
        match stream_redacter
            .redact_with_ocr_converter(&file_ref, item, &redacter_refs, &ocr, page)
            .await
            .unwrap()
            .1
        {
            OcrStepResult::Redacted(records) => records,
            OcrStepResult::ImageFormatNotSupported => panic!("a PNG is supported"),
        }
    }

    #[tokio::test]
    async fn an_ocr_redaction_is_boxed_by_the_drawn_words_on_the_page() {
        for page in [None, Some(2)] {
            let records = ocr_redaction(page).await;

            let boxes: Vec<_> = records
                .iter()
                .map(|(_, detection)| {
                    (
                        detection.label.to_string(),
                        detection.location,
                        detection.value.as_str(),
                    )
                })
                .collect();
            assert_eq!(
                boxes,
                vec![
                    (
                        "email".to_string(),
                        DetectionLocation::Image {
                            page,
                            x1: 310.0,
                            y1: 193.0,
                            x2: 622.0,
                            y2: 218.0
                        },
                        "john.smith@example.com"
                    ),
                    (
                        "phone-international".to_string(),
                        DetectionLocation::Image {
                            page,
                            x1: 310.0,
                            y1: 262.0,
                            x2: 540.0,
                            y2: 290.0
                        },
                        "+1 (555) 123-4567"
                    ),
                ]
            );
        }
    }

    /// Ink extents of two values of the form, measured from the PNG fixture and from page 1
    /// of the PDF fixture as pdfium renders it (one pixel per point): `(x1, y1, x2, y2)`. The
    /// phone number is three OCR words, so its box is a union.
    const FORM_FIXTURE_INK: [(&str, Option<usize>, &str, [f32; 4]); 4] = [
        (
            "test-fixtures/media/form-example.png",
            None,
            "email",
            [310.0, 195.0, 620.0, 218.0],
        ),
        (
            "test-fixtures/media/form-example.png",
            None,
            "phone-international",
            [313.0, 265.0, 538.0, 286.0],
        ),
        (
            "test-fixtures/documents/customer-form.pdf",
            Some(1),
            "email",
            [97.0, 148.0, 234.0, 159.0],
        ),
        (
            "test-fixtures/documents/customer-form.pdf",
            Some(1),
            "phone-international",
            [102.0, 173.0, 200.0, 184.0],
        ),
    ];

    /// Pdfium binds once per process, so the image and the PDF share one set of converters.
    async fn form_converters() -> FileConverters<'static> {
        use crate::addon_store::{AddonStore, AddonStoreOptions};
        let store = AddonStore::new(&AddonStoreOptions::default());
        let converters = FileConverters::new()
            .init(reporter(), &store)
            .await
            .unwrap();
        assert!(
            converters.ocr.is_some() && converters.pdf_image_converter.is_some(),
            "the ci-ocr feature needs the OCR model and pdfium"
        );
        converters
    }

    /// `found` must be a box on `page` that covers the ink of the value and reaches little past
    /// it.
    fn assert_box_on_ink(
        path: &str,
        page: Option<usize>,
        label: &str,
        found: &Detection,
        [ink_x1, ink_y1, ink_x2, ink_y2]: [f32; 4],
    ) {
        let DetectionLocation::Image {
            page: found_page,
            x1,
            y1,
            x2,
            y2,
        } = found.location
        else {
            panic!("{path}: an image location is expected, got {found:?}");
        };
        assert_eq!(found_page, page, "{path}");
        assert!(
            x1 <= ink_x1 + 2.0 && y1 <= ink_y1 + 2.0 && x2 >= ink_x2 - 2.0 && y2 >= ink_y2 - 2.0,
            "{path}: the {label} box {:?} does not cover its ink",
            (x1, y1, x2, y2)
        );
        assert!(
            x2 - x1 <= ink_x2 - ink_x1 + 10.0 && y2 - y1 <= ink_y2 - ink_y1 + 10.0,
            "{path}: the {label} box {:?} reaches past its value",
            (x1, y1, x2, y2)
        );
    }

    /// Pdfium binds once per process, so detection and redaction share one test.
    #[tokio::test]
    #[cfg_attr(not(feature = "ci-ocr"), ignore)]
    async fn detections_and_redactions_in_the_form_fixtures_are_boxed_where_the_values_are_printed()
    {
        use crate::redacters::test_support::pdfium_test_lock;
        let _guard = pdfium_test_lock().lock().await;
        let converters = form_converters().await;
        let redacters = vec![email_and_phone_rules().await];

        for (path, page, label, ink) in FORM_FIXTURE_INK {
            let media_type = mime_guess::from_path(path).first().unwrap();
            let data = std::fs::read(path).unwrap();
            let file_ref = file_ref(path, media_type);
            let detected = detect(&converters, &redacters, &file_ref, data.clone().into())
                .await
                .unwrap();
            let (_, found) = detected
                .found
                .detections
                .iter()
                .find(|(_, detection)| detection.label == DetectionLabel::new(label))
                .unwrap_or_else(|| panic!("{path}: no {label} found"));
            assert_box_on_ink(path, page, label, found, ink);

            let redactions = redact(&converters, &redacters, &file_ref, &data).await;
            let (_, replaced) = redactions
                .iter()
                .find(|(_, detection)| detection.label == DetectionLabel::new(label))
                .unwrap_or_else(|| panic!("{path}: no {label} redacted"));
            assert_box_on_ink(path, page, label, replaced, ink);
        }
    }

    fn one_chunk(
        data: &[u8],
    ) -> impl Stream<Item = AppResult<bytes::Bytes>> + Send + Sync + Unpin + 'static {
        futures::stream::iter(vec![Ok(bytes::Bytes::copy_from_slice(data))])
    }

    /// The bytes in chunks of the size the probe for a NUL byte reads, as a file arrives from a
    /// bucket or a disk.
    fn in_probe_chunks(
        data: &[u8],
    ) -> impl Stream<Item = AppResult<bytes::Bytes>> + Send + Sync + Unpin + 'static {
        futures::stream::iter(
            data.chunks(NOT_TEXT_PROBE_SIZE)
                .map(|chunk| Ok(bytes::Bytes::copy_from_slice(chunk)))
                .collect::<Vec<_>>(),
        )
    }

    /// A file without an extension, which no redacter supports by its type.
    fn env_file() -> FileSystemRef {
        FileSystemRef {
            relative_path: ".env".into(),
            media_type: None,
            file_size: None,
        }
    }

    /// Valid UTF-8 with a NUL byte, bytes that are not UTF-8, and valid UTF-8 whose first NUL
    /// byte comes after the probe has read its fill.
    fn not_text_files() -> Vec<Vec<u8>> {
        let mut nul_after_the_probe = b"ADMIN_EMAIL=a@b.io\n".repeat(NOT_TEXT_PROBE_SIZE / 16);
        nul_after_the_probe.push(0);
        vec![
            b"ADMIN_EMAIL=a@b.io\0".to_vec(),
            vec![0xff, 0xfe, b'a', b'@'],
            nul_after_the_probe,
        ]
    }

    #[tokio::test]
    async fn an_unsupported_file_is_scanned_as_text_only_when_asked_and_when_it_is_text() {
        let converters = FileConverters::new();
        let redacters = vec![email_rules().await];
        let mut options = base_options();
        let by_default = StreamRedacter::new(&options, &converters)
            .create_detect_plan(&redacters, &env_file())
            .await
            .unwrap()
            .supported_redacters
            .len();
        assert_eq!(by_default, 0);

        options.unsupported_as_text = true;
        let stream_redacter = StreamRedacter::new(&options, &converters);
        let plan = stream_redacter
            .create_detect_plan(&redacters, &env_file())
            .await
            .unwrap();
        let text = stream_redacter
            .detect_stream_collected(one_chunk(b"ADMIN_EMAIL=a@b.io\n"), &plan, &env_file())
            .await
            .unwrap();
        assert_eq!(
            located(&text),
            vec![(
                DetectionLocation::Text {
                    start: 12,
                    end: 18,
                    line: 1,
                    column: 13
                },
                "a@b.io"
            )]
        );
        for data in not_text_files() {
            let result = stream_redacter
                .detect_stream_collected(in_probe_chunks(&data), &plan, &env_file())
                .await
                .unwrap();
            assert_eq!(result.blocked, Some(RedactionBlocked::NotText), "{data:?}");
            assert!(result.found.detections.is_empty(), "{data:?}");
        }
    }

    #[tokio::test]
    async fn a_text_file_longer_than_the_probe_is_scanned_whole() {
        let converters = FileConverters::new();
        let redacters = vec![email_rules().await];
        let mut options = base_options();
        options.unsupported_as_text = true;
        let stream_redacter = StreamRedacter::new(&options, &converters);
        let plan = stream_redacter
            .create_detect_plan(&redacters, &env_file())
            .await
            .unwrap();
        let mut text = "# padding\n".repeat(NOT_TEXT_PROBE_SIZE / 4);
        text.push_str("ADMIN_EMAIL=a@b.io\n");

        let result = stream_redacter
            .detect_stream_collected(in_probe_chunks(text.as_bytes()), &plan, &env_file())
            .await
            .unwrap();

        assert_eq!(result.blocked, None);
        assert_eq!(
            result
                .found
                .detections
                .iter()
                .map(|(_, detection)| detection.value.as_str())
                .collect::<Vec<_>>(),
            vec!["a@b.io"]
        );
    }

    #[tokio::test]
    async fn an_unsupported_file_that_is_not_text_is_left_as_it_is_by_redaction() {
        let converters = FileConverters::new();
        let redacters = vec![email_rules().await];
        let mut options = base_options();
        options.unsupported_as_text = true;
        let stream_redacter = StreamRedacter::new(&options, &converters);

        let plan = stream_redacter
            .create_redact_plan(&redacters, &env_file())
            .await
            .unwrap();
        let redacted = redact_and_list(
            &stream_redacter,
            one_chunk(b"ADMIN_EMAIL=a@b.io"),
            plan,
            &env_file(),
        )
        .await
        .unwrap();
        let output = redacted.written.unwrap();
        assert_eq!(output.concat(), b"ADMIN_EMAIL=[REDACTED]");

        for data in not_text_files() {
            let plan = stream_redacter
                .create_redact_plan(&redacters, &env_file())
                .await
                .unwrap();
            let result =
                redact_and_list(&stream_redacter, in_probe_chunks(&data), plan, &env_file())
                    .await
                    .unwrap();
            assert_eq!(result.number_of_redactions, 0, "{data:?}");
            assert_eq!(result.summary.blocked, Some(RedactionBlocked::NotText));
            let output = result.written.unwrap();
            assert_eq!(output.concat(), data, "the file is left as it is");
        }
    }

    #[tokio::test]
    async fn a_text_file_longer_than_the_probe_is_redacted_whole() {
        let converters = FileConverters::new();
        let redacters = vec![email_rules().await];
        let mut options = base_options();
        options.unsupported_as_text = true;
        let stream_redacter = StreamRedacter::new(&options, &converters);
        let plan = stream_redacter
            .create_redact_plan(&redacters, &env_file())
            .await
            .unwrap();
        let padding = "# padding\n".repeat(NOT_TEXT_PROBE_SIZE / 4);

        let result = redact_and_list(
            &stream_redacter,
            in_probe_chunks(format!("{padding}ADMIN_EMAIL=a@b.io\n").as_bytes()),
            plan,
            &env_file(),
        )
        .await
        .unwrap();

        assert_eq!(result.summary.blocked, None);
        assert_eq!(result.number_of_redactions, 1);
        let output = result.written.unwrap();
        assert_eq!(
            output.concat(),
            format!("{padding}ADMIN_EMAIL=[REDACTED]\n").as_bytes()
        );
    }

    #[tokio::test]
    async fn a_binary_file_is_recognised_from_its_first_bytes_without_reading_the_rest() {
        let converters = FileConverters::new();
        let redacters = vec![email_rules().await];
        let mut options = base_options();
        options.unsupported_as_text = true;
        let stream_redacter = StreamRedacter::new(&options, &converters);
        let plan = stream_redacter
            .create_detect_plan(&redacters, &env_file())
            .await
            .unwrap();
        let unreadable_after_the_head = futures::stream::iter(vec![
            Ok(bytes::Bytes::from_static(b"\x7fELF\0\0\0")),
            Err(AppError::SystemError {
                message: "read past the first bytes".to_string(),
            }),
        ]);

        let result = stream_redacter
            .detect_stream_collected(unreadable_after_the_head, &plan, &env_file())
            .await
            .expect("the rest of a binary file is not read");

        assert_eq!(result.blocked, Some(RedactionBlocked::NotText));
    }

    #[tokio::test]
    async fn a_binary_file_in_several_chunks_is_left_whole_by_redaction() {
        let converters = FileConverters::new();
        let redacters = vec![email_rules().await];
        let mut options = base_options();
        options.unsupported_as_text = true;
        let stream_redacter = StreamRedacter::new(&options, &converters);
        let plan = stream_redacter
            .create_redact_plan(&redacters, &env_file())
            .await
            .unwrap();
        let chunks = futures::stream::iter(vec![
            Ok(bytes::Bytes::from_static(b"\0head")),
            Ok(bytes::Bytes::from_static(b"a@b.io")),
        ]);

        let result = redact_and_list(&stream_redacter, chunks, plan, &env_file())
            .await
            .unwrap();

        assert_eq!(result.summary.blocked, Some(RedactionBlocked::NotText));
        let output = result.written.unwrap();
        assert_eq!(output.concat(), b"\0heada@b.io");
    }

    #[tokio::test]
    async fn a_marker_in_a_table_cell_leaves_out_only_the_findings_of_that_cell() {
        let converters = FileConverters::new();
        let redacters = vec![email_rules().await];
        let table = "email,note\na@b.io,redacter:allow\nc@d.io redacter:allow,x\ne@f.io,y\n";

        let result = detect(
            &converters,
            &redacters,
            &file_ref("people.csv", mime::TEXT_CSV),
            table.into(),
        )
        .await
        .unwrap();

        assert_eq!(
            located(&result)
                .iter()
                .map(|(_, value)| *value)
                .collect::<Vec<_>>(),
            ["a@b.io", "e@f.io"]
        );
        assert_eq!(result.found.ignored.len(), 1);
    }

    async fn secret_rules() -> Redacters<'static> {
        Redacters::LocalRules(local_rules([RuleGroup::Secrets].into(), vec![]).await)
    }

    #[tokio::test]
    async fn a_marker_inside_any_finding_is_not_a_marker() {
        let converters = FileConverters::new();
        let redacters = vec![secret_rules().await, email_rules().await];

        let result = detect(
            &converters,
            &redacters,
            &file_ref("env.txt", mime::TEXT_PLAIN),
            "API_KEY=xyz_redacter:allow_123456789 mail a@b.io\n".into(),
        )
        .await
        .unwrap();

        assert_eq!(result.found.detections.len(), 2);
        assert!(result.found.ignored.is_empty());
    }

    #[tokio::test]
    async fn a_marker_on_the_last_line_of_a_multi_line_finding_ignores_it() {
        let converters = FileConverters::new();
        let redacters = vec![secret_rules().await];
        let text = "-----BEGIN RSA PRIVATE KEY-----\nMIIB\nAB==\n-----END RSA PRIVATE KEY----- # redacter:allow\n";

        let result = detect(
            &converters,
            &redacters,
            &file_ref("key.pem", mime::TEXT_PLAIN),
            text.into(),
        )
        .await
        .unwrap();

        assert!(result.found.detections.is_empty());
        assert_eq!(result.found.ignored.len(), 1);
    }
    /// The detections of `redacters` in `text` read whole, and in pieces of `layout`, in
    /// chunks of 7 bytes so that no chunk ends where a line does.
    async fn detect_whole_and_in_pieces(
        redacters: &[Redacters<'static>],
        text: &str,
        layout: PieceLayout,
    ) -> (Collected, Collected) {
        let file = file_ref("app.log", mime::TEXT_PLAIN);
        let whole = detect(
            &FileConverters::new(),
            redacters,
            &file,
            bytes::Bytes::from(text.to_string()),
        )
        .await
        .unwrap();
        let chunks = text
            .as_bytes()
            .chunks(7)
            .map(|chunk| Ok(bytes::Bytes::copy_from_slice(chunk)))
            .collect::<Vec<_>>();
        let references: Vec<&Redacters> = redacters.iter().collect();
        let mut in_pieces = Collected::default();
        StreamRedacter::detect_text(
            futures::stream::iter(chunks),
            layout,
            false,
            &references,
            None,
            &file,
            &mut |found| {
                in_pieces.found.absorb(found);
                Ok(())
            },
        )
        .await
        .unwrap();
        (whole, in_pieces)
    }

    /// The way a report orders what it lists: by place.
    fn by_location(mut findings: Vec<(RedacterType, Detection)>) -> Vec<(RedacterType, Detection)> {
        findings.sort_by(|(_, a), (_, b)| a.location.partial_cmp(&b.location).unwrap());
        findings
    }

    fn assert_same_findings(whole: Collected, in_pieces: Collected) {
        let (whole_detections, piece_detections) = (
            by_location(whole.found.detections),
            by_location(in_pieces.found.detections),
        );
        let missing: Vec<_> = whole_detections
            .iter()
            .filter(|found| !piece_detections.contains(found))
            .collect();
        let extra: Vec<_> = piece_detections
            .iter()
            .filter(|found| !whole_detections.contains(found))
            .collect();
        assert!(
            missing.is_empty() && extra.is_empty(),
            "missing in pieces: {missing:?}\nonly in pieces: {extra:?}"
        );
        assert_eq!(piece_detections, whole_detections);
        assert_eq!(
            by_location(in_pieces.found.ignored),
            by_location(whole.found.ignored)
        );
        assert_eq!(in_pieces.found.unlocated, whole.found.unlocated);
    }

    const KEY_BLOCK: &str = "-----BEGIN RSA PRIVATE KEY-----\n\
        MIIEowIBAAKCAQEAu1SU1LfVLPHCozMxH2Mo4lgOEePzNm0tRgeLezV6ffAt0gun\n\
        VTLw7onLRnrq0/IzW7yWR7QkrmBL7jTKEn5u+qQQvfnJRk59PqapcHcd0lXCbl2G\n\
        h0fvBLD3eRy6pbyBtqY08c6rN3j4JfNXx6NkQcb/kVApkbNmVoQpvE6wPFEwXn1q\n\
        -----END RSA PRIVATE KEY-----\n";

    /// Lines a reader would find in an application log, with values that need the lines around
    /// them: a passport number a blank line after its keyword, a private key block, and a value
    /// the author marked to be ignored.
    fn log_with_values_across_lines() -> String {
        let mut log = String::new();
        for line in 0..90 {
            log.push_str(&format!(
                "2026-11-01 INFO request {line} sent to user{line}@example.com in 3ms\n"
            ));
            match line % 13 {
                3 => log.push_str("Passport\n\nAB1234567 on file\n"),
                7 => log.push_str(KEY_BLOCK),
                11 => log.push_str("copy ops@example.com # redacter:allow\n"),
                _ => {}
            }
            if line % 9 == 0 {
                log.push_str("\n\n");
            }
        }
        log
    }

    #[tokio::test]
    async fn a_text_read_in_pieces_gives_the_findings_of_the_whole_text() {
        let redacters = vec![Redacters::LocalRules(
            local_rules(RuleGroup::all(), vec![]).await,
        )];
        let log = log_with_values_across_lines();
        let crlf_multibyte = log
            .replace("sent to", "envoyé à Zoë, Σίσυφος, 日本語 and")
            .replace('\n', "\r\n");
        for text in [&log, &crlf_multibyte] {
            for layout in [
                PieceLayout::new(60, 300),
                PieceLayout::new(500, 900),
                PieceLayout::new(2000, 700),
            ] {
                let (whole, in_pieces) = detect_whole_and_in_pieces(&redacters, text, layout).await;
                assert!(
                    whole.found.detections.len() > 100,
                    "the log has values to find"
                );
                assert!(!whole.found.ignored.is_empty());
                assert_same_findings(whole, in_pieces);
            }
        }
    }

    #[tokio::test]
    async fn a_marker_in_a_value_cut_by_a_window_edge_ignores_nothing() {
        let redacters = vec![secret_rules().await, email_rules().await];
        let mut text = ".".repeat(1200);
        text.replace_range(100..117, " ops@example.com ");
        text.replace_range(530..565, "\"password\":\"xyz_redacter:allow_123\"");

        let (whole, in_pieces) = detect_whole_and_in_pieces(&redacters, &text, cut_at_400()).await;

        assert_eq!(whole.found.detections.len(), 2);
        assert!(whole.found.ignored.is_empty());
        assert_same_findings(whole, in_pieces);
    }

    async fn user_rules(rules: &[&str]) -> Vec<Redacters<'static>> {
        let rules = rules
            .iter()
            .map(|rule| parse_inline_rule(rule).unwrap())
            .collect();
        vec![Redacters::LocalRules(
            local_rules(BTreeSet::new(), rules).await,
        )]
    }

    /// `count` lines of ten bytes, with the `marked` ones replaced by a line of ten bytes.
    fn ten_byte_lines(count: usize, marked: &[(usize, &str)]) -> String {
        (0..count)
            .map(|line| {
                marked
                    .iter()
                    .find(|(at, _)| *at == line)
                    .map_or("012345678\n", |(_, text)| text)
            })
            .collect()
    }

    /// The first cut falls at byte 400 of these texts, and the windows reach 160 bytes past it.
    fn cut_at_400() -> PieceLayout {
        PieceLayout::new(400, 160)
    }

    #[tokio::test]
    async fn findings_that_overlap_across_a_cut_are_reported_once() {
        let text = ten_byte_lines(
            100,
            &[
                (20, "YYYY open\n"),
                (38, "XXXX open\n"),
                (39, "QQQQ clos\n"),
                (42, "WWWW clos\n"),
            ],
        );
        let rules = user_rules(&["ywrap=(?s)YYYY.*?QQQQ", "xwrap=(?s)XXXX.*?WWWW"]).await;

        let (whole, in_pieces) = detect_whole_and_in_pieces(&rules, &text, cut_at_400()).await;

        assert_eq!(whole.found.detections.len(), 1);
        assert_same_findings(whole, in_pieces);
    }

    #[tokio::test]
    async fn the_longer_of_two_findings_that_start_together_is_reported_across_a_cut() {
        let text = ten_byte_lines(
            100,
            &[
                (39, "BEGIN....\n"),
                (41, "END.......\n"),
                (57, "FINISH...\n"),
            ],
        );
        let rules = user_rules(&["short=(?s)BEGIN.*?END", "long=(?s)BEGIN.*?FINISH"]).await;

        let (whole, in_pieces) = detect_whole_and_in_pieces(&rules, &text, cut_at_400()).await;

        assert_eq!(whole.found.detections.len(), 1);
        assert!(
            in_pieces
                .found
                .detections
                .iter()
                .any(|found| found == &whole.found.detections[0]),
            "{:?}",
            in_pieces.found.detections
        );
    }

    #[tokio::test]
    async fn a_value_across_a_cut_is_found_up_to_twice_the_context_and_missed_beyond() {
        let rules = user_rules(&["block=(?s)BEGIN.*?END"]).await;
        for first_line in 10..40 {
            let text = ten_byte_lines(
                100,
                &[
                    (first_line, "BEGIN....\n"),
                    (first_line + 31, "END.......\n"),
                ],
            );

            let (whole, in_pieces) = detect_whole_and_in_pieces(&rules, &text, cut_at_400()).await;

            assert_eq!(
                in_pieces.found.detections.len(),
                1,
                "from line {first_line}"
            );
            assert_same_findings(whole, in_pieces);
        }

        let too_long = ten_byte_lines(100, &[(23, "BEGIN....\n"), (56, "END.......\n")]);
        let (whole, in_pieces) = detect_whole_and_in_pieces(&rules, &too_long, cut_at_400()).await;
        assert_eq!(whole.found.detections.len(), 1);
        assert!(in_pieces.found.detections.is_empty());
    }

    #[tokio::test]
    async fn a_line_longer_than_a_window_keeps_its_findings_lines_and_columns() {
        let redacters = vec![email_rules().await];
        let long_line: String = (0..400)
            .map(|word| {
                if word % 37 == 0 {
                    format!("héllo user{word}@example.com ")
                } else {
                    "日本語 Σίσυφος ".to_string()
                }
            })
            .collect();
        let text = format!("first user@example.org\n{long_line}\nlast a@b.io\n");

        for layout in [PieceLayout::new(50, 30), PieceLayout::new(300, 100)] {
            let (whole, in_pieces) = detect_whole_and_in_pieces(&redacters, &text, layout).await;
            assert!(whole.found.detections.len() > 10);
            assert_same_findings(whole, in_pieces);
        }
    }

    #[tokio::test]
    async fn only_text_that_every_redacter_reads_in_pieces_is_planned_in_pieces() {
        let converters = FileConverters::new();
        let redacters = vec![email_rules().await];
        let plan = async |options: RedacterBaseOptions, file: FileSystemRef| {
            let stream_redacter = StreamRedacter::new(&options, &converters);
            let plan = stream_redacter
                .create_detect_plan(&redacters, &file)
                .await
                .unwrap();
            matches!(plan.read, PlannedRead::TextPieces(_))
        };
        let text = file_ref("app.log", mime::TEXT_PLAIN);
        let sampled = RedacterBaseOptions {
            sampling_size: Some(100),
            ..base_options()
        };
        let as_text = RedacterBaseOptions {
            unsupported_as_text: true,
            ..base_options()
        };
        let unknown = file_ref("data.bin", mime::APPLICATION_OCTET_STREAM);

        assert!(plan(base_options(), text.clone()).await);
        assert!(!plan(sampled, text).await);
        assert!(plan(as_text.clone(), unknown.clone()).await);
        assert!(
            !plan(
                RedacterBaseOptions {
                    sampling_size: Some(100),
                    ..as_text
                },
                unknown
            )
            .await
        );
        assert!(!plan(base_options(), file_ref("table.csv", mime::TEXT_CSV)).await);
    }

    #[tokio::test]
    async fn a_chain_reads_the_contexts_of_all_its_steps_and_a_scan_the_widest() {
        let converters = FileConverters::new();
        let redacters = vec![email_rules().await, secret_rules().await];
        let options = base_options();
        let stream_redacter = StreamRedacter::new(&options, &converters);
        let file = file_ref("app.log", mime::TEXT_PLAIN);
        let context = TextPieceLimits::LOCAL.context_bytes;

        let redaction = stream_redacter
            .create_redact_plan(&redacters, &file)
            .await
            .unwrap();
        let scan = stream_redacter
            .create_detect_plan(&redacters, &file)
            .await
            .unwrap();

        let window = TextPieceLimits::LOCAL.max_request_bytes;
        let rounding = crate::redacters::CUT_ROUNDING_BYTES;
        let concurrency = TextPieceLimits::LOCAL.concurrent_requests;
        assert_eq!(
            redaction.read,
            PlannedRead::TextPieces(
                PieceLayout::new(window - 2 * (2 * context + rounding), 2 * context)
                    .with_whole_text_up_to(window)
                    .reading(concurrency)
            )
        );
        assert_eq!(
            scan.read,
            PlannedRead::TextPieces(
                PieceLayout::new(window - 2 * (context + rounding), context)
                    .with_whole_text_up_to(window)
                    .reading(concurrency)
            )
        );
    }

    async fn text_item_of(
        options: &RedacterBaseOptions,
        chunks: Vec<AppResult<bytes::Bytes>>,
    ) -> AppResult<RedacterDataItem> {
        StreamRedacter::new(options, &FileConverters::new())
            .stream_to_text_redact_item(
                futures::stream::iter(chunks),
                &file_ref("app.log", mime::TEXT_PLAIN),
            )
            .await
    }

    #[tokio::test]
    async fn a_sample_is_read_without_the_rest_of_the_file() {
        let options = RedacterBaseOptions {
            sampling_size: Some(10),
            ..base_options()
        };
        let chunks = vec![
            Ok(bytes::Bytes::from("0123456789".repeat(10))),
            Err(AppError::SystemError {
                message: "read past the sample".to_string(),
            }),
        ];

        let item = text_item_of(&options, chunks).await.unwrap();

        assert!(matches!(
            item.content,
            RedacterDataItemContent::Value(ref text) if text == "0123456789"
        ));
    }

    #[tokio::test]
    async fn a_sample_that_ends_inside_a_character_keeps_the_whole_characters() {
        let options = RedacterBaseOptions {
            sampling_size: Some(1),
            ..base_options()
        };
        let chunks = vec![Ok(bytes::Bytes::from("日本語日本語"))];

        let item = text_item_of(&options, chunks).await.unwrap();

        assert!(matches!(
            item.content,
            RedacterDataItemContent::Value(ref text) if text == "日"
        ));
    }

    #[tokio::test]
    async fn a_text_that_is_not_utf8_fails_with_the_byte_it_fails_at() {
        let chunks = vec![Ok(bytes::Bytes::from_static(b"fine \xff text"))];

        let error = text_item_of(&base_options(), chunks).await.unwrap_err();

        assert!(
            matches!(error, AppError::TextNotUtf8 { offset: 5 }),
            "{error:?}"
        );
    }

    /// What a copy of a text writes and lists.
    struct Copied {
        output: Vec<u8>,
        chunks: usize,
        redactions: Vec<(RedacterType, Detection)>,
    }

    /// `data` as a stream of chunks of `chunk_size` bytes.
    fn in_chunks(
        data: &[u8],
        chunk_size: usize,
    ) -> impl Stream<Item = AppResult<bytes::Bytes>> + Send + Sync + Unpin + 'static {
        futures::stream::iter(
            data.chunks(chunk_size)
                .map(|chunk| Ok(bytes::Bytes::copy_from_slice(chunk)))
                .collect::<Vec<_>>(),
        )
    }

    /// Copies the file `file` that `input` reads, as `read` plans. The stream and the
    /// redactions are polled together, as `cp` does.
    async fn copy_stream_with(
        options: &RedacterBaseOptions,
        redacters: &[Redacters<'static>],
        file: &FileSystemRef,
        input: impl Stream<Item = AppResult<bytes::Bytes>> + Send + Sync + Unpin + 'static,
        read: PlannedRead,
    ) -> AppResult<(
        AppResult<Vec<bytes::Bytes>>,
        AppResult<Vec<(RedacterType, Detection)>>,
    )> {
        let converters = FileConverters::new();
        let stream_redacter = StreamRedacter::new(options, &converters);
        let mut plan = stream_redacter.create_redact_plan(redacters, file).await?;
        plan.read = read;
        let result = redact_and_list(&stream_redacter, input, plan, file).await?;
        Ok((result.written, result.listed))
    }

    /// Copies `content` of the file `file` in chunks of 7 bytes, so that no chunk ends where a
    /// line does, read as `read` plans.
    async fn copy_file_with(
        options: &RedacterBaseOptions,
        redacters: &[Redacters<'static>],
        file: &FileSystemRef,
        content: &[u8],
        read: PlannedRead,
    ) -> AppResult<(
        AppResult<Vec<bytes::Bytes>>,
        AppResult<Vec<(RedacterType, Detection)>>,
    )> {
        copy_stream_with(options, redacters, file, in_chunks(content, 7), read).await
    }

    async fn copy_text_with(
        options: &RedacterBaseOptions,
        redacters: &[Redacters<'static>],
        text: &[u8],
        layout: Option<PieceLayout>,
    ) -> AppResult<(
        AppResult<Vec<bytes::Bytes>>,
        AppResult<Vec<(RedacterType, Detection)>>,
    )> {
        let read = layout.map_or(PlannedRead::Whole, PlannedRead::TextPieces);
        let file = file_ref("app.log", mime::TEXT_PLAIN);
        copy_file_with(options, redacters, &file, text, read).await
    }

    async fn copied_file(
        options: &RedacterBaseOptions,
        redacters: &[Redacters<'static>],
        file: &FileSystemRef,
        content: &[u8],
        read: PlannedRead,
    ) -> Copied {
        let (stream, redactions) = copy_file_with(options, redacters, file, content, read)
            .await
            .unwrap();
        let chunks = stream.unwrap();
        Copied {
            chunks: chunks.len(),
            output: chunks.concat(),
            redactions: redactions.unwrap(),
        }
    }

    async fn copy_text(
        options: &RedacterBaseOptions,
        redacters: &[Redacters<'static>],
        text: &[u8],
        layout: Option<PieceLayout>,
    ) -> Copied {
        let read = layout.map_or(PlannedRead::Whole, PlannedRead::TextPieces);
        let file = file_ref("app.log", mime::TEXT_PLAIN);
        copied_file(options, redacters, &file, text, read).await
    }

    const SMALL_BATCH: PlannedRead = PlannedRead::TableBatches { batch_bytes: 300 };
    const ONE_ROW_BATCHES: PlannedRead = PlannedRead::TableBatches { batch_bytes: 1 };

    fn csv_file() -> FileSystemRef {
        file_ref("people.csv", mime::TEXT_CSV)
    }

    /// Rows with an email, an address and a note that holds a comma, quotes and a line break,
    /// which a batch boundary must not cut.
    fn people(rows: usize) -> String {
        let mut table = String::from("id,contact,note\r\n");
        for row in 0..rows {
            table.push_str(&format!(
                "{row},user{row}@example.com,\"host 10.1.{row}.3, \"\"main\"\"\nthen mail ops{row}@example.com\"\r\n"
            ));
        }
        table
    }

    async fn copied_table(
        options: &RedacterBaseOptions,
        redacters: &[Redacters<'static>],
        table: &str,
        read: PlannedRead,
    ) -> Copied {
        copied_file(options, redacters, &csv_file(), table.as_bytes(), read).await
    }

    async fn email_and_network_rules() -> Vec<Redacters<'static>> {
        vec![
            email_rules().await,
            Redacters::LocalRules(local_rules([RuleGroup::Network].into(), vec![]).await),
        ]
    }

    #[tokio::test]
    async fn a_table_copied_in_batches_is_the_table_copied_whole() {
        let redacters = email_and_network_rules().await;
        let table = people(40);
        let options = base_options();
        let copy = |read| copied_table(&options, &redacters, &table, read);

        let whole = copy(PlannedRead::Whole).await;
        let in_batches = copy(SMALL_BATCH).await;

        assert_eq!(whole.chunks, 1);
        assert!(in_batches.chunks > 1, "{} chunks", in_batches.chunks);
        assert_eq!(
            String::from_utf8_lossy(&in_batches.output),
            String::from_utf8_lossy(&whole.output)
        );
        assert_eq!(whole.redactions.len(), 40 * 3);
        assert_eq!(
            by_location(in_batches.redactions),
            by_location(whole.redactions)
        );
    }

    #[tokio::test]
    async fn tables_of_other_shapes_are_copied_in_batches_as_they_are_whole() {
        let redacters = vec![email_rules().await];
        let options = base_options();
        let tables = [
            "\u{feff}id,mail\r\n1,a@example.com\r\n2,b@example.com\r\n",
            "id,mail\n1,a@example.com\n2,b@example.com\n3,c@example.com\n",
            "id,mail\n1,a@example.com\n2,b@example.com",
        ];

        for table in tables {
            let copy = |read| copied_table(&options, &redacters, table, read);
            let whole = copy(PlannedRead::Whole).await;
            let in_batches = copy(ONE_ROW_BATCHES).await;

            assert_eq!(
                String::from_utf8_lossy(&in_batches.output),
                String::from_utf8_lossy(&whole.output),
                "{table:?}"
            );
            assert_eq!(
                by_location(in_batches.redactions),
                by_location(whole.redactions),
                "{table:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_table_is_written_with_the_delimiter_it_is_read_with() {
        let redacters = vec![email_rules().await];
        let options = RedacterBaseOptions {
            csv_delimiter: Some(b';'),
            ..base_options()
        };
        let table = "id;note\n1;x,y\n2;z\n3;w\n";

        for read in [PlannedRead::Whole, ONE_ROW_BATCHES] {
            let copied = copied_table(&options, &redacters, table, read).await;

            assert_eq!(String::from_utf8(copied.output).unwrap(), table, "{read:?}");
        }
    }

    #[tokio::test]
    async fn a_table_read_without_headers_is_written_without_them() {
        let redacters = vec![email_rules().await];
        let options = RedacterBaseOptions {
            csv_headers_disable: true,
            ..base_options()
        };
        let table = "1,a\n2,b\n3,c\n";

        for read in [PlannedRead::Whole, ONE_ROW_BATCHES] {
            let copied = copied_table(&options, &redacters, table, read).await;

            assert_eq!(String::from_utf8(copied.output).unwrap(), table, "{read:?}");
        }
    }

    #[tokio::test]
    async fn a_row_of_the_wrong_length_after_the_first_batch_fails_the_copy() {
        let redacters = vec![email_rules().await];
        let table = format!("{}9,only one cell\r\n", people(10));

        let (stream, redactions) = copy_file_with(
            &base_options(),
            &redacters,
            &csv_file(),
            table.as_bytes(),
            SMALL_BATCH,
        )
        .await
        .unwrap();

        assert!(stream.is_err());
        assert!(redactions.is_err());
    }

    /// The detections of `redacters` in `content` of the file `file`, in chunks of `chunk_size`
    /// bytes, read as `read` plans.
    async fn detect_as(
        options: &RedacterBaseOptions,
        redacters: &[Redacters<'static>],
        file: &FileSystemRef,
        content: &[u8],
        read: PlannedRead,
        chunk_size: usize,
    ) -> AppResult<Collected> {
        let converters = FileConverters::new();
        let stream_redacter = StreamRedacter::new(options, &converters);
        let mut plan = stream_redacter.create_detect_plan(redacters, file).await?;
        plan.read = read;
        stream_redacter
            .detect_stream_collected(in_chunks(content, chunk_size), &plan, file)
            .await
    }

    /// The detections of `redacters` in `table`, read as `read` plans.
    async fn detect_table_as(
        redacters: &[Redacters<'static>],
        table: &str,
        read: PlannedRead,
    ) -> Collected {
        detect_as(
            &base_options(),
            redacters,
            &csv_file(),
            table.as_bytes(),
            read,
            7,
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn a_scan_of_a_table_in_batches_finds_what_a_scan_of_the_whole_table_finds() {
        let redacters = email_and_network_rules().await;
        let table = format!("{}x,\"eve@example.com redacter:allow\",n\r\n", people(40));

        let whole = detect_table_as(&redacters, &table, PlannedRead::Whole).await;
        let in_batches = detect_table_as(&redacters, &table, SMALL_BATCH).await;

        assert_eq!(whole.found.detections.len(), 40 * 3);
        assert_eq!(whole.found.ignored.len(), 1);
        assert_eq!(
            in_batches.found.ignored[0].1.location,
            DetectionLocation::TableCell {
                row: 41,
                column: 2,
                start: 0,
                end: 15
            }
        );
        assert_same_findings(whole, in_batches);
    }

    /// The read planned for a CSV file when every one of `redacters` supports a table as it is,
    /// as GCP DLP does, so that only [`RedacterType::reads_tables_by_cell`] decides the read.
    async fn planned_read_of_supported_tables<'a>(
        stream_redacter: &'a StreamRedacter<'a>,
        redacters: &'a [Redacters<'a>],
    ) -> PlannedRead {
        stream_redacter
            .create_plan(
                redacters,
                &csv_file(),
                async |_, _| Ok(RedactSupport::Supported),
                PieceLayout::chained,
            )
            .await
            .unwrap()
            .read
    }

    #[tokio::test]
    async fn a_table_is_planned_in_batches_only_when_every_redacter_reads_its_cells() {
        let converters = FileConverters::new();
        let options = base_options();
        let stream_redacter = StreamRedacter::new(&options, &converters);
        let by_cell = email_rules().await;
        let whole_table = Redacters::Presidio(
            PresidioRedacter::new(
                PresidioRedacterOptions {
                    text_analyze_url: Some("http://localhost:5002/analyze".parse().unwrap()),
                    image_redact_url: None,
                    categories: None,
                },
                reporter(),
            )
            .await
            .unwrap(),
        );
        let planned = async |redacters: Vec<Redacters<'static>>| {
            planned_read_of_supported_tables(&stream_redacter, &redacters).await
        };

        assert_eq!(
            planned(vec![by_cell.clone()]).await,
            PlannedRead::TableBatches {
                batch_bytes: TABLE_BATCH_BYTES
            }
        );
        assert_eq!(planned(vec![whole_table.clone()]).await, PlannedRead::Whole);
        assert_eq!(
            planned(vec![by_cell, whole_table]).await,
            PlannedRead::Whole
        );
    }

    #[tokio::test]
    async fn a_table_read_as_text_is_not_planned_in_batches() {
        let converters = FileConverters::new();
        let options = base_options();
        let stream_redacter = StreamRedacter::new(&options, &converters);
        let text_only = vec![Redacters::Presidio(
            PresidioRedacter::new(
                PresidioRedacterOptions {
                    text_analyze_url: Some("http://localhost:5002/analyze".parse().unwrap()),
                    image_redact_url: None,
                    categories: None,
                },
                reporter(),
            )
            .await
            .unwrap(),
        )];

        let plan = stream_redacter
            .create_redact_plan(&text_only, &csv_file())
            .await
            .unwrap();

        assert!(plan.leave_data_table_as_text);
        assert_eq!(plan.read, PlannedRead::Whole);
    }

    /// The names in the cells are found by the model, which reads `"{header}: {cell}"`, so a
    /// batch gives the findings the whole table gives. The model is installed with
    /// `redacter addons install local-ner`; a test never downloads it.
    #[cfg(feature = "local-ner")]
    #[tokio::test]
    #[cfg_attr(not(feature = "ci-local-ner"), ignore)]
    async fn the_model_finds_the_same_names_in_a_table_read_in_batches() {
        let store = crate::addon_store::AddonStore::new(&Default::default());
        let options = crate::redacters::LocalNerRedacterOptions::validated(
            crate::redacters::Entity::all(),
            crate::redacters::Score::from_static(0.5),
        )
        .unwrap();
        let redacters = vec![Redacters::LocalNer(
            crate::redacters::LocalNerRedacter::new(options, reporter(), &store)
                .await
                .unwrap(),
        )];
        let mut table = String::from("name,city\n");
        for person in ["John Michael Smith", "Maria Garcia Lopez", "Wei Zhang Chen"] {
            table.push_str(&format!("{person},Madrid\n"));
        }

        let whole = detect_table_as(&redacters, &table, PlannedRead::Whole).await;
        let in_batches = detect_table_as(&redacters, &table, ONE_ROW_BATCHES).await;

        assert!(!whole.found.detections.is_empty());
        assert_same_findings(whole, in_batches);
    }

    /// The copy of `text` whole and in pieces of `layout`, which must be the same file. The
    /// pieces copy must arrive in more than one chunk, or the text was not read in pieces at all.
    async fn assert_same_file_in_pieces(
        options: &RedacterBaseOptions,
        redacters: &[Redacters<'static>],
        text: &str,
        layout: PieceLayout,
    ) -> (Copied, Copied) {
        let whole = copy_text(options, redacters, text.as_bytes(), None).await;
        let in_pieces = copy_text(options, redacters, text.as_bytes(), Some(layout)).await;

        assert!(in_pieces.chunks > 1, "{} chunks", in_pieces.chunks);
        assert_eq!(whole.chunks, 1);
        assert_eq!(
            String::from_utf8_lossy(&in_pieces.output),
            String::from_utf8_lossy(&whole.output)
        );
        (whole, in_pieces)
    }

    /// As [`assert_same_file_in_pieces`], and the list of redactions must be the whole text's:
    /// the same values, in the same places, with the same labels.
    async fn assert_same_copy_in_pieces(
        options: &RedacterBaseOptions,
        redacters: &[Redacters<'static>],
        text: &str,
        layout: PieceLayout,
    ) -> Copied {
        let (whole, in_pieces) = assert_same_file_in_pieces(options, redacters, text, layout).await;

        let missing: Vec<_> = whole
            .redactions
            .iter()
            .filter(|found| !in_pieces.redactions.contains(found))
            .collect();
        let extra: Vec<_> = in_pieces
            .redactions
            .iter()
            .filter(|found| !whole.redactions.contains(found))
            .collect();
        assert!(
            missing.is_empty() && extra.is_empty(),
            "missing in pieces: {missing:?}\nonly in pieces: {extra:?}"
        );
        assert_eq!(in_pieces.redactions, whole.redactions);
        in_pieces
    }

    #[tokio::test]
    async fn a_text_read_in_pieces_is_redacted_and_listed_as_the_whole_text_is() {
        let redacters = vec![Redacters::LocalRules(
            local_rules(RuleGroup::all(), vec![]).await,
        )];
        let log = log_with_values_across_lines();
        let crlf_multibyte = log
            .replace("sent to", "envoyé à Zoë, Σίσυφος, 日本語 and")
            .replace('\n', "\r\n");
        for text in [&log, &crlf_multibyte] {
            for layout in [
                PieceLayout::new(60, 300),
                PieceLayout::new(500, 900),
                PieceLayout::new(2000, 700),
            ] {
                let copied =
                    assert_same_copy_in_pieces(&base_options(), &redacters, text, layout).await;
                assert!(
                    copied.redactions.len() > 100,
                    "the log has values to redact"
                );
            }
        }
    }

    #[tokio::test]
    async fn a_chain_of_redacters_reads_the_masks_of_the_ones_before_it_in_every_window() {
        let redacters = vec![email_rules().await, secret_rules().await];
        let log = log_with_values_across_lines()
            .replace(
                " in 3ms",
                " token=ghp_abcdefghijklmnopqrstuvwxyz0123456789 ok",
            )
            .replace("INFO", "password=\"hunter2hunter2\" INFO");
        for layout in [PieceLayout::new(300, 200), PieceLayout::new(1500, 500)] {
            let (whole, in_pieces) =
                assert_same_file_in_pieces(&base_options(), &redacters, &log, layout).await;

            // The whole text's diff can pair the mask of one step with the mask of another and
            // list a change too large, so the list is checked against the text it places.
            let by_label = |copied: &Copied| {
                let mut labels: Vec<_> = copied
                    .redactions
                    .iter()
                    .map(|(_, detection)| detection.label.to_string())
                    .collect();
                labels.sort();
                labels.dedup();
                labels
            };
            assert!(by_label(&in_pieces).len() > 2);
            assert_eq!(by_label(&in_pieces), by_label(&whole));
            for (_, detection) in &in_pieces.redactions {
                let DetectionLocation::Text { start, end, .. } = detection.location else {
                    panic!("a text location is expected");
                };
                assert_eq!(&log[start..end], detection.value);
            }
        }
    }

    #[tokio::test]
    async fn a_value_across_a_cut_is_masked_whole_and_listed_once() {
        let redacters = vec![Redacters::LocalRules(
            local_rules(RuleGroup::all(), vec![]).await,
        )];
        // The first piece ends at 372, the start of the second line of a block of 258 bytes that
        // starts at 340. The first window ends 160 after the cut, before the block does, and only
        // the second window sees it whole.
        let mut text = ".".repeat(339);
        text.push('\n');
        text.push_str(KEY_BLOCK);
        text.push_str(&format!("{}\n", ".".repeat(700)));
        text.push_str("tail ana@example.com\n");
        let layout = PieceLayout::new(400, 160);

        let copied = assert_same_copy_in_pieces(&base_options(), &redacters, &text, layout).await;

        let output = String::from_utf8(copied.output).unwrap();
        assert!(!output.contains("MIIEow") && !output.contains("ana@example"));
        let key_blocks = copied
            .redactions
            .iter()
            .filter(|(_, detection)| detection.label == DetectionLabel::new("private-key-block"))
            .count();
        assert_eq!(key_blocks, 1);
    }

    #[tokio::test]
    async fn values_across_the_edges_of_windows_in_a_line_of_multibyte_text_are_masked_whole() {
        let redacters = vec![email_rules().await];
        let layout = PieceLayout::new(400, 160);
        // The first piece ends at 400 and the second window starts 160 before it, at 240, while
        // the first window ends 160 after it, at 560: each value crosses one of those places, in
        // every position it can have across it.
        for edge in [240, 400, 560] {
            for shift in 0..24 {
                let mut text = "日本語 Σίσυφος ".repeat(80);
                while !text.is_char_boundary(edge - shift) {
                    text.remove(0);
                }
                let mut at = edge - shift;
                while !text.is_char_boundary(at) {
                    at -= 1;
                }
                text.replace_range(at..at, " ana.maria@example.com ");

                assert_same_copy_in_pieces(&base_options(), &redacters, &text, layout).await;
            }
        }
    }

    #[tokio::test]
    async fn a_text_that_fits_in_one_window_is_redacted_whole() {
        let redacters = vec![email_rules().await];
        let text = "Mail ana@x.io today\n".repeat(20);

        let in_one_window = copy_text(
            &base_options(),
            &redacters,
            text.as_bytes(),
            Some(PieceLayout::new(5000, 100)),
        )
        .await;
        let whole = copy_text(&base_options(), &redacters, text.as_bytes(), None).await;

        assert_eq!(in_one_window.chunks, 1);
        assert_eq!(in_one_window.output, whole.output);
        assert_eq!(in_one_window.redactions, whole.redactions);
    }

    #[tokio::test]
    async fn redactions_are_not_listed_unless_asked_for_and_the_text_is_the_same() {
        let redacters = vec![email_rules().await];
        let options = RedacterBaseOptions {
            list_redactions: false,
            ..base_options()
        };
        let text = log_with_values_across_lines();

        let copied =
            assert_same_copy_in_pieces(&options, &redacters, &text, PieceLayout::new(500, 300))
                .await;

        assert!(copied.redactions.is_empty());
        assert!(!String::from_utf8(copied.output)
            .unwrap()
            .contains("@example.com"));
    }

    #[tokio::test]
    async fn redactions_are_handed_over_a_window_at_a_time() {
        let redacters = vec![email_rules().await];
        let converters = FileConverters::new();
        let options = base_options();
        let stream_redacter = StreamRedacter::new(&options, &converters);
        let file = file_ref("app.log", mime::TEXT_PLAIN);
        let mut plan = stream_redacter
            .create_redact_plan(&redacters, &file)
            .await
            .unwrap();
        plan.read = PlannedRead::TextPieces(PieceLayout::new(500, 300));
        let mut handed_over = Vec::new();
        let mut record = |records: Vec<(RedacterType, Detection)>| {
            handed_over.push(records.len());
            Ok(())
        };

        let RedactStreamResult { output, .. } = stream_redacter
            .redact_stream(
                in_chunks("Mail ana@x.io today\n".repeat(200).as_bytes(), 7),
                plan,
                &file,
                &mut record,
            )
            .await
            .unwrap();
        let (written, run) = output
            .drive(async |stream| stream.try_collect::<Vec<_>>().await)
            .await;

        written.unwrap();
        run.unwrap();
        assert!(handed_over.len() > 2, "{handed_over:?}");
        assert_eq!(handed_over.iter().sum::<usize>(), 200);
    }

    #[tokio::test]
    async fn a_text_read_in_pieces_is_limited_to_the_chosen_categories() {
        let redacters = vec![secret_rules().await];
        let options = RedacterBaseOptions {
            categories: Some(Categories::limiting([Category::Secrets]).unwrap()),
            ..base_options()
        };

        let copied = assert_same_copy_in_pieces(
            &options,
            &redacters,
            &log_with_values_across_lines(),
            PieceLayout::new(500, 900),
        )
        .await;

        assert!(!copied.redactions.is_empty());
        assert!(String::from_utf8(copied.output)
            .unwrap()
            .contains("@example.com"));
    }

    #[tokio::test]
    async fn a_text_that_is_not_utf8_in_a_later_window_ends_the_stream_with_the_error() {
        let redacters = vec![email_rules().await];
        let mut text = "Mail ana@x.io today\n".repeat(60).into_bytes();
        let bad_at = text.len();
        text.extend_from_slice(b"bad \xff byte\nMail ana@x.io\n");

        let (stream, redactions) = copy_text_with(
            &base_options(),
            &redacters,
            &text,
            Some(PieceLayout::new(300, 100)),
        )
        .await
        .unwrap();

        assert!(stream.is_err(), "no destination may keep what was written");
        assert!(
            matches!(redactions, Err(AppError::TextNotUtf8 { offset }) if offset == bad_at + 4),
            "{:?}",
            redactions.map(|redactions| redactions.len())
        );
    }

    #[tokio::test]
    async fn a_text_that_is_not_utf8_in_the_first_window_fails_before_a_byte_is_written() {
        let redacters = vec![email_rules().await];
        let mut text = b"bad \xff byte\n".to_vec();
        text.extend_from_slice("Mail ana@x.io today\n".repeat(60).as_bytes());

        let result = copy_text_with(
            &base_options(),
            &redacters,
            &text,
            Some(PieceLayout::new(300, 100)),
        )
        .await;

        assert!(matches!(result, Err(AppError::TextNotUtf8 { offset: 4 })));
    }

    #[tokio::test]
    async fn masks_that_lengthen_the_text_do_not_stop_a_chain_from_reading_it() {
        let redacters = vec![
            Redacters::LocalRules(local_rules([RuleGroup::Network].into(), vec![]).await),
            secret_rules().await,
        ];
        // The masks of the first step make a window longer than the 4 MiB a local step is
        // given to read, which is a memory budget and not a limit.
        let lines = 230_000;
        let text: String = (0..lines)
            .map(|index| {
                format!(
                    "src 1.{}.{}.{} dst 5.6.{}.8 ok\n",
                    index % 10,
                    (index / 10) % 10,
                    (index / 100) % 10,
                    index % 10
                )
            })
            .collect();
        let expected = "src [REDACTED] dst [REDACTED] ok\n".repeat(lines);

        let layout = PieceLayout::chained(&[TextPieceLimits::LOCAL; 2])
            .unwrap()
            .unwrap();

        let copied = copy_text(&base_options(), &redacters, text.as_bytes(), Some(layout)).await;

        assert!(copied.chunks > 1);
        assert!(copied.output == expected.as_bytes());
    }

    /// The options of `cp --unsupported-as-text`.
    fn unsupported_as_text() -> RedacterBaseOptions {
        RedacterBaseOptions {
            unsupported_as_text: true,
            ..base_options()
        }
    }

    /// `data` of the file `.env`, copied as `cp --unsupported-as-text` does, read as `layout` cuts
    /// it or, for `None`, whole, in chunks of `chunk_size` bytes.
    async fn copy_unsupported(
        redacters: &[Redacters<'static>],
        data: &[u8],
        layout: Option<PieceLayout>,
        chunk_size: usize,
    ) -> (
        AppResult<Vec<bytes::Bytes>>,
        AppResult<Vec<(RedacterType, Detection)>>,
    ) {
        copy_stream_with(
            &unsupported_as_text(),
            redacters,
            &env_file(),
            in_chunks(data, chunk_size),
            layout.map_or(PlannedRead::Whole, PlannedRead::TextPieces),
        )
        .await
        .unwrap()
    }

    /// As [`copy_unsupported`] for `scan`.
    async fn scan_unsupported(
        redacters: &[Redacters<'static>],
        data: &[u8],
        layout: Option<PieceLayout>,
        chunk_size: usize,
    ) -> AppResult<Collected> {
        detect_as(
            &unsupported_as_text(),
            redacters,
            &env_file(),
            data,
            layout.map_or(PlannedRead::Whole, PlannedRead::TextPieces),
            chunk_size,
        )
        .await
    }

    /// A log of 20-byte lines with an email on each, long enough to be cut into pieces of a few
    /// hundred bytes.
    fn mail_log() -> Vec<u8> {
        b"Mail ana@x.io today\n".repeat(60)
    }

    fn with_byte_at(mut data: Vec<u8>, position: usize, byte: u8) -> Vec<u8> {
        data[position] = byte;
        data
    }

    /// Pieces of 300 bytes with 100 of context: the first window of `mail_log` ends at byte 400.
    const FIRST_WINDOW_END: usize = 400;

    fn pieces_of_300_with_100_of_context() -> Option<PieceLayout> {
        Some(PieceLayout::new(300, 100))
    }

    #[tokio::test]
    async fn an_unsupported_file_read_in_pieces_is_redacted_listed_and_scanned_as_the_whole_file_is(
    ) {
        let redacters = vec![Redacters::LocalRules(
            local_rules(RuleGroup::all(), vec![]).await,
        )];
        let log = log_with_values_across_lines()
            .replace("sent to", "envoyé à Zoë, Σίσυφος, 日本語 and")
            .replace('\n', "\r\n");
        let (whole, whole_redactions) = copy_unsupported(&redacters, log.as_bytes(), None, 7).await;

        for layout in [PieceLayout::new(500, 900), PieceLayout::new(2000, 700)] {
            let (stream, redactions) =
                copy_unsupported(&redacters, log.as_bytes(), Some(layout), 7).await;
            assert!(stream.as_ref().unwrap().len() > 1);
            assert!(stream.unwrap().concat() == whole.as_ref().unwrap().concat());
            assert_eq!(redactions.unwrap(), *whole_redactions.as_ref().unwrap());

            let whole_scan = scan_unsupported(&redacters, log.as_bytes(), None, 7)
                .await
                .unwrap();
            let scan = scan_unsupported(&redacters, log.as_bytes(), Some(layout), 7)
                .await
                .unwrap();
            assert!(whole_scan.found.detections.len() > 100);
            assert_eq!(scan.blocked, None);
            assert_same_findings(whole_scan, scan);
        }
    }

    /// The first window decides: a NUL or non-UTF-8 byte inside it leaves the file as it is,
    /// and one past it fails the copy and the scan, because pieces were already written. The
    /// outcome does not depend on where the chunks of the input end.
    #[tokio::test]
    async fn the_first_window_decides_whether_a_file_that_is_not_text_is_left_or_fails() {
        let redacters = vec![email_rules().await];
        let layout = pieces_of_300_with_100_of_context();
        for bad_byte in [0, 0xff] {
            for chunk_size in [1, 7, 4096] {
                for position in [50, FIRST_WINDOW_END - 1] {
                    let data = with_byte_at(mail_log(), position, bad_byte);
                    let context = format!("{bad_byte:#x} at {position}, chunks of {chunk_size}");
                    let (stream, redactions) =
                        copy_unsupported(&redacters, &data, layout, chunk_size).await;
                    assert!(stream.unwrap().concat() == data, "{context}");
                    assert!(redactions.unwrap().is_empty(), "{context}");

                    let scan = scan_unsupported(&redacters, &data, layout, chunk_size)
                        .await
                        .unwrap();
                    assert_eq!(scan.blocked, Some(RedactionBlocked::NotText), "{context}");
                    assert!(scan.found.detections.is_empty(), "{context}");
                }

                for position in [FIRST_WINDOW_END, 900] {
                    let data = with_byte_at(mail_log(), position, bad_byte);
                    let context = format!("{bad_byte:#x} at {position}, chunks of {chunk_size}");
                    let (stream, redactions) =
                        copy_unsupported(&redacters, &data, layout, chunk_size).await;
                    assert!(stream.is_err(), "{context}");
                    assert!(redactions.is_err(), "{context}");

                    let error = scan_unsupported(&redacters, &data, layout, chunk_size)
                        .await
                        .err()
                        .expect("the scan fails");
                    assert!(error.is_not_text(), "{context}: {error:?}");
                }
            }
        }
    }

    #[tokio::test]
    async fn an_empty_unsupported_file_is_copied_and_scanned_as_it_is_whole() {
        let redacters = vec![email_rules().await];
        let layout = pieces_of_300_with_100_of_context();
        let (whole, _) = copy_unsupported(&redacters, b"", None, 7).await;
        let (in_pieces, redactions) = copy_unsupported(&redacters, b"", layout, 7).await;
        assert!(in_pieces.unwrap().concat() == whole.unwrap().concat());
        assert!(redactions.unwrap().is_empty());

        let whole = scan_unsupported(&redacters, b"", None, 7).await.unwrap();
        let in_pieces = scan_unsupported(&redacters, b"", layout, 7).await.unwrap();
        assert_eq!(in_pieces.blocked, whole.blocked);
        assert!(in_pieces.found.detections.is_empty());
    }

    /// A stream that panics when polled after it ended, as a bare `unfold` does.
    fn single_pass(
        data: &[u8],
    ) -> impl Stream<Item = AppResult<bytes::Bytes>> + Send + Sync + Unpin + 'static {
        Box::pin(futures::stream::unfold(
            Some(bytes::Bytes::copy_from_slice(data)),
            |chunk| async move { chunk.map(|chunk| (Ok(chunk), None)) },
        ))
    }

    #[tokio::test]
    async fn a_short_file_that_is_not_text_is_read_once() {
        let redacters = vec![email_rules().await];
        let data = b"Caf\xe9 owner ana@example.org\n";

        let (stream, _) = copy_stream_with(
            &unsupported_as_text(),
            &redacters,
            &env_file(),
            single_pass(data),
            PlannedRead::TextPieces(PieceLayout::new(300, 100)),
        )
        .await
        .unwrap();

        assert!(stream.unwrap().concat() == data);
    }

    /// The copy of `content` as `cp` plans it, without a read chosen by the test.
    async fn copied_as_planned(redacters: &[Redacters<'static>], content: &[u8]) -> Copied {
        let converters = FileConverters::new();
        let options = base_options();
        let stream_redacter = StreamRedacter::new(&options, &converters);
        let file = file_ref("app.log", mime::TEXT_PLAIN);
        let plan = stream_redacter
            .create_redact_plan(redacters, &file)
            .await
            .unwrap();
        let result = redact_and_list(&stream_redacter, in_chunks(content, 64 * 1024), plan, &file)
            .await
            .unwrap();
        let chunks = result.written.unwrap();
        Copied {
            chunks: chunks.len(),
            output: chunks.concat(),
            redactions: result.listed.unwrap(),
        }
    }

    /// One line of short sentences with an email in place of two of them every `step` bytes and
    /// across `piece_bytes`, the end of the first piece of a text cut as `cp` cuts it, and the
    /// emails planted.
    fn sentences_with_planted_emails(
        len: usize,
        step: usize,
        piece_bytes: usize,
    ) -> (String, Vec<String>) {
        const SENTENCE: &str = "the cat sat on the mat. ";
        let unit = SENTENCE.len();
        let mut line = SENTENCE.repeat(len / unit + 1);
        line.truncate(len);
        let room = |at: &usize| at + 3 * unit < len;
        let mut starts: Vec<usize> = (step..)
            .step_by(step)
            .take_while(room)
            .map(|at| at / unit * unit)
            .chain(
                (piece_bytes..)
                    .step_by(piece_bytes)
                    .take_while(room)
                    .map(|end| (end - 30).div_ceil(unit) * unit),
            )
            .collect();
        starts.sort();
        starts.dedup_by(|later, earlier| *later < *earlier + 2 * unit);
        let mut emails = Vec::new();
        for start in starts {
            let email = format!("planted.user.{}@example.com", emails.len() + 1);
            let padding = format!(" {SENTENCE}{SENTENCE}");
            let filled = format!("{email}{}", &padding[..2 * unit - email.len()]);
            line.replace_range(start..start + 2 * unit, &filled);
            emails.push(email);
        }
        (line, emails)
    }

    /// `text` with each of `values` replaced by what `mask` makes of it.
    fn masked_values(text: &str, values: &[String], mask: impl Fn(&str) -> String) -> String {
        values.iter().fold(text.to_string(), |text, value| {
            text.replace(value, &mask(value))
        })
    }

    fn per_character(value: &str) -> String {
        "X".repeat(value.chars().count())
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-aws"), ignore)]
    async fn comprehend_masks_every_planted_email_of_a_text_over_its_request_limit() {
        let region = std::env::var("TEST_AWS_REGION").expect("TEST_AWS_REGION required");
        let options = crate::redacters::AwsComprehendRedacterOptions {
            region: Some(aws_config::Region::new(region)),
        };
        let redacters = vec![Redacters::AwsComprehend(
            crate::redacters::AwsComprehendRedacter::new(options, reporter())
                .await
                .unwrap(),
        )];
        let limits = redacters[0].text_piece_limits().unwrap();
        let piece_bytes = limits.max_request_bytes
            - 2 * (limits.context_bytes + crate::redacters::CUT_ROUNDING_BYTES);
        let (text, emails) =
            sentences_with_planted_emails(3 * limits.max_request_bytes, 7001, piece_bytes);

        let copied = copied_as_planned(&redacters, text.as_bytes()).await;

        assert_eq!(
            String::from_utf8(copied.output).unwrap(),
            masked_values(&text, &emails, per_character)
        );
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-aws"), ignore)]
    async fn guardrails_masks_every_planted_email_of_a_text_over_its_request_limit() {
        let region = std::env::var("TEST_AWS_REGION").expect("TEST_AWS_REGION required");
        let guardrail_id = std::env::var("TEST_AWS_BEDROCK_GUARDRAIL_ID")
            .expect("TEST_AWS_BEDROCK_GUARDRAIL_ID required");
        let options = crate::redacters::AwsBedrockGuardrailsRedacterOptions {
            region: Some(aws_config::Region::new(region)),
            guardrail_id: guardrail_id.into(),
            guardrail_version: "DRAFT".to_string().into(),
        };
        let redacters = vec![Redacters::AwsBedrockGuardrails(
            crate::redacters::AwsBedrockGuardrailsRedacter::new(options, reporter())
                .await
                .unwrap(),
        )];
        let limits = redacters[0].text_piece_limits().unwrap();
        let piece_bytes = limits.max_request_bytes
            - 2 * (limits.context_bytes + crate::redacters::CUT_ROUNDING_BYTES);
        let (text, emails) =
            sentences_with_planted_emails(3 * limits.max_request_bytes, 7001, piece_bytes);

        let copied = copied_as_planned(&redacters, text.as_bytes()).await;

        let output = String::from_utf8(copied.output).unwrap();
        for email in &emails {
            assert!(!output.contains(email), "{email} is left in the output");
        }
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-presidio"), ignore)]
    async fn presidio_masks_every_planted_email_of_a_text_over_its_character_limit() {
        let options = PresidioRedacterOptions {
            text_analyze_url: Some(
                url::Url::parse(
                    &std::env::var("TEST_PRESIDIO_ANALYZE_URL")
                        .expect("TEST_PRESIDIO_ANALYZE_URL required"),
                )
                .unwrap(),
            ),
            image_redact_url: None,
            categories: None,
        };
        let redacters = vec![Redacters::Presidio(
            PresidioRedacter::new(options, reporter()).await.unwrap(),
        )];
        let limits = redacters[0].text_piece_limits().unwrap();
        let piece_bytes = limits.max_request_bytes
            - 2 * (limits.context_bytes + crate::redacters::CUT_ROUNDING_BYTES);
        let (text, emails) = sentences_with_planted_emails(1_300_000, 20011, piece_bytes);

        let copied = copied_as_planned(&redacters, text.as_bytes()).await;

        assert_eq!(
            String::from_utf8(copied.output).unwrap(),
            masked_values(&text, &emails, per_character)
        );
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-gcp"), ignore)]
    async fn dlp_masks_every_email_of_a_text_over_its_request_limit_and_findings_limit() {
        crate::redacters::test_support::initialize_crypto();
        let options = crate::redacters::GcpDlpRedacterOptions {
            project_id: crate::common_types::GcpProjectId::new(
                std::env::var("TEST_GCP_PROJECT").expect("TEST_GCP_PROJECT required"),
            ),
            built_in_info_types: crate::redacters::GcpDlpRedacter::default_info_types(),
            user_defined_stored_info_types: vec![],
        };
        let redacters = vec![Redacters::GcpDlp(
            crate::redacters::GcpDlpRedacter::new(options, reporter())
                .await
                .unwrap(),
        )];
        let limits = redacters[0].text_piece_limits().unwrap();
        let users = 29_000;
        let text: String = (1..=users)
            .map(|user| format!("user{user}@example.com\n"))
            .collect();
        let expected = "[REDACTED]\n".repeat(users);

        let copied = copied_as_planned(&redacters, text.as_bytes()).await;

        assert!(text.len() > limits.max_request_bytes);
        assert_eq!(String::from_utf8(copied.output).unwrap(), expected);
    }

    async fn comprehend() -> Redacters<'static> {
        let options = crate::redacters::AwsComprehendRedacterOptions {
            region: Some(aws_config::Region::new("eu-west-1")),
        };
        Redacters::AwsComprehend(
            crate::redacters::AwsComprehendRedacter::new(options, reporter())
                .await
                .unwrap(),
        )
    }

    /// The reads planned to redact and to detect in a text with `redacters`.
    async fn planned_reads(redacters: Vec<Redacters<'static>>) -> (PlannedRead, PlannedRead) {
        let converters = FileConverters::new();
        let options = base_options();
        let stream_redacter = StreamRedacter::new(&options, &converters);
        let file = file_ref("app.log", mime::TEXT_PLAIN);
        let redaction = stream_redacter
            .create_redact_plan(&redacters, &file)
            .await
            .unwrap();
        let detection = stream_redacter
            .create_detect_plan(&redacters, &file)
            .await
            .unwrap();
        (redaction.read, detection.read)
    }

    #[tokio::test]
    async fn contexts_that_do_not_fit_a_request_leave_the_text_to_be_read_whole() {
        let (redaction, detection) =
            planned_reads(vec![comprehend().await, email_rules().await]).await;
        assert_eq!(
            (redaction, detection),
            (PlannedRead::Whole, PlannedRead::Whole)
        );

        // The first step reads the contexts of the ones after it, so a local step that comes
        // first leaves Comprehend the room it needs.
        let (redaction, _) = planned_reads(vec![email_rules().await, comprehend().await]).await;
        assert!(matches!(redaction, PlannedRead::TextPieces(_)));
    }
}
