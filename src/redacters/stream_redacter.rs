use crate::args::RedacterType;
use crate::common_types::TextImageCoords;
use crate::errors::AppError;
use crate::file_converters::ocr::Ocr;
use crate::file_converters::pdf::{PdfInfo, PdfPageInfo, PdfToImage};
use crate::file_converters::FileConverters;
use crate::file_systems::FileSystemRef;
use crate::redacters::piece_redaction::{redact_windows, written_windows, PieceRedaction};
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

/// The redactions of a file, placed in it, once they are known.
type RedactionRecords<'f> = LocalBoxFuture<'f, AppResult<Vec<(RedacterType, Detection)>>>;

/// The redacted bytes of a file and the list of what was redacted in them.
///
/// A text read in pieces is redacted as its stream is read, so the stream stops when nobody
/// polls the list and the list completes only once the stream has ended. That is why neither
/// half can be taken alone: [`RedactedOutput::drive`] polls both.
pub struct RedactedOutput<'f> {
    stream: ContentStream,
    /// What each redacter replaced, placed in the file as it was read, in plan order and in the
    /// order a reader meets it within a redacter. Empty when no redacter changed the file. An
    /// error means the file could not be redacted; the stream then ends with an error as well,
    /// so that no destination keeps a partial copy.
    redactions: RedactionRecords<'f>,
}

impl<'f> RedactedOutput<'f> {
    fn known(stream: ContentStream, redactions: Vec<(RedacterType, Detection)>) -> Self {
        Self {
            stream,
            redactions: futures::future::ready(Ok(redactions)).boxed_local(),
        }
    }

    /// Hands the stream to `consume`, which reads it to the end, and returns what `consume`
    /// returned beside the redactions.
    pub async fn drive<T>(
        self,
        consume: impl AsyncFnOnce(ContentStream) -> T,
    ) -> (T, AppResult<Vec<(RedacterType, Detection)>>) {
        futures::join!(consume(self.stream), self.redactions)
    }
}

/// The bytes of a file, in the order they were read.
pub type ContentStream =
    Box<dyn Stream<Item = AppResult<bytes::Bytes>> + Send + Sync + Unpin + 'static>;

/// How much of a file is read before a NUL byte in it settles that it is not text, so that a
/// large binary file is not read whole just to be skipped.
const NOT_TEXT_PROBE_SIZE: usize = 8 * 1024;

#[derive(Default)]
pub struct DetectStreamResult {
    /// What each redacter of the plan found, with the redacter that found it, in plan order and
    /// in the order the redacter reported. A text read in pieces gives this order for each piece
    /// in turn.
    pub detections: Vec<(RedacterType, Detection)>,
    /// How many values the redacters named without being able to place them in the file, see
    /// [`crate::redacters::Detections::unlocated`].
    pub unlocated: usize,
    /// The detections moved out of `detections` because a line they cover, or their cell, carries
    /// [`crate::redacters::ALLOW_MARKER`] outside them, kept so the caller can count them.
    pub ignored: Vec<(RedacterType, Detection)>,
    /// Set when the file could not be read as planned: OCR cannot decode its image format, or a
    /// file read as text is not text. The plan was not blocked; the step was, so the caller
    /// reports the file as unsupported.
    pub blocked: Option<RedactionBlocked>,
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
            blocked: self.blocked,
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

pub struct StreamRedactPlan<'a> {
    pub apply_pdf_image_converter: bool,
    pub apply_ocr: bool,
    pub leave_data_table_as_text: bool,
    /// No redacter supports the type of the file, and `--unsupported-as-text` asks for it to be
    /// read as text.
    pub read_unsupported_as_text: bool,
    pub supported_redacters: Vec<&'a Redacters<'a>>,
    /// How a text is cut into pieces, when the file is text and every redacter of the plan reads
    /// text in pieces, see [`Redacter::text_piece_limits`]. A text that fits in one window is
    /// read whole all the same.
    pub(crate) piece_layout: Option<PieceLayout>,
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
            piece_layout: None,
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

        // Sampling cuts the text to a prefix, and a file read as text only on its content is not
        // known to be text before it is all read.
        let reads_pieces = file_ref
            .media_type
            .as_ref()
            .is_some_and(Redacters::is_mime_text)
            && !stream_redact_plan.read_unsupported_as_text
            && self.redacter_base_options.sampling_size.is_none();
        if reads_pieces {
            let limits: Option<Vec<TextPieceLimits>> = stream_redact_plan
                .supported_redacters
                .iter()
                .map(|redacter| redacter.text_piece_limits())
                .collect();
            if let Some(limits) = limits {
                stream_redact_plan.piece_layout = piece_layout(&limits)?;
            }
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
    ) -> AppResult<RedactStreamResult<'a>> {
        let item = match redact_plan.piece_layout {
            Some(layout) => {
                let mut pieces = TextPieces::new(input, layout);
                let first = pieces
                    .next_window()
                    .await?
                    .ok_or_else(|| AppError::SystemError {
                        message: "A text has no first window".to_string(),
                    })?;
                if !first.is_whole_text() {
                    return self.redact_text_in_pieces(pieces, first, redact_plan, file_ref);
                }
                Ok(self.text_redact_item(first.text, file_ref))
            }
            None => {
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
                    output: RedactedOutput::known(data, Vec::new()),
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
                let mut writer = csv_async::AsyncWriter::from_writer(vec![]);
                writer.write_record(headers).await?;
                for row in rows {
                    writer.write_record(row).await?;
                }
                writer.flush().await?;
                let bytes = bytes::Bytes::from(writer.into_inner().await?);
                Box::new(futures::stream::iter(vec![Ok(bytes)]))
            }
        };

        Ok(RedactStreamResult {
            number_of_redactions,
            summary,
            output: RedactedOutput::known(output_stream, recorder.into_records()),
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
    ) -> AppResult<RedactStreamResult<'a>> {
        let redaction = PieceRedaction::new(
            &redact_plan.supported_redacters,
            self.redacter_base_options.categories.as_ref(),
            file_ref,
            self.redacter_base_options.list_redactions,
        )?;
        let redacters = redaction.redacters();
        tracing::debug!(
            file = %file_ref.relative_path.value(),
            redacters = redacters.len(),
            layout = ?redact_plan.piece_layout,
            "redacting in pieces"
        );
        let (written, stream) = written_windows();
        Ok(RedactStreamResult {
            number_of_redactions: redacters.len(),
            summary: RedactionSummary {
                applied: redacters,
                ..RedactionSummary::default()
            },
            output: RedactedOutput {
                stream,
                redactions: redact_windows(redaction, pieces, first, written).boxed_local(),
            },
        })
    }

    /// What the redacters of `plan` find in the file, read as `redact_stream` reads it. Every
    /// redacter reads the original content: unlike redaction, nothing is chained.
    pub async fn detect_stream<
        S: Stream<Item = AppResult<bytes::Bytes>> + Send + Unpin + Sync + 'static,
    >(
        &'a self,
        input: S,
        plan: &StreamRedactPlan<'a>,
        file_ref: &FileSystemRef,
    ) -> AppResult<DetectStreamResult> {
        let redacters = &plan.supported_redacters;
        let categories = self.redacter_base_options.categories.as_ref();
        if let Some(layout) = plan.piece_layout {
            return Self::detect_text(input, layout, redacters, categories, file_ref).await;
        }
        let Ok(item) = self
            .stream_to_redact_item(self.redacter_base_options, input, file_ref, plan)
            .await?
        else {
            return Ok(DetectStreamResult {
                blocked: Some(RedactionBlocked::NotText),
                ..DetectStreamResult::default()
            });
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
                    None => result.blocked = Some(RedactionBlocked::OcrImageFormatNotSupported),
                }
            }
            _ => result = Self::detect_all(redacters, &item, None, categories).await?,
        }
        result.set_aside_ignored(&item.content, None);
        Ok(result)
    }

    /// What `redacters` find in the text `input` holds, read in pieces of `layout`. A text that
    /// fits in one window is read whole, as `detect_stream` reads any other file.
    ///
    /// A finding is reported by the window of the piece it starts in, see `owned_by`.
    async fn detect_text<S: Stream<Item = AppResult<bytes::Bytes>> + Unpin>(
        input: S,
        layout: PieceLayout,
        redacters: &[&Redacters<'a>],
        categories: Option<&Categories>,
        file_ref: &FileSystemRef,
    ) -> AppResult<DetectStreamResult> {
        let mut pieces = TextPieces::new(input, layout);
        let mut result = DetectStreamResult::default();
        let mut previous_window_end = 0;
        let mut windows = 0;
        while let Some(window) = pieces.next_window().await? {
            let (is_whole, window_end) = (window.is_whole_text(), window.end());
            let whole_lines = window.whole_lines();
            let (piece, start) = (window.piece, window.start);
            let item = RedacterDataItem {
                content: RedacterDataItemContent::Value(window.text),
                file_ref: file_ref.clone(),
            };
            let mut found = Self::detect_all(redacters, &item, None, categories).await?;
            found.set_aside_ignored(&item.content, Some(whole_lines));
            if is_whole {
                return Ok(found);
            }
            result.absorb(found.owned_by(&piece, start, previous_window_end));
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
        Ok(result)
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
        let reader = tokio_util::io::StreamReader::new(input.map_err(std::io::Error::other));
        let mut reader = csv_async::AsyncReaderBuilder::default()
            .has_headers(!redacter_base_options.csv_headers_disable)
            .delimiter(
                redacter_base_options
                    .csv_delimiter
                    .as_ref()
                    .cloned()
                    .unwrap_or(b','),
            )
            .create_reader(reader);
        let headers = if !redacter_base_options.csv_headers_disable {
            reader
                .headers()
                .await?
                .into_iter()
                .map(|h| h.to_string())
                .collect()
        } else {
            vec![]
        };
        let records: Vec<csv_async::StringRecord> = reader.records().try_collect().await?;
        Ok(RedacterDataItem {
            content: RedacterDataItemContent::Table {
                headers,
                rows: records
                    .iter()
                    .map(|r| r.iter().map(|c| c.to_string()).collect())
                    .collect(),
            },
            file_ref: file_ref.clone(),
        })
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
        RuleGroup, UserRule,
    };
    use crate::reporter::AppReporter;
    use std::collections::BTreeSet;

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
    ) -> AppResult<DetectStreamResult> {
        detect_with(&base_options(), converters, redacters, file_ref, data).await
    }

    async fn detect_with(
        options: &RedacterBaseOptions,
        converters: &FileConverters<'_>,
        redacters: &[Redacters<'static>],
        file_ref: &FileSystemRef,
        data: bytes::Bytes,
    ) -> AppResult<DetectStreamResult> {
        let stream_redacter = StreamRedacter::new(options, converters);
        let plan = stream_redacter
            .create_detect_plan(redacters, file_ref)
            .await?;
        assert!(
            !plan.supported_redacters.is_empty(),
            "nothing can read {file_ref:?}"
        );
        stream_redacter
            .detect_stream(futures::stream::iter(vec![Ok(data)]), &plan, file_ref)
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

        let values = |result: &DetectStreamResult| {
            result
                .detections
                .iter()
                .map(|(_, detection)| detection.value.clone())
                .collect::<Vec<_>>()
        };
        assert_eq!(values(&everything), ["hunter2-Secret", "ops@example.com"]);
        assert_eq!(values(&credentials), ["hunter2-Secret"]);
    }

    fn located(result: &DetectStreamResult) -> Vec<(DetectionLocation, &str)> {
        result
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
            assert!(result.detections.is_empty(), "{media_type}");
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

    /// What a result writes and lists, read together as `cp` does.
    async fn written_and_listed(
        output: RedactedOutput<'_>,
    ) -> (
        AppResult<Vec<bytes::Bytes>>,
        AppResult<Vec<(RedacterType, Detection)>>,
    ) {
        output
            .drive(async |stream| stream.try_collect().await)
            .await
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
        let result = stream_redacter
            .redact_stream(one_chunk(data), plan, file_ref)
            .await
            .unwrap();
        written_and_listed(result.output).await.1.unwrap()
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

        let result = stream_redacter
            .redact_stream(one_chunk(b"Mail ana@x.io today"), plan, &file)
            .await
            .unwrap();

        assert_eq!(result.number_of_redactions, 1);
        let (output, redactions) = written_and_listed(result.output).await;
        assert!(redactions.unwrap().is_empty());
        let output = output.unwrap();
        assert_eq!(output.concat(), b"Mail [REDACTED] today");
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
            .detect_stream(one_chunk(b"ADMIN_EMAIL=a@b.io\n"), &plan, &env_file())
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
                .detect_stream(in_probe_chunks(&data), &plan, &env_file())
                .await
                .unwrap();
            assert_eq!(result.blocked, Some(RedactionBlocked::NotText), "{data:?}");
            assert!(result.detections.is_empty(), "{data:?}");
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
            .detect_stream(in_probe_chunks(text.as_bytes()), &plan, &env_file())
            .await
            .unwrap();

        assert_eq!(result.blocked, None);
        assert_eq!(
            result
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
        let redacted = stream_redacter
            .redact_stream(one_chunk(b"ADMIN_EMAIL=a@b.io"), plan, &env_file())
            .await
            .unwrap();
        let output = written_and_listed(redacted.output).await.0.unwrap();
        assert_eq!(output.concat(), b"ADMIN_EMAIL=[REDACTED]");

        for data in not_text_files() {
            let plan = stream_redacter
                .create_redact_plan(&redacters, &env_file())
                .await
                .unwrap();
            let result = stream_redacter
                .redact_stream(in_probe_chunks(&data), plan, &env_file())
                .await
                .unwrap();
            assert_eq!(result.number_of_redactions, 0, "{data:?}");
            assert_eq!(result.summary.blocked, Some(RedactionBlocked::NotText));
            let output = written_and_listed(result.output).await.0.unwrap();
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

        let result = stream_redacter
            .redact_stream(
                in_probe_chunks(format!("{padding}ADMIN_EMAIL=a@b.io\n").as_bytes()),
                plan,
                &env_file(),
            )
            .await
            .unwrap();

        assert_eq!(result.summary.blocked, None);
        assert_eq!(result.number_of_redactions, 1);
        let output = written_and_listed(result.output).await.0.unwrap();
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
            .detect_stream(unreadable_after_the_head, &plan, &env_file())
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

        let result = stream_redacter
            .redact_stream(chunks, plan, &env_file())
            .await
            .unwrap();

        assert_eq!(result.summary.blocked, Some(RedactionBlocked::NotText));
        let output = written_and_listed(result.output).await.0.unwrap();
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
        assert_eq!(result.ignored.len(), 1);
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

        assert_eq!(result.detections.len(), 2);
        assert!(result.ignored.is_empty());
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

        assert!(result.detections.is_empty());
        assert_eq!(result.ignored.len(), 1);
    }
    /// The detections of `redacters` in `text` read whole, and in pieces of `layout`, in
    /// chunks of 7 bytes so that no chunk ends where a line does.
    async fn detect_whole_and_in_pieces(
        redacters: &[Redacters<'static>],
        text: &str,
        layout: PieceLayout,
    ) -> (DetectStreamResult, DetectStreamResult) {
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
        let in_pieces = StreamRedacter::detect_text(
            futures::stream::iter(chunks),
            layout,
            &references,
            None,
            &file,
        )
        .await
        .unwrap();
        (whole, in_pieces)
    }

    fn assert_same_findings(whole: DetectStreamResult, in_pieces: DetectStreamResult) {
        let by_location = |findings: Vec<(RedacterType, Detection)>| {
            let mut findings = findings;
            findings.sort_by(|(_, a), (_, b)| a.location.partial_cmp(&b.location).unwrap());
            findings
        };
        let (whole_detections, piece_detections) = (
            by_location(whole.detections),
            by_location(in_pieces.detections),
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
        assert_eq!(by_location(in_pieces.ignored), by_location(whole.ignored));
        assert_eq!(in_pieces.unlocated, whole.unlocated);
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
                assert!(whole.detections.len() > 100, "the log has values to find");
                assert!(!whole.ignored.is_empty());
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

        assert_eq!(whole.detections.len(), 2);
        assert!(whole.ignored.is_empty());
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

        assert_eq!(whole.detections.len(), 1);
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

        assert_eq!(whole.detections.len(), 1);
        assert!(
            in_pieces
                .detections
                .iter()
                .any(|found| found == &whole.detections[0]),
            "{:?}",
            in_pieces.detections
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

            assert_eq!(in_pieces.detections.len(), 1, "from line {first_line}");
            assert_same_findings(whole, in_pieces);
        }

        let too_long = ten_byte_lines(100, &[(23, "BEGIN....\n"), (56, "END.......\n")]);
        let (whole, in_pieces) = detect_whole_and_in_pieces(&rules, &too_long, cut_at_400()).await;
        assert_eq!(whole.detections.len(), 1);
        assert!(in_pieces.detections.is_empty());
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
            assert!(whole.detections.len() > 10);
            assert_same_findings(whole, in_pieces);
        }
    }

    #[tokio::test]
    async fn only_text_that_every_redacter_reads_in_pieces_is_planned_in_pieces() {
        let converters = FileConverters::new();
        let redacters = vec![email_rules().await];
        let plan = |options: RedacterBaseOptions, file: FileSystemRef| {
            let (redacters, converters) = (&redacters, &converters);
            async move {
                StreamRedacter::new(&options, converters)
                    .create_detect_plan(redacters, &file)
                    .await
                    .unwrap()
                    .piece_layout
                    .is_some()
            }
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
        assert!(!plan(as_text, unknown).await);
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
        assert_eq!(
            redaction.piece_layout,
            Some(PieceLayout::new(
                window - 2 * (2 * context + rounding),
                2 * context
            ))
        );
        assert_eq!(
            scan.piece_layout,
            Some(PieceLayout::new(window - 2 * (context + rounding), context))
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

    /// Copies `text` in chunks of 7 bytes, so that no chunk ends where a line does, read in
    /// pieces of `layout` or, for `None`, whole. The stream and the redactions are polled
    /// together, as `cp` does.
    async fn copy_text_with(
        options: &RedacterBaseOptions,
        redacters: &[Redacters<'static>],
        text: &[u8],
        layout: Option<PieceLayout>,
    ) -> AppResult<(
        AppResult<Vec<bytes::Bytes>>,
        AppResult<Vec<(RedacterType, Detection)>>,
    )> {
        let converters = FileConverters::new();
        let file = file_ref("app.log", mime::TEXT_PLAIN);
        let stream_redacter = StreamRedacter::new(options, &converters);
        let mut plan = stream_redacter.create_redact_plan(redacters, &file).await?;
        plan.piece_layout = layout;
        let chunks = text
            .chunks(7)
            .map(|chunk| Ok(bytes::Bytes::copy_from_slice(chunk)))
            .collect::<Vec<_>>();
        let result = stream_redacter
            .redact_stream(futures::stream::iter(chunks), plan, &file)
            .await?;
        Ok(written_and_listed(result.output).await)
    }

    async fn copy_text(
        options: &RedacterBaseOptions,
        redacters: &[Redacters<'static>],
        text: &[u8],
        layout: Option<PieceLayout>,
    ) -> Copied {
        let (stream, redactions) = copy_text_with(options, redacters, text, layout)
            .await
            .unwrap();
        let chunks = stream.unwrap();
        Copied {
            chunks: chunks.len(),
            output: chunks.concat(),
            redactions: redactions.unwrap(),
        }
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
}
