use crate::args::RedacterType;
use crate::common_types::TextImageCoords;
use crate::errors::AppError;
use crate::file_converters::ocr::Ocr;
use crate::file_converters::pdf::{PdfInfo, PdfPageInfo, PdfToImage};
use crate::file_converters::FileConverters;
use crate::file_systems::FileSystemRef;
use crate::redacters::{
    redact_rgba_image_at_coords, words_to_redact, RedactSupport, Redacter, RedacterBaseOptions,
    RedacterDataItem, RedacterDataItemContent, Redacters,
};
use crate::AppResult;
use futures::{Stream, TryStreamExt};
use image::ImageFormat;
use rvstruct::ValueStruct;

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

pub struct RedactStreamResult {
    /// How many redaction steps ran. This still counts a step that turned out to be a
    /// no-op (see `summary`), because it decides whether the file is uploaded.
    pub number_of_redactions: usize,
    pub summary: RedactionSummary,
    pub stream: Box<dyn Stream<Item = AppResult<bytes::Bytes>> + Send + Sync + Unpin + 'static>,
}

/// The result of running one OCR conversion step.
enum OcrStepResult {
    Redacted,
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
    pub supported_redacters: Vec<&'a Redacters<'a>>,
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
        redacters: &'a Vec<Redacters<'a>>,
        file_ref: &FileSystemRef,
    ) -> AppResult<StreamRedactPlan<'a>> {
        let mut stream_redact_plan = StreamRedactPlan {
            apply_pdf_image_converter: false,
            apply_ocr: false,
            leave_data_table_as_text: false,
            supported_redacters: vec![],
            blocked: None,
        };
        // Supports natively
        for redacter in redacters {
            let supported_options = redacter.redact_support(file_ref).await?;
            if supported_options == RedactSupport::Supported {
                stream_redact_plan.supported_redacters.push(redacter);
            }
        }

        if stream_redact_plan.supported_redacters.is_empty() {
            if let Some(file_ref_media) = &file_ref.media_type {
                // Supports with conversion
                if Redacters::is_mime_table(file_ref_media) {
                    for redacter in redacters {
                        let supported_options = redacter
                            .redact_support(&FileSystemRef {
                                media_type: Some(mime::TEXT_PLAIN),
                                ..file_ref.clone()
                            })
                            .await?;
                        if supported_options == RedactSupport::Supported {
                            stream_redact_plan.supported_redacters.push(redacter);
                        }
                    }
                    if !stream_redact_plan.supported_redacters.is_empty() {
                        stream_redact_plan.leave_data_table_as_text = true;
                    }
                } else if Redacters::is_mime_pdf(file_ref_media) {
                    if self.file_converters.pdf_image_converter.is_some() {
                        for redacter in redacters {
                            let supported_options = redacter
                                .redact_support(&FileSystemRef {
                                    media_type: Some(mime::IMAGE_PNG),
                                    ..file_ref.clone()
                                })
                                .await?;
                            if supported_options == RedactSupport::Supported {
                                stream_redact_plan.supported_redacters.push(redacter);
                            }
                        }

                        if !stream_redact_plan.supported_redacters.is_empty() {
                            stream_redact_plan.apply_pdf_image_converter = true;
                        } else if self.file_converters.ocr.is_some() {
                            for redacter in redacters {
                                let supported_options = redacter
                                    .redact_support(&FileSystemRef {
                                        media_type: Some(mime::TEXT_PLAIN),
                                        ..file_ref.clone()
                                    })
                                    .await?;
                                if supported_options == RedactSupport::Supported {
                                    stream_redact_plan.supported_redacters.push(redacter);
                                }
                            }
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
                } else if Redacters::is_mime_image(file_ref_media) {
                    if self.file_converters.ocr.is_some() {
                        for redacter in redacters {
                            let supported_options = redacter
                                .redact_support(&FileSystemRef {
                                    media_type: Some(mime::TEXT_PLAIN),
                                    ..file_ref.clone()
                                })
                                .await?;
                            if supported_options == RedactSupport::Supported {
                                stream_redact_plan.supported_redacters.push(redacter);
                            }
                        }
                        if !stream_redact_plan.supported_redacters.is_empty() {
                            stream_redact_plan.apply_ocr = true;
                        }
                    } else {
                        stream_redact_plan.blocked = Some(RedactionBlocked::OcrUnavailable);
                    }
                }
            }
        }

        Ok(stream_redact_plan)
    }

    pub async fn redact_stream<
        S: Stream<Item = AppResult<bytes::Bytes>> + Send + Unpin + Sync + 'static,
    >(
        &'a self,
        input: S,
        redact_plan: StreamRedactPlan<'a>,
        file_ref: &FileSystemRef,
    ) -> AppResult<RedactStreamResult> {
        let mut redacted = self
            .stream_to_redact_item(self.redacter_base_options, input, file_ref, &redact_plan)
            .await?;
        let mut number_of_redactions = 0;
        let mut summary = RedactionSummary::default();

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
                            .redact_with_ocr_converter(file_ref, redacted, redacters, ocr.as_ref())
                            .await?;
                        redacted = item;
                        match step {
                            OcrStepResult::Redacted => {
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
                    redacted = redacter.redact(redacted).await?;
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
            stream: output_stream,
        })
    }

    async fn stream_to_redact_item<
        S: Stream<Item = AppResult<bytes::Bytes>> + Send + Unpin + Sync + 'static,
    >(
        &'a self,
        redacter_base_options: &RedacterBaseOptions,
        input: S,
        file_ref: &FileSystemRef,
        redact_plan: &StreamRedactPlan<'a>,
    ) -> AppResult<RedacterDataItem> {
        match file_ref.media_type {
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
        }
    }

    async fn stream_to_text_redact_item<
        S: Stream<Item = AppResult<bytes::Bytes>> + Send + Unpin + Sync + 'static,
    >(
        &'a self,
        input: S,
        file_ref: &FileSystemRef,
    ) -> AppResult<RedacterDataItem> {
        let all_chunks: Vec<bytes::Bytes> = input.try_collect().await?;
        let all_bytes = all_chunks.concat();
        let whole_content = String::from_utf8(all_bytes).map_err(|e| AppError::SystemError {
            message: format!("Failed to convert bytes to string: {e}"),
        })?;
        let content = if let Some(sampling_size) = self.redacter_base_options.sampling_size {
            let sampling_size = std::cmp::min(sampling_size, whole_content.len());
            whole_content
                .chars()
                .take(sampling_size)
                .collect::<String>()
        } else {
            whole_content
        };
        Ok(RedacterDataItem {
            content: RedacterDataItemContent::Value(content),
            file_ref: file_ref.clone(),
        })
    }

    async fn stream_to_table_redact_item<
        S: Stream<Item = AppResult<bytes::Bytes>> + Send + Unpin + Sync + 'static,
    >(
        &'a self,
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
                for page in pdf_info.pages {
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
                        self.redact_with_ocr_converter(
                            file_ref,
                            image_to_redact,
                            redacters,
                            ocr_engine,
                        )
                        .await?
                        .0
                    } else {
                        let mut item = image_to_redact;
                        for redacter in redacters {
                            item = redacter.redact(item).await?;
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
    /// Words are joined by line (see [`TextImageCoords::line`]) rather than all with a single
    /// space, so keyword-window rules see the same line breaks they would see reading the
    /// file as text. Which words changed is decided by aligning the original OCR words against
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
                        let (original_words, text) = words_by_line(&text_coords);

                        let mut redacted_item = RedacterDataItem {
                            content: RedacterDataItemContent::Value(text),
                            file_ref: file_ref.clone(),
                        };
                        for redacter in redacters {
                            redacted_item = redacter.redact(redacted_item).await?;
                        }

                        match redacted_item.content {
                            RedacterDataItemContent::Value(content) => {
                                let to_redact = words_to_redact(&original_words, &content);
                                let coords_with_text: Vec<&TextImageCoords> = text_coords
                                    .iter()
                                    .filter(|coord| coord.text.is_some())
                                    .collect();
                                let mut redacted_image = image.to_rgb8();
                                for (coord, redact) in coords_with_text.iter().zip(&to_redact) {
                                    if *redact {
                                        redact_rgba_image_at_coords(
                                            &mut redacted_image,
                                            &vec![(*coord).clone()],
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
                                    OcrStepResult::Redacted,
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
            _ => Ok((redacted, OcrStepResult::Redacted)),
        }
    }
}

/// Builds the text sent to the redacter chain from OCR words, and the parallel list of
/// original words the redacted text is later aligned against.
///
/// Words are joined by `" "` within a line and lines are joined by `"\n"` (using each word's
/// [`TextImageCoords::line`]), so keyword-window rules see the same line breaks they would
/// see reading the file as text; [`words_to_redact`] tokenises on any whitespace, so the
/// newlines cost nothing there. Coordinates without recognised text are skipped, and the
/// returned word list is in the same order as the coordinates that carry text, so the two can
/// be zipped back together after redaction.
fn words_by_line(text_coords: &[TextImageCoords]) -> (Vec<String>, String) {
    let mut original_words = Vec::with_capacity(text_coords.len());
    let mut text = String::new();
    let mut current_line: Option<usize> = None;

    for coord in text_coords {
        let Some(word) = &coord.text else {
            continue;
        };
        match current_line {
            Some(line) if line == coord.line => text.push(' '),
            Some(_) => text.push('\n'),
            None => {}
        }
        text.push_str(word);
        original_words.push(word.clone());
        current_line = Some(coord.line);
    }

    (original_words, text)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::redacters::{
        parse_inline_rule, LocalRulesRedacter, LocalRulesRedacterOptions, RuleGroup, UserRule,
    };
    use crate::reporter::AppReporter;

    fn coord(text: &str, line: usize) -> TextImageCoords {
        TextImageCoords {
            x1: 0.0,
            y1: 0.0,
            x2: 1.0,
            y2: 1.0,
            text: Some(text.to_string()),
            line,
        }
    }

    #[test]
    fn words_on_the_same_line_are_joined_by_a_space() {
        let coords = vec![coord("hello", 0), coord("world", 0)];
        let (words, text) = words_by_line(&coords);
        assert_eq!(words, vec!["hello".to_string(), "world".to_string()]);
        assert_eq!(text, "hello world");
    }

    #[test]
    fn a_new_line_number_starts_a_new_line_in_the_text() {
        let coords = vec![coord("first", 0), coord("second", 1), coord("third", 1)];
        let (words, text) = words_by_line(&coords);
        assert_eq!(
            words,
            vec![
                "first".to_string(),
                "second".to_string(),
                "third".to_string()
            ]
        );
        assert_eq!(text, "first\nsecond third");
    }

    #[test]
    fn coordinates_without_text_are_skipped() {
        let mut untexted = coord("ignored", 0);
        untexted.text = None;
        let coords = vec![coord("kept", 0), untexted];
        let (words, text) = words_by_line(&coords);
        assert_eq!(words, vec!["kept".to_string()]);
        assert_eq!(text, "kept");
    }

    fn test_reporter(term: &console::Term) -> AppReporter<'_> {
        AppReporter::from(term)
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
            csv_headers_disable: false,
            csv_delimiter: None,
            sampling_size: None,
            limit_dlp_requests: None,
        }
    }

    async fn local_rules(
        groups: std::collections::BTreeSet<RuleGroup>,
        user_rules: Vec<UserRule>,
    ) -> LocalRulesRedacter<'static> {
        // Leaked so the `AppReporter` (and the `Term` it borrows) outlive the redacter:
        // fine in a short-lived test process, and simpler than threading lifetimes through
        // every test that needs one.
        let term: &'static console::Term = Box::leak(Box::new(console::Term::stdout()));
        let reporter: &'static AppReporter<'static> = Box::leak(Box::new(test_reporter(term)));
        LocalRulesRedacter::new(LocalRulesRedacterOptions { groups, user_rules }, reporter)
            .await
            .expect("valid rule set")
    }

    /// A redacter that supports text/csv (so it can join the OCR/PDF-image plan branches)
    /// but never actually matches anything: enough for tests that only check plan shape.
    async fn any_local_rules() -> LocalRulesRedacter<'static> {
        local_rules(RuleGroup::all(), vec![]).await
    }

    /// A `PdfToImage` that is never called: only its presence (`Some`/`None` in
    /// `FileConverters`) matters to `create_redact_plan`'s blocked-reason logic.
    struct UnusedPdfToImage;
    impl crate::file_converters::pdf::PdfToImage for UnusedPdfToImage {
        fn convert_to_images(&self, _pdf_bytes: bytes::Bytes) -> AppResult<PdfInfo> {
            unreachable!("not called by create_redact_plan")
        }
        fn images_to_pdf(&self, _pdf_info: PdfInfo) -> AppResult<bytes::Bytes> {
            unreachable!("not called by create_redact_plan")
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
        converters.pdf_image_converter = Some(Box::new(UnusedPdfToImage));
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
        converters.pdf_image_converter = Some(Box::new(UnusedPdfToImage));
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
            .redact_with_ocr_converter(&file_ref, redacted_item, &redacter_refs, &ocr)
            .await
            .expect("redacts");
        assert!(matches!(step, OcrStepResult::Redacted));

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
}
