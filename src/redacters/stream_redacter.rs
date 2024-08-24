use crate::errors::AppError;
use crate::file_converters::ocr::Ocr;
use crate::file_converters::pdf::{PdfInfo, PdfPageInfo, PdfToImage};
use crate::file_converters::FileConverters;
use crate::file_systems::FileSystemRef;
use crate::redacters::{
    redact_rgba_image_at_coords, RedactSupport, Redacter, RedacterBaseOptions, RedacterDataItem,
    RedacterDataItemContent, Redacters,
};
use crate::AppResult;
use futures::{Stream, TryStreamExt};
use image::ImageFormat;
use indicatif::ProgressBar;
use std::collections::HashSet;

pub struct RedactStreamResult {
    pub number_of_redactions: usize,
    pub stream: Box<dyn Stream<Item = AppResult<bytes::Bytes>> + Send + Sync + Unpin + 'static>,
}

pub struct StreamRedacter<'a> {
    redacter_base_options: &'a RedacterBaseOptions,
    file_converters: &'a FileConverters<'a>,
    bar: &'a ProgressBar,
}

pub struct StreamRedactPlan {
    pub apply_pdf_image_converter: bool,
    pub apply_ocr: bool,
    pub leave_data_table_as_text: bool,
}

impl<'a> StreamRedacter<'a> {
    pub fn new(
        redacter_base_options: &'a RedacterBaseOptions,
        file_converters: &'a FileConverters<'a>,
        bar: &'a ProgressBar,
    ) -> Self {
        Self {
            redacter_base_options,
            file_converters,
            bar,
        }
    }

    pub async fn create_redact_plan(
        &'a self,
        redacters: &'a Vec<impl Redacter>,
        file_ref: &FileSystemRef,
    ) -> AppResult<(StreamRedactPlan, Vec<&'a impl Redacter>)> {
        let mut stream_redact_plan = StreamRedactPlan {
            apply_pdf_image_converter: false,
            apply_ocr: false,
            leave_data_table_as_text: false,
        };
        // Supports natively
        let mut supported_redacters = Vec::new();
        for redacter in redacters {
            let supported_options = redacter.redact_support(file_ref).await?;
            if supported_options == RedactSupport::Supported {
                supported_redacters.push(redacter);
            }
        }

        if supported_redacters.is_empty() {
            match &file_ref.media_type {
                Some(file_ref_media) => {
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
                                supported_redacters.push(redacter);
                            }
                        }
                        if !supported_redacters.is_empty() {
                            stream_redact_plan.leave_data_table_as_text = true;
                        }
                    } else if self.file_converters.pdf_image_converter.is_some()
                        && Redacters::is_mime_pdf(file_ref_media)
                    {
                        for redacter in redacters {
                            let supported_options = redacter
                                .redact_support(&FileSystemRef {
                                    media_type: Some(mime::IMAGE_PNG),
                                    ..file_ref.clone()
                                })
                                .await?;
                            if supported_options == RedactSupport::Supported {
                                supported_redacters.push(redacter);
                            }
                        }

                        if !supported_redacters.is_empty() {
                            stream_redact_plan.apply_pdf_image_converter = true;
                        }

                        if supported_redacters.is_empty() && self.file_converters.ocr.is_some() {
                            for redacter in redacters {
                                let supported_options = redacter
                                    .redact_support(&FileSystemRef {
                                        media_type: Some(mime::TEXT_PLAIN),
                                        ..file_ref.clone()
                                    })
                                    .await?;
                                if supported_options == RedactSupport::Supported {
                                    supported_redacters.push(redacter);
                                }
                            }
                            if !supported_redacters.is_empty() {
                                stream_redact_plan.apply_pdf_image_converter = true;
                                stream_redact_plan.apply_ocr = true;
                            }
                        }
                    } else if self.file_converters.ocr.is_some()
                        && Redacters::is_mime_image(file_ref_media)
                    {
                        for redacter in redacters {
                            let supported_options = redacter
                                .redact_support(&FileSystemRef {
                                    media_type: Some(mime::TEXT_PLAIN),
                                    ..file_ref.clone()
                                })
                                .await?;
                            if supported_options == RedactSupport::Supported {
                                supported_redacters.push(redacter);
                            }
                        }
                        if !supported_redacters.is_empty() {
                            stream_redact_plan.apply_ocr = true;
                        }
                    }
                }
                None => {}
            }
        }

        Ok((stream_redact_plan, supported_redacters))
    }

    pub async fn redact_stream<
        S: Stream<Item = AppResult<bytes::Bytes>> + Send + Unpin + Sync + 'static,
    >(
        &'a self,
        input: S,
        redact_plan: StreamRedactPlan,
        redacters: &[&'a impl Redacter],
        file_ref: &FileSystemRef,
    ) -> AppResult<RedactStreamResult> {
        let mut redacted = self
            .stream_to_redact_item(self.redacter_base_options, input, file_ref, &redact_plan)
            .await?;
        let mut number_of_redactions = 0;

        for (index, redacter) in redacters.iter().enumerate() {
            let width = " ".repeat(index);
            if redact_plan.apply_pdf_image_converter {
                match (
                    &self.file_converters.pdf_image_converter,
                    &self.file_converters.ocr,
                ) {
                    (Some(ref pdf_to_image), _) if !redact_plan.apply_ocr => {
                        redacted = self
                            .redact_pdf_with_images_converter(
                                file_ref,
                                redacted,
                                *redacter,
                                &width,
                                pdf_to_image.as_ref(),
                                None,
                            )
                            .await?;
                        number_of_redactions += 1;
                    }
                    (Some(ref pdf_to_image), Some(ref ocr)) => {
                        redacted = self
                            .redact_pdf_with_images_converter(
                                file_ref,
                                redacted,
                                *redacter,
                                &width,
                                pdf_to_image.as_ref(),
                                Some(ocr.as_ref()),
                            )
                            .await?;
                        number_of_redactions += 1;
                    }
                    (None, Some(_)) => {
                        self.bar.println(format!(
                            "{width}↲ Skipping redaction because PDF to image converter is not available",
                        ));
                    }
                    (Some(_), None) => {
                        self.bar.println(format!(
                            "{width}↲ Skipping redaction because OCR is not available",
                        ));
                    }
                    (None, None) => {
                        self.bar.println(format!(
                            "{width}↲ Skipping redaction because PDF/OCR are not available",
                        ));
                    }
                }
            } else if redact_plan.apply_ocr {
                match self.file_converters.ocr {
                    Some(ref ocr) => {
                        redacted = self
                            .redact_with_ocr_converter(
                                file_ref,
                                redacted,
                                *redacter,
                                &width,
                                ocr.as_ref(),
                            )
                            .await?;
                        number_of_redactions += 1;
                    }
                    None => {
                        self.bar.println(format!(
                            "{width}↲ Skipping redaction because OCR is not available",
                        ));
                    }
                }
            } else {
                self.bar.println(format!(
                    "{width}↳ Redacting using {} redacter",
                    redacter.redacter_type()
                ));
                redacted = redacter.redact(redacted).await?;
                number_of_redactions += 1;
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
        redact_plan: &StreamRedactPlan,
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
                message: format!("Media type {} is not supported for redaction", mime),
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
            message: format!("Failed to convert bytes to string: {}", e),
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
        let reader = tokio_util::io::StreamReader::new(
            input.map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err)),
        );
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
        redacter: &impl Redacter,
        width: &String,
        converter: &dyn PdfToImage,
        ocr: Option<&dyn Ocr>,
    ) -> Result<RedacterDataItem, AppError> {
        match redacted.content {
            RedacterDataItemContent::Pdf { data } => {
                self.bar.println(format!(
                    "{width}↳ Redacting using {} redacter and converting the PDF to images",
                    redacter.redacter_type()
                ));
                let pdf_info = converter.convert_to_images(data)?;
                self.bar.println(format!(
                    "{width} ↳ Converting {pdf_info_pages} images",
                    pdf_info_pages = pdf_info.pages.len()
                ));
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
                            redacter,
                            &format!("  {}", width),
                            ocr_engine,
                        )
                        .await?
                    } else {
                        redacter.redact(image_to_redact).await?
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
                Ok(RedacterDataItem {
                    content: RedacterDataItemContent::Pdf {
                        data: redact_pdf_as_images,
                    },
                    file_ref: file_ref.clone(),
                })
            }
            _ => Ok(redacted),
        }
    }

    async fn redact_with_ocr_converter(
        &'a self,
        file_ref: &FileSystemRef,
        redacted: RedacterDataItem,
        redacter: &impl Redacter,
        width: &String,
        ocr: &dyn Ocr,
    ) -> Result<RedacterDataItem, AppError> {
        match &redacted.content {
            RedacterDataItemContent::Image { data, mime_type } => {
                match ImageFormat::from_mime_type(mime_type) {
                    Some(image_format) => {
                        self.bar.println(format!(
                            "{width}↳ Redacting using {} redacter and converting the image to text using OCR engine",
                            redacter.redacter_type()
                        ));
                        let image = image::load_from_memory_with_format(data, image_format)?;
                        let text_coords = ocr.image_to_text(image.clone())?;
                        let text = text_coords
                            .iter()
                            .map(|coord| coord.text.clone())
                            .collect::<Vec<Option<String>>>()
                            .into_iter()
                            .flatten()
                            .collect::<Vec<String>>()
                            .join(" ");

                        let redacted_text = redacter
                            .redact(RedacterDataItem {
                                content: RedacterDataItemContent::Value(text),
                                file_ref: file_ref.clone(),
                            })
                            .await?;

                        match redacted_text.content {
                            RedacterDataItemContent::Value(content) => {
                                let words_set: HashSet<&str> =
                                    HashSet::from_iter(content.split(" ").collect::<Vec<_>>());
                                let mut redacted_image = image.to_rgb8();
                                for text_coord in text_coords {
                                    if let Some(text) = &text_coord.text {
                                        if !words_set.contains(text.as_str()) {
                                            redact_rgba_image_at_coords(
                                                &mut redacted_image,
                                                &vec![text_coord],
                                                0.10,
                                            );
                                        }
                                    }
                                }
                                let mut output = std::io::Cursor::new(Vec::new());
                                redacted_image.write_to(&mut output, image_format)?;
                                Ok(RedacterDataItem {
                                    file_ref: file_ref.clone(),
                                    content: RedacterDataItemContent::Image {
                                        mime_type: mime_type.clone(),
                                        data: output.into_inner().into(),
                                    },
                                })
                            }
                            _ => Err(AppError::SystemError {
                                message: "Redacted text is not returned as text".to_string(),
                            }),
                        }
                    }
                    None => {
                        self.bar.println(format!(
                            "{width}↲ Skipping redaction through OCR because image format is not supported",
                        ));
                        Ok(redacted)
                    }
                }
            }
            _ => Ok(redacted),
        }
    }
}
