use crate::errors::AppError;
use crate::file_converters::pdf::{PdfInfo, PdfPageInfo, PdfToImage};
use crate::file_converters::FileConverters;
use crate::file_systems::FileSystemRef;
use crate::redacters::{
    RedactSupportedOptions, Redacter, RedacterBaseOptions, RedacterDataItem,
    RedacterDataItemContent, Redacters,
};
use crate::AppResult;
use futures::{Stream, TryStreamExt};
use image::ImageFormat;
use indicatif::ProgressBar;

pub struct RedactStreamResult {
    pub number_of_redactions: usize,
    pub stream: Box<dyn Stream<Item = AppResult<bytes::Bytes>> + Send + Sync + Unpin + 'static>,
}

pub async fn redact_stream<
    S: Stream<Item = AppResult<bytes::Bytes>> + Send + Unpin + Sync + 'static,
>(
    redacters: &Vec<&impl Redacter>,
    redacter_base_options: &RedacterBaseOptions,
    input: S,
    file_ref: &FileSystemRef,
    file_converters: &FileConverters,
    bar: &ProgressBar,
) -> AppResult<RedactStreamResult> {
    let mut redacters_supported_options = Vec::with_capacity(redacters.len());
    for redacter in redacters {
        let supported_options = redacter.redact_supported_options(file_ref).await?;
        redacters_supported_options.push((*redacter, supported_options));
    }

    let mut redacted = stream_to_redact_item(
        redacter_base_options,
        input,
        file_ref,
        &redacters_supported_options,
    )
    .await?;
    let mut number_of_redactions = 0;

    for (index, (redacter, options)) in redacters_supported_options.iter().enumerate() {
        let width = " ".repeat(index);
        match options {
            RedactSupportedOptions::Supported => {
                bar.println(format!(
                    "{width}↳ Redacting using {} redacter",
                    redacter.redacter_type()
                ));
                redacted = redacter.redact(redacted).await?;
                number_of_redactions += 1;
            }
            RedactSupportedOptions::SupportedAsImages => {
                match file_converters.pdf_image_converter {
                    Some(ref converter) => {
                        redacted = redact_pdf_with_images_converter(
                            file_ref,
                            bar,
                            redacted,
                            *redacter,
                            &width,
                            converter.as_ref(),
                        )
                        .await?;
                        number_of_redactions += 1;
                    }
                    None => {
                        bar.println(format!(
                            "{width}↲ Skipping redaction because PDF to image converter is not available",
                        ));
                    }
                }
            }
            RedactSupportedOptions::SupportedAsText => {
                if matches!(redacted.content, RedacterDataItemContent::Value(_)) {
                    bar.println(format!(
                        "{width}↳ Redacting as text using {} redacter",
                        redacter.redacter_type()
                    ));
                    redacted = redacter.redact(redacted).await?;
                    number_of_redactions += 1;
                }
            }
            RedactSupportedOptions::Unsupported => {}
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
        RedacterDataItemContent::Pdf { data } => Box::new(futures::stream::iter(vec![Ok(data)])),
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
    redacter_base_options: &RedacterBaseOptions,
    input: S,
    file_ref: &FileSystemRef,
    redacters_supported_options: &[(&impl Redacter, RedactSupportedOptions)],
) -> AppResult<RedacterDataItem> {
    match file_ref.media_type {
        Some(ref mime)
            if Redacters::is_mime_text(mime)
                || (Redacters::is_mime_table(mime)
                    && redacters_supported_options
                        .iter()
                        .any(|(_, o)| matches!(o, RedactSupportedOptions::SupportedAsText))
                    && !redacters_supported_options
                        .iter()
                        .all(|(_, o)| matches!(o, RedactSupportedOptions::Supported))) =>
        {
            stream_to_text_redact_item(redacter_base_options, input, file_ref).await
        }
        Some(ref mime) if Redacters::is_mime_image(mime) => {
            stream_to_image_redact_item(input, file_ref, mime.clone()).await
        }
        Some(ref mime) if Redacters::is_mime_table(mime) => {
            stream_to_table_redact_item(redacter_base_options, input, file_ref).await
        }
        Some(ref mime) if Redacters::is_mime_pdf(mime) => {
            stream_to_pdf_redact_item(input, file_ref).await
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
    redacter_base_options: &RedacterBaseOptions,
    input: S,
    file_ref: &FileSystemRef,
) -> AppResult<RedacterDataItem> {
    let all_chunks: Vec<bytes::Bytes> = input.try_collect().await?;
    let all_bytes = all_chunks.concat();
    let whole_content = String::from_utf8(all_bytes).map_err(|e| AppError::SystemError {
        message: format!("Failed to convert bytes to string: {}", e),
    })?;
    let content = if let Some(sampling_size) = redacter_base_options.sampling_size {
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
    file_ref: &FileSystemRef,
    bar: &ProgressBar,
    redacted: RedacterDataItem,
    redacter: &impl Redacter,
    width: &String,
    converter: &dyn PdfToImage,
) -> Result<RedacterDataItem, AppError> {
    match redacted.content {
        RedacterDataItemContent::Pdf { data } => {
            bar.println(format!(
                "{width}↳ Redacting using {} redacter and converting the PDF to images",
                redacter.redacter_type()
            ));
            let pdf_info = converter.convert_to_images(data)?;
            bar.println(format!(
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
                let redacted_image = redacter.redact(image_to_redact).await?;
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
