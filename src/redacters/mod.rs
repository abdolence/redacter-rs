use crate::AppResult;
use csv_async::StringRecord;
use futures::{Stream, TryStreamExt};
use gcloud_sdk::prost::bytes;
use mime::Mime;
use std::fmt::Display;

use crate::errors::AppError;
use crate::filesystems::FileSystemRef;
use crate::reporter::AppReporter;

mod gcp_dlp;
pub use gcp_dlp::*;

mod aws_comprehend;
pub use aws_comprehend::*;

mod ms_presidio;
pub use ms_presidio::*;

mod gemini_llm;
pub use gemini_llm::*;

#[derive(Debug, Clone)]
pub struct RedacterDataItem {
    pub content: RedacterDataItemContent,
    pub file_ref: FileSystemRef,
}

#[derive(Debug, Clone)]
pub enum RedacterDataItemContent {
    Value(String),
    Table {
        headers: Vec<String>,
        rows: Vec<Vec<String>>,
    },
    Image {
        mime_type: Mime,
        data: bytes::Bytes,
    },
}

#[derive(Clone)]
pub enum Redacters<'a> {
    GcpDlp(GcpDlpRedacter<'a>),
    AwsComprehendDlp(AwsComprehendRedacter<'a>),
    MsPresidio(MsPresidioRedacter<'a>),
    GeminiLlm(GeminiLlmRedacter<'a>),
}

#[derive(Debug, Clone)]
pub struct RedacterOptions {
    pub provider_options: RedacterProviderOptions,
    pub allow_unsupported_copies: bool,
    pub csv_headers_disable: bool,
    pub csv_delimiter: Option<u8>,
}

#[derive(Debug, Clone)]
pub enum RedacterProviderOptions {
    GcpDlp(GcpDlpRedacterOptions),
    AwsComprehend(AwsComprehendRedacterOptions),
    MsPresidio(MsPresidioRedacterOptions),
    GeminiLlm(GeminiLlmRedacterOptions),
}

impl Display for RedacterOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.provider_options {
            RedacterProviderOptions::GcpDlp(_) => write!(f, "gcp-dlp"),
            RedacterProviderOptions::AwsComprehend(_) => write!(f, "aws-comprehend-dlp"),
            RedacterProviderOptions::MsPresidio(_) => write!(f, "ms-presidio"),
            RedacterProviderOptions::GeminiLlm(_) => write!(f, "gemini-llm"),
        }
    }
}

impl<'a> Redacters<'a> {
    pub async fn new_redacter(
        redacter_options: &RedacterOptions,
        reporter: &'a AppReporter<'a>,
    ) -> AppResult<Self> {
        match redacter_options.provider_options {
            RedacterProviderOptions::GcpDlp(ref options) => Ok(Redacters::GcpDlp(
                GcpDlpRedacter::new(redacter_options.clone(), options.clone(), reporter).await?,
            )),
            RedacterProviderOptions::AwsComprehend(ref options) => Ok(Redacters::AwsComprehendDlp(
                AwsComprehendRedacter::new(redacter_options.clone(), options.clone(), reporter)
                    .await?,
            )),
            RedacterProviderOptions::MsPresidio(ref options) => Ok(Redacters::MsPresidio(
                MsPresidioRedacter::new(redacter_options.clone(), options.clone(), reporter)
                    .await?,
            )),
            RedacterProviderOptions::GeminiLlm(ref options) => Ok(Redacters::GeminiLlm(
                GeminiLlmRedacter::new(redacter_options.clone(), options.clone(), reporter).await?,
            )),
        }
    }

    pub fn is_mime_text(mime: &Mime) -> bool {
        let mime_subtype_as_str = mime.subtype().as_str().to_lowercase();
        (mime.type_() == mime::TEXT
            && (mime.subtype() == mime::PLAIN
                || mime.subtype() == mime::HTML
                || mime.subtype() == mime::XML
                || mime.subtype() == mime::CSS
                || mime.subtype() == "x-yaml"
                || mime.subtype() == "yaml"))
            || (mime.type_() == mime::APPLICATION
                && (mime.subtype() == mime::XML
                    || mime.subtype() == mime::JSON
                    || mime_subtype_as_str == "yaml"
                    || mime_subtype_as_str == "x-yaml"))
    }

    pub fn is_mime_table(mime: &Mime) -> bool {
        mime.type_() == mime::TEXT && mime.subtype() == mime::CSV
    }

    pub fn is_mime_image(mime: &Mime) -> bool {
        mime.type_() == mime::IMAGE
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RedactSupportedOptions {
    Supported,
    SupportedAsText,
    Unsupported,
}

pub trait Redacter {
    async fn redact(&self, input: RedacterDataItem) -> AppResult<RedacterDataItemContent>;

    async fn redact_supported_options(
        &self,
        file_ref: &FileSystemRef,
    ) -> AppResult<RedactSupportedOptions>;

    fn options(&self) -> &RedacterOptions;

    async fn redact_stream<
        S: Stream<Item = AppResult<bytes::Bytes>> + Send + Unpin + Sync + 'static,
    >(
        &self,
        input: S,
        file_ref: &FileSystemRef,
    ) -> AppResult<Box<dyn Stream<Item = AppResult<bytes::Bytes>> + Send + Sync + Unpin + 'static>>
    {
        let supported_options = self.redact_supported_options(file_ref).await?;
        let content_to_redact = match file_ref.media_type {
            Some(ref mime)
                if Redacters::is_mime_text(mime)
                    || (Redacters::is_mime_table(mime)
                        && matches!(
                            supported_options,
                            RedactSupportedOptions::SupportedAsText
                        )) =>
            {
                let all_chunks: Vec<bytes::Bytes> = input.try_collect().await?;
                let all_bytes = all_chunks.concat();
                let content = String::from_utf8(all_bytes).map_err(|e| AppError::SystemError {
                    message: format!("Failed to convert bytes to string: {}", e),
                })?;
                Ok(RedacterDataItem {
                    content: RedacterDataItemContent::Value(content),
                    file_ref: file_ref.clone(),
                })
            }
            Some(ref mime) if Redacters::is_mime_image(mime) => {
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
            Some(ref mime) if Redacters::is_mime_table(mime) => {
                let reader = tokio_util::io::StreamReader::new(
                    input.map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err)),
                );
                let mut reader = csv_async::AsyncReaderBuilder::default()
                    .has_headers(!self.options().csv_headers_disable)
                    .delimiter(
                        self.options()
                            .csv_delimiter
                            .as_ref()
                            .cloned()
                            .unwrap_or(b','),
                    )
                    .create_reader(reader);
                let headers = if !self.options().csv_headers_disable {
                    reader
                        .headers()
                        .await?
                        .into_iter()
                        .map(|h| h.to_string())
                        .collect()
                } else {
                    vec![]
                };
                let records: Vec<StringRecord> = reader.records().try_collect().await?;
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
            Some(ref mime) => Err(AppError::SystemError {
                message: format!("Media type {} is not supported for redaction", mime),
            }),
            None => Err(AppError::SystemError {
                message: "Media type is not provided to redact".to_string(),
            }),
        }?;

        let content = self.redact(content_to_redact).await?;

        match content {
            RedacterDataItemContent::Value(content) => {
                let bytes = bytes::Bytes::from(content.into_bytes());
                Ok(Box::new(futures::stream::iter(vec![Ok(bytes)])))
            }
            RedacterDataItemContent::Image { data, .. } => {
                Ok(Box::new(futures::stream::iter(vec![Ok(data)])))
            }
            RedacterDataItemContent::Table { headers, rows } => {
                let mut writer = csv_async::AsyncWriter::from_writer(vec![]);
                writer.write_record(headers).await?;
                for row in rows {
                    writer.write_record(row).await?;
                }
                writer.flush().await?;
                let bytes = bytes::Bytes::from(writer.into_inner().await?);
                Ok(Box::new(futures::stream::iter(vec![Ok(bytes)])))
            }
        }
    }
}

impl<'a> Redacter for Redacters<'a> {
    async fn redact(&self, input: RedacterDataItem) -> AppResult<RedacterDataItemContent> {
        match self {
            Redacters::GcpDlp(redacter) => redacter.redact(input).await,
            Redacters::AwsComprehendDlp(redacter) => redacter.redact(input).await,
            Redacters::MsPresidio(redacter) => redacter.redact(input).await,
            Redacters::GeminiLlm(redacter) => redacter.redact(input).await,
        }
    }

    async fn redact_supported_options(
        &self,
        file_ref: &FileSystemRef,
    ) -> AppResult<RedactSupportedOptions> {
        match self {
            Redacters::GcpDlp(redacter) => redacter.redact_supported_options(file_ref).await,
            Redacters::AwsComprehendDlp(redacter) => {
                redacter.redact_supported_options(file_ref).await
            }
            Redacters::MsPresidio(redacter) => redacter.redact_supported_options(file_ref).await,
            Redacters::GeminiLlm(redacter) => redacter.redact_supported_options(file_ref).await,
        }
    }

    fn options(&self) -> &RedacterOptions {
        match self {
            Redacters::GcpDlp(redacter) => redacter.options(),
            Redacters::AwsComprehendDlp(redacter) => redacter.options(),
            Redacters::MsPresidio(redacter) => redacter.options(),
            Redacters::GeminiLlm(redacter) => redacter.options(),
        }
    }
}
