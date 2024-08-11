use crate::errors::AppError;
use crate::filesystems::FileSystemRef;
use crate::reporter::AppReporter;
use crate::AppResult;
use futures::{Stream, TryStreamExt};
use gcloud_sdk::prost::bytes;
use indicatif::ProgressBar;
use mime::Mime;
use std::fmt::Display;

mod gcp_dlp;
pub use gcp_dlp::*;

mod aws_comprehend;
pub use aws_comprehend::*;

mod ms_presidio;
pub use ms_presidio::*;

mod gemini_llm;
pub use gemini_llm::*;

mod open_ai_llm;
use crate::args::RedacterType;
pub use open_ai_llm::*;

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
    AwsComprehend(AwsComprehendRedacter<'a>),
    MsPresidio(MsPresidioRedacter<'a>),
    GeminiLlm(GeminiLlmRedacter<'a>),
    OpenAiLlm(OpenAiLlmRedacter<'a>),
}

#[derive(Debug, Clone)]
pub struct RedacterOptions {
    pub provider_options: Vec<RedacterProviderOptions>,
    pub base_options: RedacterBaseOptions,
}

#[derive(Debug, Clone)]
pub struct RedacterBaseOptions {
    pub allow_unsupported_copies: bool,
    pub csv_headers_disable: bool,
    pub csv_delimiter: Option<u8>,
    pub sampling_size: Option<usize>,
}

#[derive(Debug, Clone)]
pub enum RedacterProviderOptions {
    GcpDlp(GcpDlpRedacterOptions),
    AwsComprehend(AwsComprehendRedacterOptions),
    MsPresidio(MsPresidioRedacterOptions),
    GeminiLlm(GeminiLlmRedacterOptions),
    OpenAiLlm(OpenAiLlmRedacterOptions),
}

impl Display for RedacterOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let to_display = self
            .provider_options
            .iter()
            .map(|o| match o {
                RedacterProviderOptions::GcpDlp(_) => "gcp-dlp".to_string(),
                RedacterProviderOptions::AwsComprehend(_) => "aws-comprehend-dlp".to_string(),
                RedacterProviderOptions::MsPresidio(_) => "ms-presidio".to_string(),
                RedacterProviderOptions::GeminiLlm(_) => "gemini-llm".to_string(),
                RedacterProviderOptions::OpenAiLlm(_) => "openai-llm".to_string(),
            })
            .collect::<Vec<String>>()
            .join(", ");
        write!(f, "{}", to_display)
    }
}

impl<'a> Redacters<'a> {
    pub async fn new_redacter(
        provider_options: RedacterProviderOptions,
        reporter: &'a AppReporter<'a>,
    ) -> AppResult<Self> {
        match provider_options {
            RedacterProviderOptions::GcpDlp(options) => Ok(Redacters::GcpDlp(
                GcpDlpRedacter::new(options, reporter).await?,
            )),
            RedacterProviderOptions::AwsComprehend(options) => Ok(Redacters::AwsComprehend(
                AwsComprehendRedacter::new(options, reporter).await?,
            )),
            RedacterProviderOptions::MsPresidio(options) => Ok(Redacters::MsPresidio(
                MsPresidioRedacter::new(options, reporter).await?,
            )),
            RedacterProviderOptions::GeminiLlm(options) => Ok(Redacters::GeminiLlm(
                GeminiLlmRedacter::new(options, reporter).await?,
            )),
            RedacterProviderOptions::OpenAiLlm(options) => Ok(Redacters::OpenAiLlm(
                OpenAiLlmRedacter::new(options, reporter).await?,
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
    async fn redact(&self, input: RedacterDataItem) -> AppResult<RedacterDataItem>;

    async fn redact_supported_options(
        &self,
        file_ref: &FileSystemRef,
    ) -> AppResult<RedactSupportedOptions>;

    fn redacter_type(&self) -> RedacterType;
}

impl<'a> Redacter for Redacters<'a> {
    async fn redact(&self, input: RedacterDataItem) -> AppResult<RedacterDataItem> {
        match self {
            Redacters::GcpDlp(redacter) => redacter.redact(input).await,
            Redacters::AwsComprehend(redacter) => redacter.redact(input).await,
            Redacters::MsPresidio(redacter) => redacter.redact(input).await,
            Redacters::GeminiLlm(redacter) => redacter.redact(input).await,
            Redacters::OpenAiLlm(redacter) => redacter.redact(input).await,
        }
    }

    async fn redact_supported_options(
        &self,
        file_ref: &FileSystemRef,
    ) -> AppResult<RedactSupportedOptions> {
        match self {
            Redacters::GcpDlp(redacter) => redacter.redact_supported_options(file_ref).await,
            Redacters::AwsComprehend(redacter) => redacter.redact_supported_options(file_ref).await,
            Redacters::MsPresidio(redacter) => redacter.redact_supported_options(file_ref).await,
            Redacters::GeminiLlm(redacter) => redacter.redact_supported_options(file_ref).await,
            Redacters::OpenAiLlm(redacter) => redacter.redact_supported_options(file_ref).await,
        }
    }

    fn redacter_type(&self) -> RedacterType {
        match self {
            Redacters::GcpDlp(_) => RedacterType::GcpDlp,
            Redacters::AwsComprehend(_) => RedacterType::AwsComprehend,
            Redacters::MsPresidio(_) => RedacterType::MsPresidio,
            Redacters::GeminiLlm(_) => RedacterType::GeminiLlm,
            Redacters::OpenAiLlm(_) => RedacterType::OpenAiLlm,
        }
    }
}

pub async fn redact_stream<
    S: Stream<Item = AppResult<bytes::Bytes>> + Send + Unpin + Sync + 'static,
>(
    redacters: &Vec<&impl Redacter>,
    redacter_base_options: &RedacterBaseOptions,
    input: S,
    file_ref: &FileSystemRef,
    bar: &ProgressBar,
) -> AppResult<Box<dyn Stream<Item = AppResult<bytes::Bytes>> + Send + Sync + Unpin + 'static>> {
    let mut redacters_supported_options = Vec::with_capacity(redacters.len());
    for redacter in redacters {
        let supported_options = redacter.redact_supported_options(file_ref).await?;
        redacters_supported_options.push((redacter, supported_options));
    }

    let mut item_to_redact = match file_ref.media_type {
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
            let all_chunks: Vec<bytes::Bytes> = input.try_collect().await?;
            let all_bytes = all_chunks.concat();
            let whole_content =
                String::from_utf8(all_bytes).map_err(|e| AppError::SystemError {
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
        Some(ref mime) => Err(AppError::SystemError {
            message: format!("Media type {} is not supported for redaction", mime),
        }),
        None => Err(AppError::SystemError {
            message: "Media type is not provided to redact".to_string(),
        }),
    }?;

    for (redacter, options) in redacters_supported_options {
        if !matches!(options, RedactSupportedOptions::Unsupported) {
            bar.println(format!(
                "↳ Redacting using {} redacter",
                redacter.redacter_type()
            ));
            item_to_redact = redacter.redact(item_to_redact).await?;
        }
    }

    match item_to_redact.content {
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
