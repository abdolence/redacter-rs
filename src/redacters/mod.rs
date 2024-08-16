use crate::file_systems::FileSystemRef;
use crate::reporter::AppReporter;
use crate::AppResult;
use gcloud_sdk::prost::bytes;
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
pub use open_ai_llm::*;

mod simple_image_redacter;
pub use simple_image_redacter::*;
mod stream_redacter;
pub use stream_redacter::*;

use crate::args::RedacterType;

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
    Pdf {
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
                RedacterProviderOptions::AwsComprehend(_) => "aws-comprehend".to_string(),
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
                || mime.subtype() == "yaml"
                || mime.subtype() == "markdown"
                || mime.subtype().as_str().starts_with("x-")))
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

    pub fn is_mime_pdf(mime: &Mime) -> bool {
        *mime == mime::APPLICATION_PDF
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RedactSupportedOptions {
    Supported,
    SupportedAsText,
    SupportedAsImages,
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
