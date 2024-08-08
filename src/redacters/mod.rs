use crate::AppResult;
use gcloud_sdk::prost::bytes;
use mime::Mime;
use std::fmt::Display;

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
