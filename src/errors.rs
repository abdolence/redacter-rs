use crate::args::RedacterType;
use crate::redacters::ModelStop;
use gcloud_sdk::tonic::metadata::errors::InvalidMetadataValue;
use gcloud_sdk::tonic::Code;
use indicatif::style::TemplateError;
use reqwest::StatusCode;
use std::time::SystemTimeError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("Unknown file system is specified: {file_path}")]
    UnknownFileSystem { file_path: String },
    #[error("Unknown file system is specified: {redacter_type}")]
    UnknownRedacter { redacter_type: String },
    #[error("Input/output error")]
    InputOutputError(#[from] std::io::Error),
    #[error("Destination '{destination}' doesn't support multiple files. Trailing slash needed?")]
    DestinationDoesNotSupportMultipleFiles { destination: String },
    #[error("Google Cloud REST SDK error:\n{0}")]
    GoogleCloudRestSdkError(#[from] gcloud_sdk::error::Error),
    #[error("Google Cloud SDK error:\n{0}")]
    GoogleCloudGrpcError(#[from] gcloud_sdk::tonic::Status),
    #[error("Google Cloud invalid metadata value:\n{0}")]
    GoogleCloudInvalidMetadataValue(#[from] InvalidMetadataValue),
    #[error(
        "Google Cloud Storage object '{object}' failed the whole-object CRC32C check: expected \
         {expected:#010x}, computed {computed:#010x}"
    )]
    GoogleCloudObjectChecksumMismatch {
        object: String,
        expected: u32,
        computed: u32,
    },
    #[error(
        "Google Cloud Storage object '{object}' could not be verified: neither the read nor the \
         object metadata carries a CRC32C"
    )]
    GoogleCloudObjectChecksumMissing { object: String },
    #[error("Google Cloud Storage object '{object}' has {expected} bytes but {read} were read")]
    GoogleCloudObjectSizeMismatch {
        object: String,
        expected: i64,
        read: i64,
    },
    #[error("AWS SDK error occurred")]
    AwsSdkError(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("AWS Bedrock error: {message}")]
    AwsBedrockError { message: String },
    #[error("MIME error:\n{0}")]
    MimeError(#[from] mime::FromStrError),
    #[error("HTTP client error:\n{0}")]
    HttpClientError(#[from] reqwest::Error),
    #[error("Failed to {operation}. HTTP status: {status}.")]
    RequestRejected {
        operation: String,
        status: StatusCode,
    },
    #[error("Zip error:\n{0}")]
    ZipError(#[from] zip::result::ZipError),
    #[error("A background task failed: {0}")]
    BackgroundTaskFailed(#[from] tokio::task::JoinError),
    #[error("CSV parser error:\n{0}")]
    CsvParserError(#[from] csv_async::Error),
    #[error("Redacter config error: {message}")]
    RedacterConfigError { message: String },
    #[error("{redacter} named {count} value(s) it could not place in the text, so they cannot be redacted")]
    UnlocatedValues {
        redacter: RedacterType,
        count: usize,
    },
    #[error(transparent)]
    Categories(#[from] crate::redacters::CategoriesError),
    #[error("Template error: {0}")]
    TemplateError(#[from] TemplateError),
    #[cfg(feature = "pdf-render")]
    #[error("PDF conversion error: {0}")]
    PdfiumError(#[from] pdfium_render::prelude::PdfiumError),
    #[error("Image conversion error: {0}")]
    ImageError(#[from] image::ImageError),
    #[cfg(feature = "clipboard")]
    #[error("Clipboard error: {0}")]
    ClipboardError(#[from] arboard::Error),
    #[error("SystemTimeError: {0}")]
    SystemTimeError(#[from] SystemTimeError),
    #[error("JSON serialization error: {0}")]
    JsonSerializeError(#[from] serde_json::Error),
    #[cfg(feature = "ocr")]
    #[error("Model load error: {0}")]
    OcrModelLoadError(#[from] rten::LoadError),
    #[cfg(feature = "ocr")]
    #[error("OCR image error: {0}")]
    OcrImageError(#[from] ocrs::ImageSourceError),
    #[error("Addon store error: {0}")]
    AddonStoreError(#[from] crate::addon_store::AddonStoreError),
    #[error("{failed} of {requested} addons are not installed")]
    AddonsNotInstalled { failed: usize, requested: usize },
    #[cfg(feature = "local-ner")]
    #[error("Local NER error: {0}")]
    LocalNerError(#[from] crate::redacters::local_ner::LocalNerError),
    #[cfg(feature = "local-gliner")]
    #[error("Local GLiNER error: {0}")]
    LocalGlinerError(#[from] crate::redacters::local_gliner::LocalGlinerError),
    #[cfg(feature = "local-credential-ml")]
    #[error("Local credential ML error: {0}")]
    LocalCredentialMlError(#[from] crate::redacters::local_credential_ml::LocalCredentialMlError),
    /// Carries the position only: a serde error names the value it could not read, and the
    /// answer is made of the scanned content.
    #[error("A model's answer is not the expected JSON (line {line}, column {column})")]
    UnreadableModelAnswer { line: usize, column: usize },
    /// Names the provider's finish reason and blocked categories, never the content.
    #[error("The model did not complete its answer: {0}")]
    ModelAnswerStopped(ModelStop),
    #[error("The model's answer is empty")]
    EmptyModelAnswer,
    #[error("{redacter_type} returned a result without {missing}")]
    IncompleteResult {
        redacter_type: RedacterType,
        missing: &'static str,
    },
    #[error("The text is not valid UTF-8 at byte {offset}")]
    TextNotUtf8 { offset: usize },
    #[error("The text holds a NUL byte at byte {offset}")]
    TextHasNul { offset: usize },
    #[error(
        "A request of at most {max_request_bytes} bytes has no room for a piece beside \
         {context_bytes} bytes of context on each side"
    )]
    TextPieceLimitsTooTight {
        max_request_bytes: usize,
        context_bytes: usize,
    },
    /// The provider found more in the item than it returns, and says so. A text can be asked
    /// again in parts.
    #[error("{redacter} found more in the item than it returns")]
    FindingsTruncated { redacter: RedacterType },
    #[error("A detection at {start}..{end} is outside the content it was given")]
    DetectionOutsideContent { start: usize, end: usize },
    #[error("Invalid ignore file: {source}")]
    InvalidIgnoreFile {
        #[source]
        source: ignore::Error,
    },
    #[error("Score must be a number between 0 and 1, got `{value}`")]
    InvalidScore { value: String },
    #[error("System error: {message}")]
    SystemError { message: String },
    #[error("System error: {message}")]
    SystemErrorWithCause {
        message: String,
        cause: Box<dyn std::fmt::Debug + Send + Sync + 'static>,
    },
}

impl AppError {
    /// Whether the error says the bytes read as text are not text: not UTF-8, or holding a NUL
    /// byte.
    pub fn is_not_text(&self) -> bool {
        matches!(self, Self::TextNotUtf8 { .. } | Self::TextHasNul { .. })
    }

    /// A redacter was handed content of a type it does not support: `redact_support` said no
    /// and the caller ignored it.
    pub fn unsupported_content() -> Self {
        Self::SystemError {
            message: "The redacter does not support this type of content".to_string(),
        }
    }

    /// A provider answered `operation` with the HTTP status `status`. The response body is left
    /// out because it can quote the scanned text.
    pub fn request_rejected(operation: &str, status: StatusCode) -> Self {
        Self::RequestRejected {
            operation: operation.to_string(),
            status,
        }
    }

    pub fn incomplete_result(redacter_type: RedacterType, missing: &'static str) -> Self {
        Self::IncompleteResult {
            redacter_type,
            missing,
        }
    }

    /// Whether a provider refused the call for want of quota, so that the same call may succeed
    /// a moment later: gRPC `ResourceExhausted` and `Unavailable`, HTTP 429 and 503.
    pub fn is_throttled(&self) -> bool {
        match self {
            Self::GoogleCloudGrpcError(status) => {
                matches!(status.code(), Code::ResourceExhausted | Code::Unavailable)
            }
            // 503 is a transient refusal like gRPC `Unavailable`, which is retried too.
            Self::RequestRejected { status, .. } => matches!(
                *status,
                StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE
            ),
            _ => false,
        }
    }
}

impl<
        O: std::error::Error + std::fmt::Debug + Send + Sync + 'static,
        H: std::fmt::Debug + Send + Sync + 'static,
    > From<aws_sdk_s3::error::SdkError<O, H>> for AppError
{
    fn from(err: aws_sdk_s3::error::SdkError<O, H>) -> Self {
        Self::AwsSdkError(Box::new(err))
    }
}

impl From<anyhow::Error> for AppError {
    fn from(err: anyhow::Error) -> Self {
        Self::SystemErrorWithCause {
            message: err.to_string(),
            cause: Box::new(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gcloud_sdk::tonic::Status;

    fn rejected(status: StatusCode) -> AppError {
        AppError::request_rejected("analyze text", status)
    }

    #[test]
    fn quota_refusals_are_throttled() {
        let throttled = [
            AppError::from(Status::resource_exhausted("quota")),
            AppError::from(Status::unavailable("try later")),
            rejected(StatusCode::TOO_MANY_REQUESTS),
            rejected(StatusCode::SERVICE_UNAVAILABLE),
        ];
        for error in throttled {
            assert!(error.is_throttled(), "{error:?}");
        }
    }

    #[test]
    fn other_failures_are_not_throttled() {
        let failures = [
            AppError::from(Status::permission_denied("no")),
            AppError::from(Status::invalid_argument("bad")),
            rejected(StatusCode::BAD_REQUEST),
            rejected(StatusCode::INTERNAL_SERVER_ERROR),
            AppError::SystemError {
                message: "resource exhausted".to_string(),
            },
        ];
        for error in failures {
            assert!(!error.is_throttled(), "{error:?}");
        }
    }
}
