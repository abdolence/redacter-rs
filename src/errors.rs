use gcloud_sdk::tonic::metadata::errors::InvalidMetadataValue;
use indicatif::style::TemplateError;
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
    #[error("Google Cloud REST SDK API error:\n{0:?}")]
    GoogleCloudRestSdkApiError(Box<dyn std::fmt::Debug + Send + Sync + 'static>),
    #[error("Google Cloud SDK error:\n{0}")]
    GoogleCloudGrpcError(#[from] gcloud_sdk::tonic::Status),
    #[error("Google Cloud invalid metadata value:\n{0}")]
    GoogleCloudInvalidMetadataValue(#[from] InvalidMetadataValue),
    #[error("AWS SDK error occurred")]
    AwsSdkError(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("MIME error:\n{0}")]
    MimeError(#[from] mime::FromStrError),
    #[error("HTTP client error:\n{0}")]
    HttpClientError(#[from] reqwest::Error),
    #[error("Zip error:\n{0}")]
    ZipError(#[from] zip::result::ZipError),
    #[error("CSV parser error:\n{0}")]
    CsvParserError(#[from] csv_async::Error),
    #[error("Redacter config error: {message}")]
    RedacterConfigError { message: String },
    #[error("Template error: {0}")]
    TemplateError(#[from] TemplateError),
    #[error("PDF conversion error: {0}")]
    PdfiumError(#[from] pdfium_render::prelude::PdfiumError),
    #[error("Image conversion error: {0}")]
    ImageError(#[from] image::ImageError),
    #[cfg(feature = "clipboard")]
    #[error("Clipboard error: {0}")]
    ClipboardError(#[from] arboard::Error),
    #[error("SystemTimeError: {0}")]
    SystemTimeError(#[from] SystemTimeError),
    #[error("System error: {message}")]
    SystemError { message: String },
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

impl<T: std::fmt::Debug + Send + Sync + 'static>
    From<gcloud_sdk::google_rest_apis::storage_v1::Error<T>> for AppError
{
    fn from(err: gcloud_sdk::google_rest_apis::storage_v1::Error<T>) -> Self {
        Self::GoogleCloudRestSdkApiError(Box::new(err))
    }
}
