use gcloud_sdk::google_rest_apis::storage_v1::objects_api::{
    StoragePeriodObjectsPeriodGetError, StoragePeriodObjectsPeriodInsertError,
    StoragePeriodObjectsPeriodListError,
};
use gcloud_sdk::tonic::metadata::errors::InvalidMetadataValue;
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
    #[error("Google Cloud SDK error")]
    GoogleCloudRestSdkError(#[from] gcloud_sdk::error::Error),
    #[error("Google Cloud Storage download error")]
    GoogleCloudStorageGetObjectError(
        #[from] gcloud_sdk::google_rest_apis::storage_v1::Error<StoragePeriodObjectsPeriodGetError>,
    ),
    #[error("Google Cloud Storage upload error")]
    GoogleCloudStorageInsertObjectError(
        #[from]
        gcloud_sdk::google_rest_apis::storage_v1::Error<StoragePeriodObjectsPeriodInsertError>,
    ),
    #[error("Google Cloud Storage upload error")]
    GoogleCloudStorageListObjectError(
        #[from]
        gcloud_sdk::google_rest_apis::storage_v1::Error<StoragePeriodObjectsPeriodListError>,
    ),
    #[error("Google Cloud SDK error")]
    GoogleCloudGrpcError(#[from] gcloud_sdk::tonic::Status),
    #[error("Google Cloud invalid metadata value")]
    GoogleCloudInvalidMetadataValue(#[from] InvalidMetadataValue),
    #[error("AWS SDK error occurred")]
    AwsSdkError(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("MIME error")]
    MimeError(#[from] mime::FromStrError),
    #[error("Zip error")]
    ZipError(#[from] zip::result::ZipError),
    #[error("CSV parser error")]
    CsvParserError(#[from] csv_async::Error),
    #[error("Redacter config error: {message}")]
    RedacterConfigError { message: String },
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
