use crate::errors::AppError;
use crate::file_systems::FileSystemRef;
use crate::reporter::AppReporter;
use crate::AppResult;
use gcloud_sdk::prost::bytes;
use gcloud_sdk::tonic;
use mime::Mime;
use std::fmt::Display;
use std::future::Future;

mod gcp_dlp;
pub use gcp_dlp::*;

mod gcp_vertex_ai;
pub use gcp_vertex_ai::*;

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

mod redacter_throttler;
pub use redacter_throttler::*;

use crate::args::RedacterType;
use crate::common_types::DlpRequestLimit;

/// Longest edge, in pixels, an image is scaled down to before it is sent to an LLM.
pub const LLM_MAX_IMAGE_DIMENSION: u32 = 1024;

/// Instruction given to an image editing model on the native redaction path.
pub const NATIVE_IMAGE_REDACTION_PROMPT: &str =
    "Find everything in the attached image that looks like personal information and generate a \
     new image with that information erased and replaced by a plain black rectangle. The \
     personal information must be gone from the new image: do not write it inside the black \
     rectangles, do not write it on top of them, and do not highlight it. Keep everything else \
     in the image identical, with the same size, layout and formatting. Do not add any other \
     words.";

/// How the LLM redacters redact images.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, clap::ValueEnum)]
pub enum LlmImageMode {
    /// Edit the image with the model and fall back to coordinates when it cannot.
    #[default]
    Auto,
    /// Always edit the image with the model, failing when it cannot.
    Native,
    /// Always ask the model for PII coordinates and black them out locally.
    Coords,
}

impl Display for LlmImageMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LlmImageMode::Auto => write!(f, "auto"),
            LlmImageMode::Native => write!(f, "native"),
            LlmImageMode::Coords => write!(f, "coords"),
        }
    }
}

/// Why a native image editing attempt did not return a redacted image.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeImageEditFailure {
    /// The model or the endpoint cannot edit images, so the coordinate path may work instead.
    Unsupported,
    /// Authentication, permission, quota, transport or data errors, which are never retried.
    Fatal,
}

/// A failed native image editing attempt, keeping the original error for propagation.
#[derive(Debug, thiserror::Error)]
#[error("{error}")]
pub struct NativeImageEditError {
    pub failure: NativeImageEditFailure,
    #[source]
    pub error: AppError,
}

impl NativeImageEditError {
    pub fn fatal(error: AppError) -> Self {
        Self {
            failure: NativeImageEditFailure::Fatal,
            error,
        }
    }

    pub fn unsupported(error: AppError) -> Self {
        Self {
            failure: NativeImageEditFailure::Unsupported,
            error,
        }
    }

    /// The model answered, but without an image: it cannot edit images.
    pub fn no_image_in_response() -> Self {
        Self::unsupported(AppError::SystemError {
            message: "No image data in the model response".to_string(),
        })
    }
}

impl From<AppError> for NativeImageEditError {
    fn from(error: AppError) -> Self {
        Self::fatal(error)
    }
}

impl From<tonic::Status> for NativeImageEditError {
    fn from(status: tonic::Status) -> Self {
        Self {
            failure: classify_grpc_native_failure(status.code()),
            error: AppError::GoogleCloudGrpcError(status),
        }
    }
}

/// Classifies a gRPC failure of a native image editing call.
pub fn classify_grpc_native_failure(code: tonic::Code) -> NativeImageEditFailure {
    match code {
        tonic::Code::InvalidArgument
        | tonic::Code::NotFound
        | tonic::Code::FailedPrecondition
        | tonic::Code::Unimplemented => NativeImageEditFailure::Unsupported,
        _ => NativeImageEditFailure::Fatal,
    }
}

/// Classifies an HTTP failure of a native image editing call.
pub fn classify_http_native_failure(status_code: u16) -> NativeImageEditFailure {
    match status_code {
        400 | 404 => NativeImageEditFailure::Unsupported,
        _ => NativeImageEditFailure::Fatal,
    }
}

/// Whether a failed native attempt should be retried through the coordinate path.
pub fn should_fall_back_to_coords(mode: LlmImageMode, failure: NativeImageEditFailure) -> bool {
    matches!(
        (mode, failure),
        (LlmImageMode::Auto, NativeImageEditFailure::Unsupported)
    )
}

/// An image decoded, scaled down to the model input limit and re-encoded.
#[derive(Clone, Debug)]
pub struct PreparedImage {
    pub format: image::ImageFormat,
    pub data: bytes::Bytes,
    pub width: u32,
    pub height: u32,
}

/// Decodes, scales down and re-encodes an image so it fits the model input limits.
pub fn prepare_image_for_llm(mime_type: &Mime, data: &bytes::Bytes) -> AppResult<PreparedImage> {
    let format =
        image::ImageFormat::from_mime_type(mime_type).ok_or_else(|| AppError::SystemError {
            message: format!("Unsupported image mime type: {mime_type}"),
        })?;
    let image = image::load_from_memory_with_format(data, format)?;
    let resized = image.resize(
        LLM_MAX_IMAGE_DIMENSION,
        LLM_MAX_IMAGE_DIMENSION,
        image::imageops::FilterType::Gaussian,
    );
    let mut encoded = std::io::Cursor::new(Vec::new());
    resized.write_to(&mut encoded, format)?;
    Ok(PreparedImage {
        format,
        data: encoded.into_inner().into(),
        width: resized.width(),
        height: resized.height(),
    })
}

/// Redacts an image with the native editing path, the coordinate path, or the first
/// falling back to the second, according to `mode`.
pub async fn redact_image_with_mode<'a, NativeFn, NativeFut, CoordsFn, CoordsFut>(
    mode: LlmImageMode,
    input: RedacterDataItem,
    reporter: &'a AppReporter<'a>,
    native: NativeFn,
    coords: CoordsFn,
) -> AppResult<RedacterDataItem>
where
    NativeFn: FnOnce(RedacterDataItem) -> NativeFut,
    NativeFut: Future<Output = Result<RedacterDataItem, NativeImageEditError>>,
    CoordsFn: FnOnce(RedacterDataItem) -> CoordsFut,
    CoordsFut: Future<Output = AppResult<RedacterDataItem>>,
{
    match mode {
        LlmImageMode::Coords => coords(input).await,
        LlmImageMode::Auto | LlmImageMode::Native => {
            let fallback_input = input.clone();
            match native(input).await {
                Ok(redacted) => Ok(redacted),
                Err(err) if should_fall_back_to_coords(mode, err.failure) => {
                    reporter.report(format!(
                        "Native image redaction is not available ({}). Falling back to redacting by coordinates.",
                        err.error
                    ))?;
                    coords(fallback_input).await
                }
                Err(err) => Err(err.error),
            }
        }
    }
}

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
    GcpVertexAi(GcpVertexAiRedacter<'a>),
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
    pub limit_dlp_requests: Option<DlpRequestLimit>,
}

#[derive(Debug, Clone)]
pub enum RedacterProviderOptions {
    GcpDlp(GcpDlpRedacterOptions),
    AwsComprehend(AwsComprehendRedacterOptions),
    MsPresidio(MsPresidioRedacterOptions),
    GeminiLlm(GeminiLlmRedacterOptions),
    OpenAiLlm(OpenAiLlmRedacterOptions),
    GcpVertexAi(GcpVertexAiRedacterOptions),
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
                RedacterProviderOptions::OpenAiLlm(_) => "open-ai-llm".to_string(),
                RedacterProviderOptions::GcpVertexAi(_) => "gcp-vertex-ai".to_string(),
            })
            .collect::<Vec<String>>()
            .join(", ");
        write!(f, "{to_display}")
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
            RedacterProviderOptions::GcpVertexAi(options) => Ok(Redacters::GcpVertexAi(
                GcpVertexAiRedacter::new(options, reporter).await?,
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
pub enum RedactSupport {
    Supported,
    Unsupported,
}

pub trait Redacter {
    async fn redact(&self, input: RedacterDataItem) -> AppResult<RedacterDataItem>;

    async fn redact_support(&self, file_ref: &FileSystemRef) -> AppResult<RedactSupport>;

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
            Redacters::GcpVertexAi(redacter) => redacter.redact(input).await,
        }
    }

    async fn redact_support(&self, file_ref: &FileSystemRef) -> AppResult<RedactSupport> {
        match self {
            Redacters::GcpDlp(redacter) => redacter.redact_support(file_ref).await,
            Redacters::AwsComprehend(redacter) => redacter.redact_support(file_ref).await,
            Redacters::MsPresidio(redacter) => redacter.redact_support(file_ref).await,
            Redacters::GeminiLlm(redacter) => redacter.redact_support(file_ref).await,
            Redacters::OpenAiLlm(redacter) => redacter.redact_support(file_ref).await,
            Redacters::GcpVertexAi(redacter) => redacter.redact_support(file_ref).await,
        }
    }

    fn redacter_type(&self) -> RedacterType {
        match self {
            Redacters::GcpDlp(_) => RedacterType::GcpDlp,
            Redacters::AwsComprehend(_) => RedacterType::AwsComprehend,
            Redacters::MsPresidio(_) => RedacterType::MsPresidio,
            Redacters::GeminiLlm(_) => RedacterType::GeminiLlm,
            Redacters::OpenAiLlm(_) => RedacterType::OpenAiLlm,
            Redacters::GcpVertexAi(_) => RedacterType::GcpVertexAi,
        }
    }
}
#[cfg(test)]
pub mod test_support {
    use super::*;
    use crate::file_systems::FileSystemRef;

    /// Installs the rustls crypto provider once per test process. Several providers are
    /// present in the dependency tree, so rustls cannot pick one on its own.
    pub fn initialize_crypto() {
        static INIT_CRYPTO: std::sync::Once = std::sync::Once::new();
        INIT_CRYPTO.call_once(|| {
            let _ = rustls::crypto::ring::default_provider().install_default();
        });
    }

    /// Sample fixtures under `test-fixtures/documents/`, each carrying fake personal
    /// information, that exercise a text, table, structured and PDF media type.
    pub const TEST_DOCUMENTS_DIR: &str = "test-fixtures/documents/";
    pub const TEST_DOCUMENT_NAMES: [&str; 5] = [
        "customer-note.txt",
        "customers.csv",
        "customer.json",
        "customer-profile.html",
        "customer-form.pdf",
    ];
    pub const TEST_DOCUMENT_SAMPLE_EMAIL: &str = "john.smith@example.com";
    pub const TEST_DOCUMENT_SAMPLE_PHONE: &str = "+1 (555) 123-4567";

    /// Serializes any test that binds the pdfium library, directly or through
    /// `command_copy`. Pdfium is a C library with process-wide global state: binding it
    /// concurrently from two test threads has been observed to crash the whole test
    /// process (SIGTRAP/SIGSEGV) rather than fail just the offending test.
    ///
    /// A `tokio::sync::Mutex` rather than a `std` one, so async tests can hold the guard
    /// across `.await` points without tripping `clippy::await_holding_lock`. Synchronous
    /// tests take it with `blocking_lock` instead.
    pub fn pdfium_test_lock() -> &'static tokio::sync::Mutex<()> {
        static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
    }

    /// A 1000x720 PNG of a filled-in form carrying fake personal information.
    pub const TEST_IMAGE_FIXTURE: &str = "test-fixtures/media/form-example.png";

    pub fn test_image_item() -> RedacterDataItem {
        let data = std::fs::read(TEST_IMAGE_FIXTURE).expect("the image fixture is committed");
        RedacterDataItem {
            file_ref: FileSystemRef {
                relative_path: "form-example.png".into(),
                media_type: Some(mime::IMAGE_PNG),
                file_size: Some(data.len()),
            },
            content: RedacterDataItemContent::Image {
                mime_type: mime::IMAGE_PNG,
                data: data.into(),
            },
        }
    }

    /// Checks that a redacted item is an image differing from the input and writes it out
    /// so the result can be looked at. Returns the path it was written to.
    pub fn check_and_save_redacted_image(
        original: &RedacterDataItem,
        redacted: &RedacterDataItem,
        output_name: &str,
    ) -> std::path::PathBuf {
        let RedacterDataItemContent::Image {
            data: original_data,
            ..
        } = &original.content
        else {
            panic!("the fixture is an image");
        };
        let RedacterDataItemContent::Image { mime_type, data } = &redacted.content else {
            panic!("expected an image back, got {:?}", redacted.content);
        };
        assert_eq!(
            mime_type.type_(),
            mime::IMAGE,
            "expected an image mime type, got {mime_type}"
        );
        assert!(!data.is_empty(), "the redacted image is empty");
        assert_ne!(
            data, original_data,
            "the redacted image is identical to the input"
        );
        let output_path = std::env::var("TEST_REDACTED_OUTPUT_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| std::env::temp_dir())
            .join(output_name);
        std::fs::write(&output_path, data).expect("the output directory is writable");
        output_path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_mode_falls_back_only_when_the_model_cannot_edit_images() {
        assert!(should_fall_back_to_coords(
            LlmImageMode::Auto,
            NativeImageEditFailure::Unsupported
        ));
        assert!(!should_fall_back_to_coords(
            LlmImageMode::Auto,
            NativeImageEditFailure::Fatal
        ));
    }

    #[test]
    fn native_and_coords_modes_never_fall_back() {
        for failure in [
            NativeImageEditFailure::Unsupported,
            NativeImageEditFailure::Fatal,
        ] {
            assert!(!should_fall_back_to_coords(LlmImageMode::Native, failure));
            assert!(!should_fall_back_to_coords(LlmImageMode::Coords, failure));
        }
    }

    #[test]
    fn grpc_failures_are_classified_by_code() {
        for code in [
            tonic::Code::InvalidArgument,
            tonic::Code::NotFound,
            tonic::Code::FailedPrecondition,
            tonic::Code::Unimplemented,
        ] {
            assert_eq!(
                classify_grpc_native_failure(code),
                NativeImageEditFailure::Unsupported,
                "{code:?} should allow a fallback"
            );
        }
        for code in [
            tonic::Code::Unauthenticated,
            tonic::Code::PermissionDenied,
            tonic::Code::ResourceExhausted,
            tonic::Code::Unavailable,
            tonic::Code::Internal,
            tonic::Code::DeadlineExceeded,
        ] {
            assert_eq!(
                classify_grpc_native_failure(code),
                NativeImageEditFailure::Fatal,
                "{code:?} should propagate"
            );
        }
    }

    #[test]
    fn http_failures_are_classified_by_status() {
        assert_eq!(
            classify_http_native_failure(400),
            NativeImageEditFailure::Unsupported
        );
        assert_eq!(
            classify_http_native_failure(404),
            NativeImageEditFailure::Unsupported
        );
        for status_code in [401, 403, 429, 500, 503] {
            assert_eq!(
                classify_http_native_failure(status_code),
                NativeImageEditFailure::Fatal,
                "{status_code} should propagate"
            );
        }
    }

    #[test]
    fn a_response_without_an_image_allows_a_fallback() {
        assert_eq!(
            NativeImageEditError::no_image_in_response().failure,
            NativeImageEditFailure::Unsupported
        );
    }
}
