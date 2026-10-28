use crate::addon_store::AddonStore;
use crate::errors::AppError;
use crate::file_systems::FileSystemRef;
use crate::reporter::AppReporter;
use crate::AppResult;
use aws_sdk_bedrockruntime::error::{DisplayErrorContext, ProvideErrorMetadata, SdkError};
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

mod aws_bedrock;
pub use aws_bedrock::*;

mod aws_bedrock_guardrails;
pub use aws_bedrock_guardrails::*;

mod ms_presidio;
pub use ms_presidio::*;

mod gemini_llm;
pub use gemini_llm::*;

mod local_rules;
pub use local_rules::*;

#[cfg(feature = "local-ner")]
pub(crate) mod local_ner;
#[cfg(feature = "local-ner")]
pub use local_ner::{Entity, LocalNerRedacter, LocalNerRedacterOptions};

#[cfg(feature = "local-gliner")]
pub(crate) mod local_gliner;
#[cfg(feature = "local-gliner")]
pub use local_gliner::{LocalGlinerRedacter, LocalGlinerRedacterOptions};

/// Byte-span helpers shared by the local redacters (`local-rules`, `local-ner`, `local-gliner`).
pub(crate) mod text_spans;

mod detection;
pub use detection::*;

mod open_ai_llm;
pub use open_ai_llm::*;

mod llm_detection;
pub use llm_detection::*;

mod model_answer;
pub use model_answer::*;

mod ocr_alignment;
pub use ocr_alignment::*;

mod simple_image_redacter;
pub use simple_image_redacter::*;
mod stream_redacter;
pub use stream_redacter::*;
mod stream_detector;
pub use stream_detector::*;

mod redacter_throttler;
pub use redacter_throttler::*;

use crate::args::RedacterType;
use crate::common_types::{DlpRequestLimit, TextImageCoords};

/// Longest edge, in pixels, an image is scaled down to before it is sent to an LLM.
pub const LLM_MAX_IMAGE_DIMENSION: u32 = 1024;

/// Formats a Bedrock SDK failure, keeping the service error code and message when the call
/// reached the service and the whole source chain when it did not.
pub(crate) fn bedrock_error<E, R>(context: &str, err: &SdkError<E, R>) -> AppError
where
    E: ProvideErrorMetadata + std::error::Error + 'static,
    R: std::fmt::Debug,
{
    let message = match (err.code(), err.message()) {
        (Some(code), Some(message)) => format!("{context}: {code}: {message}"),
        (Some(code), None) => format!("{context}: {code}"),
        (None, _) => format!("{context}: {}", DisplayErrorContext(err)),
    };
    AppError::AwsBedrockError { message }
}

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
    /// Edit the image with the model, verify the edit with the coordinate pass, and fall
    /// back to coordinates entirely when the model cannot edit images.
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
    /// Size of the image before it was scaled down, which is the size detections report in.
    pub original_width: u32,
    pub original_height: u32,
}

/// The kind of a box in an image whose text no text detection classified: the coordinate pass
/// locates text a model takes for personal information, and does not classify it.
const IMAGE_DETECTION_LABEL: &str = "personal_information";

/// What a model found in an image: boxes in pixels of `image`, the scaled-down copy it saw.
#[derive(Debug, Clone)]
pub struct ImagePii {
    pub image: PreparedImage,
    pub coords: Vec<TextImageCoords>,
}

impl ImagePii {
    /// The boxes as detections in pixels of the image as it was given, clamped to the image
    /// and ordered. The value is the text the model read, empty when it gave none.
    pub fn into_detections(self) -> Vec<Detection> {
        let image = self.image;
        let scale_x = image.original_width as f32 / image.width as f32;
        let scale_y = image.original_height as f32 / image.height as f32;
        let to_x = |x: f32| x.clamp(0.0, image.width as f32) * scale_x;
        let to_y = |y: f32| y.clamp(0.0, image.height as f32) * scale_y;
        self.coords
            .into_iter()
            .map(|coords| {
                let (x1, x2) = (to_x(coords.x1), to_x(coords.x2));
                let (y1, y2) = (to_y(coords.y1), to_y(coords.y2));
                Detection {
                    label: DetectionLabel::new(IMAGE_DETECTION_LABEL),
                    location: DetectionLocation::Image {
                        page: None,
                        x1: x1.min(x2),
                        y1: y1.min(y2),
                        x2: x1.max(x2),
                        y2: y1.max(y2),
                    },
                    score: None,
                    value: coords.text.unwrap_or_default(),
                }
            })
            .collect()
    }
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
        original_width: image.width(),
        original_height: image.height(),
    })
}

/// Instruction given to a Gemini model on the coordinate-based image redaction path.
///
/// Gemini's vision models answer bounding-box questions in a normalized 0-1000 coordinate
/// space as `box_2d: [ymin, xmin, ymax, xmax]`, regardless of how the prompt is worded, so
/// the prompt asks for that convention explicitly instead of pixel coordinates. The caller
/// converts the normalized boxes to pixel coordinates locally with
/// [`normalized_box_to_image_coords`], using the dimensions of the image the model actually
/// saw (from [`prepare_image_for_llm`]).
pub const GEMINI_COORDS_REDACTION_PROMPT: &str =
    "Detect all personal information in the image. Return a JSON array of objects \
     {box_2d, text} where box_2d is [ymin, xmin, ymax, xmax] normalized to 0-1000.";

/// One detection from the Gemini coordinate redaction path: a bounding box normalized to
/// 0-1000 as `[ymin, xmin, ymax, xmax]`, per Gemini's bounding-box convention, together with
/// the detected text.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct NormalizedPiiBox {
    pub box_2d: Vec<f32>,
    pub text: Option<String>,
}

impl NormalizedPiiBox {
    /// The pixel boxes of a model's JSON answer, for an image of `width` by `height` as the
    /// model saw it. A box without four numbers is left out; an answer that is not a JSON
    /// array of boxes is an error, since reading it as "no boxes" would report a clean image.
    pub fn coords_from_answer(
        answer: &str,
        width: u32,
        height: u32,
    ) -> AppResult<Vec<TextImageCoords>> {
        let boxes: Vec<Self> = parse_answer(answer)?;
        Ok(boxes
            .into_iter()
            .filter_map(|found| {
                let box_2d: [f32; 4] = found.box_2d.get(..4)?.try_into().ok()?;
                Some(normalized_box_to_image_coords(
                    box_2d, width, height, found.text,
                ))
            })
            .collect())
    }
}

/// Converts one Gemini-style normalized bounding box (`[ymin, xmin, ymax, xmax]`, each in
/// 0..=1000) to pixel coordinates within an image of the given size.
///
/// Out-of-range inputs are clamped to the normalized 0..=1000 range before scaling, and the
/// resulting pixel coordinates are ordered (`x1 <= x2`, `y1 <= y2`) so a degenerate or
/// reversed box still produces a well-formed rectangle instead of one with negative width or
/// height.
pub fn normalized_box_to_image_coords(
    box_2d: [f32; 4],
    width: u32,
    height: u32,
    text: Option<String>,
) -> TextImageCoords {
    let [ymin, xmin, ymax, xmax] = box_2d;
    let clamp_normalized = |v: f32| v.clamp(0.0, 1000.0);
    let to_x = |v: f32| clamp_normalized(v) / 1000.0 * width as f32;
    let to_y = |v: f32| clamp_normalized(v) / 1000.0 * height as f32;

    let (raw_x1, raw_x2) = (to_x(xmin), to_x(xmax));
    let (raw_y1, raw_y2) = (to_y(ymin), to_y(ymax));

    TextImageCoords {
        x1: raw_x1.min(raw_x2),
        y1: raw_y1.min(raw_y2),
        x2: raw_x1.max(raw_x2),
        y2: raw_y1.max(raw_y2),
        text,
        line: 0,
    }
}

/// Redacts an image with the native editing path, the coordinate path, or both,
/// according to `mode`.
///
/// In `Auto` mode a successful native edit is not trusted on its own: the model can draw
/// the black boxes but still write the personal information back inside or beside them.
/// The edited image is passed through the coordinate path as a verification step, and any
/// PII the coordinate path still finds gets blacked out. A failure of that verification
/// pass is propagated as an error rather than returning the unverified native result. When
/// the native edit itself fails in a way that means the model cannot edit images at all,
/// the coordinate path is used as a full fallback instead.
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
        LlmImageMode::Native => native(input).await.map_err(|err| err.error),
        LlmImageMode::Auto => {
            let fallback_input = input.clone();
            match native(input).await {
                Ok(redacted) => coords(redacted).await,
                Err(err) if should_fall_back_to_coords(mode, err.failure) => {
                    reporter.report_debug(format!(
                        "Native image redaction is not available ({}). Falling back to redacting by coordinates.",
                        err.error
                    ));
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

impl RedacterDataItem {
    fn is_blank_text(&self) -> bool {
        matches!(&self.content, RedacterDataItemContent::Value(text) if text.trim().is_empty())
    }
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
    AwsBedrock(AwsBedrockRedacter<'a>),
    AwsBedrockGuardrails(AwsBedrockGuardrailsRedacter<'a>),
    LocalRules(LocalRulesRedacter<'a>),
    #[cfg(feature = "local-ner")]
    LocalNer(LocalNerRedacter<'a>),
    #[cfg(feature = "local-gliner")]
    LocalGliner(LocalGlinerRedacter<'a>),
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
    AwsBedrock(AwsBedrockRedacterOptions),
    AwsBedrockGuardrails(AwsBedrockGuardrailsRedacterOptions),
    LocalRules(LocalRulesRedacterOptions),
    #[cfg(feature = "local-ner")]
    LocalNer(LocalNerRedacterOptions),
    #[cfg(feature = "local-gliner")]
    LocalGliner(LocalGlinerRedacterOptions),
}

impl RedacterProviderOptions {
    /// The redacter these options configure, so the type is named in exactly one place.
    pub fn redacter_type(&self) -> RedacterType {
        match self {
            RedacterProviderOptions::GcpDlp(_) => RedacterType::GcpDlp,
            RedacterProviderOptions::AwsComprehend(_) => RedacterType::AwsComprehend,
            RedacterProviderOptions::MsPresidio(_) => RedacterType::MsPresidio,
            RedacterProviderOptions::GeminiLlm(_) => RedacterType::GeminiLlm,
            RedacterProviderOptions::OpenAiLlm(_) => RedacterType::OpenAiLlm,
            RedacterProviderOptions::GcpVertexAi(_) => RedacterType::GcpVertexAi,
            RedacterProviderOptions::AwsBedrock(_) => RedacterType::AwsBedrock,
            RedacterProviderOptions::AwsBedrockGuardrails(_) => RedacterType::AwsBedrockGuardrails,
            RedacterProviderOptions::LocalRules(_) => RedacterType::LocalRules,
            #[cfg(feature = "local-ner")]
            RedacterProviderOptions::LocalNer(_) => RedacterType::LocalNer,
            #[cfg(feature = "local-gliner")]
            RedacterProviderOptions::LocalGliner(_) => RedacterType::LocalGliner,
        }
    }
}

impl Display for RedacterOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let to_display = self
            .provider_options
            .iter()
            .map(|o| o.redacter_type().to_string())
            .collect::<Vec<String>>()
            .join(", ");
        write!(f, "{to_display}")
    }
}

impl<'a> Redacters<'a> {
    #[cfg_attr(
        not(any(feature = "local-ner", feature = "local-gliner")),
        allow(unused_variables)
    )]
    pub async fn new_redacter(
        provider_options: RedacterProviderOptions,
        reporter: &'a AppReporter<'a>,
        addons: &AddonStore,
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
            RedacterProviderOptions::AwsBedrock(options) => Ok(Redacters::AwsBedrock(
                AwsBedrockRedacter::new(options, reporter).await?,
            )),
            RedacterProviderOptions::AwsBedrockGuardrails(options) => {
                Ok(Redacters::AwsBedrockGuardrails(
                    AwsBedrockGuardrailsRedacter::new(options, reporter).await?,
                ))
            }
            RedacterProviderOptions::LocalRules(options) => Ok(Redacters::LocalRules(
                LocalRulesRedacter::new(options, reporter).await?,
            )),
            #[cfg(feature = "local-ner")]
            RedacterProviderOptions::LocalNer(options) => Ok(Redacters::LocalNer(
                LocalNerRedacter::new(options, reporter, addons).await?,
            )),
            #[cfg(feature = "local-gliner")]
            RedacterProviderOptions::LocalGliner(options) => Ok(Redacters::LocalGliner(
                LocalGlinerRedacter::new(options, reporter, addons).await?,
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

/// What the local redacters support natively: text and tabular media types, which they read
/// as strings. Images and PDFs reach them only as text extracted by the stream redacter.
pub(crate) fn text_or_table_support(file_ref: &FileSystemRef) -> RedactSupport {
    match file_ref.media_type.as_ref() {
        Some(media_type)
            if Redacters::is_mime_text(media_type) || Redacters::is_mime_table(media_type) =>
        {
            RedactSupport::Supported
        }
        _ => RedactSupport::Unsupported,
    }
}

pub trait Redacter {
    async fn redact(&self, input: RedacterDataItem) -> AppResult<RedacterDataItem>;

    async fn redact_support(&self, file_ref: &FileSystemRef) -> AppResult<RedactSupport>;

    /// Finds sensitive data in `input` without changing it. Every location points into
    /// `input` itself: a text range for a text item, a cell for a table, a box for an image.
    async fn detect(&self, input: &RedacterDataItem) -> AppResult<Vec<Detection>>;

    /// Whether `detect` reads this file as it is. Unsupported images and PDF pages are read
    /// with OCR when the redacter supports text.
    async fn detect_support(&self, file_ref: &FileSystemRef) -> AppResult<RedactSupport> {
        self.redact_support(file_ref).await
    }

    fn redacter_type(&self) -> RedacterType;
}

impl<'a> Redacter for Redacters<'a> {
    // Text with nothing in it never reaches the redacter, in `redact` or in `detect`: a model
    // has nothing to answer for it and would make something up (written to the file by
    // `redact`), and the DLP services reject empty content as an invalid request.
    async fn redact(&self, input: RedacterDataItem) -> AppResult<RedacterDataItem> {
        if input.is_blank_text() {
            return Ok(input);
        }
        match self {
            Redacters::GcpDlp(redacter) => redacter.redact(input).await,
            Redacters::AwsComprehend(redacter) => redacter.redact(input).await,
            Redacters::MsPresidio(redacter) => redacter.redact(input).await,
            Redacters::GeminiLlm(redacter) => redacter.redact(input).await,
            Redacters::OpenAiLlm(redacter) => redacter.redact(input).await,
            Redacters::GcpVertexAi(redacter) => redacter.redact(input).await,
            Redacters::AwsBedrock(redacter) => redacter.redact(input).await,
            Redacters::AwsBedrockGuardrails(redacter) => redacter.redact(input).await,
            Redacters::LocalRules(redacter) => redacter.redact(input).await,
            #[cfg(feature = "local-ner")]
            Redacters::LocalNer(redacter) => redacter.redact(input).await,
            #[cfg(feature = "local-gliner")]
            Redacters::LocalGliner(redacter) => redacter.redact(input).await,
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
            Redacters::AwsBedrock(redacter) => redacter.redact_support(file_ref).await,
            Redacters::AwsBedrockGuardrails(redacter) => redacter.redact_support(file_ref).await,
            Redacters::LocalRules(redacter) => redacter.redact_support(file_ref).await,
            #[cfg(feature = "local-ner")]
            Redacters::LocalNer(redacter) => redacter.redact_support(file_ref).await,
            #[cfg(feature = "local-gliner")]
            Redacters::LocalGliner(redacter) => redacter.redact_support(file_ref).await,
        }
    }

    async fn detect(&self, input: &RedacterDataItem) -> AppResult<Vec<Detection>> {
        if input.is_blank_text() {
            return Ok(Vec::new());
        }
        match self {
            Redacters::GcpDlp(redacter) => redacter.detect(input).await,
            Redacters::AwsComprehend(redacter) => redacter.detect(input).await,
            Redacters::MsPresidio(redacter) => redacter.detect(input).await,
            Redacters::GeminiLlm(redacter) => redacter.detect(input).await,
            Redacters::OpenAiLlm(redacter) => redacter.detect(input).await,
            Redacters::GcpVertexAi(redacter) => redacter.detect(input).await,
            Redacters::AwsBedrock(redacter) => redacter.detect(input).await,
            Redacters::AwsBedrockGuardrails(redacter) => redacter.detect(input).await,
            Redacters::LocalRules(redacter) => redacter.detect(input).await,
            #[cfg(feature = "local-ner")]
            Redacters::LocalNer(redacter) => redacter.detect(input).await,
            #[cfg(feature = "local-gliner")]
            Redacters::LocalGliner(redacter) => redacter.detect(input).await,
        }
    }

    async fn detect_support(&self, file_ref: &FileSystemRef) -> AppResult<RedactSupport> {
        match self {
            Redacters::GcpDlp(redacter) => redacter.detect_support(file_ref).await,
            Redacters::AwsComprehend(redacter) => redacter.detect_support(file_ref).await,
            Redacters::MsPresidio(redacter) => redacter.detect_support(file_ref).await,
            Redacters::GeminiLlm(redacter) => redacter.detect_support(file_ref).await,
            Redacters::OpenAiLlm(redacter) => redacter.detect_support(file_ref).await,
            Redacters::GcpVertexAi(redacter) => redacter.detect_support(file_ref).await,
            Redacters::AwsBedrock(redacter) => redacter.detect_support(file_ref).await,
            Redacters::AwsBedrockGuardrails(redacter) => redacter.detect_support(file_ref).await,
            Redacters::LocalRules(redacter) => redacter.detect_support(file_ref).await,
            #[cfg(feature = "local-ner")]
            Redacters::LocalNer(redacter) => redacter.detect_support(file_ref).await,
            #[cfg(feature = "local-gliner")]
            Redacters::LocalGliner(redacter) => redacter.detect_support(file_ref).await,
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
            Redacters::AwsBedrock(_) => RedacterType::AwsBedrock,
            Redacters::AwsBedrockGuardrails(_) => RedacterType::AwsBedrockGuardrails,
            Redacters::LocalRules(_) => RedacterType::LocalRules,
            #[cfg(feature = "local-ner")]
            Redacters::LocalNer(_) => RedacterType::LocalNer,
            #[cfg(feature = "local-gliner")]
            Redacters::LocalGliner(_) => RedacterType::LocalGliner,
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
    /// information, that exercise a text, table, structured and PDF media type. The
    /// multilingual note is German, French and Spanish text for the local redacters.
    pub const TEST_DOCUMENTS_DIR: &str = "test-fixtures/documents/";
    pub const TEST_DOCUMENT_NAMES: [&str; 9] = [
        "customer-note.txt",
        "customers.csv",
        "customer.json",
        "customer-profile.html",
        "customer-form.pdf",
        "multilingual.txt",
        "dates-en.txt",
        "false-positives-en.txt",
        "contextual-en.txt",
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

/// Fixtures and assertions shared by the live `detect` tests of the LLM redacters.
#[cfg(test)]
pub mod live_detection {
    use crate::redacters::test_support::test_image_item;
    use crate::redacters::{
        Detection, DetectionLocation, RedacterDataItem, RedacterDataItemContent,
    };

    pub const NOTE_FIXTURE: &str = "test-fixtures/documents/customer-note.txt";

    /// Width the form fixture is enlarged to: not 1000, so a box scaled by the wrong factor
    /// misses, and past the model input limit, so boxes must be mapped back to the original.
    pub const FORM_WIDTH: u32 = 1500;

    const FORM_FIXTURE_WIDTH: u32 = 1000;

    /// Pixel boxes `[x1, y1, x2, y2]` of values in `form-example.png` at its own 1000 px
    /// width, measured from the dark pixels of each field.
    const FORM_VALUES: [(&str, [f32; 4], Option<&str>); 4] = [
        (
            "John Michael Smith",
            [309.0, 125.0, 536.0, 147.0],
            Some("name"),
        ),
        (
            "john.smith@example.com",
            [310.0, 195.0, 619.0, 217.0],
            Some("email"),
        ),
        (
            "+1 (555) 123-4567",
            [313.0, 265.0, 537.0, 285.0],
            Some("phone"),
        ),
        ("4111 1111 1111 1111", [311.0, 545.0, 574.0, 562.0], None),
    ];

    pub fn note_item() -> RedacterDataItem {
        let text = std::fs::read_to_string(NOTE_FIXTURE).expect("the note fixture is committed");
        RedacterDataItem {
            file_ref: crate::file_systems::FileSystemRef {
                relative_path: "customer-note.txt".into(),
                media_type: Some(mime::TEXT_PLAIN),
                file_size: Some(text.len()),
            },
            content: RedacterDataItemContent::Value(text),
        }
    }

    /// The form fixture enlarged to [`FORM_WIDTH`], with its height scaled alike.
    pub fn enlarged_form_item() -> RedacterDataItem {
        let mut item = test_image_item();
        let RedacterDataItemContent::Image { data, .. } = &item.content else {
            unreachable!("the fixture is an image");
        };
        let original = image::load_from_memory(data).expect("the fixture decodes");
        let height = original.height() * FORM_WIDTH / original.width();
        let enlarged =
            original.resize_exact(FORM_WIDTH, height, image::imageops::FilterType::Lanczos3);
        let mut png = std::io::Cursor::new(Vec::new());
        enlarged
            .write_to(&mut png, image::ImageFormat::Png)
            .expect("the image encodes");
        item.content = RedacterDataItemContent::Image {
            mime_type: mime::IMAGE_PNG,
            data: png.into_inner().into(),
        };
        item
    }

    /// Asserts the note's name, email address and phone number are found at their own text
    /// ranges under a label that names the kind of data.
    pub fn assert_note_found(item: &RedacterDataItem, detections: &[Detection]) {
        let RedacterDataItemContent::Value(text) = &item.content else {
            panic!("the note is text");
        };
        for (value, label_word) in [
            ("John Michael Smith", "name"),
            ("john.smith@example.com", "email"),
            ("+1 (555) 123-4567", "phone"),
        ] {
            let found = detections.iter().any(|detection| {
                matches!(
                    detection.location,
                    DetectionLocation::Text { start, end, .. } if text.get(start..end) == Some(value)
                ) && detection
                    .label
                    .to_string()
                    .to_lowercase()
                    .contains(label_word)
            });
            assert!(
                found,
                "{value} not found as *{label_word}* in {detections:?}"
            );
        }
    }

    /// Asserts a box covers most of each value's pixels in the enlarged form without being
    /// much larger than it.
    pub fn assert_form_found(detections: &[Detection]) {
        let scale = FORM_WIDTH as f32 / FORM_FIXTURE_WIDTH as f32;
        for (value, expected, label_word) in FORM_VALUES {
            let expected = expected.map(|pixel| pixel * scale);
            let found = detections.iter().any(|detection| {
                matches!(
                    detection.location,
                    DetectionLocation::Image { x1, y1, x2, y2, .. }
                        if covers([x1, y1, x2, y2], expected)
                ) && label_word
                    .is_none_or(|word| detection.label.to_string().to_lowercase().contains(word))
            });
            assert!(
                found,
                "no box covers {value} at {expected:?} under a label with {label_word:?} in {detections:?}"
            );
        }
    }

    fn covers(found: [f32; 4], expected: [f32; 4]) -> bool {
        let area = |[x1, y1, x2, y2]: [f32; 4]| (x2 - x1).max(0.0) * (y2 - y1).max(0.0);
        let overlap = area([
            found[0].max(expected[0]),
            found[1].max(expected[1]),
            found[2].min(expected[2]),
            found[3].min(expected[3]),
        ]);
        overlap >= 0.6 * area(expected) && area(found) <= 3.0 * area(expected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_systems::FileSystemRef;

    /// A minimal in-memory image item tagged with a string, so a test can trace which
    /// closure produced which item without touching a real image file.
    fn dummy_image_item(tag: &str) -> RedacterDataItem {
        RedacterDataItem {
            file_ref: FileSystemRef {
                relative_path: "image.png".into(),
                media_type: Some(mime::IMAGE_PNG),
                file_size: Some(tag.len()),
            },
            content: RedacterDataItemContent::Image {
                mime_type: mime::IMAGE_PNG,
                data: tag.as_bytes().to_vec().into(),
            },
        }
    }

    fn image_tag(item: &RedacterDataItem) -> &str {
        match &item.content {
            RedacterDataItemContent::Image { data, .. } => {
                std::str::from_utf8(data).expect("the test tag is valid utf8")
            }
            other => panic!("expected an image item, got {other:?}"),
        }
    }

    fn blank_text_item() -> RedacterDataItem {
        RedacterDataItem {
            file_ref: FileSystemRef {
                relative_path: "blank.txt".into(),
                media_type: Some(mime::TEXT_PLAIN),
                file_size: Some(3),
            },
            content: RedacterDataItemContent::Value(" \n\t".to_string()),
        }
    }

    /// The redacter's service is unreachable, so only a guard in front of it can succeed.
    async fn redacter_with_unreachable_service<'a>(reporter: &'a AppReporter<'a>) -> Redacters<'a> {
        let unreachable = url::Url::parse("http://127.0.0.1:1/analyze").unwrap();
        Redacters::MsPresidio(
            ms_presidio::MsPresidioRedacter::new(
                ms_presidio::MsPresidioRedacterOptions {
                    text_analyze_url: Some(unreachable),
                    image_redact_url: None,
                },
                reporter,
            )
            .await
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn blank_text_has_no_detections_and_is_not_sent_to_the_service() {
        let term = console::Term::stdout();
        let reporter = test_reporter(&term);
        let redacter = redacter_with_unreachable_service(&reporter).await;
        assert!(redacter
            .detect(&blank_text_item())
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn blank_text_is_redacted_to_itself_without_the_service() {
        let term = console::Term::stdout();
        let reporter = test_reporter(&term);
        let redacter = redacter_with_unreachable_service(&reporter).await;
        let redacted = redacter.redact(blank_text_item()).await.unwrap();
        assert!(matches!(
            redacted.content,
            RedacterDataItemContent::Value(text) if text == " \n\t"
        ));
    }

    fn test_reporter(term: &console::Term) -> AppReporter<'_> {
        AppReporter::from(term)
    }

    #[tokio::test]
    async fn auto_mode_verifies_a_successful_native_edit_with_coords() {
        let term = console::Term::stdout();
        let reporter = test_reporter(&term);
        let coords_saw = std::cell::RefCell::new(None);

        let result = redact_image_with_mode(
            LlmImageMode::Auto,
            dummy_image_item("input"),
            &reporter,
            |item| async move { Ok(dummy_image_item(&format!("{}-native", image_tag(&item)))) },
            |item| {
                let tag = image_tag(&item).to_string();
                *coords_saw.borrow_mut() = Some(tag.clone());
                async move { Ok(dummy_image_item(&format!("{tag}-coords"))) }
            },
        )
        .await
        .expect("the verification pass succeeds");

        assert_eq!(
            coords_saw.into_inner().as_deref(),
            Some("input-native"),
            "coords must verify the native output, not the original input"
        );
        assert_eq!(image_tag(&result), "input-native-coords");
    }

    #[tokio::test]
    async fn auto_mode_propagates_a_failed_verification_pass() {
        let term = console::Term::stdout();
        let reporter = test_reporter(&term);

        let result = redact_image_with_mode(
            LlmImageMode::Auto,
            dummy_image_item("input"),
            &reporter,
            |item| async move { Ok(item) },
            |_item| async move {
                Err(AppError::SystemError {
                    message: "still legible".to_string(),
                })
            },
        )
        .await;

        match result {
            Err(AppError::SystemError { message }) => assert_eq!(message, "still legible"),
            other => panic!("expected the verification failure to propagate, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn native_mode_never_calls_coords() {
        let term = console::Term::stdout();
        let reporter = test_reporter(&term);
        let coords_called = std::cell::Cell::new(false);

        let result = redact_image_with_mode(
            LlmImageMode::Native,
            dummy_image_item("input"),
            &reporter,
            |item| async move { Ok(dummy_image_item(&format!("{}-native", image_tag(&item)))) },
            |item| {
                coords_called.set(true);
                async move { Ok(item) }
            },
        )
        .await
        .expect("the native edit succeeds");

        assert!(!coords_called.get(), "coords must not run in native mode");
        assert_eq!(image_tag(&result), "input-native");
    }

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

    #[test]
    fn normalized_boxes_are_read_from_an_answer_and_a_short_box_is_left_out() {
        let answer =
            r#"[{"box_2d":[0,0,1000,500],"text":"Ana"},{"box_2d":[1,2,3],"text":"short"}]"#;

        let coords = NormalizedPiiBox::coords_from_answer(answer, 100, 200).unwrap();

        assert_eq!(coords.len(), 1);
        assert_eq!(coords[0].text.as_deref(), Some("Ana"));
        assert_eq!((coords[0].x2, coords[0].y2), (50.0, 200.0));
    }

    #[test]
    fn an_unreadable_box_answer_is_an_error_without_the_content_it_could_not_read() {
        let error = NormalizedPiiBox::coords_from_answer(
            r#"[{"box_2d":"John Smith","text":null}]"#,
            100,
            100,
        )
        .expect_err("box_2d is not an array");

        assert!(matches!(error, AppError::UnreadableModelAnswer { .. }));
        assert!(!error.to_string().contains("John"));
    }

    #[test]
    fn normalized_box_converts_to_pixel_coordinates() {
        let coords = normalized_box_to_image_coords([233.0, 98.0, 248.0, 345.0], 740, 1018, None);
        assert!((coords.x1 - 72.5).abs() < 0.5, "x1 = {}", coords.x1);
        assert!((coords.y1 - 237.2).abs() < 0.5, "y1 = {}", coords.y1);
        assert!((coords.x2 - 255.3).abs() < 0.5, "x2 = {}", coords.x2);
        assert!((coords.y2 - 252.5).abs() < 0.5, "y2 = {}", coords.y2);
    }

    #[test]
    fn normalized_box_clamps_out_of_range_values() {
        let coords = normalized_box_to_image_coords([-50.0, -10.0, 2000.0, 1500.0], 100, 200, None);
        assert_eq!(coords.x1, 0.0);
        assert_eq!(coords.y1, 0.0);
        assert_eq!(coords.x2, 100.0);
        assert_eq!(coords.y2, 200.0);
    }

    #[test]
    fn normalized_box_keeps_degenerate_boxes_ordered() {
        // ymax/xmax reversed with ymin/xmin: the box is degenerate, but the pixel
        // coordinates it produces must still be ordered (x1 <= x2, y1 <= y2).
        let coords = normalized_box_to_image_coords([500.0, 500.0, 100.0, 100.0], 1000, 1000, None);
        assert!(coords.x1 <= coords.x2);
        assert!(coords.y1 <= coords.y2);
    }

    #[test]
    fn normalized_box_carries_the_detected_text_through() {
        let coords = normalized_box_to_image_coords(
            [0.0, 0.0, 1000.0, 1000.0],
            10,
            10,
            Some("14 March 1985".to_string()),
        );
        assert_eq!(coords.text.as_deref(), Some("14 March 1985"));
    }

    #[test]
    fn image_boxes_are_reported_in_pixels_of_the_image_as_given() {
        let found = ImagePii {
            image: PreparedImage {
                format: image::ImageFormat::Png,
                data: Default::default(),
                width: 1024,
                height: 737,
                original_width: 2048,
                original_height: 1474,
            },
            coords: vec![
                TextImageCoords {
                    x1: 300.0,
                    y1: 80.0,
                    x2: 100.0,
                    y2: 50.0,
                    text: Some("X1234567".to_string()),
                    line: 0,
                },
                TextImageCoords {
                    x1: 900.0,
                    y1: 700.0,
                    x2: 1100.0,
                    y2: 800.0,
                    text: None,
                    line: 0,
                },
            ],
        };

        let detections = found.into_detections();

        assert_eq!(detections[0].value, "X1234567");
        assert_eq!(
            detections[0].location,
            DetectionLocation::Image {
                page: None,
                x1: 200.0,
                y1: 100.0,
                x2: 600.0,
                y2: 160.0
            },
            "a reversed box is ordered and doubled to the original size"
        );
        assert_eq!(
            detections[1].location,
            DetectionLocation::Image {
                page: None,
                x1: 1800.0,
                y1: 1400.0,
                x2: 2048.0,
                y2: 1474.0
            },
            "a box beyond the image is clamped to it"
        );
    }
}
