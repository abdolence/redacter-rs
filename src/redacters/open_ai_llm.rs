use base64::Engine;
use rand::RngExt;
use rvstruct::ValueStruct;
use serde::{Deserialize, Serialize};

use crate::args::RedacterType;
use crate::common_types::TextImageCoords;
use crate::errors::AppError;
use crate::file_systems::FileSystemRef;
use crate::redacters::{
    classify_http_native_failure, parse_answer, prepare_image_for_llm, redact_image_at_coords,
    redact_image_with_mode, sensitive_data_examples, text_redaction_instruction, Detections,
    ImageBoxes, LlmImageMode, LlmTextFinding, ModelAnswer, ModelStop, NativeImageEditError,
    RedactSupport, Redacter, RedacterDataItem, RedacterDataItemContent, Redacters,
    TextDetectionPrompt, NATIVE_IMAGE_REDACTION_PROMPT,
};
use crate::reporter::AppReporter;
use crate::AppResult;

#[derive(Debug, Clone, ValueStruct)]
pub struct OpenAiLlmApiKey(String);

#[derive(Debug, Clone, ValueStruct)]
pub struct OpenAiModelName(String);

#[derive(Debug, Clone)]
pub struct OpenAiLlmRedacterOptions {
    pub api_key: OpenAiLlmApiKey,
    pub model: Option<OpenAiModelName>,
    pub image_model: Option<OpenAiModelName>,
    pub image_mode: LlmImageMode,
}

#[derive(Clone)]
pub struct OpenAiLlmRedacter<'a> {
    client: reqwest::Client,
    open_ai_llm_options: OpenAiLlmRedacterOptions,
    reporter: &'a AppReporter<'a>,
}

#[derive(Serialize, Clone, Debug)]
struct OpenAiLlmAnalyzeRequest {
    model: String,
    messages: Vec<OpenAiLlmAnalyzeMessageRequest>,
    response_format: Option<OpenAiLlmResponseFormat>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct OpenAiLlmAnalyzeMessageRequest {
    role: String,
    content: Vec<OpenAiLlmAnalyzeMessageContent>,
}

/// The message of a choice. A refusal or a filtered answer has no content.
#[derive(Deserialize, Clone, Debug)]
struct OpenAiLlmAnalyzeMessageResponse {
    content: Option<String>,
    refusal: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum OpenAiLlmAnalyzeMessageContent {
    Text {
        text: String,
    },
    ImageUrl {
        image_url: OpenAiLlmAnalyzeMessageContentUrl,
    },
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct OpenAiLlmAnalyzeMessageContentUrl {
    url: String,
}

#[derive(Deserialize)]
struct OpenAiErrorResponse {
    error: OpenAiErrorBody,
}

/// The identifying fields of an API error; its `message` is left out on purpose.
#[derive(Deserialize)]
struct OpenAiErrorBody {
    r#type: Option<String>,
    code: Option<String>,
}

#[derive(Deserialize, Clone, Debug)]
struct OpenAiLlmAnalyzeResponse {
    choices: Vec<OpenAiLlmAnalyzeChoice>,
}

#[derive(Deserialize, Clone, Debug)]
struct OpenAiLlmAnalyzeChoice {
    message: OpenAiLlmAnalyzeMessageResponse,
    finish_reason: Option<String>,
}

impl From<OpenAiLlmAnalyzeChoice> for ModelAnswer {
    fn from(choice: OpenAiLlmAnalyzeChoice) -> Self {
        let stop = match choice.finish_reason.as_deref() {
            Some("stop") if choice.message.refusal.is_some() => {
                Some(ModelStop::Withheld("refusal".to_string()))
            }
            Some("stop") => None,
            Some("length") => Some(ModelStop::MaxTokens),
            Some(other) => Some(ModelStop::Withheld(other.to_string())),
            None => Some(ModelStop::Withheld("no finish_reason".to_string())),
        };
        ModelAnswer::new(choice.message.content.unwrap_or_default(), stop)
    }
}

impl From<OpenAiLlmAnalyzeResponse> for ModelAnswer {
    fn from(mut response: OpenAiLlmAnalyzeResponse) -> Self {
        response.choices.pop().map_or_else(
            || {
                ModelAnswer::new(
                    String::new(),
                    Some(ModelStop::Withheld("no choices".to_string())),
                )
            },
            ModelAnswer::from,
        )
    }
}

#[derive(Serialize, Clone, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum OpenAiLlmResponseFormat {
    JsonSchema { json_schema: OpenAiLlmJsonSchema },
}

#[derive(Serialize, Clone, Debug)]
struct OpenAiLlmJsonSchema {
    name: String,
    schema: serde_json::Value,
}

#[derive(Deserialize, Clone, Debug)]
struct OpenAiLlmTextCoordsResponse {
    text_coords: Vec<TextImageCoords>,
}

/// The answer to the text detection request: OpenAI's response schemas must be objects, so
/// the findings sit under a key.
#[derive(Deserialize, Debug)]
struct OpenAiLlmTextFindings {
    findings: Vec<LlmTextFinding>,
}

#[derive(Deserialize, Clone, Debug)]
struct OpenAiImageEditResponse {
    data: Vec<OpenAiImageEditResponseItem>,
}

#[derive(Deserialize, Clone, Debug)]
struct OpenAiImageEditResponseItem {
    b64_json: Option<String>,
}

impl<'a> OpenAiLlmRedacter<'a> {
    /// Chat model, also used to locate sensitive data in images.
    const DEFAULT_MODEL: &'static str = "gpt-5.6-luna";
    /// Image editing model, used for the native image redaction path.
    const DEFAULT_IMAGE_MODEL: &'static str = "gpt-image-2";

    pub async fn new(
        open_ai_llm_options: OpenAiLlmRedacterOptions,
        reporter: &'a AppReporter<'a>,
    ) -> AppResult<Self> {
        let client = reqwest::Client::new();
        Ok(Self {
            client,
            open_ai_llm_options,
            reporter,
        })
    }

    pub async fn redact_text_file(&self, input: RedacterDataItem) -> AppResult<RedacterDataItem> {
        let text_content = match input.content {
            RedacterDataItemContent::Value(content) => Ok(content),
            _ => Err(AppError::SystemError {
                message: "Unsupported item for text redacting".to_string(),
            }),
        }?;

        let mut rand = rand::rng();
        let generate_random_text_separator = format!("---{}", rand.random::<u64>());

        let analyze_request = OpenAiLlmAnalyzeRequest {
            model: self.model_name(),
            messages: vec![
                OpenAiLlmAnalyzeMessageRequest {
                    role: "system".to_string(),
                    content: vec![OpenAiLlmAnalyzeMessageContent::Text {
                        text: text_redaction_instruction(&generate_random_text_separator),
                    }],
                },
                OpenAiLlmAnalyzeMessageRequest {
                    role: "system".to_string(),
                    content: vec![OpenAiLlmAnalyzeMessageContent::Text {
                        text: format!("{}\n", generate_random_text_separator),
                    }],
                },
                OpenAiLlmAnalyzeMessageRequest {
                    role: "user".to_string(),
                    content: vec![OpenAiLlmAnalyzeMessageContent::Text { text: text_content }],
                },
                OpenAiLlmAnalyzeMessageRequest {
                    role: "system".to_string(),
                    content: vec![OpenAiLlmAnalyzeMessageContent::Text {
                        text: format!("{}\n", generate_random_text_separator),
                    }],
                },
            ],
            response_format: None,
        };
        let redacted_content = self
            .complete(&analyze_request, "redact the text")
            .await?
            .into_text_without_separator(&generate_random_text_separator)?;
        Ok(RedacterDataItem {
            file_ref: input.file_ref,
            content: RedacterDataItemContent::Value(redacted_content),
        })
    }

    /// Asks the model for the boxes of sensitive data in the image, which is scaled
    /// down to the model's input limit first.
    async fn find_image_boxes(
        &self,
        mime_type: &mime::Mime,
        data: &gcloud_sdk::prost::bytes::Bytes,
    ) -> AppResult<ImageBoxes> {
        let prepared_image = prepare_image_for_llm(mime_type, data)?;

        let analyze_request = OpenAiLlmAnalyzeRequest {
            model: self.model_name(),
            messages: vec![
                OpenAiLlmAnalyzeMessageRequest {
                    role: "system".to_string(),
                    content: vec![OpenAiLlmAnalyzeMessageContent::Text {
                        text: format!(
                            concat!(
                                "Find anything in the attached image that looks like personal or sensitive information, such as ",
                                sensitive_data_examples!(),
                                ". Return their coordinates with x1,y1,x2,y2 as pixel coordinates and the corresponding text. \
                                 The coordinates should be in the format of the top left corner (x1, y1) and the bottom right corner (x2, y2). \
                                 The image width is: {}. The image height is: {}."
                            ),
                            prepared_image.width, prepared_image.height
                        )
                    }],
                },
                OpenAiLlmAnalyzeMessageRequest {
                    role: "user".to_string(),
                    content: vec![OpenAiLlmAnalyzeMessageContent::ImageUrl { image_url: OpenAiLlmAnalyzeMessageContentUrl {
                        url: format!("data:{};base64,{}", mime_type, base64::engine::general_purpose::STANDARD.encode(&prepared_image.data))
                    }}],
                },
            ],
            response_format: Some(OpenAiLlmResponseFormat::JsonSchema {
                json_schema: OpenAiLlmJsonSchema {
                    name: "image_redact".to_string(),
                    schema: serde_json::json!({
                        "type": "object",
                        "properties": {
                            "text_coords": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "x1": {
                                            "type": "number"
                                        },
                                        "y1": {
                                            "type": "number"
                                        },
                                        "x2": {
                                            "type": "number"
                                        },
                                        "y2": {
                                            "type": "number"
                                        },
                                        "text": {
                                            "type": "string"
                                        }
                                    },
                                    "required": ["x1", "y1", "x2", "y2"]
                                }
                            },
                        },
                        "required": ["text_coords"]
                    })
                }
            })
        };
        let answer = self
            .complete(&analyze_request, "locate the sensitive data")
            .await?
            .into_text()?;
        let pii_image_coords: OpenAiLlmTextCoordsResponse = parse_answer(&answer)?;
        Ok(ImageBoxes {
            image: prepared_image,
            coords: pii_image_coords.text_coords,
        })
    }

    pub async fn redact_image_file_using_coords(
        &self,
        input: RedacterDataItem,
    ) -> AppResult<RedacterDataItem> {
        match input.content {
            RedacterDataItemContent::Image { mime_type, data } => {
                let found = self.find_image_boxes(&mime_type, &data).await?;
                Ok(RedacterDataItem {
                    file_ref: input.file_ref,
                    content: RedacterDataItemContent::Image {
                        mime_type: mime_type.clone(),
                        data: redact_image_at_coords(
                            mime_type,
                            found.image.data,
                            found.coords,
                            0.25,
                        )?,
                    },
                })
            }
            _ => Err(AppError::SystemError {
                message: "Unsupported item for image redacting".to_string(),
            }),
        }
    }

    /// The error of a rejected request. It carries the status and OpenAI's error type and code
    /// but not the body: the message of an API error can quote the scanned text, and both the
    /// error and the log reach the terminal and the scan report.
    fn request_failure(operation: &str, status: reqwest::StatusCode, body: &str) -> AppError {
        let identifiers = serde_json::from_str::<OpenAiErrorResponse>(body)
            .map(|response| [response.error.r#type, response.error.code])
            .unwrap_or_default();
        let identifiers: Vec<String> = identifiers.into_iter().flatten().collect();
        tracing::debug!(%status, ?identifiers, operation, "OpenAI rejected the request");
        let mut message = format!("Failed to {operation}. HTTP status: {status}.");
        if !identifiers.is_empty() {
            message.push_str(&format!(" OpenAI error: {}.", identifiers.join(", ")));
        }
        AppError::SystemError { message }
    }

    /// Sends a chat completion request and returns its last choice with how it ended.
    /// `operation` names what the request was for in the error of a rejected one.
    async fn complete(
        &self,
        request: &OpenAiLlmAnalyzeRequest,
        operation: &str,
    ) -> AppResult<ModelAnswer> {
        let response = self
            .client
            .post("https://api.openai.com/v1/chat/completions")
            .header(
                "Authorization",
                format!("Bearer {}", self.open_ai_llm_options.api_key.value()),
            )
            .json(request)
            .send()
            .await?;
        if !response.status().is_success()
            || response
                .headers()
                .get("content-type")
                .iter()
                .all(|v| *v != mime::APPLICATION_JSON.as_ref())
        {
            let response_status = response.status();
            let response_text = response.text().await.unwrap_or_default();
            return Err(Self::request_failure(
                operation,
                response_status,
                &response_text,
            ));
        }
        let open_ai_response: OpenAiLlmAnalyzeResponse = response.json().await?;
        Ok(open_ai_response.into())
    }

    fn model_name(&self) -> String {
        self.open_ai_llm_options
            .model
            .as_ref()
            .map(|model| model.value().clone())
            .unwrap_or_else(|| Self::DEFAULT_MODEL.to_string())
    }

    async fn detect_in_text(&self, text: &str) -> AppResult<Detections> {
        let system_message = |text: String| OpenAiLlmAnalyzeMessageRequest {
            role: "system".to_string(),
            content: vec![OpenAiLlmAnalyzeMessageContent::Text { text }],
        };
        let prompt = TextDetectionPrompt::new(text);
        let request = OpenAiLlmAnalyzeRequest {
            model: self.model_name(),
            messages: vec![
                system_message(prompt.instruction),
                system_message(prompt.fence.clone()),
                OpenAiLlmAnalyzeMessageRequest {
                    role: "user".to_string(),
                    content: vec![OpenAiLlmAnalyzeMessageContent::Text { text: prompt.text }],
                },
                system_message(prompt.fence),
            ],
            response_format: Some(OpenAiLlmResponseFormat::JsonSchema {
                json_schema: OpenAiLlmJsonSchema {
                    name: "text_detect".to_string(),
                    schema: serde_json::json!({
                        "type": "object",
                        "properties": {
                            "findings": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "text": { "type": "string" },
                                        "type": { "type": "string" }
                                    },
                                    "required": ["text", "type"]
                                }
                            }
                        },
                        "required": ["findings"]
                    }),
                },
            }),
        };
        let answer = self
            .complete(&request, "find the sensitive data")
            .await?
            .into_text()?;
        let findings: OpenAiLlmTextFindings = parse_answer(&answer)?;
        LlmTextFinding::locate(text, findings.findings)
    }

    /// Output format accepted by the images/edits endpoint for the given input format.
    fn native_output_format(format: image::ImageFormat) -> &'static str {
        match format {
            image::ImageFormat::Jpeg => "jpeg",
            image::ImageFormat::WebP => "webp",
            _ => "png",
        }
    }

    pub async fn redact_image_file_natively(
        &self,
        input: RedacterDataItem,
    ) -> Result<RedacterDataItem, NativeImageEditError> {
        let model_name = self
            .open_ai_llm_options
            .image_model
            .as_ref()
            .map(|model_name| model_name.value().clone())
            .unwrap_or_else(|| Self::DEFAULT_IMAGE_MODEL.to_string());

        match input.content {
            RedacterDataItemContent::Image { mime_type, data } => {
                let prepared_image = prepare_image_for_llm(&mime_type, &data)?;
                let output_format = Self::native_output_format(prepared_image.format);
                let image_part = reqwest::multipart::Part::bytes(prepared_image.data.to_vec())
                    .file_name(input.file_ref.relative_path.filename())
                    .mime_str(mime_type.as_ref())
                    .map_err(AppError::from)?;
                let form = reqwest::multipart::Form::new()
                    .part("image", image_part)
                    .text("prompt", NATIVE_IMAGE_REDACTION_PROMPT)
                    .text("model", model_name)
                    .text("n", "1")
                    .text("output_format", output_format);

                let response = self
                    .client
                    .post("https://api.openai.com/v1/images/edits")
                    .header(
                        "Authorization",
                        format!("Bearer {}", self.open_ai_llm_options.api_key.value()),
                    )
                    .multipart(form)
                    .send()
                    .await
                    .map_err(AppError::from)?;

                if !response.status().is_success() {
                    let response_status = response.status();
                    let response_text = response.text().await.unwrap_or_default();
                    return Err(NativeImageEditError {
                        failure: classify_http_native_failure(response_status.as_u16()),
                        error: Self::request_failure(
                            "edit the image",
                            response_status,
                            &response_text,
                        ),
                    });
                }

                let edit_response: OpenAiImageEditResponse =
                    response.json().await.map_err(AppError::from)?;
                match edit_response
                    .data
                    .into_iter()
                    .find_map(|item| item.b64_json)
                {
                    Some(encoded_image) => {
                        let redacted_image_data = base64::engine::general_purpose::STANDARD
                            .decode(encoded_image)
                            .map_err(|err| {
                                NativeImageEditError::fatal(AppError::SystemError {
                                    message: format!("Failed to decode the edited image: {err}"),
                                })
                            })?;
                        let redacted_mime_type: mime::Mime = format!("image/{output_format}")
                            .parse()
                            .map_err(AppError::from)?;
                        Ok(RedacterDataItem {
                            file_ref: input.file_ref,
                            content: RedacterDataItemContent::Image {
                                mime_type: redacted_mime_type,
                                data: redacted_image_data.into(),
                            },
                        })
                    }
                    None => Err(NativeImageEditError::no_image_in_response()),
                }
            }
            _ => Err(NativeImageEditError::fatal(AppError::SystemError {
                message: "Unsupported item for image redacting".to_string(),
            })),
        }
    }
}

impl<'a> Redacter for OpenAiLlmRedacter<'a> {
    async fn redact(&self, input: RedacterDataItem) -> AppResult<RedacterDataItem> {
        match &input.content {
            RedacterDataItemContent::Value(_) => self.redact_text_file(input).await,
            RedacterDataItemContent::Image { .. } => {
                redact_image_with_mode(
                    self.open_ai_llm_options.image_mode,
                    input,
                    self.reporter,
                    |item| self.redact_image_file_natively(item),
                    |item| self.redact_image_file_using_coords(item),
                )
                .await
            }
            RedacterDataItemContent::Table { .. } | RedacterDataItemContent::Pdf { .. } => {
                Err(AppError::unsupported_content())
            }
        }
    }

    async fn redact_support(&self, file_ref: &FileSystemRef) -> AppResult<RedactSupport> {
        Ok(match file_ref.media_type.as_ref() {
            Some(media_type) if Redacters::is_mime_text(media_type) => RedactSupport::Supported,
            Some(media_type) if Redacters::is_mime_image(media_type) => RedactSupport::Supported,
            _ => RedactSupport::Unsupported,
        })
    }

    async fn detect(&self, input: &RedacterDataItem) -> AppResult<Detections> {
        match &input.content {
            RedacterDataItemContent::Value(text) => self.detect_in_text(text).await,
            RedacterDataItemContent::Image { mime_type, data } => {
                self.find_image_boxes(mime_type, data)
                    .await?
                    .into_labelled_detections(
                        |text| async move { self.detect_in_text(&text).await },
                    )
                    .await
            }
            RedacterDataItemContent::Table { .. } | RedacterDataItemContent::Pdf { .. } => {
                Err(AppError::unsupported_content())
            }
        }
    }

    fn redacter_type(&self) -> RedacterType {
        RedacterType::OpenAiLlm
    }
}

#[allow(unused_imports)]
#[cfg(test)]
mod tests {
    use console::Term;

    use crate::redacters::live_detection;
    use crate::redacters::test_support::{
        check_and_save_redacted_image, initialize_crypto, test_image_item,
    };
    use crate::redacters::{DetectionLocation, RedacterProviderOptions};

    use super::*;

    const PLANTED_VALUE: &str = "jane.doe@example.com";

    #[test]
    fn a_failed_request_does_not_repeat_the_response_body() {
        let bodies = [
            format!(
                r#"{{"error": {{"message": "Invalid input {PLANTED_VALUE}", "type": "invalid_request_error", "code": "string_above_max_length"}}}}"#
            ),
            format!("upstream said: {PLANTED_VALUE}"),
        ];
        for body in bodies {
            let error = OpenAiLlmRedacter::request_failure(
                "analyze text",
                reqwest::StatusCode::BAD_REQUEST,
                &body,
            );
            assert!(!error.to_string().contains(PLANTED_VALUE), "{body}");
        }
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-open-ai"), ignore)]
    async fn redact_text_file_test() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let term = Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        initialize_crypto();
        let Some(test_api_key) = test_open_ai_key(&term)? else {
            return Ok(());
        };
        let test_content = "Hello, John";

        let file_ref = FileSystemRef {
            relative_path: "temp_file.txt".into(),
            media_type: Some(mime::TEXT_PLAIN),
            file_size: Some(test_content.len()),
        };

        let content = RedacterDataItemContent::Value(test_content.to_string());
        let input = RedacterDataItem { file_ref, content };

        let redacter = OpenAiLlmRedacter::new(
            OpenAiLlmRedacterOptions {
                api_key: test_api_key.into(),
                model: None,
                image_model: None,
                image_mode: LlmImageMode::Auto,
            },
            &reporter,
        )
        .await?;

        let redacted_item = redacter.redact(input).await?;
        match redacted_item.content {
            RedacterDataItemContent::Value(value) => {
                assert_eq!(value.trim(), "Hello, [REDACTED]");
            }
            _ => panic!("Unexpected redacted content type"),
        }

        Ok(())
    }

    /// The OpenAI key is not available on every machine: without it the tests are skipped.
    fn test_open_ai_key(term: &Term) -> Result<Option<String>, std::io::Error> {
        match std::env::var("TEST_OPEN_AI_KEY") {
            Ok(api_key) => Ok(Some(api_key)),
            Err(_) => {
                term.write_line("TEST_OPEN_AI_KEY is not set, skipping the test")?;
                Ok(None)
            }
        }
    }

    async fn redact_image_file_test_with_mode(
        image_mode: LlmImageMode,
        output_name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let term = Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        initialize_crypto();
        let Some(test_api_key) = test_open_ai_key(&term)? else {
            return Ok(());
        };

        let input = test_image_item();
        let redacter = OpenAiLlmRedacter::new(
            OpenAiLlmRedacterOptions {
                api_key: test_api_key.into(),
                model: None,
                image_model: None,
                image_mode,
            },
            &reporter,
        )
        .await?;

        let redacted_item = redacter.redact(input.clone()).await?;
        let output_path = check_and_save_redacted_image(&input, &redacted_item, output_name);
        term.write_line(&format!(
            "Redacted image written to {}",
            output_path.display()
        ))?;

        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-open-ai"), ignore)]
    async fn redact_image_file_auto_mode_test(
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        redact_image_file_test_with_mode(LlmImageMode::Auto, "open-ai-auto.png").await
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-open-ai"), ignore)]
    async fn redact_image_file_coords_mode_test(
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        redact_image_file_test_with_mode(LlmImageMode::Coords, "open-ai-coords.png").await
    }

    fn answer_of(response: &str) -> AppResult<String> {
        let response: OpenAiLlmAnalyzeResponse = serde_json::from_str(response).unwrap();
        ModelAnswer::from(response).into_text()
    }

    #[test]
    fn an_answer_that_did_not_finish_is_an_error_and_not_an_empty_scan() {
        let content_filter = r#"{"choices":[{"message":{"role":"assistant","content":null},"finish_reason":"content_filter"}]}"#;
        let refusal = r#"{"choices":[{"message":{"role":"assistant","content":null,"refusal":"No."},"finish_reason":"stop"}]}"#;
        let no_finish_reason = r#"{"choices":[{"message":{"role":"assistant","content":"{\"findings\":[]}"},"finish_reason":null}]}"#;
        let no_choices = r#"{"choices":[]}"#;
        let truncated = r#"{"choices":[{"message":{"role":"assistant","content":"{\"findings\": [{"},"finish_reason":"length"}]}"#;
        for (response, reaches_output_limit) in [
            (content_filter, false),
            (refusal, false),
            (no_finish_reason, false),
            (no_choices, false),
            (truncated, true),
        ] {
            let Err(AppError::ModelAnswerStopped(stop)) = answer_of(response) else {
                panic!("{response} is not an error");
            };
            assert_eq!(
                stop == ModelStop::MaxTokens,
                reaches_output_limit,
                "{response}"
            );
        }
    }

    #[test]
    fn a_redaction_that_is_only_the_separators_is_an_error() {
        let echoed = r#"{"choices":[{"message":{"role":"assistant","content":"---1234\n---1234\n"},"finish_reason":"stop"}]}"#;
        let response: OpenAiLlmAnalyzeResponse = serde_json::from_str(echoed).unwrap();
        assert!(matches!(
            ModelAnswer::from(response).into_text_without_separator("---1234"),
            Err(AppError::EmptyModelAnswer)
        ));
    }

    #[test]
    fn a_completed_answer_is_its_content() {
        let completed = r#"{"choices":[{"message":{"role":"assistant","content":"{\"findings\":[]}"},"finish_reason":"stop"}]}"#;
        assert_eq!(answer_of(completed).unwrap(), r#"{"findings":[]}"#);
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-open-ai"), ignore)]
    async fn detect_text_file_test() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let term = Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        initialize_crypto();
        let Some(test_api_key) = test_open_ai_key(&term)? else {
            return Ok(());
        };
        let redacter = OpenAiLlmRedacter::new(
            OpenAiLlmRedacterOptions {
                api_key: test_api_key.into(),
                model: None,
                image_model: None,
                image_mode: LlmImageMode::Auto,
            },
            &reporter,
        )
        .await?;

        let input = live_detection::note_item();
        let detections = redacter.detect(&input).await?.found;

        live_detection::assert_note_found(&input, &detections);
        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-open-ai"), ignore)]
    async fn detect_image_file_test() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let term = Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        initialize_crypto();
        let Some(test_api_key) = test_open_ai_key(&term)? else {
            return Ok(());
        };
        let redacter = OpenAiLlmRedacter::new(
            OpenAiLlmRedacterOptions {
                api_key: test_api_key.into(),
                model: None,
                image_model: None,
                image_mode: LlmImageMode::Auto,
            },
            &reporter,
        )
        .await?;

        let detections = redacter
            .detect(&live_detection::enlarged_form_item())
            .await?
            .found;

        live_detection::assert_form_found(&detections);
        Ok(())
    }
}
