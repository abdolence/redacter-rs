use crate::args::RedacterType;
use crate::common_types::{GcpProjectId, GcpRegion};
use crate::errors::AppError;
use crate::file_systems::FileSystemRef;
use crate::redacters::{
    prepare_image_for_llm, redact_image_with_mode, retrying_once, text_redaction_instruction,
    Categories, Detections, GeminiCandidate, ImageBoxes, LlmImageMode, LlmTextFinding, ModelAnswer,
    NativeImageEditError, NormalizedPiiBox, RedactSupport, Redacted, Redacter, RedacterDataItem,
    RedacterDataItemContent, Redacters, TextDetectionPrompt, GEMINI_COORDS_REDACTION_PROMPT,
    NATIVE_IMAGE_REDACTION_PROMPT,
};
use crate::reporter::AppReporter;
use crate::AppResult;
use gcloud_sdk::google::cloud::aiplatform::v1 as vertex_api;
use gcloud_sdk::{tonic, GoogleApi, GoogleAuthMiddleware};
use mime::Mime;
use rand::RngExt;
use rvstruct::ValueStruct;

#[derive(Debug, Clone)]
pub struct GcpVertexAiRedacterOptions {
    pub project_id: GcpProjectId,
    pub gcp_region: GcpRegion,
    pub image_mode: LlmImageMode,
    pub text_model: Option<GcpVertexAiModelName>,
    pub image_model: Option<GcpVertexAiModelName>,
    pub block_none_harmful: bool,
    /// The categories to look for; every category when `None`.
    #[expect(
        dead_code,
        reason = "read once the redacter narrows its detection by category"
    )]
    pub categories: Option<Categories>,
}

#[derive(Debug, Clone, ValueStruct)]
pub struct GcpVertexAiModelName(String);

fn text_part(text: String) -> vertex_api::Part {
    vertex_api::Part {
        data: Some(vertex_api::part::Data::Text(text)),
        ..std::default::Default::default()
    }
}

fn schema_of(r#type: vertex_api::Type) -> vertex_api::Schema {
    vertex_api::Schema {
        r#type: r#type.into(),
        ..std::default::Default::default()
    }
}

/// Schema of an array of objects whose listed properties are all required.
fn findings_schema(properties: Vec<(&str, vertex_api::Schema)>) -> vertex_api::Schema {
    vertex_api::Schema {
        r#type: vertex_api::Type::Array.into(),
        items: Some(Box::new(vertex_api::Schema {
            r#type: vertex_api::Type::Object.into(),
            required: properties
                .iter()
                .map(|(name, _)| name.to_string())
                .collect(),
            properties: properties
                .into_iter()
                .map(|(name, schema)| (name.to_string(), schema))
                .collect(),
            ..std::default::Default::default()
        })),
        ..std::default::Default::default()
    }
}

impl From<vertex_api::GenerateContentResponse> for ModelAnswer {
    fn from(response: vertex_api::GenerateContentResponse) -> Self {
        let prompt_block_reason = response
            .prompt_feedback
            .as_ref()
            .map(|feedback| feedback.block_reason())
            .filter(|reason| {
                *reason != vertex_api::generate_content_response::prompt_feedback::BlockedReason::Unspecified
            })
            .map(|reason| reason.as_str_name());
        let candidate = response
            .candidates
            .first()
            .map(|candidate| GeminiCandidate {
                finish_reason: vertex_api::candidate::FinishReason::try_from(
                    candidate.finish_reason,
                )
                .map(|reason| reason.as_str_name())
                .map_err(|unknown| unknown.0),
                blocked_categories: candidate
                    .safety_ratings
                    .iter()
                    .filter(|rating| rating.blocked)
                    .map(|rating| rating.category().as_str_name().to_string())
                    .collect(),
                text: candidate
                    .content
                    .iter()
                    .flat_map(|content| &content.parts)
                    .filter_map(|part| match &part.data {
                        Some(vertex_api::part::Data::Text(text)) => Some(text.as_str()),
                        _ => None,
                    })
                    .collect(),
            });
        ModelAnswer::from_gemini(candidate, prompt_block_reason)
    }
}

#[derive(Clone)]
pub struct GcpVertexAiRedacter<'a> {
    client: GoogleApi<gcloud_sdk::google::cloud::aiplatform::v1::prediction_service_client::PredictionServiceClient<GoogleAuthMiddleware>>,
    options: GcpVertexAiRedacterOptions,
    reporter: &'a AppReporter<'a>,
    safety_setting: gcloud_sdk::google::cloud::aiplatform::v1::safety_setting::HarmBlockThreshold,
}

/// Endpoint host serving Vertex AI in the given location.
///
/// The `global`, `us` and `eu` multi-regions are served by the bare
/// `aiplatform.googleapis.com` host; every other location has its own
/// regional host.
pub fn vertex_ai_endpoint(region: &GcpRegion) -> String {
    match region.value().as_str() {
        "global" | "us" | "eu" => "https://aiplatform.googleapis.com".to_string(),
        regional_location => format!("https://{regional_location}-aiplatform.googleapis.com"),
    }
}

impl<'a> GcpVertexAiRedacter<'a> {
    /// Text model, also used to locate sensitive data in images.
    const DEFAULT_TEXT_MODEL: &'static str = "publishers/google/models/gemini-3.8-flash";
    /// Image editing model, used for the native image redaction path.
    const DEFAULT_IMAGE_MODEL: &'static str = "publishers/google/models/gemini-3.1-flash-image";

    pub async fn new(
        options: GcpVertexAiRedacterOptions,
        reporter: &'a AppReporter<'a>,
    ) -> AppResult<Self> {
        let client =
            GoogleApi::from_function(
                gcloud_sdk::google::cloud::aiplatform::v1::prediction_service_client::PredictionServiceClient::new,
                vertex_ai_endpoint(&options.gcp_region),
                None,
            ).await?;

        let safety_setting = if options.block_none_harmful {
            gcloud_sdk::google::cloud::aiplatform::v1::safety_setting::HarmBlockThreshold::BlockNone
        } else {
            gcloud_sdk::google::cloud::aiplatform::v1::safety_setting::HarmBlockThreshold::BlockOnlyHigh
        };

        Ok(GcpVertexAiRedacter {
            client,
            options,
            reporter,
            safety_setting,
        })
    }

    fn text_model_path(&self) -> String {
        let model_name = self
            .options
            .text_model
            .as_ref()
            .map(|model_name| model_name.value().to_string())
            .unwrap_or_else(|| Self::DEFAULT_TEXT_MODEL.to_string());
        format!(
            "projects/{}/locations/{}/{}",
            self.options.project_id.value(),
            self.options.gcp_region.value(),
            model_name
        )
    }

    pub async fn redact_text_file(&self, input: RedacterDataItem) -> AppResult<RedacterDataItem> {
        let model_path = self.text_model_path();

        let mut rand = rand::rng();
        let generate_random_text_separator = format!("---{}", rand.random::<u64>());

        match input.content {
            RedacterDataItemContent::Value(input_content) => {
                let request =
                    gcloud_sdk::google::cloud::aiplatform::v1::GenerateContentRequest {
                        model: model_path,
                        safety_settings: vec![
                            gcloud_sdk::google::cloud::aiplatform::v1::HarmCategory::HateSpeech,
                            gcloud_sdk::google::cloud::aiplatform::v1::HarmCategory::SexuallyExplicit,
                            gcloud_sdk::google::cloud::aiplatform::v1::HarmCategory::DangerousContent,
                            gcloud_sdk::google::cloud::aiplatform::v1::HarmCategory::Harassment,
                        ].into_iter().map(|category| gcloud_sdk::google::cloud::aiplatform::v1::SafetySetting {
                            category: category.into(),
                            threshold: self.safety_setting.into(),
                            method: gcloud_sdk::google::cloud::aiplatform::v1::safety_setting::HarmBlockMethod::Unspecified.into(),
                        }).collect(),
                        contents: vec![
                            gcloud_sdk::google::cloud::aiplatform::v1::Content {
                                parts: vec![
                                    gcloud_sdk::google::cloud::aiplatform::v1::Part {
                                        data: Some(
                                            gcloud_sdk::google::cloud::aiplatform::v1::part::Data::Text(
                                                text_redaction_instruction(&generate_random_text_separator),
                                            ),
                                        ),
                                        ..std::default::Default::default()
                                    },
                                    gcloud_sdk::google::cloud::aiplatform::v1::Part {
                                        data: Some(
                                            gcloud_sdk::google::cloud::aiplatform::v1::part::Data::Text(
                                                format!("{}\n", generate_random_text_separator)
                                            )
                                        ),
                                        ..std::default::Default::default()
                                    },
                                    gcloud_sdk::google::cloud::aiplatform::v1::Part {
                                        data: Some(
                                            gcloud_sdk::google::cloud::aiplatform::v1::part::Data::Text(
                                                input_content,
                                            ),
                                        ),
                                        ..std::default::Default::default()
                                    },
                                    gcloud_sdk::google::cloud::aiplatform::v1::Part {
                                        data: Some(
                                            gcloud_sdk::google::cloud::aiplatform::v1::part::Data::Text(
                                                format!("{}\n", generate_random_text_separator)
                                            )
                                        ),
                                        ..std::default::Default::default()
                                    }
                                ],
                                role: "user".to_string(),
                            },
                        ],
                        generation_config: Some(
                            gcloud_sdk::google::cloud::aiplatform::v1::GenerationConfig {
                                candidate_count: Some(1),
                                temperature: Some(0.2),
                                ..std::default::Default::default()
                            },
                        ),
                        ..std::default::Default::default()
                    };
                let redacted_content_text =
                    retrying_once(|| async { self.generate(request.clone()).await?.into_text() })
                        .await?;
                Ok(RedacterDataItem {
                    file_ref: input.file_ref,
                    content: RedacterDataItemContent::Value(redacted_content_text),
                })
            }
            _ => Err(AppError::SystemError {
                message: "Unsupported item for text redacting".to_string(),
            }),
        }
    }

    /// Sends `request` and returns the first candidate's text with how it ended.
    async fn generate(
        &self,
        request: vertex_api::GenerateContentRequest,
    ) -> AppResult<ModelAnswer> {
        let mut request = tonic::Request::new(request);
        request.metadata_mut().insert(
            "x-goog-user-project",
            gcloud_sdk::tonic::metadata::MetadataValue::<tonic::metadata::Ascii>::try_from(
                self.options.project_id.as_ref(),
            )?,
        );
        let response = self.client.get().generate_content(request).await?;
        Ok(response.into_inner().into())
    }

    /// Asks the text model for JSON conforming to `response_schema` and returns its answer.
    /// An answer that was filtered, cut short or empty is an error.
    async fn generate_json(
        &self,
        parts: Vec<vertex_api::Part>,
        response_schema: vertex_api::Schema,
    ) -> AppResult<String> {
        let request = vertex_api::GenerateContentRequest {
            model: self.text_model_path(),
            safety_settings: [
                vertex_api::HarmCategory::HateSpeech,
                vertex_api::HarmCategory::SexuallyExplicit,
                vertex_api::HarmCategory::DangerousContent,
                vertex_api::HarmCategory::Harassment,
            ]
            .into_iter()
            .map(|category| vertex_api::SafetySetting {
                category: category.into(),
                threshold: self.safety_setting.into(),
                method: vertex_api::safety_setting::HarmBlockMethod::Unspecified.into(),
            })
            .collect(),
            contents: vec![vertex_api::Content {
                parts,
                role: "user".to_string(),
            }],
            generation_config: Some(vertex_api::GenerationConfig {
                candidate_count: Some(1),
                temperature: Some(0.2),
                response_mime_type: mime::APPLICATION_JSON.to_string(),
                response_schema: Some(response_schema),
                ..std::default::Default::default()
            }),
            ..std::default::Default::default()
        };
        retrying_once(|| async { self.generate(request.clone()).await?.into_text() }).await
    }

    async fn detect_in_text(&self, text: &str) -> AppResult<Detections> {
        let prompt = TextDetectionPrompt::new(text);
        let answer = self
            .generate_json(
                vec![
                    text_part(prompt.instruction),
                    text_part(prompt.fence.clone()),
                    text_part(prompt.text),
                    text_part(prompt.fence),
                ],
                findings_schema(vec![
                    ("text", schema_of(vertex_api::Type::String)),
                    ("type", schema_of(vertex_api::Type::String)),
                ]),
            )
            .await?;
        LlmTextFinding::detections_from_answer(&answer, text)
    }

    pub async fn redact_image_file_natively(
        &self,
        input: RedacterDataItem,
    ) -> Result<RedacterDataItem, NativeImageEditError> {
        let model_name = self
            .options
            .image_model
            .as_ref()
            .map(|model_name| model_name.value().to_string())
            .unwrap_or_else(|| Self::DEFAULT_IMAGE_MODEL.to_string());

        let model_path = format!(
            "projects/{}/locations/{}/{}",
            self.options.project_id.value(),
            self.options.gcp_region.value(),
            model_name
        );

        match input.content {
            RedacterDataItemContent::Image { mime_type, data } => {
                let prepared_image = prepare_image_for_llm(&mime_type, &data)?;

                let mut request = tonic::Request::new(
                    gcloud_sdk::google::cloud::aiplatform::v1::GenerateContentRequest {
                        model: model_path,
                        safety_settings: vec![
                            gcloud_sdk::google::cloud::aiplatform::v1::HarmCategory::HateSpeech,
                            gcloud_sdk::google::cloud::aiplatform::v1::HarmCategory::SexuallyExplicit,
                            gcloud_sdk::google::cloud::aiplatform::v1::HarmCategory::DangerousContent,
                            gcloud_sdk::google::cloud::aiplatform::v1::HarmCategory::Harassment,
                        ].into_iter().map(|category| gcloud_sdk::google::cloud::aiplatform::v1::SafetySetting {
                            category: category.into(),
                            threshold: self.safety_setting.into(),
                            method: gcloud_sdk::google::cloud::aiplatform::v1::safety_setting::HarmBlockMethod::Unspecified.into(),
                        }).collect(),
                        contents: vec![
                            gcloud_sdk::google::cloud::aiplatform::v1::Content {
                                parts: vec![
                                    gcloud_sdk::google::cloud::aiplatform::v1::Part {
                                        data: Some(
                                            gcloud_sdk::google::cloud::aiplatform::v1::part::Data::Text(
                                                NATIVE_IMAGE_REDACTION_PROMPT.to_string(),
                                            ),
                                        ),
                                        metadata: None,
                                        ..std::default::Default::default()
                                    },
                                    gcloud_sdk::google::cloud::aiplatform::v1::Part {
                                        data: Some(
                                            gcloud_sdk::google::cloud::aiplatform::v1::part::Data::InlineData(
                                                gcloud_sdk::google::cloud::aiplatform::v1::Blob {
                                                    mime_type: mime_type.to_string(),
                                                    data: prepared_image.data.to_vec(),
                                                }
                                            ),
                                        ),
                                        metadata: None,
                                        ..std::default::Default::default()
                                    }
                                ],
                                role: "user".to_string(),
                            },
                        ],
                        generation_config: Some(
                            gcloud_sdk::google::cloud::aiplatform::v1::GenerationConfig {
                                candidate_count: Some(1),
                                response_modalities: vec![
                                    gcloud_sdk::google::cloud::aiplatform::v1::generation_config::Modality::Image as i32,
                                    gcloud_sdk::google::cloud::aiplatform::v1::generation_config::Modality::Text as i32,
                                ],
                                ..std::default::Default::default()
                            },
                        ),
                        ..std::default::Default::default()
                    },
                );
                request.metadata_mut().insert(
                    "x-goog-user-project",
                    gcloud_sdk::tonic::metadata::MetadataValue::<tonic::metadata::Ascii>::try_from(
                        self.options.project_id.as_ref(),
                    )
                    .map_err(AppError::from)?,
                );

                let response = self.client.get().generate_content(request).await?;

                let redacted_image_blob = response
                    .into_inner()
                    .candidates
                    .pop()
                    .and_then(|candidate| candidate.content)
                    .and_then(|content| {
                        content.parts.into_iter().find_map(|part| match part.data {
                            Some(
                                gcloud_sdk::google::cloud::aiplatform::v1::part::Data::InlineData(
                                    blob,
                                ),
                            ) => Some(blob),
                            _ => None,
                        })
                    });

                match redacted_image_blob {
                    Some(blob) => {
                        let redacted_mime_type: Mime =
                            blob.mime_type.parse().map_err(AppError::from)?;
                        Ok(RedacterDataItem {
                            file_ref: input.file_ref,
                            content: RedacterDataItemContent::Image {
                                mime_type: redacted_mime_type,
                                data: blob.data.into(),
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

    /// Asks the model for the boxes of sensitive data in the image, which is scaled
    /// down to the model's input limit first.
    async fn find_image_boxes(
        &self,
        mime_type: &Mime,
        data: &gcloud_sdk::prost::bytes::Bytes,
    ) -> AppResult<ImageBoxes> {
        let prepared_image = prepare_image_for_llm(mime_type, data)?;
        let answer = self
            .generate_json(
                vec![
                    text_part(GEMINI_COORDS_REDACTION_PROMPT.to_string()),
                    vertex_api::Part {
                        data: Some(vertex_api::part::Data::InlineData(vertex_api::Blob {
                            mime_type: mime_type.to_string(),
                            data: prepared_image.data.to_vec(),
                        })),
                        ..std::default::Default::default()
                    },
                ],
                findings_schema(vec![
                    (
                        "box_2d",
                        vertex_api::Schema {
                            min_items: 4,
                            items: Some(Box::new(schema_of(vertex_api::Type::Integer))),
                            ..schema_of(vertex_api::Type::Array)
                        },
                    ),
                    ("text", schema_of(vertex_api::Type::String)),
                ]),
            )
            .await?;
        let coords = NormalizedPiiBox::coords_from_answer(
            &answer,
            prepared_image.width,
            prepared_image.height,
        )?;
        Ok(ImageBoxes {
            image: prepared_image,
            coords,
        })
    }

    pub async fn redact_image_file_using_coords(
        &self,
        input: RedacterDataItem,
    ) -> AppResult<Redacted> {
        match input.content {
            RedacterDataItemContent::Image { mime_type, data } => {
                let found = self.find_image_boxes(&mime_type, &data).await?;
                found.redact(mime_type, input.file_ref, 0.20)
            }
            _ => Err(AppError::SystemError {
                message: "Unsupported item for image redacting".to_string(),
            }),
        }
    }
}

impl<'a> Redacter for GcpVertexAiRedacter<'a> {
    async fn redact(&self, input: RedacterDataItem) -> AppResult<Redacted> {
        match &input.content {
            RedacterDataItemContent::Value(_) => {
                self.redact_text_file(input).await.map(Redacted::unlocated)
            }
            RedacterDataItemContent::Image { .. } => {
                redact_image_with_mode(
                    self.options.image_mode,
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
        RedacterType::GcpVertexAi
    }
}

#[allow(unused_imports)]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::redacters::test_support::{
        check_and_save_redacted_image, initialize_crypto, test_image_item,
    };
    use crate::redacters::RedacterProviderOptions;
    use crate::redacters::{live_detection, ModelStop};
    use console::Term;

    #[test]
    fn vertex_ai_endpoint_test() {
        assert_eq!(
            vertex_ai_endpoint(&GcpRegion::new("global".to_string())),
            "https://aiplatform.googleapis.com"
        );
        assert_eq!(
            vertex_ai_endpoint(&GcpRegion::new("us".to_string())),
            "https://aiplatform.googleapis.com"
        );
        assert_eq!(
            vertex_ai_endpoint(&GcpRegion::new("eu".to_string())),
            "https://aiplatform.googleapis.com"
        );
        assert_eq!(
            vertex_ai_endpoint(&GcpRegion::new("us-central1".to_string())),
            "https://us-central1-aiplatform.googleapis.com"
        );
        assert_eq!(
            vertex_ai_endpoint(&GcpRegion::new("europe-west1".to_string())),
            "https://europe-west1-aiplatform.googleapis.com"
        );
    }

    fn response(
        finish_reason: vertex_api::candidate::FinishReason,
        text: &str,
        blocked: &[vertex_api::HarmCategory],
    ) -> vertex_api::GenerateContentResponse {
        let parts = if text.is_empty() {
            Vec::new()
        } else {
            vec![text_part(text.to_string())]
        };
        vertex_api::GenerateContentResponse {
            candidates: vec![vertex_api::Candidate {
                finish_reason: finish_reason.into(),
                content: Some(vertex_api::Content {
                    parts,
                    role: "model".to_string(),
                }),
                safety_ratings: blocked
                    .iter()
                    .map(|category| vertex_api::SafetyRating {
                        category: (*category).into(),
                        blocked: true,
                        ..std::default::Default::default()
                    })
                    .collect(),
                ..std::default::Default::default()
            }],
            ..std::default::Default::default()
        }
    }

    #[test]
    fn a_safety_stop_without_content_names_the_blocked_categories() {
        let stopped = response(
            vertex_api::candidate::FinishReason::Safety,
            "",
            &[vertex_api::HarmCategory::DangerousContent],
        );
        let error = ModelAnswer::from(stopped).into_text().unwrap_err();
        assert!(matches!(
            error,
            AppError::ModelAnswerStopped(ModelStop::Safety { ref categories })
                if !categories.is_empty()
        ));
    }

    #[test]
    fn a_response_without_candidates_names_the_block_reason_of_the_prompt() {
        let blocked = vertex_api::GenerateContentResponse {
            prompt_feedback: Some(vertex_api::generate_content_response::PromptFeedback {
                block_reason: vertex_api::generate_content_response::prompt_feedback::BlockedReason::ProhibitedContent.into(),
                ..std::default::Default::default()
            }),
            ..std::default::Default::default()
        };
        assert!(matches!(
            ModelAnswer::from(blocked).into_text(),
            Err(AppError::ModelAnswerStopped(ModelStop::PromptBlocked(_)))
        ));
    }

    #[test]
    fn a_finish_reason_this_build_does_not_know_is_a_stop() {
        let mut unknown = response(vertex_api::candidate::FinishReason::Stop, "[]", &[]);
        unknown.candidates[0].finish_reason = 9999;
        assert!(matches!(
            ModelAnswer::from(unknown).into_text(),
            Err(AppError::ModelAnswerStopped(ModelStop::Withheld(_)))
        ));
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-gcp-vertex-ai"), ignore)]
    async fn redact_text_file_test() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let term = Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        initialize_crypto();
        let test_gcp_project_id =
            std::env::var("TEST_GCP_PROJECT").expect("TEST_GCP_PROJECT required");
        let test_gcp_region =
            std::env::var("TEST_GCP_REGION").unwrap_or_else(|_| "global".to_string());
        let test_content = "Hello, John";

        let file_ref = FileSystemRef {
            relative_path: "temp_file.txt".into(),
            media_type: Some(mime::TEXT_PLAIN),
            file_size: Some(test_content.len()),
        };

        let content = RedacterDataItemContent::Value(test_content.to_string());
        let input = RedacterDataItem { file_ref, content };

        let redacter = GcpVertexAiRedacter::new(
            GcpVertexAiRedacterOptions {
                categories: None,
                project_id: GcpProjectId::new(test_gcp_project_id),
                gcp_region: GcpRegion::new(test_gcp_region),
                image_mode: LlmImageMode::Auto,
                text_model: None,
                image_model: None,
                block_none_harmful: false,
            },
            &reporter,
        )
        .await?;

        let redacted_item = redacter.redact(input).await?;
        match redacted_item.item.content {
            RedacterDataItemContent::Value(value) => {
                assert_eq!(value.trim(), "Hello, [REDACTED]");
            }
            _ => panic!("Unexpected redacted content type"),
        }

        Ok(())
    }

    async fn redact_image_file_test_with_mode(
        image_mode: LlmImageMode,
        output_name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let term = Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        initialize_crypto();
        let test_gcp_project_id =
            std::env::var("TEST_GCP_PROJECT").expect("TEST_GCP_PROJECT required");
        let test_gcp_region =
            std::env::var("TEST_GCP_REGION").unwrap_or_else(|_| "global".to_string());

        let input = test_image_item();
        let redacter = GcpVertexAiRedacter::new(
            GcpVertexAiRedacterOptions {
                categories: None,
                project_id: GcpProjectId::new(test_gcp_project_id),
                gcp_region: GcpRegion::new(test_gcp_region),
                image_mode,
                text_model: None,
                image_model: None,
                block_none_harmful: false,
            },
            &reporter,
        )
        .await?;

        let redacted_item = redacter.redact(input.clone()).await?;
        let output_path = check_and_save_redacted_image(&input, &redacted_item.item, output_name);
        term.write_line(&format!(
            "Redacted image written to {}",
            output_path.display()
        ))?;

        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-gcp-vertex-ai"), ignore)]
    async fn redact_image_file_auto_mode_test(
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        redact_image_file_test_with_mode(LlmImageMode::Auto, "vertex-ai-auto.png").await
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-gcp-vertex-ai"), ignore)]
    async fn redact_image_file_coords_mode_test(
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        redact_image_file_test_with_mode(LlmImageMode::Coords, "vertex-ai-coords.png").await
    }

    async fn detection_test_redacter<'a>(
        reporter: &'a AppReporter<'a>,
    ) -> Result<GcpVertexAiRedacter<'a>, Box<dyn std::error::Error + Send + Sync>> {
        initialize_crypto();
        let test_gcp_project_id =
            std::env::var("TEST_GCP_PROJECT").expect("TEST_GCP_PROJECT required");
        let test_gcp_region =
            std::env::var("TEST_GCP_REGION").unwrap_or_else(|_| "global".to_string());
        Ok(GcpVertexAiRedacter::new(
            GcpVertexAiRedacterOptions {
                categories: None,
                project_id: GcpProjectId::new(test_gcp_project_id),
                gcp_region: GcpRegion::new(test_gcp_region),
                image_mode: LlmImageMode::Auto,
                text_model: None,
                image_model: None,
                block_none_harmful: false,
            },
            reporter,
        )
        .await?)
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-gcp-vertex-ai"), ignore)]
    async fn detect_text_file_test() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let term = Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let redacter = detection_test_redacter(&reporter).await?;

        let input = live_detection::note_item();
        let detections = redacter.detect(&input).await?.found;

        live_detection::assert_note_found(&input, &detections);
        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-gcp-vertex-ai"), ignore)]
    async fn detect_image_file_test() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let term = Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let redacter = detection_test_redacter(&reporter).await?;

        let detections = redacter
            .detect(&live_detection::enlarged_form_item())
            .await?
            .found;

        live_detection::assert_form_found(&detections);
        Ok(())
    }
}
