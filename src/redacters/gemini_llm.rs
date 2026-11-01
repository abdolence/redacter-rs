use crate::args::RedacterType;
use crate::common_types::GcpProjectId;
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
use gcloud_sdk::google::ai::generativelanguage::v1beta as gemini_api;
use gcloud_sdk::google::ai::generativelanguage::v1beta::generative_service_client::GenerativeServiceClient;
use gcloud_sdk::{tonic, GoogleApi, GoogleAuthMiddleware};
use mime::Mime;
use rand::RngExt;
use rvstruct::ValueStruct;

#[derive(Debug, Clone)]
pub struct GeminiLlmRedacterOptions {
    pub project_id: GcpProjectId,
    pub gemini_model: Option<GeminiLlmModelName>,
    pub gemini_image_model: Option<GeminiLlmModelName>,
    pub image_mode: LlmImageMode,
    /// The categories to look for; every category when `None`.
    #[expect(
        dead_code,
        reason = "read once the redacter narrows its detection by category"
    )]
    pub categories: Option<Categories>,
}

#[derive(Debug, Clone, ValueStruct)]
pub struct GeminiLlmModelName(String);

fn text_part(text: String) -> gemini_api::Part {
    gemini_api::Part {
        data: Some(gemini_api::part::Data::Text(text)),
        ..std::default::Default::default()
    }
}

fn schema_of(r#type: gemini_api::Type) -> gemini_api::Schema {
    gemini_api::Schema {
        r#type: r#type.into(),
        ..std::default::Default::default()
    }
}

/// Schema of an array of objects whose listed properties are all required.
fn findings_schema(properties: Vec<(&str, gemini_api::Schema)>) -> gemini_api::Schema {
    gemini_api::Schema {
        r#type: gemini_api::Type::Array.into(),
        items: Some(Box::new(gemini_api::Schema {
            r#type: gemini_api::Type::Object.into(),
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

impl From<gemini_api::GenerateContentResponse> for ModelAnswer {
    fn from(response: gemini_api::GenerateContentResponse) -> Self {
        let prompt_block_reason = response
            .prompt_feedback
            .as_ref()
            .map(|feedback| feedback.block_reason())
            .filter(|reason| {
                *reason != gemini_api::generate_content_response::prompt_feedback::BlockReason::Unspecified
            })
            .map(|reason| reason.as_str_name());
        let candidate = response
            .candidates
            .first()
            .map(|candidate| GeminiCandidate {
                finish_reason: gemini_api::candidate::FinishReason::try_from(
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
                        Some(gemini_api::part::Data::Text(text)) => Some(text.as_str()),
                        _ => None,
                    })
                    .collect(),
            });
        ModelAnswer::from_gemini(candidate, prompt_block_reason)
    }
}

#[derive(Clone)]
pub struct GeminiLlmRedacter<'a> {
    client: GoogleApi<GenerativeServiceClient<GoogleAuthMiddleware>>,
    gemini_llm_options: crate::redacters::GeminiLlmRedacterOptions,
    reporter: &'a AppReporter<'a>,
}

impl<'a> GeminiLlmRedacter<'a> {
    /// Text model, also used to locate sensitive data in images.
    const DEFAULT_GEMINI_MODEL: &'static str = "models/gemini-3.8-flash";
    /// Image editing model, used for the native image redaction path.
    const DEFAULT_GEMINI_IMAGE_MODEL: &'static str = "models/gemini-3.1-flash-image";

    pub async fn new(
        gemini_llm_options: GeminiLlmRedacterOptions,
        reporter: &'a AppReporter<'a>,
    ) -> AppResult<Self> {
        let client =
            GoogleApi::from_function_with_scopes(
                gcloud_sdk::google::ai::generativelanguage::v1beta::generative_service_client::GenerativeServiceClient::new, "https://generativelanguage.googleapis.com", None,
                vec![
                    "https://www.googleapis.com/auth/cloud-platform".to_string(),
                    "https://www.googleapis.com/auth/generative-language".to_string()
                ],
            ).await?;
        Ok(GeminiLlmRedacter {
            client,
            gemini_llm_options,
            reporter,
        })
    }

    fn text_model_name(&self) -> String {
        self.gemini_llm_options
            .gemini_model
            .as_ref()
            .map(|model_name| model_name.value().to_string())
            .unwrap_or_else(|| Self::DEFAULT_GEMINI_MODEL.to_string())
    }

    pub async fn redact_text_file(&self, input: RedacterDataItem) -> AppResult<RedacterDataItem> {
        let model_name = self.text_model_name();
        let mut rand = rand::rng();
        let generate_random_text_separator = format!("---{}", rand.random::<u64>());

        match input.content {
            RedacterDataItemContent::Value(input_content) => {
                let request =
                    gcloud_sdk::google::ai::generativelanguage::v1beta::GenerateContentRequest {
                        model: model_name,
                        safety_settings: vec![
                            gcloud_sdk::google::ai::generativelanguage::v1beta::HarmCategory::HateSpeech,
                            gcloud_sdk::google::ai::generativelanguage::v1beta::HarmCategory::SexuallyExplicit,
                            gcloud_sdk::google::ai::generativelanguage::v1beta::HarmCategory::DangerousContent,
                            gcloud_sdk::google::ai::generativelanguage::v1beta::HarmCategory::Harassment,
                            ].into_iter().map(|category| gcloud_sdk::google::ai::generativelanguage::v1beta::SafetySetting {
                                category: category.into(),
                                threshold: gcloud_sdk::google::ai::generativelanguage::v1beta::safety_setting::HarmBlockThreshold::BlockNone.into(),
                            }).collect(),
                        contents: vec![
                            gcloud_sdk::google::ai::generativelanguage::v1beta::Content {
                                parts: vec![
                                    gcloud_sdk::google::ai::generativelanguage::v1beta::Part {
                                        data: Some(
                                            gcloud_sdk::google::ai::generativelanguage::v1beta::part::Data::Text(
                                                text_redaction_instruction(&generate_random_text_separator),
                                            ),
                                        ),
                                        ..std::default::Default::default()
                                    },
                                    gcloud_sdk::google::ai::generativelanguage::v1beta::Part {
                                        data: Some(
                                            gcloud_sdk::google::ai::generativelanguage::v1beta::part::Data::Text(
                                                format!("{}\n",generate_random_text_separator)
                                            )
                                        ),
                                        ..std::default::Default::default()
                                    },
                                    gcloud_sdk::google::ai::generativelanguage::v1beta::Part {
                                        data: Some(
                                            gcloud_sdk::google::ai::generativelanguage::v1beta::part::Data::Text(
                                                input_content,
                                            ),
                                        ),
                                        ..std::default::Default::default()
                                    },
                                    gcloud_sdk::google::ai::generativelanguage::v1beta::Part {
                                        data: Some(
                                            gcloud_sdk::google::ai::generativelanguage::v1beta::part::Data::Text(
                                                format!("{}\n",generate_random_text_separator)
                                            )
                                        ),
                                        ..std::default::Default::default()
                                    }
                                ],
                                role: "user".to_string(),
                            },
                        ],
                        generation_config: Some(
                            gcloud_sdk::google::ai::generativelanguage::v1beta::GenerationConfig {
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
                    gemini_api::Part {
                        data: Some(gemini_api::part::Data::InlineData(gemini_api::Blob {
                            mime_type: mime_type.to_string(),
                            data: prepared_image.data.to_vec(),
                        })),
                        ..std::default::Default::default()
                    },
                ],
                findings_schema(vec![
                    (
                        "box_2d",
                        gemini_api::Schema {
                            min_items: 4,
                            items: Some(Box::new(schema_of(gemini_api::Type::Integer))),
                            ..schema_of(gemini_api::Type::Array)
                        },
                    ),
                    ("text", schema_of(gemini_api::Type::String)),
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
                found.redact(mime_type, input.file_ref, 0.25)
            }
            _ => Err(AppError::SystemError {
                message: "Unsupported item for image redacting".to_string(),
            }),
        }
    }

    /// Sends `request` and returns the first candidate's text with how it ended.
    async fn generate(
        &self,
        request: gemini_api::GenerateContentRequest,
    ) -> AppResult<ModelAnswer> {
        let mut request = tonic::Request::new(request);
        request.metadata_mut().insert(
            "x-goog-user-project",
            gcloud_sdk::tonic::metadata::MetadataValue::<tonic::metadata::Ascii>::try_from(
                self.gemini_llm_options.project_id.as_ref(),
            )?,
        );
        let response = self.client.get().generate_content(request).await?;
        Ok(response.into_inner().into())
    }

    /// Asks the text model for JSON conforming to `response_schema` and returns its answer.
    /// An answer that was filtered, cut short or empty is an error.
    async fn generate_json(
        &self,
        parts: Vec<gemini_api::Part>,
        response_schema: gemini_api::Schema,
    ) -> AppResult<String> {
        let request = gemini_api::GenerateContentRequest {
            model: self.text_model_name(),
            safety_settings: [
                gemini_api::HarmCategory::HateSpeech,
                gemini_api::HarmCategory::SexuallyExplicit,
                gemini_api::HarmCategory::DangerousContent,
                gemini_api::HarmCategory::Harassment,
            ]
            .into_iter()
            .map(|category| gemini_api::SafetySetting {
                category: category.into(),
                threshold: gemini_api::safety_setting::HarmBlockThreshold::BlockNone.into(),
            })
            .collect(),
            contents: vec![gemini_api::Content {
                parts,
                role: "user".to_string(),
            }],
            generation_config: Some(gemini_api::GenerationConfig {
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
                    ("text", schema_of(gemini_api::Type::String)),
                    ("type", schema_of(gemini_api::Type::String)),
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
            .gemini_llm_options
            .gemini_image_model
            .as_ref()
            .map(|model_name| model_name.value().to_string())
            .unwrap_or_else(|| Self::DEFAULT_GEMINI_IMAGE_MODEL.to_string());

        match input.content {
            RedacterDataItemContent::Image { mime_type, data } => {
                let prepared_image = prepare_image_for_llm(&mime_type, &data)?;

                let mut request = tonic::Request::new(
                    gcloud_sdk::google::ai::generativelanguage::v1beta::GenerateContentRequest {
                        model: model_name,
                        safety_settings: vec![
                            gcloud_sdk::google::ai::generativelanguage::v1beta::HarmCategory::HateSpeech,
                            gcloud_sdk::google::ai::generativelanguage::v1beta::HarmCategory::SexuallyExplicit,
                            gcloud_sdk::google::ai::generativelanguage::v1beta::HarmCategory::DangerousContent,
                            gcloud_sdk::google::ai::generativelanguage::v1beta::HarmCategory::Harassment,
                        ].into_iter().map(|category| gcloud_sdk::google::ai::generativelanguage::v1beta::SafetySetting {
                            category: category.into(),
                            threshold: gcloud_sdk::google::ai::generativelanguage::v1beta::safety_setting::HarmBlockThreshold::BlockNone.into(),
                        }).collect(),
                        contents: vec![
                            gcloud_sdk::google::ai::generativelanguage::v1beta::Content {
                                parts: vec![
                                    gcloud_sdk::google::ai::generativelanguage::v1beta::Part {
                                        data: Some(
                                            gcloud_sdk::google::ai::generativelanguage::v1beta::part::Data::Text(
                                                NATIVE_IMAGE_REDACTION_PROMPT.to_string(),
                                            ),
                                        ),
                                        ..std::default::Default::default()
                                    },
                                    gcloud_sdk::google::ai::generativelanguage::v1beta::Part {
                                        data: Some(
                                            gcloud_sdk::google::ai::generativelanguage::v1beta::part::Data::InlineData(
                                                gcloud_sdk::google::ai::generativelanguage::v1beta::Blob {
                                                    mime_type: mime_type.to_string(),
                                                    data: prepared_image.data.to_vec(),
                                                }
                                            ),
                                        ),
                                        ..std::default::Default::default()
                                    }
                                ],
                                role: "user".to_string(),
                            },
                        ],
                        generation_config: Some(
                            gcloud_sdk::google::ai::generativelanguage::v1beta::GenerationConfig {
                                candidate_count: Some(1),
                                response_modalities: vec![
                                    gcloud_sdk::google::ai::generativelanguage::v1beta::generation_config::Modality::Image as i32,
                                    gcloud_sdk::google::ai::generativelanguage::v1beta::generation_config::Modality::Text as i32,
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
                        self.gemini_llm_options.project_id.as_ref(),
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
                                gcloud_sdk::google::ai::generativelanguage::v1beta::part::Data::InlineData(
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
}

impl<'a> Redacter for GeminiLlmRedacter<'a> {
    async fn redact(&self, input: RedacterDataItem) -> AppResult<Redacted> {
        match &input.content {
            RedacterDataItemContent::Value(_) => {
                self.redact_text_file(input).await.map(Redacted::unlocated)
            }
            RedacterDataItemContent::Image { .. } => {
                redact_image_with_mode(
                    self.gemini_llm_options.image_mode,
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
        RedacterType::GeminiLlm
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

    fn response(
        finish_reason: gemini_api::candidate::FinishReason,
        text: &str,
        blocked: &[gemini_api::HarmCategory],
    ) -> gemini_api::GenerateContentResponse {
        let parts = if text.is_empty() {
            Vec::new()
        } else {
            vec![text_part(text.to_string())]
        };
        gemini_api::GenerateContentResponse {
            candidates: vec![gemini_api::Candidate {
                finish_reason: finish_reason.into(),
                content: Some(gemini_api::Content {
                    parts,
                    role: "model".to_string(),
                }),
                safety_ratings: blocked
                    .iter()
                    .map(|category| gemini_api::SafetyRating {
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
            gemini_api::candidate::FinishReason::Safety,
            "",
            &[gemini_api::HarmCategory::DangerousContent],
        );
        assert!(matches!(
            ModelAnswer::from(stopped).into_text(),
            Err(AppError::ModelAnswerStopped(ModelStop::Safety { ref categories }))
                if !categories.is_empty()
        ));
    }

    #[test]
    fn a_response_without_candidates_names_the_block_reason_of_the_prompt() {
        let blocked = gemini_api::GenerateContentResponse {
            prompt_feedback: Some(gemini_api::generate_content_response::PromptFeedback {
                block_reason: gemini_api::generate_content_response::prompt_feedback::BlockReason::ProhibitedContent.into(),
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
        let mut unknown = response(gemini_api::candidate::FinishReason::Stop, "[]", &[]);
        unknown.candidates[0].finish_reason = 9999;
        assert!(matches!(
            ModelAnswer::from(unknown).into_text(),
            Err(AppError::ModelAnswerStopped(ModelStop::Withheld(_)))
        ));
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-gcp-llm"), ignore)]
    async fn redact_text_file_test() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let term = Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        initialize_crypto();
        let test_gcp_project_id =
            std::env::var("TEST_GCP_PROJECT").expect("TEST_GCP_PROJECT required");
        let test_content = "Hello, John";

        let file_ref = FileSystemRef {
            relative_path: "temp_file.txt".into(),
            media_type: Some(mime::TEXT_PLAIN),
            file_size: Some(test_content.len()),
        };

        let content = RedacterDataItemContent::Value(test_content.to_string());
        let input = RedacterDataItem { file_ref, content };

        let redacter = GeminiLlmRedacter::new(
            GeminiLlmRedacterOptions {
                categories: None,
                project_id: GcpProjectId::new(test_gcp_project_id),
                gemini_model: None,
                gemini_image_model: None,
                image_mode: LlmImageMode::Auto,
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

        let input = test_image_item();
        let redacter = GeminiLlmRedacter::new(
            GeminiLlmRedacterOptions {
                categories: None,
                project_id: GcpProjectId::new(test_gcp_project_id),
                gemini_model: None,
                gemini_image_model: None,
                image_mode,
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
    #[cfg_attr(not(feature = "ci-gcp-llm"), ignore)]
    async fn redact_image_file_auto_mode_test(
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        redact_image_file_test_with_mode(LlmImageMode::Auto, "gemini-auto.png").await
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-gcp-llm"), ignore)]
    async fn redact_image_file_coords_mode_test(
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        redact_image_file_test_with_mode(LlmImageMode::Coords, "gemini-coords.png").await
    }

    async fn detection_test_redacter<'a>(
        reporter: &'a AppReporter<'a>,
    ) -> Result<GeminiLlmRedacter<'a>, Box<dyn std::error::Error + Send + Sync>> {
        initialize_crypto();
        let test_gcp_project_id =
            std::env::var("TEST_GCP_PROJECT").expect("TEST_GCP_PROJECT required");
        Ok(GeminiLlmRedacter::new(
            GeminiLlmRedacterOptions {
                categories: None,
                project_id: GcpProjectId::new(test_gcp_project_id),
                gemini_model: None,
                gemini_image_model: None,
                image_mode: LlmImageMode::Auto,
            },
            reporter,
        )
        .await?)
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-gcp-llm"), ignore)]
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
    #[cfg_attr(not(feature = "ci-gcp-llm"), ignore)]
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
