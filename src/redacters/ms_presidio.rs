use rvstruct::ValueStruct;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::args::RedacterType;
use crate::errors::AppError;
use crate::file_systems::FileSystemRef;
use crate::redacters::{
    unsupported_type_error, CharOffsets, Detection, DetectionLabel, DetectionLocation,
    RedactSupport, Redacter, RedacterDataItem, RedacterDataItemContent, Redacters, Score,
};
use crate::reporter::AppReporter;
use crate::AppResult;

#[derive(Debug, Clone)]
pub struct MsPresidioRedacterOptions {
    pub text_analyze_url: Option<Url>,
    pub image_redact_url: Option<Url>,
}

#[derive(Clone)]
pub struct MsPresidioRedacter<'a> {
    client: reqwest::Client,
    ms_presidio_options: MsPresidioRedacterOptions,
    #[allow(dead_code)]
    reporter: &'a AppReporter<'a>,
}

#[derive(Serialize, Clone, Debug)]
struct MsPresidioAnalyzeRequest {
    text: String,
    language: String,
}

#[derive(Deserialize, Clone, Debug)]
struct MsPresidioAnalyzedItem {
    entity_type: String,
    start: Option<usize>,
    end: Option<usize>,
    score: Option<f32>,
}

impl MsPresidioAnalyzedItem {
    fn is_disallowed(&self) -> bool {
        MsPresidioRedacter::DISALLOW_ENTITY_TYPES.contains(&self.entity_type.as_str())
    }
}

impl<'a> MsPresidioRedacter<'a> {
    /// List of entity types that should be disallowed for redacting
    /// since they produce a lot of false positives
    const DISALLOW_ENTITY_TYPES: [&'static str; 1] = ["US_DRIVER_LICENSE"];

    pub async fn new(
        ms_presidio_options: MsPresidioRedacterOptions,
        reporter: &'a AppReporter<'a>,
    ) -> AppResult<Self> {
        let client = reqwest::Client::new();
        Ok(Self {
            client,
            ms_presidio_options,
            reporter,
        })
    }

    async fn analyze(&self, text: &str) -> AppResult<Vec<MsPresidioAnalyzedItem>> {
        let analyze_url = self.ms_presidio_options.text_analyze_url.as_ref().ok_or(
            AppError::RedacterConfigError {
                message: "Text analyze URL is not configured".to_string(),
            },
        )?;
        let analyze_request = MsPresidioAnalyzeRequest {
            text: text.to_string(),
            language: "en".to_string(),
        };
        let response = self
            .client
            .post(analyze_url.clone())
            .json(&analyze_request)
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
            return Err(AppError::SystemError {
                message: format!(
                    "Failed to analyze text: {response_text}. HTTP status: {response_status}."
                ),
            });
        }
        Ok(response.json().await?)
    }

    /// Turns the analyzer's items for `text` into detections, converting its character
    /// offsets to byte offsets and leaving out the disallowed entity types.
    fn detections_in(text: &str, items: &[MsPresidioAnalyzedItem]) -> AppResult<Vec<Detection>> {
        let offsets = CharOffsets::new(text);
        items
            .iter()
            .filter(|item| !item.is_disallowed())
            .map(|item| {
                let (Some(start), Some(end)) = (item.start, item.end) else {
                    return Err(AppError::SystemError {
                        message: format!("MS Presidio returned an item without offsets: {item:?}"),
                    });
                };
                let (byte_start, byte_end) =
                    offsets
                        .byte_range(start, end)
                        .ok_or(AppError::DetectionOutsideContent {
                            redacter_type: RedacterType::MsPresidio,
                            location: DetectionLocation::Text { start, end },
                        })?;
                Ok(Detection {
                    label: DetectionLabel::new(&item.entity_type),
                    location: DetectionLocation::Text {
                        start: byte_start,
                        end: byte_end,
                    },
                    score: item.score.map(Score::try_from).transpose()?,
                    value: text[byte_start..byte_end].to_string(),
                })
            })
            .collect()
    }

    pub async fn redact_text_file(&self, input: RedacterDataItem) -> AppResult<RedacterDataItem> {
        let text_content = match input.content {
            RedacterDataItemContent::Value(content) => Ok(content),
            _ => Err(AppError::SystemError {
                message: "Unsupported item for text redacting".to_string(),
            }),
        }?;

        let response_items = self.analyze(&text_content).await?;
        let redacted_text_content = response_items
            .iter()
            .filter(|item| !item.is_disallowed())
            .fold(text_content, |acc, entity| {
                match (entity.start, entity.end) {
                    (Some(start), Some(end)) => [
                        acc[..start].to_string(),
                        "X".repeat(end - start),
                        acc[end..].to_string(),
                    ]
                    .concat(),
                    (Some(start), None) => {
                        acc[..start].to_string() + "X".repeat(acc.len() - start).as_str()
                    }
                    (None, Some(end)) => ["X".repeat(end), acc[end..].to_string()].concat(),
                    _ => acc,
                }
            });
        Ok(RedacterDataItem {
            file_ref: input.file_ref,
            content: RedacterDataItemContent::Value(redacted_text_content),
        })
    }

    pub async fn redact_image_file(&self, input: RedacterDataItem) -> AppResult<RedacterDataItem> {
        let redact_url = self.ms_presidio_options.image_redact_url.as_ref().ok_or(
            AppError::RedacterConfigError {
                message: "Image redact URL is not configured".to_string(),
            },
        )?;

        match input.content {
            RedacterDataItemContent::Image { mime_type, data } => {
                self.reporter.report_debug(format!(
                    "Redacting an image file: {} ({:?})",
                    input.file_ref.relative_path.value(),
                    input.file_ref.media_type
                ));
                let file_part = reqwest::multipart::Part::bytes(data.to_vec())
                    .file_name(input.file_ref.relative_path.filename())
                    .mime_str(mime_type.as_ref())
                    .unwrap();
                let form = reqwest::multipart::Form::new().part("image", file_part);
                let response = self
                    .client
                    .post(redact_url.clone())
                    .multipart(form)
                    .send()
                    .await?;
                if !response.status().is_success() {
                    let response_status = response.status();
                    let response_text = response.text().await.unwrap_or_default();
                    return Err(AppError::SystemError {
                        message: format!(
                            "Failed to redact image: {response_text}. HTTP status: {response_status}."
                        ),
                    });
                }
                let redacted_image_bytes = response.bytes().await?;
                Ok(RedacterDataItem {
                    file_ref: input.file_ref,
                    content: RedacterDataItemContent::Image {
                        mime_type,
                        data: redacted_image_bytes,
                    },
                })
            }
            _ => Err(AppError::SystemError {
                message: "Unsupported item for image redacting".to_string(),
            }),
        }
    }
}

impl<'a> Redacter for MsPresidioRedacter<'a> {
    async fn redact(&self, input: RedacterDataItem) -> AppResult<RedacterDataItem> {
        match &input.content {
            RedacterDataItemContent::Value(_) => self.redact_text_file(input).await,
            RedacterDataItemContent::Image { .. } => self.redact_image_file(input).await,
            RedacterDataItemContent::Table { .. } | RedacterDataItemContent::Pdf { .. } => {
                Err(AppError::SystemError {
                    message: "Attempt to redact of unsupported table type".to_string(),
                })
            }
        }
    }

    async fn redact_support(&self, file_ref: &FileSystemRef) -> AppResult<RedactSupport> {
        Ok(match file_ref.media_type.as_ref() {
            Some(media_type)
                if Redacters::is_mime_text(media_type)
                    && self.ms_presidio_options.text_analyze_url.is_some() =>
            {
                RedactSupport::Supported
            }
            Some(media_type)
                if Redacters::is_mime_image(media_type)
                    && self.ms_presidio_options.image_redact_url.is_some() =>
            {
                RedactSupport::Supported
            }
            _ => RedactSupport::Unsupported,
        })
    }

    /// Only text goes to the analyzer: the image endpoint redacts without reporting where, so
    /// images are read with OCR and their text is analysed.
    async fn detect_support(&self, file_ref: &FileSystemRef) -> AppResult<RedactSupport> {
        Ok(match file_ref.media_type.as_ref() {
            Some(media_type)
                if Redacters::is_mime_text(media_type)
                    && self.ms_presidio_options.text_analyze_url.is_some() =>
            {
                RedactSupport::Supported
            }
            _ => RedactSupport::Unsupported,
        })
    }

    async fn detect(&self, input: &RedacterDataItem) -> AppResult<Vec<Detection>> {
        match &input.content {
            RedacterDataItemContent::Value(text) => {
                Self::detections_in(text, &self.analyze(text).await?)
            }
            RedacterDataItemContent::Table { .. }
            | RedacterDataItemContent::Image { .. }
            | RedacterDataItemContent::Pdf { .. } => Err(unsupported_type_error()),
        }
    }

    fn redacter_type(&self) -> RedacterType {
        RedacterType::MsPresidio
    }
}

#[cfg(test)]
mod tests {
    use console::Term;

    use super::*;

    /// An analyzer response as the Presidio service sends it: offsets count characters.
    const ANALYZER_RESPONSE: &str = r#"[
        {"entity_type": "PERSON", "start": 7, "end": 17, "score": 0.85,
         "analysis_explanation": null,
         "recognition_metadata": {"recognizer_name": "SpacyRecognizer"}},
        {"entity_type": "EMAIL_ADDRESS", "start": 19, "end": 34, "score": 1.0,
         "analysis_explanation": null,
         "recognition_metadata": {"recognizer_name": "EmailRecognizer"}},
        {"entity_type": "US_DRIVER_LICENSE", "start": 0, "end": 5, "score": 0.01}
    ]"#;

    fn score(value: f32) -> Option<Score> {
        Some(Score::try_from(value).unwrap())
    }

    #[test]
    fn analyzer_items_become_detections_with_byte_ranges_and_scores() {
        let text = "Grüße, Zoë Keller: zoe@example.com";
        let items: Vec<MsPresidioAnalyzedItem> = serde_json::from_str(ANALYZER_RESPONSE).unwrap();
        assert_eq!(text.chars().count(), 34);

        let detections = MsPresidioRedacter::detections_in(text, &items).unwrap();

        let name = text.find("Zoë Keller").unwrap();
        let email = text.find("zoe@example.com").unwrap();
        assert_eq!(
            detections,
            vec![
                Detection {
                    label: DetectionLabel::new("PERSON"),
                    location: DetectionLocation::Text {
                        start: name,
                        end: name + "Zoë Keller".len()
                    },
                    score: score(0.85),
                    value: "Zoë Keller".to_string(),
                },
                Detection {
                    label: DetectionLabel::new("EMAIL_ADDRESS"),
                    location: DetectionLocation::Text {
                        start: email,
                        end: email + "zoe@example.com".len()
                    },
                    score: score(1.0),
                    value: "zoe@example.com".to_string(),
                },
            ],
            "the disallowed US_DRIVER_LICENSE item is left out"
        );
    }

    #[test]
    fn an_item_reaching_past_the_text_is_an_error() {
        let items: Vec<MsPresidioAnalyzedItem> =
            serde_json::from_str(r#"[{"entity_type": "PERSON", "start": 0, "end": 9}]"#).unwrap();
        assert!(matches!(
            MsPresidioRedacter::detections_in("Grüße", &items),
            Err(AppError::DetectionOutsideContent { .. })
        ));
    }

    #[test]
    fn an_item_without_a_score_is_unscored() {
        let items: Vec<MsPresidioAnalyzedItem> =
            serde_json::from_str(r#"[{"entity_type": "PERSON", "start": 0, "end": 3}]"#).unwrap();
        let detections = MsPresidioRedacter::detections_in("Zoë", &items).unwrap();
        assert_eq!(detections.len(), 1);
        assert_eq!(detections[0].score, None);
    }

    #[tokio::test]
    async fn images_are_read_with_ocr_rather_than_by_the_image_endpoint() {
        let term = Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let redacter = MsPresidioRedacter::new(
            MsPresidioRedacterOptions {
                text_analyze_url: Some(Url::parse("http://localhost:5002/analyze").unwrap()),
                image_redact_url: Some(Url::parse("http://localhost:5003/redact").unwrap()),
            },
            &reporter,
        )
        .await
        .unwrap();
        let file = |media_type: mime::Mime| FileSystemRef {
            relative_path: "file".into(),
            media_type: Some(media_type),
            file_size: None,
        };

        assert!(matches!(
            redacter
                .redact_support(&file(mime::IMAGE_PNG))
                .await
                .unwrap(),
            RedactSupport::Supported
        ));
        assert!(matches!(
            redacter
                .detect_support(&file(mime::IMAGE_PNG))
                .await
                .unwrap(),
            RedactSupport::Unsupported
        ));
        assert!(matches!(
            redacter
                .detect_support(&file(mime::TEXT_PLAIN))
                .await
                .unwrap(),
            RedactSupport::Supported
        ));
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-ms-presidio"), ignore)]
    async fn redact_text_file_test() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let term = Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let test_analyze_url: Url = Url::parse(
            std::env::var("TEST_MS_PRESIDIO_ANALYZE_URL")
                .expect("TEST_MS_PRESIDIO_ANALYZE_URL required")
                .as_str(),
        )?;
        let test_content = "Hello, John";

        let file_ref = FileSystemRef {
            relative_path: "temp_file.txt".into(),
            media_type: Some(mime::TEXT_PLAIN),
            file_size: Some(test_content.len()),
        };

        let content = RedacterDataItemContent::Value(test_content.to_string());
        let input = RedacterDataItem { file_ref, content };

        let redacter = MsPresidioRedacter::new(
            MsPresidioRedacterOptions {
                text_analyze_url: Some(test_analyze_url),
                image_redact_url: None,
            },
            &reporter,
        )
        .await?;

        let redacted_item = redacter.redact(input).await?;
        match redacted_item.content {
            RedacterDataItemContent::Value(value) => {
                assert_eq!(value, "Hello, XXXX");
            }
            _ => panic!("Unexpected redacted content type"),
        }

        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-ms-presidio"), ignore)]
    async fn detect_text_finds_values_at_their_byte_offsets() {
        let term = Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let test_analyze_url: Url = Url::parse(
            std::env::var("TEST_MS_PRESIDIO_ANALYZE_URL")
                .expect("TEST_MS_PRESIDIO_ANALYZE_URL required")
                .as_str(),
        )
        .unwrap();
        let text = "Grüße aus Köln, Zoë Keller schreibt an zoe.keller@example.com";
        let input = RedacterDataItem {
            file_ref: FileSystemRef {
                relative_path: "temp_file.txt".into(),
                media_type: Some(mime::TEXT_PLAIN),
                file_size: Some(text.len()),
            },
            content: RedacterDataItemContent::Value(text.to_string()),
        };
        let redacter = MsPresidioRedacter::new(
            MsPresidioRedacterOptions {
                text_analyze_url: Some(test_analyze_url),
                image_redact_url: None,
            },
            &reporter,
        )
        .await
        .unwrap();

        let detections = redacter.detect(&input).await.unwrap();

        let email = text.find("zoe.keller@example.com").unwrap();
        assert!(
            detections.iter().any(|detection| {
                detection.label == DetectionLabel::new("EMAIL_ADDRESS")
                    && detection.location
                        == DetectionLocation::Text {
                            start: email,
                            end: email + "zoe.keller@example.com".len(),
                        }
                    && detection.value == "zoe.keller@example.com"
                    && detection.score.is_some()
            }),
            "{detections:?}"
        );
    }
}
