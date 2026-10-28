use crate::args::RedacterType;
use crate::errors::AppError;
use crate::file_systems::FileSystemRef;
use crate::redacters::{
    mask_detections, CharOffsets, Detection, DetectionLabel, DetectionLocation, RedactSupport,
    Redacter, RedacterDataItem, RedacterDataItemContent, Redacters, Score,
};
use crate::reporter::AppReporter;
use crate::AppResult;
use aws_config::Region;
use aws_sdk_comprehend::types::PiiEntity;

#[derive(Debug, Clone)]
pub struct AwsComprehendRedacterOptions {
    pub region: Option<Region>,
}

#[derive(Clone)]
pub struct AwsComprehendRedacter<'a> {
    client: aws_sdk_comprehend::Client,
    #[allow(dead_code)]
    reporter: &'a AppReporter<'a>,
}

impl<'a> AwsComprehendRedacter<'a> {
    pub async fn new(
        aws_dlp_options: AwsComprehendRedacterOptions,
        reporter: &'a AppReporter<'a>,
    ) -> AppResult<Self> {
        let region_provider = aws_config::meta::region::RegionProviderChain::first_try(
            aws_dlp_options.region.clone(),
        )
        .or_default_provider();
        let shared_config = aws_config::from_env().region(region_provider).load().await;
        let client = aws_sdk_comprehend::Client::new(&shared_config);
        Ok(Self { client, reporter })
    }

    pub async fn redact_text_file(&self, input: RedacterDataItem) -> AppResult<RedacterDataItem> {
        let text_content = match input.content {
            RedacterDataItemContent::Value(content) => Ok(content),
            _ => Err(AppError::SystemError {
                message: "Unsupported item for text redacting".to_string(),
            }),
        }?;

        let redacted_content =
            mask_detections(&text_content, &self.detect_text(&text_content).await?);
        Ok(RedacterDataItem {
            file_ref: input.file_ref,
            content: RedacterDataItemContent::Value(redacted_content),
        })
    }
}

impl<'a> AwsComprehendRedacter<'a> {
    /// Turns the entities Comprehend found in `text` into detections, converting its
    /// character offsets to byte offsets.
    fn detections_in(text: &str, entities: &[PiiEntity]) -> AppResult<Vec<Detection>> {
        let offsets = CharOffsets::new(text);
        entities
            .iter()
            .map(|entity| {
                let (Some(begin), Some(end), Some(entity_type)) =
                    (entity.begin_offset, entity.end_offset, entity.r#type())
                else {
                    return Err(AppError::SystemError {
                        message: format!(
                            "AWS Comprehend returned a PII entity without a type or offsets: \
                             {entity:?}"
                        ),
                    });
                };
                let (start, end) = match (usize::try_from(begin), usize::try_from(end)) {
                    (Ok(start), Ok(end)) => (start, end),
                    _ => {
                        return Err(AppError::SystemError {
                            message: format!(
                                "AWS Comprehend returned the invalid offsets {begin}..{end}"
                            ),
                        })
                    }
                };
                let (byte_start, byte_end) =
                    offsets
                        .byte_range(start, end)
                        .ok_or(AppError::DetectionOutsideContent {
                            redacter_type: RedacterType::AwsComprehend,
                            location: DetectionLocation::Text { start, end },
                        })?;
                Ok(Detection {
                    label: DetectionLabel::new(entity_type.as_str()),
                    location: DetectionLocation::Text {
                        start: byte_start,
                        end: byte_end,
                    },
                    score: entity.score.map(Score::try_from).transpose()?,
                    value: text[byte_start..byte_end].to_string(),
                })
            })
            .collect()
    }

    async fn detect_text(&self, text: &str) -> AppResult<Vec<Detection>> {
        let result = self
            .client
            .detect_pii_entities()
            .language_code(aws_sdk_comprehend::types::LanguageCode::En)
            .text(text)
            .send()
            .await?;
        Self::detections_in(text, result.entities())
    }
}

impl<'a> Redacter for AwsComprehendRedacter<'a> {
    async fn redact(&self, input: RedacterDataItem) -> AppResult<RedacterDataItem> {
        match &input.content {
            RedacterDataItemContent::Value(_) => self.redact_text_file(input).await,
            RedacterDataItemContent::Table { .. }
            | RedacterDataItemContent::Image { .. }
            | RedacterDataItemContent::Pdf { .. } => Err(AppError::unsupported_content()),
        }
    }

    async fn redact_support(&self, file_ref: &FileSystemRef) -> AppResult<RedactSupport> {
        Ok(match file_ref.media_type.as_ref() {
            Some(media_type) if Redacters::is_mime_text(media_type) => RedactSupport::Supported,
            _ => RedactSupport::Unsupported,
        })
    }

    async fn detect(&self, input: &RedacterDataItem) -> AppResult<Vec<Detection>> {
        match &input.content {
            RedacterDataItemContent::Value(text) => self.detect_text(text).await,
            RedacterDataItemContent::Table { .. }
            | RedacterDataItemContent::Image { .. }
            | RedacterDataItemContent::Pdf { .. } => Err(AppError::unsupported_content()),
        }
    }

    fn redacter_type(&self) -> RedacterType {
        RedacterType::AwsComprehend
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_comprehend::types::PiiEntityType;
    use console::Term;

    fn entity(
        entity_type: PiiEntityType,
        score: f32,
        begin_offset: i32,
        end_offset: i32,
    ) -> PiiEntity {
        PiiEntity::builder()
            .r#type(entity_type)
            .score(score)
            .begin_offset(begin_offset)
            .end_offset(end_offset)
            .build()
    }

    fn score(value: f32) -> Option<Score> {
        Some(Score::try_from(value).unwrap())
    }

    #[test]
    fn entities_keep_type_and_score_and_get_byte_ranges_from_character_offsets() {
        let text = "Grüße, Zoë Keller: zoe@example.com";
        let name_chars = (7, 17);
        let email_chars = (19, 34);
        assert_eq!(text.chars().count(), email_chars.1 as usize);
        assert!(text.len() > text.chars().count());

        let detections = AwsComprehendRedacter::detections_in(
            text,
            &[
                entity(PiiEntityType::Name, 0.99, name_chars.0, name_chars.1),
                entity(PiiEntityType::Email, 0.97, email_chars.0, email_chars.1),
            ],
        )
        .unwrap();

        let name = text.find("Zoë Keller").unwrap();
        let email = text.find("zoe@example.com").unwrap();
        assert_eq!(
            detections,
            vec![
                Detection {
                    label: DetectionLabel::new("NAME"),
                    location: DetectionLocation::Text {
                        start: name,
                        end: name + "Zoë Keller".len()
                    },
                    score: score(0.99),
                    value: "Zoë Keller".to_string(),
                },
                Detection {
                    label: DetectionLabel::new("EMAIL"),
                    location: DetectionLocation::Text {
                        start: email,
                        end: email + "zoe@example.com".len()
                    },
                    score: score(0.97),
                    value: "zoe@example.com".to_string(),
                },
            ]
        );
    }

    #[test]
    fn an_entity_reaching_past_the_text_is_an_error() {
        let result = AwsComprehendRedacter::detections_in(
            "Grüße",
            &[entity(PiiEntityType::Name, 0.9, 0, 6)],
        );
        assert!(matches!(
            result,
            Err(AppError::DetectionOutsideContent { .. })
        ));
    }

    #[test]
    fn an_entity_without_offsets_is_an_error_rather_than_dropped() {
        let without_end = PiiEntity::builder()
            .r#type(PiiEntityType::Name)
            .score(0.9)
            .begin_offset(0)
            .build();
        assert!(AwsComprehendRedacter::detections_in("Zoë", &[without_end]).is_err());
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-aws"), ignore)]
    async fn redact_text_file_test() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let term = Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let test_aws_region = std::env::var("TEST_AWS_REGION").expect("TEST_AWS_REGION required");
        let test_content = "Hello, John";

        let file_ref = FileSystemRef {
            relative_path: "temp_file.txt".into(),
            media_type: Some(mime::TEXT_PLAIN),
            file_size: Some(test_content.len()),
        };

        let content = RedacterDataItemContent::Value(test_content.to_string());
        let input = RedacterDataItem { file_ref, content };

        let redacter = AwsComprehendRedacter::new(
            AwsComprehendRedacterOptions {
                region: Some(Region::new(test_aws_region)),
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
    #[cfg_attr(not(feature = "ci-aws"), ignore)]
    async fn detect_text_finds_values_at_their_byte_offsets() {
        let term = Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let test_aws_region = std::env::var("TEST_AWS_REGION").expect("TEST_AWS_REGION required");
        let text = "Grüße aus Köln, Zoë Keller schreibt an zoe.keller@example.com";
        let input = RedacterDataItem {
            file_ref: FileSystemRef {
                relative_path: "temp_file.txt".into(),
                media_type: Some(mime::TEXT_PLAIN),
                file_size: Some(text.len()),
            },
            content: RedacterDataItemContent::Value(text.to_string()),
        };
        let redacter = AwsComprehendRedacter::new(
            AwsComprehendRedacterOptions {
                region: Some(Region::new(test_aws_region)),
            },
            &reporter,
        )
        .await
        .unwrap();

        let detections = redacter.detect(&input).await.unwrap();

        let email = text.find("zoe.keller@example.com").unwrap();
        assert!(
            detections.iter().any(|detection| {
                detection.label == DetectionLabel::new("EMAIL")
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
