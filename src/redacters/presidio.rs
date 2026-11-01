use rvstruct::ValueStruct;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::args::RedacterType;
use crate::errors::AppError;
use crate::file_systems::FileSystemRef;
use crate::redacters::{
    mask_detections, Categories, Category, CharOffsets, Detection, DetectionLabel, Detections,
    RedactSupport, Redacted, Redacter, RedacterDataItem, RedacterDataItemContent, Redacters, Score,
    TextLines,
};
use crate::reporter::AppReporter;
use crate::AppResult;

#[derive(Debug, Clone)]
pub struct PresidioRedacterOptions {
    pub text_analyze_url: Option<Url>,
    pub image_redact_url: Option<Url>,
    /// The categories to look for; every category when `None`.
    pub categories: Option<Categories>,
}

#[derive(Clone)]
pub struct PresidioRedacter<'a> {
    client: reqwest::Client,
    presidio_options: PresidioRedacterOptions,
    reporter: &'a AppReporter<'a>,
}

#[derive(Serialize, Clone, Debug)]
struct PresidioAnalyzeRequest {
    text: String,
    language: String,
}

#[derive(Deserialize, Clone, Debug)]
struct PresidioAnalyzedItem {
    entity_type: String,
    start: Option<usize>,
    end: Option<usize>,
    score: Option<f32>,
}

impl PresidioAnalyzedItem {
    fn is_disallowed(&self) -> bool {
        PresidioRedacter::DISALLOW_ENTITY_TYPES.contains(&self.entity_type.as_str())
    }
}

impl<'a> PresidioRedacter<'a> {
    /// The error of a rejected request: the status only. The response body can quote the
    /// scanned text, and both the error and the log reach the terminal and the scan report.
    fn request_failure(operation: &str, status: reqwest::StatusCode) -> AppError {
        tracing::debug!(%status, operation, "Presidio rejected the request");
        AppError::SystemError {
            message: format!("Failed to {operation}. HTTP status: {status}."),
        }
    }

    /// The entities of the supported-entities list of Presidio, by what they hold.
    const ENTITY_CATEGORIES: &'static [(&'static str, Category)] = &[
        ("PERSON", Category::Person),
        ("EMAIL_ADDRESS", Category::Contact),
        ("PHONE_NUMBER", Category::Contact),
        ("ZA_MOBILE_NUMBER", Category::Contact),
        ("ZA_TELEPHONE_NUMBER", Category::Contact),
        ("UK_POSTCODE", Category::Address),
        ("CA_POSTAL_CODE", Category::Address),
        ("DE_PLZ", Category::Address),
        ("CREDIT_CARD", Category::Financial),
        ("CRYPTO", Category::Financial),
        ("IBAN_CODE", Category::Financial),
        ("US_BANK_NUMBER", Category::Financial),
        ("IP_ADDRESS", Category::Network),
        ("MAC_ADDRESS", Category::Network),
        ("URL", Category::Network),
        ("DATE_TIME", Category::Other),
        ("NRP", Category::Other),
        ("LOCATION", Category::Other),
        ("UUID", Category::Other),
        ("MEDICAL_BIOLOGICAL_ATTRIBUTE", Category::Other),
        ("MEDICAL_BIOLOGICAL_STRUCTURE", Category::Other),
        ("MEDICAL_CLINICAL_EVENT", Category::Other),
        ("MEDICAL_DISEASE_DISORDER", Category::Other),
        ("MEDICAL_FAMILY_HISTORY", Category::Other),
        ("MEDICAL_HISTORY", Category::Other),
        ("MEDICAL_MEDICATION", Category::Other),
        ("MEDICAL_THERAPEUTIC_PROCEDURE", Category::Other),
        ("MEDICAL_LICENSE", Category::IdNumber),
        ("US_DRIVER_LICENSE", Category::IdNumber),
        ("US_ITIN", Category::IdNumber),
        ("US_CLAIM_NUMBER", Category::IdNumber),
        ("US_HEALTH_INSURANCE_MEMBER_ID", Category::IdNumber),
        ("US_MBI", Category::IdNumber),
        ("US_NPI", Category::IdNumber),
        ("US_PASSPORT", Category::IdNumber),
        ("US_PRESCRIPTION_NUMBER", Category::IdNumber),
        ("US_PRIOR_AUTHORIZATION_NUMBER", Category::IdNumber),
        ("US_PROVIDER_TAX_ID", Category::IdNumber),
        ("US_REFERRAL_NUMBER", Category::IdNumber),
        ("US_SSN", Category::IdNumber),
        ("UK_DRIVING_LICENCE", Category::IdNumber),
        ("UK_NHS", Category::IdNumber),
        ("UK_NINO", Category::IdNumber),
        ("UK_PASSPORT", Category::IdNumber),
        ("UK_VEHICLE_REGISTRATION", Category::IdNumber),
        ("ES_NIF", Category::IdNumber),
        ("ES_NIE", Category::IdNumber),
        ("ES_PASSPORT", Category::IdNumber),
        ("IT_FISCAL_CODE", Category::IdNumber),
        ("IT_DRIVER_LICENSE", Category::IdNumber),
        ("IT_VAT_CODE", Category::IdNumber),
        ("IT_PASSPORT", Category::IdNumber),
        ("IT_IDENTITY_CARD", Category::IdNumber),
        ("PL_PESEL", Category::IdNumber),
        ("SG_NRIC_FIN", Category::IdNumber),
        ("SG_UEN", Category::IdNumber),
        ("AU_ABN", Category::IdNumber),
        ("AU_ACN", Category::IdNumber),
        ("AU_TFN", Category::IdNumber),
        ("AU_MEDICARE", Category::IdNumber),
        ("IN_PAN", Category::IdNumber),
        ("IN_AADHAAR", Category::IdNumber),
        ("IN_VEHICLE_REGISTRATION", Category::IdNumber),
        ("IN_VOTER", Category::IdNumber),
        ("IN_PASSPORT", Category::IdNumber),
        ("IN_GSTIN", Category::IdNumber),
        ("FI_PERSONAL_IDENTITY_CODE", Category::IdNumber),
        ("KR_DRIVER_LICENSE", Category::IdNumber),
        ("KR_FRN", Category::IdNumber),
        ("KR_PASSPORT", Category::IdNumber),
        ("KR_BRN", Category::IdNumber),
        ("KR_RRN", Category::IdNumber),
        ("NG_NIN", Category::IdNumber),
        ("NG_VEHICLE_REGISTRATION", Category::IdNumber),
        ("PH_PASSPORT", Category::IdNumber),
        ("PH_TIN", Category::IdNumber),
        ("PH_UMID", Category::IdNumber),
        ("CA_SIN", Category::IdNumber),
        ("SE_ORGANISATIONSNUMMER", Category::IdNumber),
        ("SE_PERSONNUMMER", Category::IdNumber),
        ("ZA_ID_NUMBER", Category::IdNumber),
        ("ZA_PASSPORT", Category::IdNumber),
        ("ZA_INCOME_TAX_NUMBER", Category::IdNumber),
        ("ZA_DRIVER_LICENSE", Category::IdNumber),
        ("ZA_VAT_NUMBER", Category::IdNumber),
        ("ZA_COMPANY_REGISTRATION", Category::IdNumber),
        ("ZA_TRAFFIC_REGISTER_NUMBER", Category::IdNumber),
        ("ZA_LICENSE_PLATE", Category::IdNumber),
        ("TH_TNIN", Category::IdNumber),
        ("TR_NATIONAL_ID", Category::IdNumber),
        ("TR_LICENSE_PLATE", Category::IdNumber),
        ("DE_TAX_ID", Category::IdNumber),
        ("DE_TAX_NUMBER", Category::IdNumber),
        ("DE_PASSPORT", Category::IdNumber),
        ("DE_ID_CARD", Category::IdNumber),
        ("DE_SOCIAL_SECURITY", Category::IdNumber),
        ("DE_HEALTH_INSURANCE", Category::IdNumber),
        ("DE_KFZ", Category::IdNumber),
        ("DE_HANDELSREGISTER", Category::IdNumber),
    ];

    /// The category of a finding of the entity type `name`: that of the supported entity of
    /// the name, `other` for the entity of a custom recogniser.
    pub fn entity_category(name: &str) -> Category {
        Self::ENTITY_CATEGORIES
            .iter()
            .find(|(entity, _)| *entity == name)
            .map_or(Category::Other, |(_, category)| *category)
    }

    /// List of entity types that should be disallowed for redacting
    /// since they produce a lot of false positives
    const DISALLOW_ENTITY_TYPES: [&'static str; 1] = ["US_DRIVER_LICENSE"];

    pub async fn new(
        presidio_options: PresidioRedacterOptions,
        reporter: &'a AppReporter<'a>,
    ) -> AppResult<Self> {
        let client = reqwest::Client::new();
        Ok(Self {
            client,
            presidio_options,
            reporter,
        })
    }

    async fn analyze(&self, text: &str) -> AppResult<Vec<PresidioAnalyzedItem>> {
        let analyze_url = self.presidio_options.text_analyze_url.as_ref().ok_or(
            AppError::RedacterConfigError {
                message: "Text analyze URL is not configured".to_string(),
            },
        )?;
        let analyze_request = PresidioAnalyzeRequest {
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
            return Err(Self::request_failure("analyze text", response.status()));
        }
        Ok(response.json().await?)
    }

    async fn detect_text(&self, text: &str) -> AppResult<Vec<Detection>> {
        Self::detections_in(text, &self.analyze(text).await?)
    }

    /// Turns the analyzer's items for `text` into detections, converting its character
    /// offsets to byte offsets and leaving out the disallowed entity types.
    fn detections_in(text: &str, items: &[PresidioAnalyzedItem]) -> AppResult<Vec<Detection>> {
        let offsets = CharOffsets::new(text);
        let lines = TextLines::new(text);
        items
            .iter()
            .filter(|item| !item.is_disallowed())
            .map(|item| {
                let (Some(start), Some(end)) = (item.start, item.end) else {
                    return Err(AppError::SystemError {
                        message: format!("Presidio returned an item without offsets: {item:?}"),
                    });
                };
                let range = offsets
                    .byte_range(start, end)
                    .ok_or(AppError::DetectionOutsideContent { start, end })?;
                lines.detection(
                    range,
                    DetectionLabel::new(&item.entity_type),
                    item.score.map(Score::try_from).transpose()?,
                )
            })
            .collect()
    }

    pub async fn redact_text_file(&self, input: RedacterDataItem) -> AppResult<Redacted> {
        let text_content = match input.content {
            RedacterDataItemContent::Value(content) => Ok(content),
            _ => Err(AppError::SystemError {
                message: "Unsupported item for text redacting".to_string(),
            }),
        }?;

        let detections = self.detect_text(&text_content).await?;
        let redacted_text_content = mask_detections(&text_content, &detections);
        Ok(Redacted::located(
            RedacterDataItem {
                file_ref: input.file_ref,
                content: RedacterDataItemContent::Value(redacted_text_content),
            },
            detections,
        ))
    }

    pub async fn redact_image_file(&self, input: RedacterDataItem) -> AppResult<Redacted> {
        let redact_url = self.presidio_options.image_redact_url.as_ref().ok_or(
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
                    return Err(Self::request_failure("redact image", response.status()));
                }
                let redacted_image_bytes = response.bytes().await?;
                Ok(Redacted::unlocated(RedacterDataItem {
                    file_ref: input.file_ref,
                    content: RedacterDataItemContent::Image {
                        mime_type,
                        data: redacted_image_bytes,
                    },
                }))
            }
            _ => Err(AppError::SystemError {
                message: "Unsupported item for image redacting".to_string(),
            }),
        }
    }
}

impl<'a> Redacter for PresidioRedacter<'a> {
    async fn redact(&self, input: RedacterDataItem) -> AppResult<Redacted> {
        match &input.content {
            RedacterDataItemContent::Value(_) => self.redact_text_file(input).await,
            RedacterDataItemContent::Image { .. } => self.redact_image_file(input).await,
            RedacterDataItemContent::Table { .. } | RedacterDataItemContent::Pdf { .. } => {
                Err(AppError::unsupported_content())
            }
        }
    }

    async fn redact_support(&self, file_ref: &FileSystemRef) -> AppResult<RedactSupport> {
        Ok(match file_ref.media_type.as_ref() {
            Some(media_type)
                if Redacters::is_mime_text(media_type)
                    && self.presidio_options.text_analyze_url.is_some() =>
            {
                RedactSupport::Supported
            }
            Some(media_type)
                if Redacters::is_mime_image(media_type)
                    && self.presidio_options.image_redact_url.is_some()
                    && self.presidio_options.categories.is_none() =>
            {
                RedactSupport::Supported
            }
            _ => RedactSupport::Unsupported,
        })
    }

    /// Only text goes to the analyzer: the image endpoint redacts without reporting where, so
    /// images are read with OCR and their text is analysed. Under `--categories` redaction does
    /// the same, since the image endpoint takes no entity list and would redact every kind.
    async fn detect_support(&self, file_ref: &FileSystemRef) -> AppResult<RedactSupport> {
        Ok(match file_ref.media_type.as_ref() {
            Some(media_type)
                if Redacters::is_mime_text(media_type)
                    && self.presidio_options.text_analyze_url.is_some() =>
            {
                RedactSupport::Supported
            }
            _ => RedactSupport::Unsupported,
        })
    }

    async fn detect(&self, input: &RedacterDataItem) -> AppResult<Detections> {
        match &input.content {
            RedacterDataItemContent::Value(text) => {
                self.detect_text(text).await.map(Detections::from)
            }
            RedacterDataItemContent::Table { .. }
            | RedacterDataItemContent::Image { .. }
            | RedacterDataItemContent::Pdf { .. } => Err(AppError::unsupported_content()),
        }
    }

    fn redacter_type(&self) -> RedacterType {
        RedacterType::Presidio
    }
}

#[cfg(test)]
mod tests {
    use console::Term;

    use super::*;
    use crate::common_types::TextImageCoords;
    use crate::file_converters::{ocr::Ocr, FileConverters};
    use crate::redacters::{DetectionLocation, RedacterBaseOptions, StreamRedacter};

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
        let items: Vec<PresidioAnalyzedItem> = serde_json::from_str(ANALYZER_RESPONSE).unwrap();
        assert_eq!(text.chars().count(), 34);

        let detections = PresidioRedacter::detections_in(text, &items).unwrap();

        let name = text.find("Zoë Keller").unwrap();
        let email = text.find("zoe@example.com").unwrap();
        assert_eq!(
            detections,
            vec![
                Detection {
                    label: DetectionLabel::new("PERSON"),
                    location: DetectionLocation::Text {
                        start: name,
                        end: name + "Zoë Keller".len(),
                        line: 1,
                        column: 8,
                    },
                    score: score(0.85),
                    value: "Zoë Keller".to_string(),
                },
                Detection {
                    label: DetectionLabel::new("EMAIL_ADDRESS"),
                    location: DetectionLocation::Text {
                        start: email,
                        end: email + "zoe@example.com".len(),
                        line: 1,
                        column: 20,
                    },
                    score: score(1.0),
                    value: "zoe@example.com".to_string(),
                },
            ],
            "the disallowed US_DRIVER_LICENSE item is left out"
        );
    }

    #[test]
    fn an_item_without_a_score_is_unscored() {
        let items: Vec<PresidioAnalyzedItem> =
            serde_json::from_str(r#"[{"entity_type": "PERSON", "start": 0, "end": 3}]"#).unwrap();
        let detections = PresidioRedacter::detections_in("Zoë", &items).unwrap();
        assert_eq!(detections.len(), 1);
        assert_eq!(detections[0].score, None);
    }

    #[tokio::test]
    async fn images_are_read_with_ocr_rather_than_by_the_image_endpoint() {
        let term = Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let redacter = PresidioRedacter::new(
            PresidioRedacterOptions {
                categories: None,
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

    async fn redacter_limited_to<'a>(
        reporter: &'a AppReporter<'a>,
        categories: Option<Categories>,
    ) -> PresidioRedacter<'a> {
        PresidioRedacter::new(
            PresidioRedacterOptions {
                categories,
                text_analyze_url: Some(Url::parse("http://localhost:5002/analyze").unwrap()),
                image_redact_url: Some(Url::parse("http://localhost:5003/redact").unwrap()),
            },
            reporter,
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn images_are_never_sent_to_the_image_endpoint_when_categories_limit_the_search() {
        let term = Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let categories = Categories::limiting([Category::Credential]);
        let redacter = redacter_limited_to(&reporter, categories).await;
        let file = |media_type: mime::Mime| FileSystemRef {
            relative_path: "file".into(),
            media_type: Some(media_type),
            file_size: None,
        };

        assert_eq!(
            redacter
                .redact_support(&file(mime::IMAGE_PNG))
                .await
                .unwrap(),
            RedactSupport::Unsupported
        );
        assert_eq!(
            redacter
                .redact_support(&file(mime::TEXT_PLAIN))
                .await
                .unwrap(),
            RedactSupport::Supported
        );
    }

    struct NoWords;

    impl Ocr for NoWords {
        fn image_to_text(&self, _image: image::DynamicImage) -> AppResult<Vec<TextImageCoords>> {
            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn an_image_is_planned_with_ocr_only_when_categories_limit_the_search() {
        let term = Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let mut converters = FileConverters::new();
        converters.ocr = Some(Box::new(NoWords));
        let png = FileSystemRef {
            relative_path: "scan.png".into(),
            media_type: Some(mime::IMAGE_PNG),
            file_size: None,
        };
        let plan_with_ocr = async |categories: Option<Categories>| {
            let options = RedacterBaseOptions {
                allow_unsupported_copies: false,
                unsupported_as_text: false,
                csv_headers_disable: false,
                csv_delimiter: None,
                sampling_size: None,
                limit_dlp_requests: None,
                categories: categories.clone(),
                list_redactions: true,
            };
            let redacters = vec![Redacters::Presidio(
                redacter_limited_to(&reporter, categories).await,
            )];
            let stream_redacter = StreamRedacter::new(&options, &converters);
            let plan = stream_redacter
                .create_redact_plan(&redacters, &png)
                .await
                .unwrap();
            (plan.apply_ocr, plan.supported_redacters.len())
        };

        assert_eq!(plan_with_ocr(None).await, (false, 1));
        assert_eq!(
            plan_with_ocr(Categories::limiting([Category::Credential])).await,
            (true, 1)
        );
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-presidio"), ignore)]
    async fn redact_text_file_test() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let term = Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let test_analyze_url: Url = Url::parse(
            std::env::var("TEST_PRESIDIO_ANALYZE_URL")
                .expect("TEST_PRESIDIO_ANALYZE_URL required")
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

        let redacter = PresidioRedacter::new(
            PresidioRedacterOptions {
                categories: None,
                text_analyze_url: Some(test_analyze_url),
                image_redact_url: None,
            },
            &reporter,
        )
        .await?;

        let redacted_item = redacter.redact(input).await?;
        match redacted_item.item.content {
            RedacterDataItemContent::Value(value) => {
                assert_eq!(value, "Hello, XXXX");
            }
            _ => panic!("Unexpected redacted content type"),
        }

        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-presidio"), ignore)]
    async fn detect_text_finds_values_at_their_byte_offsets() {
        let term = Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let test_analyze_url: Url = Url::parse(
            std::env::var("TEST_PRESIDIO_ANALYZE_URL")
                .expect("TEST_PRESIDIO_ANALYZE_URL required")
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
        let redacter = PresidioRedacter::new(
            PresidioRedacterOptions {
                categories: None,
                text_analyze_url: Some(test_analyze_url),
                image_redact_url: None,
            },
            &reporter,
        )
        .await
        .unwrap();

        let detections = redacter.detect(&input).await.unwrap().found;

        let email = text.find("zoe.keller@example.com").unwrap();
        assert!(
            detections.iter().any(|detection| {
                detection.label == DetectionLabel::new("EMAIL_ADDRESS")
                    && detection.location
                        == DetectionLocation::Text {
                            start: email,
                            end: email + "zoe.keller@example.com".len(),
                            line: 1,
                            column: text[..email].chars().count() + 1,
                        }
                    && detection.value == "zoe.keller@example.com"
                    && detection.score.is_some()
            }),
            "{detections:?}"
        );
    }

    #[test]
    fn a_supported_entity_is_listed_once() {
        let mut names: Vec<&str> = PresidioRedacter::ENTITY_CATEGORIES
            .iter()
            .map(|(name, _)| *name)
            .collect();
        let listed = names.len();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), listed);
    }

    #[test]
    fn an_entity_is_placed_by_what_it_holds_and_a_custom_one_is_other() {
        assert_eq!(
            PresidioRedacter::entity_category("UK_POSTCODE"),
            Category::Address
        );
        assert_eq!(
            PresidioRedacter::entity_category("LOCATION"),
            Category::Other
        );
        assert_eq!(
            PresidioRedacter::entity_category("EMPLOYEE_BADGE"),
            Category::Other
        );
    }
}
