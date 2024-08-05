use rvstruct::ValueStruct;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::errors::AppError;
use crate::filesystems::FileSystemRef;
use crate::redacters::{
    RedactSupportedOptions, Redacter, RedacterDataItem, RedacterDataItemContent, RedacterOptions,
    Redacters,
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
    redacter_options: RedacterOptions,
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
}

impl<'a> MsPresidioRedacter<'a> {
    /// List of entity types that should be disallowed for redacting
    /// since they produce a lot of false positives
    const DISALLOW_ENTITY_TYPES: [&'static str; 1] = ["US_DRIVER_LICENSE"];

    pub async fn new(
        redacter_options: RedacterOptions,
        ms_presidio_options: MsPresidioRedacterOptions,
        reporter: &'a AppReporter<'a>,
    ) -> AppResult<Self> {
        let client = reqwest::Client::new();
        Ok(Self {
            client,
            ms_presidio_options,
            redacter_options,
            reporter,
        })
    }

    pub async fn redact_text_file(
        &self,
        input: RedacterDataItem,
    ) -> AppResult<RedacterDataItemContent> {
        self.reporter.report(format!(
            "Redacting a text file: {} ({:?})",
            input.file_ref.relative_path.value(),
            input.file_ref.media_type
        ))?;
        let text_content = match input.content {
            RedacterDataItemContent::Value(content) => Ok(content),
            _ => Err(AppError::SystemError {
                message: "Unsupported item for text redacting".to_string(),
            }),
        }?;

        let analyze_url = self.ms_presidio_options.text_analyze_url.as_ref().ok_or(
            AppError::RedacterConfigError {
                message: "Text analyze URL is not configured".to_string(),
            },
        )?;
        let analyze_request = MsPresidioAnalyzeRequest {
            text: text_content.clone(),
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
                    "Failed to analyze text: {}. HTTP status: {}.",
                    response_text, response_status
                ),
            });
        }
        let response_items: Vec<MsPresidioAnalyzedItem> = response.json().await?;
        let redacted_text_content = response_items
            .iter()
            .filter(|item| !Self::DISALLOW_ENTITY_TYPES.contains(&item.entity_type.as_str()))
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
        Ok(RedacterDataItemContent::Value(redacted_text_content))
    }

    pub async fn redact_image_file(
        &self,
        input: RedacterDataItem,
    ) -> AppResult<RedacterDataItemContent> {
        let redact_url = self.ms_presidio_options.image_redact_url.as_ref().ok_or(
            AppError::RedacterConfigError {
                message: "Image redact URL is not configured".to_string(),
            },
        )?;

        match input.content {
            RedacterDataItemContent::Image { mime_type, data } => {
                self.reporter.report(format!(
                    "Redacting an image file: {} ({:?})",
                    input.file_ref.relative_path.value(),
                    input.file_ref.media_type
                ))?;
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
                            "Failed to redact image: {}. HTTP status: {}.",
                            response_text, response_status
                        ),
                    });
                }
                let redacted_image_bytes = response.bytes().await?;
                Ok(RedacterDataItemContent::Image {
                    mime_type,
                    data: redacted_image_bytes.into(),
                })
            }
            _ => {
                return Err(AppError::SystemError {
                    message: "Unsupported item for image redacting".to_string(),
                });
            }
        }
    }
}

impl<'a> Redacter for MsPresidioRedacter<'a> {
    async fn redact(&self, input: RedacterDataItem) -> AppResult<RedacterDataItemContent> {
        match &input.content {
            RedacterDataItemContent::Value(_) => self.redact_text_file(input).await,
            RedacterDataItemContent::Image { .. } => self.redact_image_file(input).await,
            RedacterDataItemContent::Table { .. } => Err(AppError::SystemError {
                message: "Attempt to redact of unsupported table type".to_string(),
            }),
        }
    }

    async fn redact_supported_options(
        &self,
        file_ref: &FileSystemRef,
    ) -> AppResult<RedactSupportedOptions> {
        Ok(match file_ref.media_type.as_ref() {
            Some(media_type)
                if Redacters::is_mime_text(media_type)
                    && self.ms_presidio_options.text_analyze_url.is_some() =>
            {
                RedactSupportedOptions::Supported
            }
            Some(media_type)
                if Redacters::is_mime_table(media_type)
                    && self.ms_presidio_options.text_analyze_url.is_some() =>
            {
                RedactSupportedOptions::SupportedAsText
            }
            Some(media_type)
                if Redacters::is_mime_image(media_type)
                    && self.ms_presidio_options.image_redact_url.is_some() =>
            {
                RedactSupportedOptions::Supported
            }
            _ => RedactSupportedOptions::Unsupported,
        })
    }

    fn options(&self) -> &RedacterOptions {
        &self.redacter_options
    }
}

#[allow(unused_imports)]
mod tests {
    use console::Term;

    use crate::redacters::RedacterProviderOptions;

    use super::*;

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
            file_size: Some(test_content.len() as u64),
        };

        let content = RedacterDataItemContent::Value(test_content.to_string());
        let input = RedacterDataItem { file_ref, content };

        let redacter_options = RedacterOptions {
            provider_options: RedacterProviderOptions::MsPresidio(MsPresidioRedacterOptions {
                text_analyze_url: Some(test_analyze_url.clone()),
                image_redact_url: None,
            }),
            allow_unsupported_copies: false,
            csv_headers_disable: false,
            csv_delimiter: None,
        };

        let redacter = MsPresidioRedacter::new(
            redacter_options,
            MsPresidioRedacterOptions {
                text_analyze_url: Some(test_analyze_url),
                image_redact_url: None,
            },
            &reporter,
        )
        .await?;

        let redacted_content = redacter.redact(input).await?;
        match redacted_content {
            RedacterDataItemContent::Value(value) => {
                assert_eq!(value, "Hello, XXXX");
            }
            _ => panic!("Unexpected redacted content type"),
        }

        Ok(())
    }
}
