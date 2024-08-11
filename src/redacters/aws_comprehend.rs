use crate::args::RedacterType;
use crate::errors::AppError;
use crate::filesystems::FileSystemRef;
use crate::redacters::{
    RedactSupportedOptions, Redacter, RedacterDataItem, RedacterDataItemContent, Redacters,
};
use crate::reporter::AppReporter;
use crate::AppResult;
use aws_config::Region;

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

        let aws_request = self
            .client
            .detect_pii_entities()
            .language_code(aws_sdk_comprehend::types::LanguageCode::En)
            .text(text_content.clone());

        let result = aws_request.send().await?;
        let redacted_content = result.entities.iter().fold(text_content, |acc, entity| {
            entity.iter().fold(acc, |acc, entity| {
                match (entity.begin_offset, entity.end_offset) {
                    (Some(start), Some(end)) => [
                        acc[..start as usize].to_string(),
                        "X".repeat((end - start) as usize),
                        acc[end as usize..].to_string(),
                    ]
                    .concat(),
                    (Some(start), None) => {
                        acc[..start as usize].to_string()
                            + "X".repeat(acc.len() - start as usize).as_str()
                    }
                    (None, Some(end)) => {
                        ["X".repeat(end as usize), acc[end as usize..].to_string()].concat()
                    }
                    _ => acc,
                }
            })
        });
        Ok(RedacterDataItem {
            file_ref: input.file_ref,
            content: RedacterDataItemContent::Value(redacted_content),
        })
    }
}

impl<'a> Redacter for AwsComprehendRedacter<'a> {
    async fn redact(&self, input: RedacterDataItem) -> AppResult<RedacterDataItem> {
        match &input.content {
            RedacterDataItemContent::Value(_) => self.redact_text_file(input).await,
            RedacterDataItemContent::Table { .. } | RedacterDataItemContent::Image { .. } => {
                Err(AppError::SystemError {
                    message: "Attempt to redact of unsupported type".to_string(),
                })
            }
        }
    }

    async fn redact_supported_options(
        &self,
        file_ref: &FileSystemRef,
    ) -> AppResult<RedactSupportedOptions> {
        Ok(match file_ref.media_type.as_ref() {
            Some(media_type) if Redacters::is_mime_text(media_type) => {
                RedactSupportedOptions::Supported
            }
            Some(media_type) if Redacters::is_mime_table(media_type) => {
                RedactSupportedOptions::SupportedAsText
            }
            _ => RedactSupportedOptions::Unsupported,
        })
    }

    fn redacter_type(&self) -> RedacterType {
        RedacterType::AwsComprehend
    }
}

#[allow(unused_imports)]
mod tests {
    use super::*;
    use crate::redacters::RedacterProviderOptions;
    use console::Term;

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
            file_size: Some(test_content.len() as u64),
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
}
