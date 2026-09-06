use aws_config::Region;
use aws_sdk_bedrockruntime::types::{
    GuardrailAction, GuardrailContentBlock, GuardrailContentSource, GuardrailTextBlock,
};
use rvstruct::ValueStruct;

use crate::args::RedacterType;
use crate::errors::AppError;
use crate::file_systems::FileSystemRef;
use crate::redacters::{
    bedrock_error, RedactSupport, Redacter, RedacterDataItem, RedacterDataItemContent, Redacters,
};
use crate::reporter::AppReporter;
use crate::AppResult;

#[derive(Debug, Clone, ValueStruct)]
pub struct AwsBedrockGuardrailId(String);

#[derive(Debug, Clone, ValueStruct)]
pub struct AwsBedrockGuardrailVersion(String);

#[derive(Debug, Clone)]
pub struct AwsBedrockGuardrailsRedacterOptions {
    pub region: Option<Region>,
    pub guardrail_id: AwsBedrockGuardrailId,
    pub guardrail_version: AwsBedrockGuardrailVersion,
}

#[derive(Clone)]
pub struct AwsBedrockGuardrailsRedacter<'a> {
    client: aws_sdk_bedrockruntime::Client,
    options: AwsBedrockGuardrailsRedacterOptions,
    #[allow(dead_code)]
    reporter: &'a AppReporter<'a>,
}

/// Interprets the response of an `ApplyGuardrail` call.
///
/// Measured against the live service: when the guardrail's sensitive information filters are
/// set to anonymize, an intervention comes back with `action` `GUARDRAIL_INTERVENED` and the
/// anonymized text (placeholders such as `{NAME}` kept as-is) in `outputs`; no PII found comes
/// back as `action` `NONE` with empty `outputs`. When a filter is instead set to block, the
/// service still reports `GUARDRAIL_INTERVENED`, but `outputs` carries the guardrail's blocked
/// message rather than redacted text - that must never be handed back as if it were redacted
/// content, so a blocked entity or regex match is treated as a configuration error.
fn interpret_guardrail_output(
    output: &aws_sdk_bedrockruntime::operation::apply_guardrail::ApplyGuardrailOutput,
    guardrail_id: &str,
) -> Result<Option<String>, AppError> {
    use aws_sdk_bedrockruntime::types::GuardrailSensitiveInformationPolicyAction as PolicyAction;

    let blocked = output.assessments().iter().any(|assessment| {
        assessment
            .sensitive_information_policy()
            .is_some_and(|policy| {
                policy
                    .pii_entities()
                    .iter()
                    .any(|entity| matches!(entity.action(), PolicyAction::Blocked))
                    || policy
                        .regexes()
                        .iter()
                        .any(|regex| matches!(regex.action(), PolicyAction::Blocked))
            })
    });
    if blocked {
        return Err(AppError::AwsBedrockError {
            message: format!(
                "AWS Bedrock guardrail '{guardrail_id}' is configured to block sensitive \
                 information instead of anonymizing it; set its sensitive information \
                 filters' action to Anonymize so the redacted text can be returned"
            ),
        });
    }

    match output.action() {
        GuardrailAction::GuardrailIntervened if !output.outputs().is_empty() => Ok(Some(
            output
                .outputs()
                .iter()
                .filter_map(|content| content.text())
                .collect::<String>(),
        )),
        _ => Ok(None),
    }
}

impl<'a> AwsBedrockGuardrailsRedacter<'a> {
    pub async fn new(
        options: AwsBedrockGuardrailsRedacterOptions,
        reporter: &'a AppReporter<'a>,
    ) -> AppResult<Self> {
        let region_provider =
            aws_config::meta::region::RegionProviderChain::first_try(options.region.clone())
                .or_default_provider();
        let shared_config = aws_config::from_env().region(region_provider).load().await;
        let client = aws_sdk_bedrockruntime::Client::new(&shared_config);
        Ok(Self {
            client,
            options,
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

        let text_block = GuardrailTextBlock::builder()
            .text(text_content.clone())
            .build()
            .map_err(|err| AppError::AwsBedrockError {
                message: format!("Failed to build the guardrail text block: {err}"),
            })?;

        let result = self
            .client
            .apply_guardrail()
            .guardrail_identifier(self.options.guardrail_id.value().clone())
            .guardrail_version(self.options.guardrail_version.value().clone())
            .source(GuardrailContentSource::Input)
            .content(GuardrailContentBlock::Text(text_block))
            .send()
            .await
            .map_err(|err| bedrock_error("Failed to apply the guardrail", &err))?;

        let redacted_content =
            interpret_guardrail_output(&result, self.options.guardrail_id.value())?
                .unwrap_or(text_content);

        Ok(RedacterDataItem {
            file_ref: input.file_ref,
            content: RedacterDataItemContent::Value(redacted_content),
        })
    }
}

impl<'a> Redacter for AwsBedrockGuardrailsRedacter<'a> {
    async fn redact(&self, input: RedacterDataItem) -> AppResult<RedacterDataItem> {
        match &input.content {
            RedacterDataItemContent::Value(_) => self.redact_text_file(input).await,
            RedacterDataItemContent::Table { .. }
            | RedacterDataItemContent::Image { .. }
            | RedacterDataItemContent::Pdf { .. } => Err(AppError::SystemError {
                message: "Attempt to redact of unsupported type".to_string(),
            }),
        }
    }

    async fn redact_support(&self, file_ref: &FileSystemRef) -> AppResult<RedactSupport> {
        Ok(match file_ref.media_type.as_ref() {
            Some(media_type) if Redacters::is_mime_text(media_type) => RedactSupport::Supported,
            _ => RedactSupport::Unsupported,
        })
    }

    fn redacter_type(&self) -> RedacterType {
        RedacterType::AwsBedrockGuardrails
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_bedrockruntime::operation::apply_guardrail::ApplyGuardrailOutput;
    use aws_sdk_bedrockruntime::types::{
        GuardrailAssessment, GuardrailOutputContent, GuardrailPiiEntityFilter,
        GuardrailPiiEntityType, GuardrailRegexFilter,
        GuardrailSensitiveInformationPolicyAction as PolicyAction,
        GuardrailSensitiveInformationPolicyAssessment,
    };
    use console::Term;

    fn output_with_action(action: GuardrailAction, outputs: Vec<&str>) -> ApplyGuardrailOutput {
        let outputs = outputs
            .into_iter()
            .map(|text| {
                GuardrailOutputContent::builder()
                    .text(text.to_string())
                    .build()
            })
            .collect();
        ApplyGuardrailOutput::builder()
            .action(action)
            .set_outputs(Some(outputs))
            .set_assessments(Some(Vec::new()))
            .build()
            .expect("action, outputs and assessments are set above")
    }

    #[test]
    fn no_intervention_returns_none() {
        let output = output_with_action(GuardrailAction::None, vec![]);
        let redacted = interpret_guardrail_output(&output, "gr-1").unwrap();
        assert_eq!(redacted, None);
    }

    #[test]
    fn masked_intervention_returns_the_anonymized_text() {
        let output =
            output_with_action(GuardrailAction::GuardrailIntervened, vec!["Hello, {NAME}"]);
        let redacted = interpret_guardrail_output(&output, "gr-1").unwrap();
        assert_eq!(redacted, Some("Hello, {NAME}".to_string()));
    }

    #[test]
    fn multiple_outputs_are_concatenated_in_order() {
        let output = output_with_action(
            GuardrailAction::GuardrailIntervened,
            vec!["Hello, {NAME}. ", "Call {PHONE}."],
        );
        let redacted = interpret_guardrail_output(&output, "gr-1").unwrap();
        assert_eq!(redacted, Some("Hello, {NAME}. Call {PHONE}.".to_string()));
    }

    #[test]
    fn intervened_with_no_outputs_returns_none() {
        let output = output_with_action(GuardrailAction::GuardrailIntervened, vec![]);
        let redacted = interpret_guardrail_output(&output, "gr-1").unwrap();
        assert_eq!(redacted, None);
    }

    #[test]
    fn blocked_pii_entity_is_rejected_naming_the_guardrail() {
        let assessment = GuardrailAssessment::builder()
            .sensitive_information_policy(
                GuardrailSensitiveInformationPolicyAssessment::builder()
                    .pii_entities(
                        GuardrailPiiEntityFilter::builder()
                            .r#match("John")
                            .r#type(GuardrailPiiEntityType::Name)
                            .action(PolicyAction::Blocked)
                            .build()
                            .expect("match, type and action are set above"),
                    )
                    .set_regexes(Some(Vec::new()))
                    .build()
                    .expect("pii_entities and regexes are set above"),
            )
            .build();
        let output = ApplyGuardrailOutput::builder()
            .action(GuardrailAction::GuardrailIntervened)
            .outputs(
                GuardrailOutputContent::builder()
                    .text("Blocked by guardrail")
                    .build(),
            )
            .assessments(assessment)
            .build()
            .expect("action, outputs and assessments are set above");

        let err = interpret_guardrail_output(&output, "gr-42").unwrap_err();
        let message = err.to_string();
        assert!(message.contains("gr-42"), "message was: {message}");
        assert!(message.to_lowercase().contains("anonymize"));
    }

    #[test]
    fn blocked_regex_is_rejected() {
        let assessment = GuardrailAssessment::builder()
            .sensitive_information_policy(
                GuardrailSensitiveInformationPolicyAssessment::builder()
                    .regexes(
                        GuardrailRegexFilter::builder()
                            .name("custom-id")
                            .action(PolicyAction::Blocked)
                            .build()
                            .expect("action is set above"),
                    )
                    .set_pii_entities(Some(Vec::new()))
                    .build()
                    .expect("pii_entities and regexes are set above"),
            )
            .build();
        let output = ApplyGuardrailOutput::builder()
            .action(GuardrailAction::GuardrailIntervened)
            .outputs(
                GuardrailOutputContent::builder()
                    .text("Blocked by guardrail")
                    .build(),
            )
            .assessments(assessment)
            .build()
            .expect("action, outputs and assessments are set above");

        assert!(interpret_guardrail_output(&output, "gr-42").is_err());
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-aws"), ignore)]
    async fn redact_text_file_test() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let term = Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let test_aws_region = std::env::var("TEST_AWS_REGION").expect("TEST_AWS_REGION required");
        let test_guardrail_id = std::env::var("TEST_AWS_BEDROCK_GUARDRAIL_ID")
            .expect("TEST_AWS_BEDROCK_GUARDRAIL_ID required");
        let test_content = "Hello, John";

        let file_ref = FileSystemRef {
            relative_path: "temp_file.txt".into(),
            media_type: Some(mime::TEXT_PLAIN),
            file_size: Some(test_content.len()),
        };

        let content = RedacterDataItemContent::Value(test_content.to_string());
        let input = RedacterDataItem { file_ref, content };

        let redacter = AwsBedrockGuardrailsRedacter::new(
            AwsBedrockGuardrailsRedacterOptions {
                region: Some(Region::new(test_aws_region)),
                guardrail_id: AwsBedrockGuardrailId::from(test_guardrail_id),
                guardrail_version: AwsBedrockGuardrailVersion::from("DRAFT".to_string()),
            },
            &reporter,
        )
        .await?;

        let redacted_item = redacter.redact(input).await?;
        match redacted_item.content {
            RedacterDataItemContent::Value(value) => {
                assert_eq!(value, "Hello, {NAME}");
            }
            _ => panic!("Unexpected redacted content type"),
        }

        Ok(())
    }
}
