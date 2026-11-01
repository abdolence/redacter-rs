use aws_config::Region;
use aws_sdk_bedrockruntime::operation::apply_guardrail::ApplyGuardrailOutput;
use aws_sdk_bedrockruntime::types::{
    GuardrailAction, GuardrailContentBlock, GuardrailContentSource, GuardrailPiiEntityType,
    GuardrailTextBlock,
};
use rvstruct::ValueStruct;

use crate::args::RedacterType;
use crate::errors::AppError;
use crate::file_systems::FileSystemRef;
use crate::redacters::{
    bedrock_error, Category, Detection, DetectionLabel, Detections, RedactSupport, Redacted,
    Redacter, RedacterDataItem, RedacterDataItemContent, Redacters,
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
    output: &ApplyGuardrailOutput,
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
    /// The category of a finding labelled `name`: the PII entity type of the name, `other` for
    /// any other name, such as a regex filter's, or a type this version of the SDK does not know.
    #[cfg_attr(not(test), expect(dead_code, reason = "wired in with --categories"))]
    pub fn label_category(name: &str) -> Category {
        match GuardrailPiiEntityType::from(name) {
            GuardrailPiiEntityType::Name | GuardrailPiiEntityType::Username => Category::Person,
            GuardrailPiiEntityType::Email | GuardrailPiiEntityType::Phone => Category::Contact,
            GuardrailPiiEntityType::Address => Category::Address,
            GuardrailPiiEntityType::DriverId
            | GuardrailPiiEntityType::LicensePlate
            | GuardrailPiiEntityType::VehicleIdentificationNumber
            | GuardrailPiiEntityType::CaHealthNumber
            | GuardrailPiiEntityType::CaSocialInsuranceNumber
            | GuardrailPiiEntityType::UkNationalHealthServiceNumber
            | GuardrailPiiEntityType::UkNationalInsuranceNumber
            | GuardrailPiiEntityType::UkUniqueTaxpayerReferenceNumber
            | GuardrailPiiEntityType::UsIndividualTaxIdentificationNumber
            | GuardrailPiiEntityType::UsPassportNumber
            | GuardrailPiiEntityType::UsSocialSecurityNumber => Category::IdNumber,
            GuardrailPiiEntityType::CreditDebitCardCvv
            | GuardrailPiiEntityType::CreditDebitCardExpiry
            | GuardrailPiiEntityType::CreditDebitCardNumber
            | GuardrailPiiEntityType::InternationalBankAccountNumber
            | GuardrailPiiEntityType::SwiftCode
            | GuardrailPiiEntityType::UsBankAccountNumber
            | GuardrailPiiEntityType::UsBankRoutingNumber => Category::Financial,
            GuardrailPiiEntityType::AwsAccessKey
            | GuardrailPiiEntityType::AwsSecretKey
            | GuardrailPiiEntityType::Password
            | GuardrailPiiEntityType::Pin => Category::Credential,
            GuardrailPiiEntityType::IpAddress
            | GuardrailPiiEntityType::MacAddress
            | GuardrailPiiEntityType::Url => Category::Network,
            GuardrailPiiEntityType::Age => Category::Other,
            _ => Category::Other,
        }
    }

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

    async fn apply_guardrail(&self, text: &str) -> AppResult<ApplyGuardrailOutput> {
        let text_block = GuardrailTextBlock::builder()
            .text(text)
            .build()
            .map_err(|err| AppError::AwsBedrockError {
                message: format!("Failed to build the guardrail text block: {err}"),
            })?;

        self.client
            .apply_guardrail()
            .guardrail_identifier(self.options.guardrail_id.value().clone())
            .guardrail_version(self.options.guardrail_version.value().clone())
            .source(GuardrailContentSource::Input)
            .content(GuardrailContentBlock::Text(text_block))
            .send()
            .await
            .map_err(|err| bedrock_error("Failed to apply the guardrail", &err))
    }

    /// The sensitive values the guardrail reported for `text`, found wherever they occur in
    /// it: the service names the matched strings but gives no offsets. Reads the assessments
    /// whatever the filters' actions are, since detection does not need the text anonymized.
    fn detections_in(text: &str, output: &ApplyGuardrailOutput) -> AppResult<Detections> {
        let mut values = Vec::new();
        for policy in output
            .assessments()
            .iter()
            .filter_map(|assessment| assessment.sensitive_information_policy())
        {
            values.extend(policy.pii_entities().iter().map(|entity| {
                (
                    entity.r#match().to_string(),
                    DetectionLabel::new(entity.r#type().as_str()),
                )
            }));
            values.extend(policy.regexes().iter().filter_map(|regex| {
                Some((
                    regex.r#match()?.to_string(),
                    DetectionLabel::new(regex.name().unwrap_or("regex")),
                ))
            }));
        }
        Detection::locate_values(text, &values)
    }

    pub async fn redact_text_file(&self, input: RedacterDataItem) -> AppResult<Redacted> {
        let text_content = match input.content {
            RedacterDataItemContent::Value(content) => Ok(content),
            _ => Err(AppError::SystemError {
                message: "Unsupported item for text redacting".to_string(),
            }),
        }?;

        let result = self.apply_guardrail(&text_content).await?;

        let anonymized = interpret_guardrail_output(&result, self.options.guardrail_id.value())?;
        let replaced =
            Self::detections_in(&text_content, &result).map(|detections| detections.found);
        let redacted_content = anonymized.unwrap_or(text_content);

        Ok(Redacted::located_or_unlocated(
            RedacterDataItem {
                file_ref: input.file_ref,
                content: RedacterDataItemContent::Value(redacted_content),
            },
            replaced,
            self.reporter,
            RedacterType::AwsBedrockGuardrails,
        ))
    }
}

impl<'a> Redacter for AwsBedrockGuardrailsRedacter<'a> {
    async fn redact(&self, input: RedacterDataItem) -> AppResult<Redacted> {
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

    async fn detect(&self, input: &RedacterDataItem) -> AppResult<Detections> {
        match &input.content {
            RedacterDataItemContent::Value(text) => {
                Self::detections_in(text, &self.apply_guardrail(text).await?)
            }
            RedacterDataItemContent::Table { .. }
            | RedacterDataItemContent::Image { .. }
            | RedacterDataItemContent::Pdf { .. } => Err(AppError::unsupported_content()),
        }
    }

    fn redacter_type(&self) -> RedacterType {
        RedacterType::AwsBedrockGuardrails
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::redacters::DetectionLocation;
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

    fn output_with_assessment(
        pii_entities: Vec<GuardrailPiiEntityFilter>,
        regexes: Vec<GuardrailRegexFilter>,
    ) -> ApplyGuardrailOutput {
        ApplyGuardrailOutput::builder()
            .action(GuardrailAction::GuardrailIntervened)
            .assessments(
                GuardrailAssessment::builder()
                    .sensitive_information_policy(
                        GuardrailSensitiveInformationPolicyAssessment::builder()
                            .set_pii_entities(Some(pii_entities))
                            .set_regexes(Some(regexes))
                            .build()
                            .expect("pii_entities and regexes are set above"),
                    )
                    .build(),
            )
            .set_outputs(Some(Vec::new()))
            .build()
            .expect("action, outputs and assessments are set above")
    }

    fn pii_entity(
        entity_type: GuardrailPiiEntityType,
        matched: &str,
        action: PolicyAction,
    ) -> GuardrailPiiEntityFilter {
        GuardrailPiiEntityFilter::builder()
            .r#match(matched)
            .r#type(entity_type)
            .action(action)
            .build()
            .expect("match, type and action are set above")
    }

    #[test]
    fn reported_values_are_located_everywhere_they_occur_in_the_text() {
        let text = "Grüße John Müller, mail john@example.com. Order AB-1234 for John Müller.";
        let output = output_with_assessment(
            vec![
                pii_entity(
                    GuardrailPiiEntityType::Name,
                    "John Müller",
                    PolicyAction::Anonymized,
                ),
                pii_entity(
                    GuardrailPiiEntityType::Email,
                    "john@example.com",
                    PolicyAction::Blocked,
                ),
            ],
            vec![GuardrailRegexFilter::builder()
                .name("order-id")
                .r#match("AB-1234")
                .action(PolicyAction::Anonymized)
                .build()
                .expect("action is set above")],
        );

        let detections = AwsBedrockGuardrailsRedacter::detections_in(text, &output)
            .unwrap()
            .found;

        let located: Vec<(String, DetectionLocation, &str)> = detections
            .iter()
            .map(|detection| {
                (
                    detection.label.to_string(),
                    detection.location,
                    detection.value.as_str(),
                )
            })
            .collect();
        let range = |needle: &str, nth: usize| {
            let start = text.match_indices(needle).nth(nth).unwrap().0;
            DetectionLocation::Text {
                start,
                end: start + needle.len(),
                line: 1,
                column: text[..start].chars().count() + 1,
            }
        };
        assert_eq!(
            located,
            vec![
                ("NAME".to_string(), range("John Müller", 0), "John Müller"),
                (
                    "EMAIL".to_string(),
                    range("john@example.com", 0),
                    "john@example.com"
                ),
                ("order-id".to_string(), range("AB-1234", 0), "AB-1234"),
                ("NAME".to_string(), range("John Müller", 1), "John Müller"),
            ]
        );
        assert!(detections.iter().all(|detection| detection.score.is_none()));
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
        match redacted_item.item.content {
            RedacterDataItemContent::Value(value) => {
                assert_eq!(value, "Hello, {NAME}");
            }
            _ => panic!("Unexpected redacted content type"),
        }

        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-aws"), ignore)]
    async fn detect_text_locates_the_values_the_guardrail_reports() {
        let term = Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let test_aws_region = std::env::var("TEST_AWS_REGION").expect("TEST_AWS_REGION required");
        let test_guardrail_id = std::env::var("TEST_AWS_BEDROCK_GUARDRAIL_ID")
            .expect("TEST_AWS_BEDROCK_GUARDRAIL_ID required");
        let redacter = AwsBedrockGuardrailsRedacter::new(
            AwsBedrockGuardrailsRedacterOptions {
                region: Some(Region::new(test_aws_region)),
                guardrail_id: AwsBedrockGuardrailId::from(test_guardrail_id),
                guardrail_version: AwsBedrockGuardrailVersion::from("DRAFT".to_string()),
            },
            &reporter,
        )
        .await
        .unwrap();
        let item = |text: &str| RedacterDataItem {
            file_ref: FileSystemRef {
                relative_path: "temp_file.txt".into(),
                media_type: Some(mime::TEXT_PLAIN),
                file_size: Some(text.len()),
            },
            content: RedacterDataItemContent::Value(text.to_string()),
        };

        let text = "Grüße aus Köln, Zoë Keller schreibt an zoe.keller@example.com";
        let detections = redacter.detect(&item(text)).await.unwrap().found;

        let email = text.find("zoe.keller@example.com").unwrap();
        assert!(
            detections.iter().any(|detection| {
                detection.label == DetectionLabel::new("EMAIL")
                    && detection.location
                        == DetectionLocation::Text {
                            start: email,
                            end: email + "zoe.keller@example.com".len(),
                            line: 1,
                            column: text[..email].chars().count() + 1,
                        }
                    && detection.value == "zoe.keller@example.com"
                    && detection.score.is_none()
            }),
            "{detections:?}"
        );
        assert!(redacter
            .detect(&item("Nothing sensitive here."))
            .await
            .unwrap()
            .found
            .is_empty());
    }

    #[test]
    fn only_age_is_other() {
        let other: Vec<&str> = GuardrailPiiEntityType::values()
            .iter()
            .copied()
            .filter(|name| AwsBedrockGuardrailsRedacter::label_category(name) == Category::Other)
            .collect();
        assert_eq!(other, ["AGE"]);
    }

    #[test]
    fn a_pii_type_is_placed_by_what_it_holds_and_a_regex_filter_is_other() {
        assert_eq!(
            AwsBedrockGuardrailsRedacter::label_category("US_SOCIAL_SECURITY_NUMBER"),
            Category::IdNumber
        );
        assert_eq!(
            AwsBedrockGuardrailsRedacter::label_category("internal-ticket"),
            Category::Other
        );
    }
}
