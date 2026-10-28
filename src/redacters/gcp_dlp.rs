use crate::args::RedacterType;
use crate::common_types::GcpProjectId;
use crate::errors::AppError;
use crate::file_systems::FileSystemRef;
use crate::redacters::{
    Detection, DetectionLabel, DetectionLocation, RedactSupport, Redacter, RedacterDataItem,
    RedacterDataItemContent, Redacters, Score,
};
use crate::reporter::AppReporter;
use crate::AppResult;
use gcloud_sdk::google::privacy::dlp::v2 as dlp;
use gcloud_sdk::google::privacy::dlp::v2::dlp_service_client::DlpServiceClient;
use gcloud_sdk::tonic::metadata::MetadataValue;
use gcloud_sdk::{tonic, GoogleApi, GoogleAuthMiddleware};
use mime::Mime;
use rvstruct::ValueStruct;
use std::collections::HashSet;
use tokio_util::bytes;

#[derive(Clone)]
pub struct GcpDlpRedacter<'a> {
    client: GoogleApi<DlpServiceClient<GoogleAuthMiddleware>>,
    gcp_dlp_options: GcpDlpRedacterOptions,
    #[allow(dead_code)]
    reporter: &'a AppReporter<'a>,
}

#[derive(Debug, Clone)]
pub struct GcpDlpRedacterOptions {
    pub project_id: GcpProjectId,
    pub user_defined_built_in_info_types: Vec<String>,
    pub user_defined_stored_info_types: Vec<String>,
}

impl<'a> GcpDlpRedacter<'a> {
    pub const INFO_TYPES: [&'static str; 20] = [
        "PHONE_NUMBER",
        "EMAIL_ADDRESS",
        "CREDIT_CARD_NUMBER",
        "LOCATION",
        "PERSON_NAME",
        "AGE",
        "DATE_OF_BIRTH",
        "FINANCIAL_ACCOUNT_NUMBER",
        "GENDER",
        "IP_ADDRESS",
        "PASSPORT",
        "AUTH_TOKEN",
        "AWS_CREDENTIALS",
        "BASIC_AUTH_HEADER",
        "VAT_NUMBER",
        "PASSWORD",
        "OAUTH_CLIENT_SECRET",
        "IBAN_CODE",
        "GCP_API_KEY",
        "ENCRYPTION_KEY",
    ];
    pub async fn new(
        gcp_dlp_options: GcpDlpRedacterOptions,
        reporter: &'a AppReporter<'a>,
    ) -> AppResult<Self> {
        let client =
            GoogleApi::from_function(DlpServiceClient::new, "https://dlp.googleapis.com", None)
                .await?;
        Ok(GcpDlpRedacter {
            client,
            gcp_dlp_options,
            reporter,
        })
    }

    pub async fn redact_text_file(&self, input: RedacterDataItem) -> AppResult<RedacterDataItem> {
        let request = self.request(dlp::DeidentifyContentRequest {
            parent: self.parent(),
            inspect_config: Some(self.create_inspect_config()),
            deidentify_config: Some(self.create_deidentify_config()),
            item: Some(input.content.try_into()?),
            ..dlp::DeidentifyContentRequest::default()
        })?;
        let response = self.client.get().deidentify_content(request).await?;

        if let Some(content_item) = response.into_inner().item {
            let content: RedacterDataItemContent = content_item.try_into()?;
            Ok(RedacterDataItem {
                file_ref: input.file_ref,
                content,
            })
        } else {
            Err(AppError::SystemError {
                message: "No content item in the response".to_string(),
            })
        }
    }

    async fn redact_image_content(
        &self,
        input_bytes_content: gcloud_sdk::google::privacy::dlp::v2::ByteContentItem,
    ) -> AppResult<bytes::Bytes> {
        let request = self.request(dlp::RedactImageRequest {
            parent: self.parent(),
            inspect_config: Some(self.create_inspect_config()),
            byte_item: Some(input_bytes_content),
            ..dlp::RedactImageRequest::default()
        })?;
        let response = self.client.get().redact_image(request).await?;
        Ok(response.into_inner().redacted_image.into())
    }

    pub async fn redact_image_file(&self, input: RedacterDataItem) -> AppResult<RedacterDataItem> {
        match &input.content {
            RedacterDataItemContent::Image { mime_type, data: _ } => {
                let output_mime = mime_type.clone();

                let content = RedacterDataItemContent::Image {
                    mime_type: output_mime,
                    data: self.redact_image_content(input.content.try_into()?).await?,
                };
                Ok(RedacterDataItem {
                    file_ref: input.file_ref,
                    content,
                })
            }
            _ => Err(AppError::unsupported_content()),
        }
    }

    fn parent(&self) -> String {
        format!(
            "projects/{}/locations/global",
            self.gcp_dlp_options.project_id.value()
        )
    }

    fn request<T>(&self, message: T) -> AppResult<tonic::Request<T>> {
        let mut request = tonic::Request::new(message);
        request.metadata_mut().insert(
            "x-goog-user-project",
            MetadataValue::<tonic::metadata::Ascii>::try_from(
                self.gcp_dlp_options.project_id.value(),
            )?,
        );
        Ok(request)
    }

    /// The DLP findings of `item`, found with the redaction path's inspect config so that
    /// scanning finds what redacting would replace. The quote is requested because it is the
    /// only text DLP gives for an image finding.
    async fn inspect(&self, item: dlp::ContentItem) -> AppResult<dlp::InspectResult> {
        let request = self.request(dlp::InspectContentRequest {
            parent: self.parent(),
            inspect_config: Some(dlp::InspectConfig {
                include_quote: true,
                ..self.create_inspect_config()
            }),
            item: Some(item),
            ..dlp::InspectContentRequest::default()
        })?;
        let response = self.client.get().inspect_content(request).await?;
        Self::inspect_result(response.into_inner())
    }

    /// The result of an inspect response. A response without one is an error: reading it as
    /// "no findings" would report the item clean.
    fn inspect_result(response: dlp::InspectContentResponse) -> AppResult<dlp::InspectResult> {
        response
            .result
            .ok_or_else(|| AppError::incomplete_result(RedacterType::GcpDlp, "an inspect result"))
    }

    /// Turns the findings DLP returned for `content` into detections. `field_names` are the
    /// column names the table was sent with; they are only read for a table.
    fn detections_in(
        content: &RedacterDataItemContent,
        field_names: &[String],
        result: dlp::InspectResult,
    ) -> AppResult<Vec<Detection>> {
        if result.findings_truncated {
            return Err(AppError::SystemError {
                message: "GCP DLP truncated its findings, so the item has more than it reported"
                    .to_string(),
            });
        }
        let mut detections = Vec::new();
        for finding in result.findings {
            let score = likelihood_score(finding.likelihood());
            let label = finding
                .info_type
                .map(|info_type| DetectionLabel::new(info_type.name))
                .ok_or_else(|| AppError::incomplete_result(RedacterType::GcpDlp, "an info type"))?;
            let location = finding
                .location
                .ok_or_else(|| AppError::incomplete_result(RedacterType::GcpDlp, "a location"))?;
            match content {
                RedacterDataItemContent::Value(text) => {
                    let (start, end) = byte_span(&location)?;
                    detections.push(Detection {
                        label,
                        location: DetectionLocation::Text { start, end },
                        score,
                        value: text_in_range(text, start, end)?,
                    });
                }
                RedacterDataItemContent::Table { rows, .. } => {
                    let (start, end) = byte_span(&location)?;
                    let (row, column) = table_cell(&location, field_names)?;
                    let cell = rows
                        .get(row)
                        .and_then(|cells| cells.get(column))
                        .ok_or_else(|| {
                            AppError::incomplete_result(RedacterType::GcpDlp, "a cell of the table")
                        })?;
                    detections.push(Detection {
                        label,
                        location: DetectionLocation::TableCell {
                            row,
                            column,
                            start,
                            end,
                        },
                        score,
                        value: text_in_range(cell, start, end)?,
                    });
                }
                RedacterDataItemContent::Image { .. } => {
                    for bounding_box in location.content_locations.iter().flat_map(|content| {
                        match &content.location {
                            Some(dlp::content_location::Location::ImageLocation(image)) => {
                                image.bounding_boxes.as_slice()
                            }
                            _ => &[],
                        }
                    }) {
                        detections.push(Detection {
                            label: label.clone(),
                            location: DetectionLocation::Image {
                                page: None,
                                x1: bounding_box.left as f32,
                                y1: bounding_box.top as f32,
                                x2: (bounding_box.left + bounding_box.width) as f32,
                                y2: (bounding_box.top + bounding_box.height) as f32,
                            },
                            score,
                            value: finding.quote.clone(),
                        });
                    }
                }
                RedacterDataItemContent::Pdf { .. } => return Err(AppError::unsupported_content()),
            }
        }
        Ok(detections)
    }

    fn create_inspect_config(&self) -> gcloud_sdk::google::privacy::dlp::v2::InspectConfig {
        gcloud_sdk::google::privacy::dlp::v2::InspectConfig {
            info_types: self
                .create_built_in_info_types()
                .iter()
                .map(|v| gcloud_sdk::google::privacy::dlp::v2::InfoType {
                    name: v.to_string(),
                    ..gcloud_sdk::google::privacy::dlp::v2::InfoType::default()
                })
                .collect(),
            custom_info_types: self
                .gcp_dlp_options
                .user_defined_stored_info_types
                .iter()
                .map(
                    |stored_info_type_name| {
                        gcloud_sdk::google::privacy::dlp::v2::CustomInfoType {
                info_type: Some(gcloud_sdk::google::privacy::dlp::v2::InfoType {
                    name: stored_info_type_name.clone(),
                    ..gcloud_sdk::google::privacy::dlp::v2::InfoType::default()
                }),
                r#type: Some(
                    gcloud_sdk::google::privacy::dlp::v2::custom_info_type::Type::StoredType(
                        gcloud_sdk::google::privacy::dlp::v2::StoredType {
                            name: format!(
                                "projects/{}/storedInfoTypes/{}",
                                self.gcp_dlp_options.project_id.value(),
                                stored_info_type_name
                            ),
                            ..gcloud_sdk::google::privacy::dlp::v2::StoredType::default()
                        },
                    ),
                ),
                ..gcloud_sdk::google::privacy::dlp::v2::CustomInfoType::default()
            }
                    },
                )
                .collect(),
            ..gcloud_sdk::google::privacy::dlp::v2::InspectConfig::default()
        }
    }

    fn create_deidentify_config(&self) -> gcloud_sdk::google::privacy::dlp::v2::DeidentifyConfig {
        let user_stored_info_types_set: HashSet<&str> = self
            .gcp_dlp_options
            .user_defined_stored_info_types
            .iter()
            .map(|s| s.as_str())
            .collect();
        gcloud_sdk::google::privacy::dlp::v2::DeidentifyConfig {
            transformation: Some(gcloud_sdk::google::privacy::dlp::v2::deidentify_config::Transformation::InfoTypeTransformations(
                gcloud_sdk::google::privacy::dlp::v2::InfoTypeTransformations {
                    transformations: vec![
                        gcloud_sdk::google::privacy::dlp::v2::info_type_transformations::InfoTypeTransformation {
                            info_types: self.create_built_in_info_types().union(
                                &user_stored_info_types_set
                            ).collect::<Vec<_>>().iter().map(|v| gcloud_sdk::google::privacy::dlp::v2::InfoType {
                                name: v.to_string(),
                                ..gcloud_sdk::google::privacy::dlp::v2::InfoType::default()
                            }).collect(),
                            primitive_transformation: Some(gcloud_sdk::google::privacy::dlp::v2::PrimitiveTransformation {
                                transformation: Some(
                                    gcloud_sdk::google::privacy::dlp::v2::primitive_transformation::Transformation::ReplaceConfig(gcloud_sdk::google::privacy::dlp::v2::ReplaceValueConfig {
                                        new_value: Some(gcloud_sdk::google::privacy::dlp::v2::Value {
                                            r#type: Some(gcloud_sdk::google::privacy::dlp::v2::value::Type::StringValue(
                                                "[REDACTED]".to_string()
                                            ))
                                        })
                                    })
                                )
                            }),
                        }
                    ]
                })),
            ..gcloud_sdk::google::privacy::dlp::v2::DeidentifyConfig::default()
        }
    }

    fn create_built_in_info_types(&self) -> HashSet<&str> {
        [
            Self::INFO_TYPES.to_vec(),
            self.gcp_dlp_options
                .user_defined_built_in_info_types
                .iter()
                .map(|v| v.as_str())
                .collect(),
        ]
        .concat()
        .into_iter()
        .collect()
    }

    fn check_supported_image_type(mime_type: &Mime) -> bool {
        Redacters::is_mime_image(mime_type)
            && (mime_type.subtype() == "png"
                || mime_type.subtype() == "jpeg"
                || mime_type.subtype() == "jpg"
                || mime_type.subtype() == "jpe"
                || mime_type.subtype() == "gif"
                || mime_type.subtype() == "bmp")
    }
}

impl<'a> Redacter for GcpDlpRedacter<'a> {
    async fn redact(&self, input: RedacterDataItem) -> AppResult<RedacterDataItem> {
        match &input.content {
            RedacterDataItemContent::Table { .. } | RedacterDataItemContent::Value(_) => {
                self.redact_text_file(input).await
            }
            RedacterDataItemContent::Image { mime_type, .. }
                if Self::check_supported_image_type(mime_type) =>
            {
                self.redact_image_file(input).await
            }
            RedacterDataItemContent::Image { .. } | RedacterDataItemContent::Pdf { .. } => {
                Err(AppError::unsupported_content())
            }
        }
    }

    async fn redact_support(&self, file_ref: &FileSystemRef) -> AppResult<RedactSupport> {
        Ok(match file_ref.media_type.as_ref() {
            Some(media_type) if Redacters::is_mime_text(media_type) => RedactSupport::Supported,
            Some(media_type) if Redacters::is_mime_table(media_type) => RedactSupport::Supported,
            Some(media_type) if Self::check_supported_image_type(media_type) => {
                RedactSupport::Supported
            }
            _ => RedactSupport::Unsupported,
        })
    }

    async fn detect(&self, input: &RedacterDataItem) -> AppResult<Vec<Detection>> {
        let mut item: dlp::ContentItem = match &input.content {
            RedacterDataItemContent::Table { .. } | RedacterDataItemContent::Value(_) => {
                input.content.clone().try_into()?
            }
            RedacterDataItemContent::Image { mime_type, .. }
                if Self::check_supported_image_type(mime_type) =>
            {
                dlp::ContentItem {
                    content_metadata: None,
                    data_item: Some(dlp::content_item::DataItem::ByteItem(
                        input.content.clone().try_into()?,
                    )),
                }
            }
            RedacterDataItemContent::Image { .. } | RedacterDataItemContent::Pdf { .. } => {
                return Err(AppError::unsupported_content());
            }
        };
        let field_names = unique_field_names(&mut item);
        let result = self.inspect(item).await?;
        Self::detections_in(&input.content, &field_names, result)
    }

    fn redacter_type(&self) -> RedacterType {
        RedacterType::GcpDlp
    }
}

/// A finding's likelihood read as a score. DLP gives five buckets and no number, so each gets a
/// fixed score. An unspecified bucket has no score.
fn likelihood_score(likelihood: dlp::Likelihood) -> Option<Score> {
    match likelihood {
        dlp::Likelihood::VeryUnlikely => Some(const { Score::from_static(0.1) }),
        dlp::Likelihood::Unlikely => Some(const { Score::from_static(0.3) }),
        dlp::Likelihood::Possible => Some(const { Score::from_static(0.5) }),
        dlp::Likelihood::Likely => Some(const { Score::from_static(0.7) }),
        dlp::Likelihood::VeryLikely => Some(const { Score::from_static(0.9) }),
        dlp::Likelihood::Unspecified => None,
    }
}

/// The finding's byte range within its containing text or table cell. DLP counts UTF-8 bytes.
fn byte_span(location: &dlp::Location) -> AppResult<(usize, usize)> {
    let range = location
        .byte_range
        .as_ref()
        .ok_or_else(|| AppError::incomplete_result(RedacterType::GcpDlp, "a byte range"))?;
    match (usize::try_from(range.start), usize::try_from(range.end)) {
        (Ok(start), Ok(end)) => Ok((start, end)),
        _ => Err(AppError::SystemError {
            message: format!(
                "A GCP DLP finding has the invalid byte range {}..{}",
                range.start, range.end
            ),
        }),
    }
}

fn text_in_range(text: &str, start: usize, end: usize) -> AppResult<String> {
    text.get(start..end)
        .map(str::to_string)
        .ok_or(AppError::DetectionOutsideContent {
            redacter_type: RedacterType::GcpDlp,
            location: DetectionLocation::Text { start, end },
        })
}

/// The row and column a table finding sits in, the column found by the name of its field.
fn table_cell(location: &dlp::Location, field_names: &[String]) -> AppResult<(usize, usize)> {
    let record = location
        .content_locations
        .iter()
        .find_map(|content| match &content.location {
            Some(dlp::content_location::Location::RecordLocation(record)) => Some(record),
            _ => None,
        })
        .ok_or_else(|| AppError::incomplete_result(RedacterType::GcpDlp, "a record location"))?;
    let row = record
        .table_location
        .as_ref()
        .and_then(|table| usize::try_from(table.row_index).ok())
        .ok_or_else(|| AppError::incomplete_result(RedacterType::GcpDlp, "a row"))?;
    let column = record
        .field_id
        .as_ref()
        .and_then(|field| field_names.iter().position(|name| *name == field.name))
        .ok_or_else(|| AppError::incomplete_result(RedacterType::GcpDlp, "a known column"))?;
    Ok((row, column))
}

/// Gives every column of a table item a distinct name, which is how DLP tells the columns
/// of a finding apart, and returns the names. The result is empty for anything but a table.
fn unique_field_names(item: &mut dlp::ContentItem) -> Vec<String> {
    let Some(dlp::content_item::DataItem::Table(table)) = &mut item.data_item else {
        return Vec::new();
    };
    let mut seen = HashSet::new();
    for (column, header) in table.headers.iter_mut().enumerate() {
        if !seen.insert(header.name.clone()) {
            header.name = format!("{} (column {column})", header.name);
            seen.insert(header.name.clone());
        }
    }
    table
        .headers
        .iter()
        .map(|header| header.name.clone())
        .collect()
}

impl TryInto<gcloud_sdk::google::privacy::dlp::v2::ContentItem> for RedacterDataItemContent {
    type Error = AppError;

    fn try_into(self) -> Result<gcloud_sdk::google::privacy::dlp::v2::ContentItem, Self::Error> {
        match self {
            RedacterDataItemContent::Value(value) => {
                Ok(gcloud_sdk::google::privacy::dlp::v2::ContentItem {
                    content_metadata: None,
                    data_item: Some(
                        gcloud_sdk::google::privacy::dlp::v2::content_item::DataItem::Value(value),
                    ),
                })
            }
            RedacterDataItemContent::Table { headers, rows } => {
                let headers = if headers.is_empty() {
                    rows.first().map_or(vec![], |row| {
                        (0..row.len())
                            .map(|i| gcloud_sdk::google::privacy::dlp::v2::FieldId {
                                name: format!("Column {i}"),
                            })
                            .collect()
                    })
                } else {
                    headers
                        .into_iter()
                        .map(|header| gcloud_sdk::google::privacy::dlp::v2::FieldId {
                            name: header,
                        })
                        .collect()
                };
                Ok(gcloud_sdk::google::privacy::dlp::v2::ContentItem {
                    content_metadata: None,
                    data_item: Some(
                        gcloud_sdk::google::privacy::dlp::v2::content_item::DataItem::Table(
                            gcloud_sdk::google::privacy::dlp::v2::Table {
                                headers,
                                rows: rows
                                    .iter()
                                    .map(|cols| gcloud_sdk::google::privacy::dlp::v2::table::Row {
                                        values: cols.iter().map(|col| {
                                            gcloud_sdk::google::privacy::dlp::v2::Value {
                                                r#type: Some(gcloud_sdk::google::privacy::dlp::v2::value::Type::StringValue(
                                                    col.to_string(),
                                                )),
                                            }
                                        }).collect()

                                    })
                                    .collect(),
                            },
                        ),
                    ),
                })
            }
            RedacterDataItemContent::Image { .. } | RedacterDataItemContent::Pdf { .. } => {
                Err(AppError::SystemError {
                    message: "Attempt to convert image content to ContentItem".to_string(),
                })
            }
        }
    }
}

impl TryFrom<gcloud_sdk::google::privacy::dlp::v2::ContentItem> for RedacterDataItemContent {
    type Error = AppError;

    fn try_from(
        value: gcloud_sdk::google::privacy::dlp::v2::ContentItem,
    ) -> Result<Self, Self::Error> {
        match value.data_item {
            Some(gcloud_sdk::google::privacy::dlp::v2::content_item::DataItem::Value(value)) => {
                Ok(RedacterDataItemContent::Value(value))
            }
            Some(gcloud_sdk::google::privacy::dlp::v2::content_item::DataItem::Table(table)) => {
                Ok(RedacterDataItemContent::Table {
                    headers: table
                        .headers
                        .into_iter()
                        .map(|header| header.name)
                        .collect(),
                    rows: table
                        .rows
                        .into_iter()
                        .map(|row| {
                            row.values
                                .into_iter()
                                .map(|value| match value.r#type {
                                    Some(gcloud_sdk::google::privacy::dlp::v2::value::Type::StringValue(
                                        value,
                                    )) => value,
                                    _ => "".to_string(),
                                })
                                .collect()
                        })
                        .collect(),
                })
            }
            _ => Err(AppError::SystemError {
                message: "Unknown data item type".to_string(),
            }),
        }
    }
}

impl TryInto<gcloud_sdk::google::privacy::dlp::v2::ByteContentItem> for RedacterDataItemContent {
    type Error = AppError;

    fn try_into(
        self,
    ) -> Result<gcloud_sdk::google::privacy::dlp::v2::ByteContentItem, Self::Error> {
        fn mime_type_to_image_type(
            mime_type: &Mime,
        ) -> gcloud_sdk::google::privacy::dlp::v2::byte_content_item::BytesType {
            match mime_type {
                mime if mime.subtype() == "png" => {
                    gcloud_sdk::google::privacy::dlp::v2::byte_content_item::BytesType::ImagePng
                }
                mime if mime.subtype() == "jpeg" || mime.subtype() == "jpg" => {
                    gcloud_sdk::google::privacy::dlp::v2::byte_content_item::BytesType::ImageJpeg
                }
                mime if mime.subtype() == "jpe" => {
                    gcloud_sdk::google::privacy::dlp::v2::byte_content_item::BytesType::ImageJpeg
                }
                mime if mime.subtype() == "gif" => {
                    gcloud_sdk::google::privacy::dlp::v2::byte_content_item::BytesType::Image
                }
                mime if mime.subtype() == "bmp" => {
                    gcloud_sdk::google::privacy::dlp::v2::byte_content_item::BytesType::ImageBmp
                }
                _ => gcloud_sdk::google::privacy::dlp::v2::byte_content_item::BytesType::Image,
            }
        }
        match self {
            RedacterDataItemContent::Image { mime_type, data } => {
                Ok(gcloud_sdk::google::privacy::dlp::v2::ByteContentItem {
                    data: data.to_vec(),
                    r#type: mime_type_to_image_type(&mime_type).into(),
                })
            }
            _ => Err(AppError::SystemError {
                message: "Attempt to convert non-image content to ByteContentItem".to_string(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use console::Term;

    fn finding(
        info_type: &str,
        likelihood: dlp::Likelihood,
        location: dlp::Location,
        quote: &str,
    ) -> dlp::Finding {
        dlp::Finding {
            info_type: Some(dlp::InfoType {
                name: info_type.to_string(),
                ..dlp::InfoType::default()
            }),
            likelihood: likelihood.into(),
            location: Some(location),
            quote: quote.to_string(),
            ..dlp::Finding::default()
        }
    }

    fn result_of(findings: Vec<dlp::Finding>) -> dlp::InspectResult {
        dlp::InspectResult {
            findings,
            ..dlp::InspectResult::default()
        }
    }

    fn byte_location(start: usize, end: usize) -> dlp::Location {
        dlp::Location {
            byte_range: Some(dlp::Range {
                start: start as i64,
                end: end as i64,
            }),
            ..dlp::Location::default()
        }
    }

    fn score(value: f32) -> Option<Score> {
        Some(Score::try_from(value).unwrap())
    }

    #[test]
    fn text_findings_keep_dlp_byte_ranges_on_multibyte_text() {
        let text = "Grüße, Zoë: write to john@example.com or call 555-0100";
        let email = text.find("john@example.com").unwrap();
        let phone = text.find("555-0100").unwrap();
        assert!(email > "Grüße, Zoë: write to ".chars().count());
        let result = result_of(vec![
            finding(
                "EMAIL_ADDRESS",
                dlp::Likelihood::VeryLikely,
                byte_location(email, email + 16),
                "john@example.com",
            ),
            finding(
                "PHONE_NUMBER",
                dlp::Likelihood::Possible,
                byte_location(phone, phone + 8),
                "555-0100",
            ),
        ]);

        let detections = GcpDlpRedacter::detections_in(
            &RedacterDataItemContent::Value(text.to_string()),
            &[],
            result,
        )
        .unwrap();

        assert_eq!(
            detections,
            vec![
                Detection {
                    label: DetectionLabel::new("EMAIL_ADDRESS"),
                    location: DetectionLocation::Text {
                        start: email,
                        end: email + 16
                    },
                    score: score(0.9),
                    value: "john@example.com".to_string(),
                },
                Detection {
                    label: DetectionLabel::new("PHONE_NUMBER"),
                    location: DetectionLocation::Text {
                        start: phone,
                        end: phone + 8
                    },
                    score: score(0.5),
                    value: "555-0100".to_string(),
                },
            ]
        );
    }

    #[test]
    fn a_range_that_does_not_fit_the_text_is_an_error() {
        let text = "Grüße";
        for (start, end) in [(0, 99), (2, 3)] {
            let result = result_of(vec![finding(
                "PERSON_NAME",
                dlp::Likelihood::Likely,
                byte_location(start, end),
                "",
            )]);
            assert!(
                matches!(
                    GcpDlpRedacter::detections_in(
                        &RedacterDataItemContent::Value(text.to_string()),
                        &[],
                        result
                    ),
                    Err(AppError::DetectionOutsideContent { .. })
                ),
                "{start}..{end}"
            );
        }
    }

    #[test]
    fn likelihood_buckets_map_to_the_fixed_score_table() {
        let score_of = |likelihood: i32| {
            let finding = dlp::Finding {
                likelihood,
                ..dlp::Finding::default()
            };
            likelihood_score(finding.likelihood())
        };
        assert_eq!(score_of(dlp::Likelihood::VeryUnlikely.into()), score(0.1));
        assert_eq!(score_of(dlp::Likelihood::Unlikely.into()), score(0.3));
        assert_eq!(score_of(dlp::Likelihood::Possible.into()), score(0.5));
        assert_eq!(score_of(dlp::Likelihood::Likely.into()), score(0.7));
        assert_eq!(score_of(dlp::Likelihood::VeryLikely.into()), score(0.9));
        assert_eq!(score_of(dlp::Likelihood::Unspecified.into()), None);
        assert_eq!(score_of(99), None);
    }

    fn cell_location(field_name: &str, row_index: i64, start: usize, end: usize) -> dlp::Location {
        dlp::Location {
            content_locations: vec![dlp::ContentLocation {
                location: Some(dlp::content_location::Location::RecordLocation(
                    dlp::RecordLocation {
                        field_id: Some(dlp::FieldId {
                            name: field_name.to_string(),
                        }),
                        table_location: Some(dlp::TableLocation { row_index }),
                        ..dlp::RecordLocation::default()
                    },
                )),
                ..dlp::ContentLocation::default()
            }],
            ..byte_location(start, end)
        }
    }

    #[test]
    fn table_findings_name_their_row_and_column() {
        let content = RedacterDataItemContent::Table {
            headers: vec!["name".to_string(), "email".to_string()],
            rows: vec![
                vec!["Ana".to_string(), "ana@example.org".to_string()],
                vec!["Zoë".to_string(), "mail zoë@example.org".to_string()],
            ],
        };
        let email_start = "mail ".len();
        let result = result_of(vec![finding(
            "EMAIL_ADDRESS",
            dlp::Likelihood::VeryLikely,
            cell_location("email", 1, email_start, "mail zoë@example.org".len()),
            "zoë@example.org",
        )]);

        let detections = GcpDlpRedacter::detections_in(
            &content,
            &["name".to_string(), "email".to_string()],
            result,
        )
        .unwrap();

        assert_eq!(
            detections,
            vec![Detection {
                label: DetectionLabel::new("EMAIL_ADDRESS"),
                location: DetectionLocation::TableCell {
                    row: 1,
                    column: 1,
                    start: email_start,
                    end: "mail zoë@example.org".len()
                },
                score: score(0.9),
                value: "zoë@example.org".to_string(),
            }]
        );
    }

    #[test]
    fn columns_with_the_same_header_are_sent_under_distinct_names() {
        let mut item: dlp::ContentItem = RedacterDataItemContent::Table {
            headers: vec!["phone".to_string(), "phone".to_string(), "city".to_string()],
            rows: vec![vec!["1".to_string(), "2".to_string(), "Oslo".to_string()]],
        }
        .try_into()
        .unwrap();

        let names = unique_field_names(&mut item);

        assert_eq!(names.len(), 3);
        assert_eq!(
            names.iter().collect::<HashSet<_>>().len(),
            3,
            "names: {names:?}"
        );
        assert_eq!(names[0], "phone", "the first column keeps its name");
        assert_eq!(names[2], "city");
        let Some(dlp::content_item::DataItem::Table(table)) = item.data_item else {
            panic!("a table item stays a table");
        };
        assert_eq!(
            table
                .headers
                .iter()
                .map(|header| header.name.clone())
                .collect::<Vec<_>>(),
            names
        );
    }

    #[test]
    fn image_findings_become_pixel_boxes_one_per_bounding_box() {
        let location = dlp::Location {
            content_locations: vec![dlp::ContentLocation {
                location: Some(dlp::content_location::Location::ImageLocation(
                    dlp::ImageLocation {
                        bounding_boxes: vec![
                            dlp::BoundingBox {
                                top: 195,
                                left: 310,
                                width: 310,
                                height: 23,
                            },
                            dlp::BoundingBox {
                                top: 220,
                                left: 10,
                                width: 50,
                                height: 20,
                            },
                        ],
                    },
                )),
                ..dlp::ContentLocation::default()
            }],
            ..dlp::Location::default()
        };
        let result = result_of(vec![finding(
            "EMAIL_ADDRESS",
            dlp::Likelihood::Likely,
            location,
            "john.smith@example.com",
        )]);
        let content = RedacterDataItemContent::Image {
            mime_type: mime::IMAGE_PNG,
            data: bytes::Bytes::new(),
        };

        let detections = GcpDlpRedacter::detections_in(&content, &[], result).unwrap();

        let boxes: Vec<DetectionLocation> = detections
            .iter()
            .map(|detection| detection.location)
            .collect();
        assert_eq!(
            boxes,
            vec![
                DetectionLocation::Image {
                    page: None,
                    x1: 310.0,
                    y1: 195.0,
                    x2: 620.0,
                    y2: 218.0
                },
                DetectionLocation::Image {
                    page: None,
                    x1: 10.0,
                    y1: 220.0,
                    x2: 60.0,
                    y2: 240.0
                },
            ]
        );
        assert!(detections
            .iter()
            .all(|detection| detection.value == "john.smith@example.com"
                && detection.score == score(0.7)));
    }

    #[test]
    fn a_response_without_a_result_is_an_error_not_a_clean_item() {
        let error = GcpDlpRedacter::inspect_result(dlp::InspectContentResponse { result: None })
            .expect_err("a response without a result says nothing about the item");
        assert!(matches!(
            error,
            AppError::IncompleteResult {
                redacter_type: RedacterType::GcpDlp,
                ..
            }
        ));
    }

    #[test]
    fn truncated_findings_are_an_error_not_a_short_list() {
        let result = dlp::InspectResult {
            findings_truncated: true,
            ..result_of(Vec::new())
        };
        assert!(GcpDlpRedacter::detections_in(
            &RedacterDataItemContent::Value("x".to_string()),
            &[],
            result
        )
        .is_err());
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-gcp"), ignore)]
    async fn redact_text_file_test() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        crate::redacters::test_support::initialize_crypto();

        let term = Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
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

        let redacter = GcpDlpRedacter::new(
            GcpDlpRedacterOptions {
                project_id: GcpProjectId::new(test_gcp_project_id),
                user_defined_built_in_info_types: vec![],
                user_defined_stored_info_types: vec![],
            },
            &reporter,
        )
        .await?;

        let redacted_item = redacter.redact(input).await?;
        match redacted_item.content {
            RedacterDataItemContent::Value(value) => {
                assert_eq!(value, "Hello, [REDACTED]");
            }
            _ => panic!("Unexpected redacted content type"),
        }

        Ok(())
    }

    async fn live_redacter<'a>(reporter: &'a AppReporter<'a>) -> GcpDlpRedacter<'a> {
        crate::redacters::test_support::initialize_crypto();
        let test_gcp_project_id =
            std::env::var("TEST_GCP_PROJECT").expect("TEST_GCP_PROJECT required");
        GcpDlpRedacter::new(
            GcpDlpRedacterOptions {
                project_id: GcpProjectId::new(test_gcp_project_id),
                user_defined_built_in_info_types: vec![],
                user_defined_stored_info_types: vec![],
            },
            reporter,
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-gcp"), ignore)]
    async fn detect_text_finds_values_at_their_byte_offsets() {
        let term = Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let text = "Grüße aus Köln, bitte schreib an zoe.keller@example.com oder ruf an: \
                    +49 30 1234 5678";
        let input = RedacterDataItem {
            file_ref: FileSystemRef {
                relative_path: "temp_file.txt".into(),
                media_type: Some(mime::TEXT_PLAIN),
                file_size: Some(text.len()),
            },
            content: RedacterDataItemContent::Value(text.to_string()),
        };

        let detections = live_redacter(&reporter).await.detect(&input).await.unwrap();

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

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-gcp"), ignore)]
    async fn detect_table_finds_values_in_their_cells() {
        let term = Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let input = RedacterDataItem {
            file_ref: FileSystemRef {
                relative_path: "temp_file.csv".into(),
                media_type: Some(mime::TEXT_CSV),
                file_size: None,
            },
            content: RedacterDataItemContent::Table {
                headers: vec![
                    "id".to_string(),
                    "contact".to_string(),
                    "contact".to_string(),
                ],
                rows: vec![
                    vec!["1".to_string(), "none".to_string(), "none".to_string()],
                    vec![
                        "2".to_string(),
                        "none".to_string(),
                        "mail zoë.keller@example.com".to_string(),
                    ],
                ],
            },
        };

        let detections = live_redacter(&reporter).await.detect(&input).await.unwrap();

        let email = "mail ".len();
        assert!(
            detections.iter().any(|detection| {
                detection.label == DetectionLabel::new("EMAIL_ADDRESS")
                    && detection.location
                        == DetectionLocation::TableCell {
                            row: 1,
                            column: 2,
                            start: email,
                            end: email + "zoë.keller@example.com".len(),
                        }
                    && detection.value == "zoë.keller@example.com"
            }),
            "{detections:?}"
        );
    }

    /// Ink extents of the email and the phone number in `test-fixtures/media/form-example.png`,
    /// measured from the pixels: `(info type, [x1, y1, x2, y2])`.
    const FORM_IMAGE_INK: [(&str, [f32; 4]); 2] = [
        ("EMAIL_ADDRESS", [310.0, 195.0, 620.0, 218.0]),
        ("PHONE_NUMBER", [313.0, 265.0, 538.0, 286.0]),
    ];

    /// DLP boxes words with padding and splits a value over several boxes, so the boxes of a
    /// value are joined, and the join must contain the ink and stay within a word's padding of it.
    #[tokio::test]
    #[cfg_attr(not(feature = "ci-gcp"), ignore)]
    async fn detect_image_boxes_cover_the_values_printed_on_the_form() {
        let term = Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let input = crate::redacters::test_support::test_image_item();

        let detections = live_redacter(&reporter).await.detect(&input).await.unwrap();

        let (horizontal_padding, vertical_padding) = (50.0_f32, 15.0_f32);
        for (info_type, [ink_x1, ink_y1, ink_x2, ink_y2]) in FORM_IMAGE_INK {
            let joined = detections
                .iter()
                .filter(|detection| detection.label == DetectionLabel::new(info_type))
                .filter_map(|detection| match detection.location {
                    DetectionLocation::Image { x1, y1, x2, y2, .. }
                        if x1 < ink_x2 && x2 > ink_x1 && y1 < ink_y2 && y2 > ink_y1 =>
                    {
                        Some([x1, y1, x2, y2])
                    }
                    _ => None,
                })
                .reduce(|a, b| {
                    [
                        a[0].min(b[0]),
                        a[1].min(b[1]),
                        a[2].max(b[2]),
                        a[3].max(b[3]),
                    ]
                })
                .unwrap_or_else(|| panic!("no {info_type} box on the ink: {detections:?}"));
            assert!(
                joined[0] <= ink_x1
                    && joined[1] <= ink_y1
                    && joined[2] >= ink_x2
                    && joined[3] >= ink_y2,
                "{info_type} boxes {joined:?} miss the ink {:?}",
                [ink_x1, ink_y1, ink_x2, ink_y2]
            );
            assert!(
                ink_x1 - joined[0] <= horizontal_padding
                    && joined[2] - ink_x2 <= horizontal_padding
                    && ink_y1 - joined[1] <= vertical_padding
                    && joined[3] - ink_y2 <= vertical_padding,
                "{info_type} boxes {joined:?} spill far past the ink {:?}",
                [ink_x1, ink_y1, ink_x2, ink_y2]
            );
        }
    }
}
