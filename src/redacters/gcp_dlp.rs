use crate::args::RedacterType;
use crate::common_types::GcpProjectId;
use crate::errors::AppError;
use crate::file_systems::FileSystemRef;
use crate::redacters::{
    RedactSupport, Redacter, RedacterDataItem, RedacterDataItemContent, Redacters,
};
use crate::reporter::AppReporter;
use crate::AppResult;
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
        let mut request = tonic::Request::new(
            gcloud_sdk::google::privacy::dlp::v2::DeidentifyContentRequest {
                parent: format!(
                    "projects/{}/locations/global",
                    self.gcp_dlp_options.project_id.value()
                ),
                inspect_config: Some(self.create_inspect_config()),
                deidentify_config: Some(self.create_deidentify_config()),
                item: Some(input.content.try_into()?),
                ..gcloud_sdk::google::privacy::dlp::v2::DeidentifyContentRequest::default()
            },
        );
        request.metadata_mut().insert(
            "x-goog-user-project",
            MetadataValue::<tonic::metadata::Ascii>::try_from(
                self.gcp_dlp_options.project_id.value(),
            )?,
        );
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
        let mut request =
            tonic::Request::new(gcloud_sdk::google::privacy::dlp::v2::RedactImageRequest {
                parent: format!(
                    "projects/{}/locations/global",
                    self.gcp_dlp_options.project_id.value()
                ),
                inspect_config: Some(self.create_inspect_config()),
                byte_item: Some(input_bytes_content),
                ..gcloud_sdk::google::privacy::dlp::v2::RedactImageRequest::default()
            });
        request.metadata_mut().insert(
            "x-goog-user-project",
            MetadataValue::<tonic::metadata::Ascii>::try_from(
                self.gcp_dlp_options.project_id.value(),
            )?,
        );
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
            _ => Err(AppError::SystemError {
                message: "Attempt to redact of unsupported image type".to_string(),
            }),
        }
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
                Err(AppError::SystemError {
                    message: "Attempt to redact of unsupported type".to_string(),
                })
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

    fn redacter_type(&self) -> RedacterType {
        RedacterType::GcpDlp
    }
}

impl TryInto<gcloud_sdk::google::privacy::dlp::v2::ContentItem> for RedacterDataItemContent {
    type Error = AppError;

    fn try_into(self) -> Result<gcloud_sdk::google::privacy::dlp::v2::ContentItem, Self::Error> {
        match self {
            RedacterDataItemContent::Value(value) => {
                Ok(gcloud_sdk::google::privacy::dlp::v2::ContentItem {
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

#[allow(unused_imports)]
mod tests {
    use super::*;
    use crate::redacters::RedacterProviderOptions;
    use console::Term;

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-gcp"), ignore)]
    async fn redact_text_file_test() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        rustls::crypto::ring::default_provider()
            .install_default()
            .expect("Failed to install rustls crypto provider");

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
}
