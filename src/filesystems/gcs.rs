use crate::filesystems::{
    AbsoluteFilePath, FileMatcher, FileMatcherResult, FileSystemConnection, FileSystemRef,
    ListFilesResult, RelativeFilePath,
};
use crate::reporter::AppReporter;
use crate::AppResult;
use futures::{Stream, TryStreamExt};
use gcloud_sdk::prost::bytes;
use rvstruct::ValueStruct;
use std::default::Default;

pub struct GoogleCloudStorageFileSystem<'a> {
    google_rest_client: gcloud_sdk::GoogleRestApi,
    bucket_name: String,
    object_name: String,
    is_dir: bool,
    reporter: &'a AppReporter<'a>,
}

impl<'a> GoogleCloudStorageFileSystem<'a> {
    pub async fn new(path: &str, reporter: &'a AppReporter<'a>) -> AppResult<Self> {
        let google_rest_client = gcloud_sdk::GoogleRestApi::new().await?;
        let (bucket_name, object_name) = GoogleCloudStorageFileSystem::parse_gcs_path(path);
        let is_dir = object_name.ends_with('/');
        Ok(GoogleCloudStorageFileSystem {
            google_rest_client,
            bucket_name,
            object_name,
            is_dir,
            reporter,
        })
    }

    fn parse_gcs_path(path: &str) -> (String, String) {
        let path = path.trim_start_matches("gs://");
        let parts: Vec<&str> = path.split('/').collect();
        let bucket = parts[0];
        if parts.len() == 1 || (parts.len() == 2 && parts[1].is_empty()) {
            (bucket.to_string(), "/".to_string())
        } else {
            let object = parts[1..].join("/");
            (bucket.to_string(), object.to_string())
        }
    }

    #[async_recursion::async_recursion]
    async fn list_files_with_token(
        &self,
        prefix: Option<String>,
        page_token: Option<String>,
        file_matcher: &Option<&FileMatcher>,
    ) -> AppResult<ListFilesResult> {
        let config = self
            .google_rest_client
            .create_google_storage_v1_config()
            .await?;
        let list_params = gcloud_sdk::google_rest_apis::storage_v1::objects_api::StoragePeriodObjectsPeriodListParams {
            bucket: self.bucket_name.clone(),
            prefix,
            page_token,
            ..gcloud_sdk::google_rest_apis::storage_v1::objects_api::StoragePeriodObjectsPeriodListParams::default()
        };
        let list = gcloud_sdk::google_rest_apis::storage_v1::objects_api::storage_objects_list(
            &config,
            list_params,
        )
        .await?;

        match list.items {
            Some(items) => Ok({
                let all_found: Vec<FileSystemRef> = items
                    .into_iter()
                    .filter(|item| item.name.iter().all(|key| !key.ends_with('/')))
                    .filter_map(|item| {
                        item.name.map(|name| FileSystemRef {
                            relative_path: name.trim_start_matches(&self.object_name).into(),
                            media_type: item.content_type.and_then(|v| v.parse().ok()),
                            file_size: item.size.and_then(|v| v.parse::<u64>().ok()),
                        })
                    })
                    .collect();
                let next_list_result =
                    if list.next_page_token.as_ref().iter().any(|v| !v.is_empty()) {
                        self.list_files_with_token(None, list.next_page_token, file_matcher)
                            .await?
                    } else {
                        ListFilesResult::EMPTY
                    };
                let all_found_len = all_found.len();
                let filtered_files: Vec<FileSystemRef> = all_found
                    .into_iter()
                    .filter(|file_ref| {
                        file_matcher.iter().all(|matcher| {
                            matches!(matcher.matches(file_ref), FileMatcherResult::Matched)
                        })
                    })
                    .collect();
                let skipped = all_found_len - filtered_files.len();
                ListFilesResult {
                    files: [filtered_files, next_list_result.files].concat(),
                    skipped: next_list_result.skipped + skipped,
                }
            }),
            None => Ok(ListFilesResult::EMPTY),
        }
    }
}

impl<'a> FileSystemConnection<'a> for GoogleCloudStorageFileSystem<'a> {
    async fn download(
        &mut self,
        file_ref: Option<&FileSystemRef>,
    ) -> AppResult<(
        FileSystemRef,
        Box<dyn Stream<Item = AppResult<bytes::Bytes>> + Send + Sync + Unpin + 'static>,
    )> {
        let config = self
            .google_rest_client
            .create_google_storage_v1_config()
            .await?;

        let object_name = self.resolve(file_ref).file_path;

        let object = gcloud_sdk::google_rest_apis::storage_v1::objects_api::storage_objects_get(
            &config,
            gcloud_sdk::google_rest_apis::storage_v1::objects_api::StoragePeriodObjectsPeriodGetParams {
                bucket: self.bucket_name.clone(),
                object: object_name.clone(),
                ..gcloud_sdk::google_rest_apis::storage_v1::objects_api::StoragePeriodObjectsPeriodGetParams::default()
            },
        ).await?;

        let relative_path: RelativeFilePath = if self.is_dir {
            object_name
                .clone()
                .trim_start_matches(&self.object_name)
                .into()
        } else {
            object_name
                .split('/')
                .last()
                .map(|file_name| file_name.to_string())
                .unwrap_or_else(|| object_name.clone())
                .into()
        };

        let found_file_ref = FileSystemRef {
            relative_path: relative_path.clone(),
            media_type: object
                .content_type
                .map(|v| v.parse())
                .transpose()?
                .or_else(|| mime_guess::from_path(relative_path.value()).first()),
            file_size: object.size.and_then(|v| v.parse::<u64>().ok()),
        };

        let stream = gcloud_sdk::google_rest_apis::storage_v1::objects_api::storage_objects_get_stream(
            &config,
            gcloud_sdk::google_rest_apis::storage_v1::objects_api::StoragePeriodObjectsPeriodGetParams {
                bucket: self.bucket_name.clone(),
                object: object_name.clone(),
                ..gcloud_sdk::google_rest_apis::storage_v1::objects_api::StoragePeriodObjectsPeriodGetParams::default()
            }
        ).await?;
        Ok((
            found_file_ref,
            Box::new(stream.map_err(|err| gcloud_sdk::error::Error::from(err).into())),
        ))
    }

    async fn upload<S: Stream<Item = AppResult<bytes::Bytes>> + Send + Unpin + Sync + 'static>(
        &mut self,
        input: S,
        file_ref: Option<&FileSystemRef>,
    ) -> AppResult<()> {
        let object_name = self.resolve(file_ref).file_path;

        let config = self
            .google_rest_client
            .create_google_storage_v1_config()
            .await?;
        let content_type = file_ref
            .and_then(|fr| fr.media_type.as_ref())
            .map(|v| v.to_string());
        let reader = sync_wrapper::SyncStream::new(input);
        let params =gcloud_sdk::google_rest_apis::storage_v1::objects_api::StoragePeriodObjectsPeriodInsertParams {
            bucket: self.bucket_name.clone(),
            name: Some(object_name),
            ..gcloud_sdk::google_rest_apis::storage_v1::objects_api::StoragePeriodObjectsPeriodInsertParams::default()
        };
        let _ = gcloud_sdk::google_rest_apis::storage_v1::objects_api::storage_objects_insert_ext_stream(
            &config,
            params,
            content_type,
            reader
        ).await?;
        Ok(())
    }

    async fn list_files(
        &mut self,
        file_matcher: Option<&FileMatcher>,
    ) -> AppResult<ListFilesResult> {
        self.reporter.report(format!(
            "Listing files in bucket: {} with prefix: {}",
            self.bucket_name, self.object_name
        ))?;
        if self.object_name.ends_with('/') {
            let prefix = if self.object_name != "/" {
                Some(self.object_name.clone())
            } else {
                None
            };
            self.list_files_with_token(prefix, None, &file_matcher)
                .await
        } else {
            Ok(ListFilesResult::EMPTY)
        }
    }

    async fn close(self) -> AppResult<()> {
        Ok(())
    }

    async fn has_multiple_files(&self) -> AppResult<bool> {
        Ok(self.is_dir)
    }

    async fn accepts_multiple_files(&self) -> AppResult<bool> {
        Ok(self.is_dir)
    }

    fn resolve(&self, file_ref: Option<&FileSystemRef>) -> AbsoluteFilePath {
        AbsoluteFilePath {
            file_path: if self.is_dir {
                let object_name_prefix = if self.object_name == "/" {
                    ""
                } else {
                    self.object_name.as_str()
                };
                format!(
                    "{}{}",
                    object_name_prefix,
                    file_ref
                        .map(|fr| fr.relative_path.value().clone())
                        .unwrap_or_default()
                )
            } else {
                self.object_name.clone()
            },
        }
    }
}

#[allow(unused_imports)]
mod tests {
    use super::*;
    use crate::reporter::AppReporter;

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-gcp"), ignore)]
    async fn upload_download_test() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let term = console::Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let test_gcp_bucket_name =
            std::env::var("TEST_GCS_BUCKET_NAME").expect("TEST_GCS_BUCKET_NAME required");

        let mut fs = GoogleCloudStorageFileSystem::new(
            &format!("gs://{}/redacter/test-upload/", test_gcp_bucket_name),
            &reporter,
        )
        .await?;

        let test_data = "test content";
        let test_data_stream = futures::stream::iter(vec![Ok(bytes::Bytes::from(test_data))]);
        fs.upload(
            test_data_stream,
            Some(&FileSystemRef {
                relative_path: "test-upload.txt".into(),
                media_type: Some(mime::TEXT_PLAIN),
                file_size: Some(test_data.len() as u64),
            }),
        )
        .await?;

        let (file_ref, down_stream) = fs
            .download(Some(&FileSystemRef {
                relative_path: "test-upload.txt".into(),
                media_type: Some(mime::TEXT_PLAIN),
                file_size: Some(test_data.len() as u64),
            }))
            .await?;

        let downloaded_bytes: Vec<bytes::Bytes> = down_stream.try_collect().await?;
        let flattened_bytes = downloaded_bytes.concat();
        let downloaded_content = std::str::from_utf8(&flattened_bytes)?;
        assert_eq!(downloaded_content, test_data);

        assert_eq!(file_ref.relative_path.value(), "test-upload.txt");
        assert_eq!(file_ref.media_type, Some(mime::TEXT_PLAIN));
        assert_eq!(file_ref.file_size, Some(test_data.len() as u64));

        fs.close().await?;

        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-gcp"), ignore)]
    async fn list_test() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let term = console::Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let test_gcp_bucket_name =
            std::env::var("TEST_GCS_BUCKET_NAME").expect("TEST_GCS_BUCKET_NAME required");

        let mut fs = GoogleCloudStorageFileSystem::new(
            &format!("gs://{}/redacter/test-list/", test_gcp_bucket_name),
            &reporter,
        )
        .await?;

        let test_data = "test content";
        let test_data_stream = futures::stream::iter(vec![Ok(bytes::Bytes::from(test_data))]);
        fs.upload(
            test_data_stream,
            Some(&FileSystemRef {
                relative_path: "test-upload.txt".into(),
                media_type: Some(mime::TEXT_PLAIN),
                file_size: Some(test_data.len() as u64),
            }),
        )
        .await?;

        let list_result = fs.list_files(None).await?;
        assert_eq!(list_result.files.len(), 1);
        let file_ref = &list_result.files[0];
        assert_eq!(file_ref.relative_path.value(), "test-upload.txt");
        assert_eq!(file_ref.media_type, Some(mime::TEXT_PLAIN));
        assert_eq!(file_ref.file_size, Some(test_data.len() as u64));

        fs.close().await?;

        Ok(())
    }
}
