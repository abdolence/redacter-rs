use crate::errors::AppError;
use crate::filesystems::{
    AbsoluteFilePath, FileMatcher, FileMatcherResult, FileSystemConnection, FileSystemRef,
    ListFilesResult, RelativeFilePath,
};
use crate::reporter::AppReporter;
use crate::AppResult;
use futures::Stream;
use futures::TryStreamExt;
use gcloud_sdk::prost::bytes::Bytes;
use rvstruct::ValueStruct;

pub struct AwsS3FileSystem<'a> {
    bucket_name: String,
    object_name: String,
    client: aws_sdk_s3::Client,
    is_dir: bool,
    reporter: &'a AppReporter<'a>,
}

impl<'a> AwsS3FileSystem<'a> {
    pub async fn new(path: &str, reporter: &'a AppReporter<'a>) -> AppResult<Self> {
        let shared_config = aws_config::load_from_env().await;
        let (bucket_name, object_name) = Self::parse_s3_path(path)?;
        let is_dir = object_name.ends_with('/');
        let client = aws_sdk_s3::Client::new(&shared_config);

        Ok(AwsS3FileSystem {
            bucket_name,
            object_name,
            client,
            is_dir,
            reporter,
        })
    }

    fn parse_s3_path(path: &str) -> AppResult<(String, String)> {
        let path_parts: Vec<&str> = path.trim_start_matches("s3://").split('/').collect();
        if path_parts.len() < 2 {
            return Err(AppError::SystemError {
                message: format!("Invalid S3 path: {}", path),
            });
        }
        if path_parts[1].is_empty() {
            Ok((path_parts[0].to_string(), "/".to_string()))
        } else {
            Ok((path_parts[0].to_string(), path_parts[1..].join("/")))
        }
    }

    #[async_recursion::async_recursion]
    async fn list_files_recursively(
        &self,
        prefix: Option<String>,
        continuation_token: Option<String>,
        file_matcher: &Option<&FileMatcher>,
    ) -> AppResult<ListFilesResult> {
        let list_req = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket_name)
            .set_prefix(prefix)
            .set_continuation_token(continuation_token.clone());
        let list_resp = list_req.send().await?;

        match list_resp.contents {
            Some(contents) => {
                let all_found: Vec<FileSystemRef> = contents
                    .into_iter()
                    .filter(|item| item.key.iter().all(|key| !key.ends_with('/')))
                    .filter_map(|item| {
                        item.key.map(|name| {
                            let relative_path: RelativeFilePath =
                                name.trim_start_matches(&self.object_name).into();
                            let media_type = mime_guess::from_path(&name).first();
                            FileSystemRef {
                                relative_path,
                                media_type,
                                file_size: item.size.map(|v| v as u64),
                            }
                        })
                    })
                    .collect();

                let next_list_result = if list_resp
                    .next_continuation_token
                    .as_ref()
                    .iter()
                    .any(|v| !v.is_empty())
                {
                    self.list_files_recursively(
                        None,
                        list_resp.next_continuation_token,
                        file_matcher,
                    )
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

                Ok(ListFilesResult {
                    files: [filtered_files, next_list_result.files].concat(),
                    skipped: next_list_result.skipped + skipped,
                })
            }
            None => Ok(ListFilesResult::EMPTY),
        }
    }
}

impl<'a> FileSystemConnection<'a> for AwsS3FileSystem<'a> {
    async fn download(
        &mut self,
        file_ref: Option<&FileSystemRef>,
    ) -> AppResult<(
        FileSystemRef,
        Box<dyn Stream<Item = AppResult<Bytes>> + Send + Sync + Unpin + 'static>,
    )> {
        let object_name = self.resolve(file_ref).file_path;
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

        let object = self
            .client
            .get_object()
            .bucket(&self.bucket_name)
            .key(&object_name)
            .send()
            .await?;

        let found_file_ref = FileSystemRef {
            relative_path: relative_path.clone(),
            media_type: object
                .content_type
                .map(|v| v.parse())
                .transpose()?
                .or_else(|| mime_guess::from_path(relative_path.value()).first()),
            file_size: object.content_length.map(|v| v as u64),
        };

        let reader = object.body.into_async_read();
        let stream = tokio_util::io::ReaderStream::new(reader).map_err(AppError::from);

        Ok((found_file_ref, Box::new(stream)))
    }

    async fn upload<S: Stream<Item = AppResult<Bytes>> + Send + Unpin + Sync + 'static>(
        &mut self,
        input: S,
        file_ref: Option<&FileSystemRef>,
    ) -> AppResult<()> {
        let object_name = self.resolve(file_ref).file_path;
        let content_type = file_ref
            .and_then(|fr| fr.media_type.as_ref())
            .map(|v| v.to_string());
        let body_bytes: Vec<Bytes> = input.try_collect().await?;
        let all_bytes = body_bytes.concat();
        let body = aws_sdk_s3::primitives::ByteStream::from(all_bytes);

        self.client
            .put_object()
            .bucket(&self.bucket_name)
            .key(&object_name)
            .set_content_type(content_type)
            .body(body)
            .send()
            .await?;

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
            self.list_files_recursively(
                if self.object_name == "/" {
                    None
                } else {
                    Some(self.object_name.clone())
                },
                None,
                &file_matcher,
            )
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
                format!(
                    "{}{}",
                    &self.object_name,
                    file_ref
                        .map(|fr| fr.relative_path.value().clone())
                        .unwrap_or_default()
                )
            } else {
                self.object_name.clone()
            },
            scheme: "s3".to_string(),
        }
    }
}

#[allow(unused_imports)]
mod tests {
    use super::*;
    use crate::reporter::AppReporter;
    use rvstruct::ValueStruct;
    use tokio_util::bytes;

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-aws"), ignore)]
    async fn upload_download_test() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let term = console::Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let test_gcp_bucket_name =
            std::env::var("TEST_AWS_BUCKET_NAME").expect("TEST_AWS_BUCKET_NAME required");

        let mut fs = AwsS3FileSystem::new(
            &format!("s3://{}/redacter/test-upload/", test_gcp_bucket_name),
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
    #[cfg_attr(not(feature = "ci-aws"), ignore)]
    async fn list_test() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let term = console::Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let test_gcp_bucket_name =
            std::env::var("TEST_AWS_BUCKET_NAME").expect("TEST_AWS_BUCKET_NAME required");

        let mut fs = AwsS3FileSystem::new(
            &format!("s3://{}/redacter/test-list/", test_gcp_bucket_name),
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
