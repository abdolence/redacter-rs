use crate::errors::AppError;
use crate::file_systems::{
    AbsoluteFilePath, FileSystemConnection, FileSystemRef, ListFilesResult, RelativeFilePath,
};
use crate::file_tools::FileMatcher;
use crate::reporter::AppReporter;
use crate::AppResult;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use bytes::BytesMut;
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
                message: format!("Invalid S3 path: {path}"),
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
        max_files_limit: Option<usize>,
    ) -> AppResult<ListFilesResult> {
        if max_files_limit.iter().any(|v| *v == 0) {
            return Ok(ListFilesResult::EMPTY);
        }

        let list_req = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket_name)
            .set_prefix(prefix)
            .set_continuation_token(continuation_token.clone());
        let list_resp = list_req.send().await?;

        match list_resp.contents {
            Some(contents) => {
                let mut listing = ListFilesResult::matching(
                    contents
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
                                    file_size: item.size.map(|v| v as usize),
                                }
                            })
                        }),
                    *file_matcher,
                    max_files_limit,
                );

                if list_resp
                    .next_continuation_token
                    .as_ref()
                    .iter()
                    .any(|v| !v.is_empty())
                {
                    let new_max_files_limit =
                        max_files_limit.map(|v| v.saturating_sub(listing.files.len()));
                    listing.merge(
                        self.list_files_recursively(
                            None,
                            list_resp.next_continuation_token,
                            file_matcher,
                            new_max_files_limit,
                        )
                        .await?,
                    );
                }
                Ok(listing)
            }
            None => Ok(ListFilesResult::EMPTY),
        }
    }
}

/// S3 rejects a part under 5 MiB, except the last one.
const MIN_PART_SIZE: usize = 5 * 1024 * 1024;

/// S3 rejects an upload of more than 10,000 parts.
const MAX_PARTS: usize = 10_000;

/// A part size derived from the expected length leaves this many parts spare, because a
/// redacted output can be longer than the input it came from.
const SPARE_PARTS: usize = 1_000;

/// One object upload to S3: a single `PutObject` while the body fits in one part, a multipart
/// upload above that, so memory holds about one part rather than the whole body.
struct ObjectUpload<'c> {
    client: &'c aws_sdk_s3::Client,
    bucket: &'c str,
    key: &'c str,
    content_type: Option<String>,
    part_size: usize,
}

struct MultipartUpload {
    id: String,
    parts: Vec<CompletedPart>,
}

impl MultipartUpload {
    fn next_part_number(&self) -> AppResult<i32> {
        let number = self.parts.len() + 1;
        match i32::try_from(number) {
            Ok(part_number) if number <= MAX_PARTS => Ok(part_number),
            _ => Err(AppError::SystemError {
                message: format!("The upload needs more than the {MAX_PARTS} parts S3 allows"),
            }),
        }
    }
}

impl ObjectUpload<'_> {
    const DEFAULT_PART_SIZE: usize = 8 * 1024 * 1024;

    /// The part size for a body of `expected_len` bytes, when that is known: large enough that
    /// the body stays within S3's part limit.
    fn part_size_for(expected_len: Option<usize>) -> usize {
        expected_len
            .map(|len| len.div_ceil(MAX_PARTS - SPARE_PARTS))
            .unwrap_or_default()
            .max(Self::DEFAULT_PART_SIZE)
            .max(MIN_PART_SIZE)
    }

    /// Writes the object. When it fails, no object is written and no stored part is left
    /// behind (S3 bills stored parts until the upload is aborted).
    async fn run<S: Stream<Item = AppResult<Bytes>> + Unpin>(self, input: S) -> AppResult<()> {
        let mut multipart = None;
        let result = self.write(input, &mut multipart).await;
        if let (Err(_), Some(multipart)) = (&result, &multipart) {
            if let Err(abort_error) = self
                .client
                .abort_multipart_upload()
                .bucket(self.bucket)
                .key(self.key)
                .upload_id(&multipart.id)
                .send()
                .await
            {
                tracing::warn!(
                    bucket = self.bucket,
                    key = self.key,
                    upload_id = multipart.id,
                    error = %aws_sdk_s3::error::DisplayErrorContext(&abort_error),
                    "Failed to abort a multipart upload; its stored parts stay billed until it is aborted"
                );
            }
        }
        result
    }

    async fn write<S: Stream<Item = AppResult<Bytes>> + Unpin>(
        &self,
        mut input: S,
        multipart: &mut Option<MultipartUpload>,
    ) -> AppResult<()> {
        let mut buffer = BytesMut::new();
        while let Some(chunk) = input.try_next().await? {
            buffer.extend_from_slice(&chunk);
            // A body of exactly one part stays a `PutObject`, so a part is cut only when more
            // bytes follow it.
            while buffer.len() > self.part_size {
                let part = buffer.split_to(self.part_size).freeze();
                self.upload_part(multipart, part).await?;
            }
        }

        if multipart.is_none() {
            self.client
                .put_object()
                .bucket(self.bucket)
                .key(self.key)
                .set_content_type(self.content_type.clone())
                .body(ByteStream::from(buffer.freeze()))
                .send()
                .await?;
            return Ok(());
        }

        let upload = self.upload_part(multipart, buffer.freeze()).await?;
        self.client
            .complete_multipart_upload()
            .bucket(self.bucket)
            .key(self.key)
            .upload_id(&upload.id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(std::mem::take(&mut upload.parts)))
                    .build(),
            )
            .send()
            .await?;
        Ok(())
    }

    /// Uploads `part` as the next part, creating the multipart upload on the first one.
    async fn upload_part<'u>(
        &self,
        multipart: &'u mut Option<MultipartUpload>,
        part: Bytes,
    ) -> AppResult<&'u mut MultipartUpload> {
        let upload = match multipart {
            Some(upload) => upload,
            None => {
                let created = self
                    .client
                    .create_multipart_upload()
                    .bucket(self.bucket)
                    .key(self.key)
                    .set_content_type(self.content_type.clone())
                    .send()
                    .await?;
                let id = created.upload_id.ok_or_else(|| AppError::SystemError {
                    message: "S3 started a multipart upload without an upload ID".to_string(),
                })?;
                multipart.insert(MultipartUpload {
                    id,
                    parts: Vec::new(),
                })
            }
        };

        let part_number = upload.next_part_number()?;
        let uploaded = self
            .client
            .upload_part()
            .bucket(self.bucket)
            .key(self.key)
            .upload_id(&upload.id)
            .part_number(part_number)
            .body(ByteStream::from(part))
            .send()
            .await?;
        upload.parts.push(
            CompletedPart::builder()
                .part_number(part_number)
                .set_e_tag(uploaded.e_tag)
                .build(),
        );
        Ok(upload)
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
                .next_back()
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
            file_size: object.content_length.map(|v| v as usize),
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
        ObjectUpload {
            client: &self.client,
            bucket: &self.bucket_name,
            key: &object_name,
            content_type,
            part_size: ObjectUpload::part_size_for(file_ref.and_then(|fr| fr.file_size)),
        }
        .run(input)
        .await
    }

    async fn list_files(
        &mut self,
        file_matcher: Option<&FileMatcher>,
        max_files_limit: Option<usize>,
    ) -> AppResult<ListFilesResult> {
        self.reporter.report_debug(format!(
            "Listing files in bucket: {} with prefix: {}",
            self.bucket_name, self.object_name
        ));
        if self.object_name.ends_with('/') {
            self.list_files_recursively(
                if self.object_name == "/" {
                    None
                } else {
                    Some(self.object_name.clone())
                },
                None,
                &file_matcher,
                max_files_limit,
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

#[cfg(test)]
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
            &format!("s3://{test_gcp_bucket_name}/redacter/test-upload/"),
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
                file_size: Some(test_data.len()),
            }),
        )
        .await?;

        let (file_ref, down_stream) = fs
            .download(Some(&FileSystemRef {
                relative_path: "test-upload.txt".into(),
                media_type: Some(mime::TEXT_PLAIN),
                file_size: Some(test_data.len()),
            }))
            .await?;

        let downloaded_bytes: Vec<bytes::Bytes> = down_stream.try_collect().await?;
        let flattened_bytes = downloaded_bytes.concat();
        let downloaded_content = std::str::from_utf8(&flattened_bytes)?;
        assert_eq!(downloaded_content, test_data);

        assert_eq!(file_ref.relative_path.value(), "test-upload.txt");
        assert_eq!(file_ref.media_type, Some(mime::TEXT_PLAIN));
        assert_eq!(file_ref.file_size, Some(test_data.len()));

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
            &format!("s3://{test_gcp_bucket_name}/redacter/test-list/"),
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
                file_size: Some(test_data.len()),
            }),
        )
        .await?;

        let list_result = fs.list_files(None, None).await?;
        assert_eq!(list_result.files.len(), 1);
        let file_ref = &list_result.files[0];
        assert_eq!(file_ref.relative_path.value(), "test-upload.txt");
        assert_eq!(file_ref.media_type, Some(mime::TEXT_PLAIN));
        assert_eq!(file_ref.file_size, Some(test_data.len()));

        fs.close().await?;

        Ok(())
    }

    const TEST_PART_SIZE: usize = 10;

    /// A client answering every multipart operation from a recorded script.
    struct FakeS3 {
        client: aws_sdk_s3::Client,
        put_object: aws_smithy_mocks::Rule,
        create_multipart_upload: aws_smithy_mocks::Rule,
        complete_multipart_upload: aws_smithy_mocks::Rule,
        abort_multipart_upload: aws_smithy_mocks::Rule,
        part_lengths: std::sync::Arc<std::sync::Mutex<Vec<usize>>>,
        completed_part_numbers: std::sync::Arc<std::sync::Mutex<Vec<i32>>>,
    }

    impl FakeS3 {
        /// `rejected_part` is the part number S3 refuses, if any.
        fn new(rejected_part: Option<i32>) -> Self {
            use aws_sdk_s3::operation::{
                abort_multipart_upload::AbortMultipartUploadOutput,
                complete_multipart_upload::CompleteMultipartUploadOutput,
                create_multipart_upload::CreateMultipartUploadOutput,
                put_object::PutObjectOutput,
                upload_part::{UploadPartError, UploadPartOutput},
            };
            use aws_smithy_mocks::{mock, MockResponse, RuleMode};

            let part_lengths = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
            let completed_part_numbers = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));

            let put_object = mock!(aws_sdk_s3::Client::put_object)
                .then_output(|| PutObjectOutput::builder().build());
            let create_multipart_upload = mock!(aws_sdk_s3::Client::create_multipart_upload)
                .then_output(|| {
                    CreateMultipartUploadOutput::builder()
                        .upload_id("upload-id")
                        .build()
                });
            let recorded_part_lengths = part_lengths.clone();
            let upload_part =
                mock!(aws_sdk_s3::Client::upload_part).then_compute_response(move |request| {
                    let part_number = request.part_number().expect("part number is set");
                    if Some(part_number) == rejected_part {
                        return MockResponse::Error(UploadPartError::generic(
                            aws_sdk_s3::error::ErrorMetadata::builder()
                                .code("AccessDenied")
                                .build(),
                        ));
                    }
                    let body = request.body().bytes().expect("part body is in memory");
                    recorded_part_lengths.lock().unwrap().push(body.len());
                    MockResponse::Output(
                        UploadPartOutput::builder()
                            .e_tag(format!("etag-{part_number}"))
                            .build(),
                    )
                });
            let recorded_part_numbers = completed_part_numbers.clone();
            let complete_multipart_upload = mock!(aws_sdk_s3::Client::complete_multipart_upload)
                .then_compute_output(move |request| {
                    *recorded_part_numbers.lock().unwrap() = request
                        .multipart_upload()
                        .expect("completed parts are listed")
                        .parts()
                        .iter()
                        .filter_map(|part| part.part_number())
                        .collect();
                    CompleteMultipartUploadOutput::builder().build()
                });
            let abort_multipart_upload = mock!(aws_sdk_s3::Client::abort_multipart_upload)
                .then_output(|| AbortMultipartUploadOutput::builder().build());

            let client = aws_smithy_mocks::mock_client!(
                aws_sdk_s3,
                RuleMode::MatchAny,
                [
                    &put_object,
                    &create_multipart_upload,
                    &upload_part,
                    &complete_multipart_upload,
                    &abort_multipart_upload,
                ]
            );
            FakeS3 {
                client,
                put_object,
                create_multipart_upload,
                complete_multipart_upload,
                abort_multipart_upload,
                part_lengths,
                completed_part_numbers,
            }
        }

        async fn upload<S: Stream<Item = AppResult<Bytes>> + Unpin>(
            &self,
            input: S,
        ) -> AppResult<()> {
            ObjectUpload {
                client: &self.client,
                bucket: "test-bucket",
                key: "test-key",
                content_type: Some("text/plain".to_string()),
                part_size: TEST_PART_SIZE,
            }
            .run(input)
            .await
        }
    }

    fn chunks_of(total_len: usize, chunk_len: usize) -> Vec<AppResult<Bytes>> {
        vec![0u8; total_len]
            .chunks(chunk_len)
            .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
            .collect()
    }

    fn broken_stream_error() -> AppError {
        AppError::SystemError {
            message: "the stream broke".to_string(),
        }
    }

    #[tokio::test]
    async fn body_within_one_part_is_a_single_put() -> Result<(), Box<dyn std::error::Error>> {
        let s3 = FakeS3::new(None);
        s3.upload(futures::stream::iter(chunks_of(TEST_PART_SIZE, 3)))
            .await?;

        assert_eq!(s3.put_object.num_calls(), 1);
        assert_eq!(s3.create_multipart_upload.num_calls(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn body_over_one_part_is_uploaded_in_whole_parts_and_a_remainder(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let s3 = FakeS3::new(None);
        s3.upload(futures::stream::iter(chunks_of(25, 7))).await?;

        assert_eq!(*s3.part_lengths.lock().unwrap(), vec![10, 10, 5]);
        assert_eq!(*s3.completed_part_numbers.lock().unwrap(), vec![1, 2, 3]);
        assert_eq!(s3.put_object.num_calls(), 0);
        assert_eq!(s3.abort_multipart_upload.num_calls(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn body_of_whole_parts_has_no_empty_last_part() -> Result<(), Box<dyn std::error::Error>>
    {
        let s3 = FakeS3::new(None);
        s3.upload(futures::stream::iter(chunks_of(2 * TEST_PART_SIZE, 4)))
            .await?;

        assert_eq!(*s3.part_lengths.lock().unwrap(), vec![10, 10]);
        assert_eq!(*s3.completed_part_numbers.lock().unwrap(), vec![1, 2]);
        Ok(())
    }

    #[tokio::test]
    async fn input_failing_after_two_parts_aborts_the_upload() {
        let s3 = FakeS3::new(None);
        let mut input = chunks_of(25, 7);
        input.push(Err(broken_stream_error()));

        let result = s3.upload(futures::stream::iter(input)).await;

        assert!(matches!(result, Err(AppError::SystemError { .. })));
        assert_eq!(s3.create_multipart_upload.num_calls(), 1);
        assert_eq!(s3.abort_multipart_upload.num_calls(), 1);
        assert_eq!(s3.complete_multipart_upload.num_calls(), 0);
    }

    #[tokio::test]
    async fn rejected_part_aborts_the_upload() {
        let s3 = FakeS3::new(Some(2));

        let result = s3.upload(futures::stream::iter(chunks_of(35, 7))).await;

        assert!(matches!(result, Err(AppError::AwsSdkError(_))));
        assert_eq!(s3.abort_multipart_upload.num_calls(), 1);
        assert_eq!(s3.complete_multipart_upload.num_calls(), 0);
    }

    #[tokio::test]
    async fn input_failing_within_the_first_part_sends_nothing() {
        let s3 = FakeS3::new(None);
        let mut input = chunks_of(4, 4);
        input.push(Err(broken_stream_error()));

        let result = s3.upload(futures::stream::iter(input)).await;

        assert!(result.is_err());
        assert_eq!(s3.put_object.num_calls(), 0);
        assert_eq!(s3.create_multipart_upload.num_calls(), 0);
        assert_eq!(s3.abort_multipart_upload.num_calls(), 0);
    }

    #[test]
    fn part_size_keeps_the_largest_uploads_within_the_part_limit() {
        let one_tebibyte: usize = 1 << 40;
        assert!(ObjectUpload::part_size_for(None) >= 5 * 1024 * 1024);
        assert!(ObjectUpload::part_size_for(Some(one_tebibyte)) * 10_000 > one_tebibyte);
    }

    async fn pending_multipart_uploads(
        fs: &AwsS3FileSystem<'_>,
        prefix: &str,
    ) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        let pending = fs
            .client
            .list_multipart_uploads()
            .bucket(&fs.bucket_name)
            .prefix(prefix)
            .send()
            .await?;
        Ok(pending.uploads().len())
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-aws"), ignore)]
    async fn multipart_upload_download_test() -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    {
        let term = console::Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let test_bucket_name =
            std::env::var("TEST_AWS_BUCKET_NAME").expect("TEST_AWS_BUCKET_NAME required");
        let prefix = "redacter/test-multipart/";
        let mut fs =
            AwsS3FileSystem::new(&format!("s3://{test_bucket_name}/{prefix}"), &reporter).await?;

        let one_mebibyte = 1024 * 1024;
        let content: Vec<u8> = (0..20 * one_mebibyte).map(|i| (i % 251) as u8).collect();
        let input = futures::stream::iter(
            content
                .chunks(one_mebibyte)
                .map(|chunk| Ok(bytes::Bytes::copy_from_slice(chunk)))
                .collect::<Vec<_>>(),
        );
        let file_ref = FileSystemRef {
            relative_path: "twenty-mebibytes.bin".into(),
            media_type: Some(mime::APPLICATION_OCTET_STREAM),
            file_size: Some(content.len()),
        };
        fs.upload(input, Some(&file_ref)).await?;

        let (downloaded_ref, down_stream) = fs.download(Some(&file_ref)).await?;
        let downloaded: Vec<bytes::Bytes> = down_stream.try_collect().await?;
        assert!(downloaded.concat() == content);
        assert_eq!(downloaded_ref.file_size, Some(content.len()));
        assert_eq!(
            downloaded_ref.media_type,
            Some(mime::APPLICATION_OCTET_STREAM)
        );
        assert_eq!(pending_multipart_uploads(&fs, prefix).await?, 0);

        fs.client
            .delete_object()
            .bucket(&fs.bucket_name)
            .key(fs.resolve(Some(&file_ref)).file_path)
            .send()
            .await?;
        fs.close().await?;
        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-aws"), ignore)]
    async fn failed_multipart_upload_leaves_nothing_behind(
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let term = console::Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let test_bucket_name =
            std::env::var("TEST_AWS_BUCKET_NAME").expect("TEST_AWS_BUCKET_NAME required");
        let prefix = "redacter/test-multipart-abort/";
        let mut fs =
            AwsS3FileSystem::new(&format!("s3://{test_bucket_name}/{prefix}"), &reporter).await?;

        let file_ref = FileSystemRef {
            relative_path: "broken.bin".into(),
            media_type: Some(mime::APPLICATION_OCTET_STREAM),
            file_size: None,
        };
        let one_mebibyte = 1024 * 1024;
        let mut input: Vec<AppResult<bytes::Bytes>> = (0..12)
            .map(|_| Ok(bytes::Bytes::from(vec![7u8; one_mebibyte])))
            .collect();
        input.push(Err(broken_stream_error()));
        let result = fs
            .upload(futures::stream::iter(input), Some(&file_ref))
            .await;

        assert!(matches!(result, Err(AppError::SystemError { .. })));
        assert_eq!(pending_multipart_uploads(&fs, prefix).await?, 0);
        let listing = fs
            .client
            .list_objects_v2()
            .bucket(&fs.bucket_name)
            .prefix(prefix)
            .send()
            .await?;
        assert!(listing.contents().is_empty());

        fs.close().await?;
        Ok(())
    }
}
