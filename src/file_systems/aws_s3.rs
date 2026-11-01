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

/// The size of a part when nothing larger is needed. S3 rejects a part under 5 MiB, except the
/// last one.
const DEFAULT_PART_SIZE: usize = 8 * 1024 * 1024;

/// S3 rejects an upload of more than 10,000 parts.
const MAX_PARTS: usize = 10_000;

/// A part size derived from the expected length leaves this many parts spare, because a
/// redacted output can be longer than the input it came from.
const SPARE_PARTS: usize = 1_000;

/// Without an expected length the part size doubles after every this many parts, so that the
/// 10,000-part limit does not cap the object at 10,000 default-sized parts.
const PARTS_PER_DOUBLING: usize = 1_000;

/// Cuts a body that arrives in chunks of any size into upload parts.
struct PartBuffer {
    buffer: BytesMut,
    base_part_size: usize,
    growing: bool,
    parts_cut: usize,
}

impl PartBuffer {
    fn new(expected_len: Option<usize>) -> Self {
        PartBuffer {
            buffer: BytesMut::new(),
            base_part_size: expected_len.map_or(DEFAULT_PART_SIZE, |len| {
                len.div_ceil(MAX_PARTS - SPARE_PARTS).max(DEFAULT_PART_SIZE)
            }),
            growing: expected_len.is_none(),
            parts_cut: 0,
        }
    }

    fn part_size(&self) -> usize {
        if self.growing {
            self.base_part_size << (self.parts_cut / PARTS_PER_DOUBLING)
        } else {
            self.base_part_size
        }
    }

    fn push(&mut self, chunk: &[u8]) {
        self.buffer.extend_from_slice(chunk);
    }

    /// Cuts the next part only when more bytes follow it, so that the last part is never
    /// empty and a body of exactly one part stays a single `PutObject`.
    fn cut_part(&mut self) -> Option<Bytes> {
        if self.buffer.len() <= self.part_size() {
            return None;
        }
        let part = self.buffer.split_to(self.part_size()).freeze();
        self.parts_cut += 1;
        Some(part)
    }

    fn into_last_part(self) -> Bytes {
        self.buffer.freeze()
    }
}

/// One object upload to S3: a single `PutObject` while the body fits in one part, a multipart
/// upload above that, so memory holds about one part rather than the whole body.
struct ObjectUpload<'c> {
    client: &'c aws_sdk_s3::Client,
    reporter: &'c AppReporter<'c>,
    bucket: &'c str,
    key: &'c str,
    content_type: Option<String>,
    expected_len: Option<usize>,
}

struct MultipartUpload {
    id: String,
    parts: Vec<CompletedPart>,
}

impl MultipartUpload {
    fn next_part_number(&self) -> AppResult<i32> {
        let number = self.parts.len() + 1;
        i32::try_from(number).map_err(|_| AppError::SystemError {
            message: format!("Part number {number} does not fit an S3 part number"),
        })
    }
}

impl ObjectUpload<'_> {
    /// Writes the object. An error after a multipart upload started aborts it, which frees its
    /// stored parts. Parts stay stored, and billed, when the abort itself fails and when the
    /// process is killed mid-upload: only a manual abort or a bucket lifecycle rule removes them.
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
                let message = format!(
                    "Failed to abort multipart upload {upload_id} of s3://{bucket}/{key}; its stored parts stay billed until it is aborted \
                     (aws s3api abort-multipart-upload --bucket {bucket} --key {key} --upload-id {upload_id}): {error}",
                    upload_id = multipart.id,
                    bucket = self.bucket,
                    key = self.key,
                    error = aws_sdk_s3::error::DisplayErrorContext(&abort_error),
                );
                if let Err(report_error) = self.reporter.report(&message) {
                    tracing::warn!(%report_error, "{message}");
                }
            }
        }
        result
    }

    async fn write<S: Stream<Item = AppResult<Bytes>> + Unpin>(
        &self,
        mut input: S,
        multipart: &mut Option<MultipartUpload>,
    ) -> AppResult<()> {
        let mut parts = PartBuffer::new(self.expected_len);
        while let Some(chunk) = input.try_next().await? {
            parts.push(&chunk);
            while let Some(part) = parts.cut_part() {
                self.upload_part(multipart, part).await?;
            }
        }
        let last_part = parts.into_last_part();

        if multipart.is_none() {
            self.client
                .put_object()
                .bucket(self.bucket)
                .key(self.key)
                .set_content_type(self.content_type.clone())
                .body(ByteStream::from(last_part))
                .send()
                .await?;
            return Ok(());
        }

        let upload = self.upload_part(multipart, last_part).await?;
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
            reporter: self.reporter,
            bucket: &self.bucket_name,
            key: &object_name,
            content_type,
            expected_len: file_ref.and_then(|fr| fr.file_size),
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

    fn small_parts(growing: bool) -> PartBuffer {
        PartBuffer {
            buffer: BytesMut::new(),
            base_part_size: 10,
            growing,
            parts_cut: 0,
        }
    }

    /// Pushes `total_len` bytes in chunks of `chunk_len` and returns the length of every part.
    fn part_lengths(mut parts: PartBuffer, total_len: usize, chunk_len: usize) -> Vec<usize> {
        let mut lengths = Vec::new();
        for chunk in vec![0u8; total_len].chunks(chunk_len) {
            parts.push(chunk);
            while let Some(part) = parts.cut_part() {
                lengths.push(part.len());
            }
        }
        lengths.push(parts.into_last_part().len());
        lengths
    }

    #[test]
    fn body_over_one_part_is_cut_into_whole_parts_and_a_remainder() {
        assert_eq!(part_lengths(small_parts(false), 25, 7), vec![10, 10, 5]);
    }

    #[test]
    fn body_of_whole_parts_has_no_empty_last_part() {
        assert_eq!(part_lengths(small_parts(false), 20, 4), vec![10, 10]);
    }

    #[test]
    fn body_within_one_part_is_not_cut() {
        assert_eq!(part_lengths(small_parts(false), 10, 3), vec![10]);
        assert_eq!(part_lengths(small_parts(false), 0, 3), vec![0]);
    }

    #[test]
    fn unknown_length_doubles_the_part_size_to_stay_within_the_part_limit() {
        let mut parts = PartBuffer::new(None);
        let mut capacity = 0;
        for parts_cut in 0..MAX_PARTS {
            parts.parts_cut = parts_cut;
            capacity += parts.part_size();
        }
        assert!(capacity > 5 * 1024 * 1024 * 1024 * 1024);

        parts.parts_cut = PARTS_PER_DOUBLING - 1;
        assert_eq!(parts.part_size(), DEFAULT_PART_SIZE);
        parts.parts_cut = PARTS_PER_DOUBLING;
        assert_eq!(parts.part_size(), 2 * DEFAULT_PART_SIZE);

        let mut known = PartBuffer::new(Some(1024));
        known.parts_cut = 5 * PARTS_PER_DOUBLING;
        assert_eq!(known.part_size(), DEFAULT_PART_SIZE);
    }

    fn broken_stream_error() -> AppError {
        AppError::SystemError {
            message: "the stream broke".to_string(),
        }
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
        let mut input: Vec<AppResult<bytes::Bytes>> = (0..20)
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
