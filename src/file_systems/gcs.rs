use crate::errors::AppError;
use crate::file_systems::{
    AbsoluteFilePath, FileSystemConnection, FileSystemRef, ListFilesResult, RelativeFilePath,
};
use crate::file_tools::{FileMatcher, FileMatcherResult};
use crate::reporter::AppReporter;
use crate::AppResult;
use futures::{Stream, StreamExt};
use gcloud_sdk::google::storage::v2::storage_client::StorageClient;
use gcloud_sdk::google::storage::v2::{
    write_object_request, ChecksummedData, GetObjectRequest, ListObjectsRequest, Object,
    ReadObjectRequest, WriteObjectRequest, WriteObjectSpec,
};
use gcloud_sdk::prost::bytes;
use gcloud_sdk::prost::bytes::{Bytes, BytesMut};
use gcloud_sdk::tonic::metadata::{Ascii, MetadataValue};
use gcloud_sdk::{tonic, GoogleApi, GoogleAuthMiddleware};
use rvstruct::ValueStruct;
use std::collections::VecDeque;
use std::default::Default;
use std::sync::{Arc, Mutex};

const GCS_GRPC_ENDPOINT: &str = "https://storage.googleapis.com";

/// The `WriteObject` API only accepts data messages whose payload is a multiple
/// of 256 KiB, apart from the last one that closes the stream.
const WRITE_OBJECT_CHUNK_SIZE: usize = 256 * 1024;

pub struct GoogleCloudStorageFileSystem<'a> {
    client: GoogleApi<StorageClient<GoogleAuthMiddleware>>,
    bucket_resource_name: String,
    routing_params: MetadataValue<Ascii>,
    object_name: String,
    is_dir: bool,
    reporter: &'a AppReporter<'a>,
}

impl<'a> GoogleCloudStorageFileSystem<'a> {
    pub async fn new(path: &str, reporter: &'a AppReporter<'a>) -> AppResult<Self> {
        let client = GoogleApi::from_function(StorageClient::new, GCS_GRPC_ENDPOINT, None).await?;
        let (bucket_name, object_name) = GoogleCloudStorageFileSystem::parse_gcs_path(path);
        let is_dir = object_name.ends_with('/');
        let bucket_resource_name = format!("projects/_/buckets/{bucket_name}");
        let routing_params = MetadataValue::<Ascii>::try_from(
            url::form_urlencoded::Serializer::new(String::new())
                .append_pair("bucket", &bucket_resource_name)
                .finish(),
        )?;
        Ok(GoogleCloudStorageFileSystem {
            client,
            bucket_resource_name,
            routing_params,
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

    /// Every Storage v2 RPC has to carry the bucket routing header, otherwise the
    /// request is not routed to the region holding the bucket.
    fn request<T>(&self, message: T) -> tonic::Request<T> {
        let mut request = tonic::Request::new(message);
        request
            .metadata_mut()
            .insert("x-goog-request-params", self.routing_params.clone());
        request
    }

    fn to_file_system_ref(&self, object: Object) -> FileSystemRef {
        FileSystemRef {
            relative_path: object.name.trim_start_matches(&self.object_name).into(),
            media_type: Self::parse_media_type(&object.content_type),
            file_size: usize::try_from(object.size).ok(),
        }
    }

    fn parse_media_type(content_type: &str) -> Option<mime::Mime> {
        if content_type.is_empty() {
            None
        } else {
            content_type.parse().ok()
        }
    }

    #[async_recursion::async_recursion]
    async fn list_files_with_token(
        &self,
        prefix: Option<String>,
        page_token: Option<String>,
        file_matcher: &Option<&FileMatcher>,
        max_files_limit: Option<usize>,
    ) -> AppResult<ListFilesResult> {
        if max_files_limit.iter().any(|v| *v == 0) {
            return Ok(ListFilesResult::EMPTY);
        }

        let list = self
            .client
            .get()
            .list_objects(self.request(ListObjectsRequest {
                parent: self.bucket_resource_name.clone(),
                prefix: prefix.clone().unwrap_or_default(),
                page_token: page_token.unwrap_or_default(),
                ..ListObjectsRequest::default()
            }))
            .await?
            .into_inner();

        let all_found: Vec<FileSystemRef> = list
            .objects
            .into_iter()
            .filter(|object| !object.name.ends_with('/'))
            .map(|object| self.to_file_system_ref(object))
            .collect();

        let all_found_len = all_found.len();
        let filtered_files: Vec<FileSystemRef> = all_found
            .into_iter()
            .filter(|file_ref| {
                file_matcher
                    .iter()
                    .all(|matcher| matches!(matcher.matches(file_ref), FileMatcherResult::Matched))
            })
            .take(max_files_limit.unwrap_or(usize::MAX))
            .collect();
        let skipped = all_found_len - filtered_files.len();

        let new_max_files_limit = max_files_limit.map(|v| v.saturating_sub(filtered_files.len()));

        let next_list_result = if !list.next_page_token.is_empty() {
            self.list_files_with_token(
                prefix,
                Some(list.next_page_token),
                file_matcher,
                new_max_files_limit,
            )
            .await?
        } else {
            ListFilesResult::EMPTY
        };

        Ok(ListFilesResult {
            files: [filtered_files, next_list_result.files].concat(),
            skipped: next_list_result.skipped + skipped,
        })
    }
}

/// Splits an arbitrary sequence of byte chunks into `WriteObject` messages with
/// 256 KiB-aligned payloads, cumulative write offsets, and a `WriteObjectSpec`
/// carried by the first message of the stream.
struct WriteObjectChunker {
    first_message: Option<write_object_request::FirstMessage>,
    buffer: BytesMut,
    write_offset: i64,
}

impl WriteObjectChunker {
    fn new(spec: WriteObjectSpec) -> Self {
        Self {
            first_message: Some(write_object_request::FirstMessage::WriteObjectSpec(spec)),
            buffer: BytesMut::new(),
            write_offset: 0,
        }
    }

    fn message(&mut self, content: Vec<u8>, finish_write: bool) -> WriteObjectRequest {
        let write_offset = self.write_offset;
        self.write_offset = write_offset.saturating_add(content.len() as i64);
        WriteObjectRequest {
            write_offset,
            finish_write,
            first_message: self.first_message.take(),
            data: Some(write_object_request::Data::ChecksummedData(
                ChecksummedData {
                    content,
                    crc32c: None,
                },
            )),
            ..WriteObjectRequest::default()
        }
    }

    /// Buffers `data` and emits every complete aligned message it now holds.
    fn push(&mut self, data: Bytes) -> Vec<WriteObjectRequest> {
        self.buffer.extend_from_slice(&data);
        let mut messages = Vec::with_capacity(self.buffer.len() / WRITE_OBJECT_CHUNK_SIZE);
        while self.buffer.len() >= WRITE_OBJECT_CHUNK_SIZE {
            let chunk = self.buffer.split_to(WRITE_OBJECT_CHUNK_SIZE);
            messages.push(self.message(chunk.to_vec(), false));
        }
        messages
    }

    /// Emits the trailing message carrying the remainder and closing the stream.
    fn finish(mut self) -> WriteObjectRequest {
        let remainder = self.buffer.split().to_vec();
        self.message(remainder, true)
    }
}

/// Turns the caller's byte stream into the `WriteObject` request stream. The
/// client stream has no error channel, so a failure of the input is parked in
/// `error_slot` and the stream is terminated without `finish_write`, which makes
/// the RPC fail; the parked error is then reported instead of the gRPC status.
fn write_object_requests<S>(
    input: S,
    spec: WriteObjectSpec,
    error_slot: Arc<Mutex<Option<AppError>>>,
) -> impl Stream<Item = WriteObjectRequest> + Send + 'static
where
    S: Stream<Item = AppResult<Bytes>> + Send + Unpin + 'static,
{
    let initial = Some((input, WriteObjectChunker::new(spec), VecDeque::new()));
    futures::stream::unfold(initial, move |state| {
        let error_slot = error_slot.clone();
        async move {
            let (mut input, mut chunker, mut pending): (S, WriteObjectChunker, VecDeque<_>) =
                state?;
            loop {
                if let Some(request) = pending.pop_front() {
                    return Some((request, Some((input, chunker, pending))));
                }
                match input.next().await {
                    Some(Ok(data)) => pending.extend(chunker.push(data)),
                    Some(Err(err)) => {
                        if let Ok(mut slot) = error_slot.lock() {
                            *slot = Some(err);
                        }
                        return None;
                    }
                    None => return Some((chunker.finish(), None)),
                }
            }
        }
    })
}

impl<'a> FileSystemConnection<'a> for GoogleCloudStorageFileSystem<'a> {
    async fn download(
        &mut self,
        file_ref: Option<&FileSystemRef>,
    ) -> AppResult<(
        FileSystemRef,
        Box<dyn Stream<Item = AppResult<bytes::Bytes>> + Send + Sync + Unpin + 'static>,
    )> {
        let object_name = self.resolve(file_ref).file_path;

        let object = self
            .client
            .get()
            .get_object(self.request(GetObjectRequest {
                bucket: self.bucket_resource_name.clone(),
                object: object_name.clone(),
                ..GetObjectRequest::default()
            }))
            .await?
            .into_inner();

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

        let found_file_ref = FileSystemRef {
            relative_path: relative_path.clone(),
            media_type: Self::parse_media_type(&object.content_type)
                .or_else(|| mime_guess::from_path(relative_path.value()).first()),
            file_size: usize::try_from(object.size).ok(),
        };

        let read_stream = self
            .client
            .get()
            .read_object(self.request(ReadObjectRequest {
                bucket: self.bucket_resource_name.clone(),
                object: object_name,
                ..ReadObjectRequest::default()
            }))
            .await?
            .into_inner();

        Ok((
            found_file_ref,
            Box::new(read_stream.map(|response| {
                response
                    .map(|response| {
                        response
                            .checksummed_data
                            .map(|data| Bytes::from(data.content))
                            .unwrap_or_default()
                    })
                    .map_err(AppError::from)
            })),
        ))
    }

    async fn upload<S: Stream<Item = AppResult<bytes::Bytes>> + Send + Unpin + Sync + 'static>(
        &mut self,
        input: S,
        file_ref: Option<&FileSystemRef>,
    ) -> AppResult<()> {
        let object_name = self.resolve(file_ref).file_path;
        let content_type = file_ref
            .and_then(|fr| fr.media_type.as_ref())
            .map(|media_type| media_type.to_string())
            .unwrap_or_default();

        let spec = WriteObjectSpec {
            resource: Some(Object {
                name: object_name,
                bucket: self.bucket_resource_name.clone(),
                content_type,
                ..Object::default()
            }),
            ..WriteObjectSpec::default()
        };

        let input_error: Arc<Mutex<Option<AppError>>> = Arc::new(Mutex::new(None));
        let requests = write_object_requests(input, spec, input_error.clone());

        let result = self.client.get().write_object(self.request(requests)).await;

        if let Some(err) = input_error.lock().ok().and_then(|mut slot| slot.take()) {
            return Err(err);
        }
        result?;
        Ok(())
    }

    async fn list_files(
        &mut self,
        file_matcher: Option<&FileMatcher>,
        max_files_limit: Option<usize>,
    ) -> AppResult<ListFilesResult> {
        self.reporter.report(format!(
            "Listing files in bucket: {} with prefix: {}",
            self.bucket_resource_name, self.object_name
        ))?;
        if self.object_name.ends_with('/') {
            let prefix = if self.object_name != "/" {
                Some(self.object_name.clone())
            } else {
                None
            };
            self.list_files_with_token(prefix, None, &file_matcher, max_files_limit)
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
    use futures::TryStreamExt;

    /// The gRPC transport needs a process-level rustls provider, which the
    /// binary installs in `main` and the test harness does not.
    fn install_crypto_provider() {
        let _ = rustls::crypto::ring::default_provider().install_default();
    }

    fn test_write_object_spec() -> WriteObjectSpec {
        WriteObjectSpec {
            resource: Some(Object {
                name: "test-upload.bin".to_string(),
                bucket: "projects/_/buckets/test-bucket".to_string(),
                content_type: mime::APPLICATION_OCTET_STREAM.to_string(),
                ..Object::default()
            }),
            ..WriteObjectSpec::default()
        }
    }

    fn message_content(request: &WriteObjectRequest) -> &[u8] {
        match request.data {
            Some(write_object_request::Data::ChecksummedData(ref data)) => data.content.as_slice(),
            None => &[],
        }
    }

    async fn chunk_input(input_chunk_sizes: &[usize]) -> (Vec<u8>, Vec<WriteObjectRequest>) {
        let mut next_byte: u8 = 0;
        let input: Vec<AppResult<Bytes>> = input_chunk_sizes
            .iter()
            .map(|size| {
                let chunk: Vec<u8> = (0..*size)
                    .map(|_| {
                        next_byte = next_byte.wrapping_add(31);
                        next_byte
                    })
                    .collect();
                Ok(Bytes::from(chunk))
            })
            .collect();
        let expected: Vec<u8> = input
            .iter()
            .flat_map(|chunk| match chunk {
                Ok(bytes) => bytes.to_vec(),
                Err(_) => Vec::new(),
            })
            .collect();
        let requests: Vec<WriteObjectRequest> = write_object_requests(
            futures::stream::iter(input),
            test_write_object_spec(),
            Arc::new(Mutex::new(None)),
        )
        .collect()
        .await;
        (expected, requests)
    }

    #[tokio::test]
    async fn write_object_chunker_aligns_and_offsets_messages() {
        // Deliberately awkward input chunking: smaller than, larger than and
        // exactly the aligned message size.
        let (expected, requests) = chunk_input(&[
            1,
            WRITE_OBJECT_CHUNK_SIZE - 1,
            WRITE_OBJECT_CHUNK_SIZE * 2 + 17,
            WRITE_OBJECT_CHUNK_SIZE,
            5,
        ])
        .await;

        assert!(requests.len() > 1, "expected multiple aligned messages");

        let (last, aligned) = match requests.split_last() {
            Some(split) => split,
            None => panic!("the chunker must always emit a final message"),
        };

        for (index, request) in aligned.iter().enumerate() {
            assert_eq!(
                message_content(request).len(),
                WRITE_OBJECT_CHUNK_SIZE,
                "message {index} is not aligned to 256 KiB"
            );
            assert!(!request.finish_write, "message {index} closes the stream");
        }
        assert!(last.finish_write, "the last message must finish the write");

        let mut offset = 0i64;
        for (index, request) in requests.iter().enumerate() {
            assert_eq!(request.write_offset, offset, "message {index} offset");
            offset += message_content(request).len() as i64;
        }
        assert_eq!(offset as usize, expected.len(), "total written bytes");

        assert!(
            matches!(
                requests[0].first_message,
                Some(write_object_request::FirstMessage::WriteObjectSpec(_))
            ),
            "the first message must carry the write object spec"
        );
        assert!(
            requests[1..]
                .iter()
                .all(|request| request.first_message.is_none()),
            "only the first message may carry the write object spec"
        );

        let written: Vec<u8> = requests
            .iter()
            .flat_map(|request| message_content(request).to_vec())
            .collect();
        assert_eq!(written, expected, "written bytes differ from the input");
    }

    #[tokio::test]
    async fn write_object_chunker_handles_an_empty_input() {
        let (expected, requests) = chunk_input(&[]).await;
        assert!(expected.is_empty());
        assert_eq!(requests.len(), 1);
        assert!(requests[0].finish_write);
        assert_eq!(requests[0].write_offset, 0);
        assert!(message_content(&requests[0]).is_empty());
        assert!(matches!(
            requests[0].first_message,
            Some(write_object_request::FirstMessage::WriteObjectSpec(_))
        ));
    }

    #[tokio::test]
    async fn write_object_chunker_reports_the_input_error() {
        let error_slot: Arc<Mutex<Option<AppError>>> = Arc::new(Mutex::new(None));
        let input = futures::stream::iter(vec![
            Ok(Bytes::from_static(b"partial")),
            Err(AppError::SystemError {
                message: "broken input".to_string(),
            }),
        ]);
        let requests: Vec<WriteObjectRequest> =
            write_object_requests(input, test_write_object_spec(), error_slot.clone())
                .collect()
                .await;

        assert!(
            requests.iter().all(|request| !request.finish_write),
            "a failed input must not finish the write"
        );
        let parked = error_slot.lock().expect("the test holds no other lock");
        assert!(matches!(*parked, Some(AppError::SystemError { .. })));
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-gcp"), ignore)]
    async fn upload_download_test() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        install_crypto_provider();
        let term = console::Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let test_gcp_bucket_name =
            std::env::var("TEST_GCS_BUCKET_NAME").expect("TEST_GCS_BUCKET_NAME required");

        let mut fs = GoogleCloudStorageFileSystem::new(
            &format!("gs://{test_gcp_bucket_name}/redacter/test-upload/"),
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
    #[cfg_attr(not(feature = "ci-gcp"), ignore)]
    async fn list_test() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        install_crypto_provider();
        let term = console::Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let test_gcp_bucket_name =
            std::env::var("TEST_GCS_BUCKET_NAME").expect("TEST_GCS_BUCKET_NAME required");

        let mut fs = GoogleCloudStorageFileSystem::new(
            &format!("gs://{test_gcp_bucket_name}/redacter/test-list/"),
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
}
