use crate::errors::AppError;
use crate::file_systems::{
    AbsoluteFilePath, FileSystemConnection, FileSystemRef, ListFilesResult, RelativeFilePath,
};
use crate::file_tools::FileMatcher;
use crate::reporter::AppReporter;
use crate::AppResult;
use futures::{Stream, StreamExt};
use gcloud_sdk::google::storage::v2::storage_client::StorageClient;
use gcloud_sdk::google::storage::v2::{
    write_object_request, ChecksummedData, GetObjectRequest, ListObjectsRequest, Object,
    ObjectChecksums, ReadObjectRequest, ReadObjectResponse, WriteObjectRequest, WriteObjectSpec,
};
use gcloud_sdk::prost::bytes;
use gcloud_sdk::prost::bytes::{Bytes, BytesMut};
use gcloud_sdk::tonic::metadata::{Ascii, MetadataValue};
use gcloud_sdk::{tonic, GoogleApi, GoogleAuthMiddleware};
use rvstruct::ValueStruct;
use std::collections::VecDeque;
use std::default::Default;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{ready, Context, Poll};

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

        let mut listing = ListFilesResult::matching(
            list.objects
                .into_iter()
                .filter(|object| !object.name.ends_with('/'))
                .map(|object| self.to_file_system_ref(object)),
            *file_matcher,
            max_files_limit,
        );

        if !list.next_page_token.is_empty() {
            let new_max_files_limit =
                max_files_limit.map(|v| v.saturating_sub(listing.files.len()));
            listing.merge(
                self.list_files_with_token(
                    prefix,
                    Some(list.next_page_token),
                    file_matcher,
                    new_max_files_limit,
                )
                .await?,
            );
        }
        Ok(listing)
    }
}

/// Splits an arbitrary sequence of byte chunks into `WriteObject` messages with
/// 256 KiB-aligned payloads, cumulative write offsets, and a `WriteObjectSpec`
/// carried by the first message of the stream. Every message carries the CRC32C
/// of its payload and the closing one the CRC32C of the whole object, so the
/// server rejects data that was damaged on the way.
struct WriteObjectChunker {
    first_message: Option<write_object_request::FirstMessage>,
    buffer: BytesMut,
    write_offset: i64,
    object_crc32c: u32,
}

impl WriteObjectChunker {
    fn new(spec: WriteObjectSpec) -> Self {
        Self {
            first_message: Some(write_object_request::FirstMessage::WriteObjectSpec(spec)),
            buffer: BytesMut::new(),
            write_offset: 0,
            object_crc32c: 0,
        }
    }

    fn message(&mut self, content: Vec<u8>, finish_write: bool) -> WriteObjectRequest {
        let write_offset = self.write_offset;
        self.write_offset = write_offset.saturating_add(content.len() as i64);
        let message_crc32c = crc32c::crc32c(&content);
        self.object_crc32c = crc32c::crc32c_append(self.object_crc32c, &content);
        WriteObjectRequest {
            write_offset,
            finish_write,
            first_message: self.first_message.take(),
            data: Some(write_object_request::Data::ChecksummedData(
                ChecksummedData {
                    content,
                    crc32c: Some(message_crc32c),
                },
            )),
            object_checksums: finish_write.then(|| ObjectChecksums {
                crc32c: Some(self.object_crc32c),
                md5_hash: Vec::new(),
            }),
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

/// Checks the CRC32C digests the server sends with a `ReadObject` stream: every
/// message that carries one is verified as it arrives, and the whole-object
/// digest, which the server may send on any message, is verified once the
/// stream ends.
struct ReadObjectVerifier {
    object: String,
    bytes_read: u64,
    computed_object_crc32c: u32,
    expected_object_crc32c: Option<u32>,
}

impl ReadObjectVerifier {
    fn new(object: String) -> Self {
        Self {
            object,
            bytes_read: 0,
            computed_object_crc32c: 0,
            expected_object_crc32c: None,
        }
    }

    /// Returns the message payload once it has passed its own CRC32C check.
    fn accept(&mut self, response: ReadObjectResponse) -> AppResult<Bytes> {
        if let Some(expected) = response
            .object_checksums
            .and_then(|checksums| checksums.crc32c)
        {
            self.expected_object_crc32c = Some(expected);
        }
        let Some(data) = response.checksummed_data else {
            return Ok(Bytes::new());
        };
        let message_crc32c = crc32c::crc32c(&data.content);
        if let Some(expected) = data.crc32c.filter(|expected| *expected != message_crc32c) {
            return Err(AppError::GoogleCloudMessageChecksumMismatch {
                object: self.object.clone(),
                offset: self.bytes_read,
                expected,
                computed: message_crc32c,
            });
        }
        self.bytes_read = self.bytes_read.saturating_add(data.content.len() as u64);
        self.computed_object_crc32c =
            crc32c::crc32c_append(self.computed_object_crc32c, &data.content);
        Ok(Bytes::from(data.content))
    }

    /// Compares the whole-object digest with the bytes seen, when the server
    /// provided one.
    fn finish(self) -> AppResult<()> {
        match self.expected_object_crc32c {
            Some(expected) if expected != self.computed_object_crc32c => {
                Err(AppError::GoogleCloudObjectChecksumMismatch {
                    object: self.object,
                    expected,
                    computed: self.computed_object_crc32c,
                })
            }
            _ => Ok(()),
        }
    }
}

/// Turns the `ReadObject` response stream into the payload stream, failing it
/// with a checksum error instead of ending normally when the data does not
/// match what the server digested. The stream ends after its first error.
struct VerifiedReadStream<S> {
    responses: S,
    verifier: Option<ReadObjectVerifier>,
}

impl<S> VerifiedReadStream<S> {
    fn new(responses: S, object: String) -> Self {
        Self {
            responses,
            verifier: Some(ReadObjectVerifier::new(object)),
        }
    }
}

impl<S> Stream for VerifiedReadStream<S>
where
    S: Stream<Item = Result<ReadObjectResponse, tonic::Status>> + Unpin,
{
    type Item = AppResult<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        let Some(verifier) = this.verifier.as_mut() else {
            return Poll::Ready(None);
        };
        match ready!(Pin::new(&mut this.responses).poll_next(cx)) {
            Some(Ok(response)) => match verifier.accept(response) {
                Ok(payload) => Poll::Ready(Some(Ok(payload))),
                Err(err) => {
                    this.verifier = None;
                    Poll::Ready(Some(Err(err)))
                }
            },
            Some(Err(status)) => {
                this.verifier = None;
                Poll::Ready(Some(Err(status.into())))
            }
            None => Poll::Ready(
                this.verifier
                    .take()
                    .and_then(|verifier| verifier.finish().err())
                    .map(Err),
            ),
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
                object: object_name.clone(),
                ..ReadObjectRequest::default()
            }))
            .await?
            .into_inner();

        Ok((
            found_file_ref,
            Box::new(VerifiedReadStream::new(read_stream, object_name)),
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
        self.reporter.report_debug(format!(
            "Listing files in bucket: {} with prefix: {}",
            self.bucket_resource_name, self.object_name
        ));
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
    use gcloud_sdk::google::storage::v2::DeleteObjectRequest;

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

    fn message_checksum(request: &WriteObjectRequest) -> Option<u32> {
        match request.data {
            Some(write_object_request::Data::ChecksummedData(ref data)) => data.crc32c,
            None => None,
        }
    }

    /// The standard CRC32C check value: the checksum of the ASCII digits "123456789".
    const CRC32C_OF_DIGITS: u32 = 0xE306_9283;

    #[tokio::test]
    async fn write_object_chunker_matches_the_crc32c_check_value() {
        let requests: Vec<WriteObjectRequest> = write_object_requests(
            futures::stream::iter(vec![Ok(Bytes::from_static(b"123456789"))]),
            test_write_object_spec(),
            Arc::new(Mutex::new(None)),
        )
        .collect()
        .await;

        assert_eq!(requests.len(), 1);
        assert_eq!(message_checksum(&requests[0]), Some(CRC32C_OF_DIGITS));
        assert_eq!(
            requests[0]
                .object_checksums
                .as_ref()
                .and_then(|checksums| checksums.crc32c),
            Some(CRC32C_OF_DIGITS)
        );
    }

    #[tokio::test]
    async fn write_object_chunker_checksums_every_message() {
        let (_, requests) =
            chunk_input(&[1, WRITE_OBJECT_CHUNK_SIZE * 3 + 17, WRITE_OBJECT_CHUNK_SIZE]).await;

        assert!(requests.len() > 2);
        for (index, request) in requests.iter().enumerate() {
            assert_eq!(
                message_checksum(request),
                Some(crc32c::crc32c(message_content(request))),
                "message {index} checksum"
            );
        }
    }

    #[tokio::test]
    async fn write_object_chunker_sends_the_object_checksum_on_the_last_message_only() {
        let (expected, requests) = chunk_input(&[
            1,
            WRITE_OBJECT_CHUNK_SIZE - 1,
            WRITE_OBJECT_CHUNK_SIZE * 2 + 17,
            5,
        ])
        .await;

        let Some((last, earlier)) = requests.split_last() else {
            panic!("the chunker must always emit a final message");
        };
        assert!(!earlier.is_empty());
        assert!(
            earlier
                .iter()
                .all(|request| request.object_checksums.is_none()),
            "only the closing message may carry the object checksums"
        );
        assert_eq!(
            last.object_checksums,
            Some(ObjectChecksums {
                crc32c: Some(crc32c::crc32c(&expected)),
                md5_hash: Vec::new(),
            })
        );
    }

    #[tokio::test]
    async fn write_object_chunker_checksums_an_empty_object() {
        let (_, requests) = chunk_input(&[]).await;

        assert_eq!(message_checksum(&requests[0]), Some(0));
        assert_eq!(
            requests[0]
                .object_checksums
                .as_ref()
                .and_then(|checksums| checksums.crc32c),
            Some(0)
        );
    }

    fn read_response(
        content: &[u8],
        message_crc32c: Option<u32>,
        object_crc32c: Option<u32>,
    ) -> ReadObjectResponse {
        ReadObjectResponse {
            checksummed_data: Some(ChecksummedData {
                content: content.to_vec(),
                crc32c: message_crc32c,
            }),
            object_checksums: object_crc32c.map(|crc32c| ObjectChecksums {
                crc32c: Some(crc32c),
                md5_hash: Vec::new(),
            }),
            ..ReadObjectResponse::default()
        }
    }

    async fn read_verified(responses: Vec<ReadObjectResponse>) -> Vec<AppResult<Bytes>> {
        VerifiedReadStream::new(
            futures::stream::iter(responses.into_iter().map(Ok)),
            "redacter/test-object.bin".to_string(),
        )
        .collect()
        .await
    }

    #[tokio::test]
    async fn verified_read_stream_passes_matching_data_through() {
        let object_crc32c = crc32c::crc32c(b"hello, world");
        let items = read_verified(vec![
            read_response(
                b"hello, ",
                Some(crc32c::crc32c(b"hello, ")),
                Some(object_crc32c),
            ),
            read_response(b"world", Some(crc32c::crc32c(b"world")), None),
        ])
        .await;

        let payloads: Vec<Bytes> = items
            .into_iter()
            .collect::<AppResult<_>>()
            .expect("matching checksums must not fail the stream");
        assert_eq!(payloads.concat(), b"hello, world");
    }

    #[tokio::test]
    async fn verified_read_stream_passes_unchecksummed_data_through() {
        let items = read_verified(vec![
            read_response(b"hello, ", None, None),
            read_response(b"world", None, None),
        ])
        .await;

        let payloads: Vec<Bytes> = items
            .into_iter()
            .collect::<AppResult<_>>()
            .expect("a server that sends no checksums must not fail the stream");
        assert_eq!(payloads.concat(), b"hello, world");
    }

    #[tokio::test]
    async fn verified_read_stream_rejects_a_corrupted_message() {
        let mut items = read_verified(vec![
            read_response(b"hello, ", Some(crc32c::crc32c(b"hello, ")), None),
            read_response(b"w0rld", Some(crc32c::crc32c(b"world")), None),
            read_response(b"never read", None, None),
        ])
        .await;

        assert_eq!(
            items.len(),
            2,
            "the stream must end at the corrupted message"
        );
        assert!(items.remove(0).is_ok());
        assert!(matches!(
            items.remove(0),
            Err(AppError::GoogleCloudMessageChecksumMismatch { offset: 7, .. })
        ));
    }

    #[tokio::test]
    async fn verified_read_stream_rejects_a_wrong_object_checksum() {
        let items = read_verified(vec![
            read_response(b"hello, ", None, Some(crc32c::crc32c(b"hello, world"))),
            read_response(b"w0rld", None, None),
        ])
        .await;

        assert_eq!(items.len(), 3, "both payloads, then the checksum failure");
        assert!(matches!(
            items.last(),
            Some(Err(AppError::GoogleCloudObjectChecksumMismatch { .. }))
        ));
    }

    #[tokio::test]
    async fn verified_read_stream_rejects_a_truncated_object() {
        let items = read_verified(vec![read_response(
            b"hello, ",
            Some(crc32c::crc32c(b"hello, ")),
            Some(crc32c::crc32c(b"hello, world")),
        )])
        .await;

        assert!(matches!(
            items.last(),
            Some(Err(AppError::GoogleCloudObjectChecksumMismatch { .. }))
        ));
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

    type LiveTestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

    async fn live_file_system<'a>(
        reporter: &'a AppReporter<'a>,
        directory: &str,
    ) -> AppResult<GoogleCloudStorageFileSystem<'a>> {
        install_crypto_provider();
        let test_gcp_bucket_name =
            std::env::var("TEST_GCS_BUCKET_NAME").expect("TEST_GCS_BUCKET_NAME required");
        GoogleCloudStorageFileSystem::new(
            &format!("gs://{test_gcp_bucket_name}/redacter/{directory}/"),
            reporter,
        )
        .await
    }

    async fn delete_live_object(
        fs: &GoogleCloudStorageFileSystem<'_>,
        object: &str,
    ) -> LiveTestResult {
        let request = fs.request(DeleteObjectRequest {
            bucket: fs.bucket_resource_name.clone(),
            object: object.to_string(),
            ..DeleteObjectRequest::default()
        });
        match fs.client.get().delete_object(request).await {
            Ok(_) => Ok(()),
            Err(status) if status.code() == tonic::Code::NotFound => Ok(()),
            Err(status) => Err(status.into()),
        }
    }

    async fn live_object_exists(
        fs: &GoogleCloudStorageFileSystem<'_>,
        object: &str,
    ) -> Result<bool, tonic::Status> {
        let request = fs.request(GetObjectRequest {
            bucket: fs.bucket_resource_name.clone(),
            object: object.to_string(),
            ..GetObjectRequest::default()
        });
        match fs.client.get().get_object(request).await {
            Ok(_) => Ok(true),
            Err(status) if status.code() == tonic::Code::NotFound => Ok(false),
            Err(status) => Err(status),
        }
    }

    /// Sends the object through `WriteObject` with `tamper` applied to every
    /// request after it was checksummed, the way a bit flip on the wire would.
    async fn write_tampered_object(
        fs: &GoogleCloudStorageFileSystem<'_>,
        object: &str,
        content: Vec<u8>,
        mut tamper: impl FnMut(&mut WriteObjectRequest) + Send + 'static,
    ) -> Result<(), tonic::Status> {
        let spec = WriteObjectSpec {
            resource: Some(Object {
                name: object.to_string(),
                bucket: fs.bucket_resource_name.clone(),
                ..Object::default()
            }),
            ..WriteObjectSpec::default()
        };
        let requests = write_object_requests(
            futures::stream::iter(vec![Ok(Bytes::from(content))]),
            spec,
            Arc::new(Mutex::new(None)),
        )
        .map(move |mut request| {
            tamper(&mut request);
            request
        });
        fs.client
            .get()
            .write_object(fs.request(requests))
            .await
            .map(|_| ())
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-gcp"), ignore)]
    async fn multi_chunk_round_trip_keeps_the_bytes() -> LiveTestResult {
        let term = console::Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let mut fs = live_file_system(&reporter, "test-crc32c-round-trip").await?;

        let content: Vec<u8> = (0..WRITE_OBJECT_CHUNK_SIZE * 3 + 12_345)
            .map(|index| (index as u32).wrapping_mul(2_654_435_761).to_le_bytes()[3])
            .collect();
        let input: Vec<AppResult<Bytes>> = content
            .chunks(100_000)
            .map(|piece| Ok(Bytes::copy_from_slice(piece)))
            .collect();
        let file_ref = FileSystemRef {
            relative_path: "round-trip.bin".into(),
            media_type: Some(mime::APPLICATION_OCTET_STREAM),
            file_size: Some(content.len()),
        };

        let outcome: LiveTestResult = async {
            fs.upload(futures::stream::iter(input), Some(&file_ref))
                .await?;
            let (_, down_stream) = fs.download(Some(&file_ref)).await?;
            let downloaded: Vec<Bytes> = down_stream.try_collect().await?;
            assert_eq!(downloaded.concat(), content);
            Ok(())
        }
        .await;

        let object = fs.resolve(Some(&file_ref)).file_path;
        delete_live_object(&fs, &object).await?;
        outcome
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-gcp"), ignore)]
    async fn upload_with_a_wrong_object_checksum_is_rejected() -> LiveTestResult {
        let term = console::Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let fs = live_file_system(&reporter, "test-crc32c-object-rejected").await?;
        let object = "redacter/test-crc32c-object-rejected/object.bin";
        delete_live_object(&fs, object).await?;

        let outcome = write_tampered_object(&fs, object, b"object checksum".to_vec(), |request| {
            if let Some(checksums) = request.object_checksums.as_mut() {
                checksums.crc32c = checksums.crc32c.map(|crc32c| !crc32c);
            }
        })
        .await;
        let created = live_object_exists(&fs, object).await;

        delete_live_object(&fs, object).await?;
        let status = outcome.expect_err("the server must reject a wrong object checksum");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(!created?, "a rejected upload must not create the object");
        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-gcp"), ignore)]
    async fn upload_with_a_wrong_message_checksum_is_rejected() -> LiveTestResult {
        let term = console::Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let fs = live_file_system(&reporter, "test-crc32c-message-rejected").await?;
        let object = "redacter/test-crc32c-message-rejected/object.bin";
        delete_live_object(&fs, object).await?;

        let outcome = write_tampered_object(&fs, object, b"message checksum".to_vec(), |request| {
            if let Some(write_object_request::Data::ChecksummedData(data)) = request.data.as_mut() {
                data.crc32c = data.crc32c.map(|crc32c| !crc32c);
            }
        })
        .await;
        let created = live_object_exists(&fs, object).await;

        delete_live_object(&fs, object).await?;
        let status = outcome.expect_err("the server must reject a wrong message checksum");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(!created?, "a rejected upload must not create the object");
        Ok(())
    }
}
