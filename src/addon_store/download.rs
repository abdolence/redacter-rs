use super::error::AddonStoreError;
use super::manifest::AddonFile;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::StreamExt;
use indicatif::ProgressBar;
use sha2::{Digest, Sha256};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::io::AsyncWriteExt;

pub type ByteStream = BoxStream<'static, Result<Bytes, AddonStoreError>>;

/// Opens a URL as a stream of chunks: reqwest in production, canned bytes in tests.
pub trait Fetcher: Send + Sync {
    fn fetch<'s>(&'s self, url: &'s str) -> BoxFuture<'s, Result<ByteStream, AddonStoreError>>;
}

/// The production fetcher: reqwest with connect and read timeouts, following redirects
/// (GitHub answers a release asset with a redirect to its CDN) and honouring the proxy
/// environment variables.
pub struct HttpFetcher {
    client: reqwest::Client,
}

impl HttpFetcher {
    pub fn new() -> Result<Self, AddonStoreError> {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(30))
            .read_timeout(Duration::from_secs(60))
            .https_only(true)
            .user_agent(concat!("redacter/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|source| AddonStoreError::HttpClient { source })?;
        Ok(Self { client })
    }
}

impl Fetcher for HttpFetcher {
    fn fetch<'s>(&'s self, url: &'s str) -> BoxFuture<'s, Result<ByteStream, AddonStoreError>> {
        Box::pin(async move {
            let response =
                self.client
                    .get(url)
                    .send()
                    .await
                    .map_err(|source| AddonStoreError::Download {
                        url: url.to_string(),
                        source,
                    })?;
            let status = response.status();
            if !status.is_success() {
                return Err(AddonStoreError::HttpStatus {
                    url: url.to_string(),
                    status: status.as_u16(),
                });
            }
            let url = url.to_string();
            let chunks = futures::stream::try_unfold(response, move |mut response| {
                let url = url.clone();
                async move {
                    match response.chunk().await {
                        Ok(Some(chunk)) => Ok(Some((chunk, response))),
                        Ok(None) => Ok(None),
                        Err(source) => Err(AddonStoreError::Download { url, source }),
                    }
                }
            });
            Ok(Box::pin(chunks) as ByteStream)
        })
    }
}

/// Streams one file into `dir`, checking its byte count and SHA-256 before it appears under
/// its final name. The bytes go to `<name>.<random>.part` and are renamed atomically; on any
/// failure the temporary file is deleted and nothing is left under the final name.
pub async fn download_file(
    fetcher: &dyn Fetcher,
    file: &AddonFile,
    dir: &Path,
    progress: &ProgressBar,
) -> Result<PathBuf, AddonStoreError> {
    std::fs::create_dir_all(dir).map_err(|source| AddonStoreError::io(dir, source))?;
    remove_stale_parts(dir, file.name)?;
    let part = tempfile::Builder::new()
        .prefix(&format!("{}.", file.name))
        .suffix(".part")
        .tempfile_in(dir)
        .map_err(|source| AddonStoreError::io(dir, source))?;
    let handle = part
        .as_file()
        .try_clone()
        .map_err(|source| AddonStoreError::io(part.path(), source))?;
    let mut writer = tokio::fs::File::from_std(handle);
    let mut hasher = Sha256::new();
    let mut written: u64 = 0;
    let mut chunks = fetcher.fetch(file.url).await?;
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk?;
        hasher.update(&chunk);
        writer
            .write_all(&chunk)
            .await
            .map_err(|source| AddonStoreError::io(part.path(), source))?;
        written += chunk.len() as u64;
        progress.inc(chunk.len() as u64);
        if written > file.size {
            return Err(AddonStoreError::SizeMismatch {
                url: file.url.to_string(),
                expected: file.size,
                actual: written,
            });
        }
    }
    writer
        .flush()
        .await
        .map_err(|source| AddonStoreError::io(part.path(), source))?;
    drop(writer);
    if written != file.size {
        return Err(AddonStoreError::SizeMismatch {
            url: file.url.to_string(),
            expected: file.size,
            actual: written,
        });
    }
    let actual = hex(hasher.finalize().as_slice());
    if actual != file.sha256 {
        return Err(AddonStoreError::ChecksumMismatch {
            url: file.url.to_string(),
            expected: file.sha256.to_string(),
            actual,
        });
    }
    // The bytes are verified; make them durable before the rename, so a crash between the
    // two cannot leave a short file under the final, checked-by-size name.
    part.as_file()
        .sync_all()
        .map_err(|source| AddonStoreError::io(part.path(), source))?;
    let target = dir.join(file.name);
    part.persist(&target)
        .map_err(|err| AddonStoreError::io(&target, err.error))?;
    Ok(target)
}

/// Deletes `<name>.*.part` leftovers of a run that was killed mid-download.
fn remove_stale_parts(dir: &Path, name: &str) -> Result<(), AddonStoreError> {
    let prefix = format!("{name}.");
    let entries = std::fs::read_dir(dir).map_err(|source| AddonStoreError::io(dir, source))?;
    for entry in entries {
        let entry = entry.map_err(|source| AddonStoreError::io(dir, source))?;
        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();
        if file_name.starts_with(&prefix) && file_name.ends_with(".part") {
            std::fs::remove_file(entry.path())
                .map_err(|source| AddonStoreError::io(&entry.path(), source))?;
        }
    }
    Ok(())
}

/// The SHA-256 of the file at `path`, in lower-case hex like the manifests.
pub(super) fn sha256_of_file(path: &Path) -> Result<String, AddonStoreError> {
    let mut file = std::fs::File::open(path).map_err(|source| AddonStoreError::io(path, source))?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0; 1 << 16];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|source| AddonStoreError::io(path, source))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex(hasher.finalize().as_slice()))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    /// sha256("hello world")
    pub const HELLO_SHA256: &str =
        "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9";
    /// sha256("abc")
    pub const ABC_SHA256: &str = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

    pub static TEST_FILES: [AddonFile; 2] = [
        AddonFile {
            name: "weights.bin",
            url: "https://example.invalid/weights.bin",
            size: 11,
            sha256: HELLO_SHA256,
        },
        AddonFile {
            name: "config.json",
            url: "https://example.invalid/config.json",
            size: 3,
            sha256: ABC_SHA256,
        },
    ];

    /// The `.part` leftovers in `dir`, so a test can assert a download left nothing behind.
    pub fn part_files(dir: &Path) -> Vec<String> {
        std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().to_string())
            .filter(|name| name.ends_with(".part"))
            .collect()
    }

    /// Serves canned bytes in 4-byte chunks and counts the fetches. The counter is shared so
    /// a test can keep reading it after the store has taken ownership of the fetcher.
    pub struct FakeFetcher {
        responses: HashMap<String, Vec<u8>>,
        calls: Arc<Mutex<usize>>,
    }

    impl FakeFetcher {
        pub fn new(responses: &[(&str, &[u8])]) -> Self {
            Self {
                responses: responses
                    .iter()
                    .map(|(url, bytes)| (url.to_string(), bytes.to_vec()))
                    .collect(),
                calls: Arc::new(Mutex::new(0)),
            }
        }

        pub fn counter(&self) -> Arc<Mutex<usize>> {
            self.calls.clone()
        }

        pub fn good() -> Self {
            Self::new(&[
                ("https://example.invalid/weights.bin", b"hello world"),
                ("https://example.invalid/config.json", b"abc"),
            ])
        }
    }

    impl Fetcher for FakeFetcher {
        fn fetch<'s>(&'s self, url: &'s str) -> BoxFuture<'s, Result<ByteStream, AddonStoreError>> {
            Box::pin(async move {
                *self.calls.lock().unwrap() += 1;
                let bytes = self.responses.get(url).cloned().ok_or_else(|| {
                    AddonStoreError::HttpStatus {
                        url: url.to_string(),
                        status: 404,
                    }
                })?;
                let chunks: Vec<Result<Bytes, AddonStoreError>> = bytes
                    .chunks(4)
                    .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
                    .collect();
                Ok(Box::pin(futures::stream::iter(chunks)) as ByteStream)
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::*;
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn downloads_verifies_and_renames_into_place() {
        let dir = TempDir::new().unwrap();
        let fetcher = FakeFetcher::good();
        let path = download_file(&fetcher, &TEST_FILES[0], dir.path(), &ProgressBar::hidden())
            .await
            .unwrap();
        assert_eq!(path, dir.path().join("weights.bin"));
        assert_eq!(std::fs::read(&path).unwrap(), b"hello world");
        assert!(part_files(dir.path()).is_empty());
    }

    #[tokio::test]
    async fn creates_the_directory_when_missing() {
        let dir = TempDir::new().unwrap();
        let nested = dir.path().join("models").join("test-model");
        let fetcher = FakeFetcher::good();
        download_file(&fetcher, &TEST_FILES[1], &nested, &ProgressBar::hidden())
            .await
            .unwrap();
        assert_eq!(std::fs::read(nested.join("config.json")).unwrap(), b"abc");
    }

    #[tokio::test]
    async fn short_download_is_a_size_mismatch_and_leaves_nothing() {
        let dir = TempDir::new().unwrap();
        let fetcher = FakeFetcher::new(&[("https://example.invalid/weights.bin", b"hello")]);
        let err = download_file(&fetcher, &TEST_FILES[0], dir.path(), &ProgressBar::hidden())
            .await
            .unwrap_err();
        match err {
            AddonStoreError::SizeMismatch {
                url,
                expected,
                actual,
            } => {
                assert_eq!(url, "https://example.invalid/weights.bin");
                assert_eq!((expected, actual), (11, 5));
            }
            other => panic!("expected SizeMismatch, got {other}"),
        }
        assert!(!dir.path().join("weights.bin").exists());
        assert!(part_files(dir.path()).is_empty());
    }

    #[tokio::test]
    async fn wrong_bytes_are_a_checksum_mismatch_and_leave_nothing() {
        let dir = TempDir::new().unwrap();
        let fetcher = FakeFetcher::new(&[("https://example.invalid/weights.bin", b"hello worle")]);
        let err = download_file(&fetcher, &TEST_FILES[0], dir.path(), &ProgressBar::hidden())
            .await
            .unwrap_err();
        match err {
            AddonStoreError::ChecksumMismatch {
                url,
                expected,
                actual,
            } => {
                assert_eq!(url, "https://example.invalid/weights.bin");
                assert_eq!(expected, HELLO_SHA256);
                assert_eq!(
                    actual,
                    "0fc30e735a0228a31cbbb969988b4f50e02e737f979f091d7d224b765443f5d4"
                );
            }
            other => panic!("expected ChecksumMismatch, got {other}"),
        }
        assert!(!dir.path().join("weights.bin").exists());
        assert!(part_files(dir.path()).is_empty());
    }

    #[tokio::test]
    async fn oversized_download_aborts_as_soon_as_it_exceeds_the_manifest_size() {
        let dir = TempDir::new().unwrap();
        // 20 bytes in 4-byte chunks against an 11-byte manifest: the third chunk (12 bytes
        // written) is where an early abort must stop, well before the fourth and fifth.
        let fetcher = FakeFetcher::new(&[(
            "https://example.invalid/weights.bin",
            b"01234567890123456789",
        )]);
        let err = download_file(&fetcher, &TEST_FILES[0], dir.path(), &ProgressBar::hidden())
            .await
            .unwrap_err();
        match err {
            AddonStoreError::SizeMismatch {
                url,
                expected,
                actual,
            } => {
                assert_eq!(url, "https://example.invalid/weights.bin");
                assert_eq!(expected, 11);
                assert_eq!(actual, 12, "must abort at the first chunk that overshoots");
            }
            other => panic!("expected SizeMismatch, got {other}"),
        }
        assert!(!dir.path().join("weights.bin").exists());
        assert!(part_files(dir.path()).is_empty());
    }

    #[tokio::test]
    async fn missing_url_surfaces_the_fetcher_error() {
        let dir = TempDir::new().unwrap();
        let fetcher = FakeFetcher::new(&[]);
        let err = download_file(&fetcher, &TEST_FILES[0], dir.path(), &ProgressBar::hidden())
            .await
            .unwrap_err();
        assert!(
            matches!(err, AddonStoreError::HttpStatus { status: 404, .. }),
            "{err}"
        );
    }

    #[tokio::test]
    async fn stale_part_files_of_the_same_name_are_removed_first() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("weights.bin.abc123.part"), b"junk").unwrap();
        std::fs::write(dir.path().join("config.json.zzz.part"), b"junk").unwrap();
        let fetcher = FakeFetcher::good();
        download_file(&fetcher, &TEST_FILES[0], dir.path(), &ProgressBar::hidden())
            .await
            .unwrap();
        assert_eq!(
            part_files(dir.path()),
            vec!["config.json.zzz.part".to_string()]
        );
    }

    #[test]
    fn hex_encodes_lowercase_two_digits_per_byte() {
        assert_eq!(hex(&[0x00, 0x0f, 0xa0, 0xff]), "000fa0ff");
    }
}
