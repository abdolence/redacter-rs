use super::manifest::Addon;
use std::path::Path;

/// Failures of the addon store. Every variant names the value the user has to act on: the
/// addon, the directory, the URL, the file, or the expected and actual size or digest.
#[derive(Debug, thiserror::Error)]
pub enum AddonStoreError {
    #[error("{addon} is not installed; run `redacter addons install {addon}`")]
    NotInstalled { addon: Addon },
    #[error(
        "{path} is missing or is not the file {addon} pins; run `redacter addons install {addon}` to repair it"
    )]
    Damaged { addon: Addon, path: String },
    #[error("{addon} is not available for this platform; install it by hand, see the README")]
    NotAvailableForPlatform { addon: Addon },
    #[error(
        "no cache directory is known on this system; pass --addons-dir or set REDACTER_ADDONS_DIR"
    )]
    NoCacheDir,
    #[error("cannot build the HTTP client: {source}")]
    HttpClient {
        #[source]
        source: reqwest::Error,
    },
    #[error("download of {url} failed: {source}")]
    Download {
        url: String,
        #[source]
        source: reqwest::Error,
    },
    #[error("download of {url} failed with HTTP status {status}")]
    HttpStatus { url: String, status: u16 },
    #[error("download of {url} has {actual} bytes, expected {expected}")]
    SizeMismatch {
        url: String,
        expected: u64,
        actual: u64,
    },
    #[error("checksum of {url} is {actual}, expected {expected}")]
    ChecksumMismatch {
        url: String,
        expected: String,
        actual: String,
    },
    #[error("file system error at {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("cannot draw the download progress bar: {reason}")]
    Progress { reason: String },
}

impl AddonStoreError {
    pub(super) fn io(path: &Path, source: std::io::Error) -> Self {
        AddonStoreError::Io {
            path: path.display().to_string(),
            source,
        }
    }
}
