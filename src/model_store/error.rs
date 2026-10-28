use super::manifest::Addon;
use std::path::Path;

/// Failures of the addon store. Every variant names the value the user has to act on: the
/// addon, the directory, the URL, the file, or the expected and actual size or digest.
#[derive(Debug, thiserror::Error)]
pub enum ModelStoreError {
    #[error("{addon} is not installed; run `redacter setup-addons {addon}`")]
    NotInstalled { addon: Addon },
    #[error(
        "{path} is not the file {addon} pins; run `redacter setup-addons {addon}` to replace it"
    )]
    Damaged { addon: Addon, path: String },
    #[error("not available for this platform; install it by hand (README, \"PDF redaction\")")]
    NotAvailableForPlatform,
    #[error(
        "no cache directory is known on this system; pass --models-dir or set REDACTER_MODELS_DIR"
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

impl ModelStoreError {
    pub(super) fn io(path: &Path, source: std::io::Error) -> Self {
        ModelStoreError::Io {
            path: path.display().to_string(),
            source,
        }
    }
}
