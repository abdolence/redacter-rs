/// Failures of the model store. Every variant names the value the user has to act on: the
/// model, the directory, the URL, the file, or the expected and actual size or digest.
#[derive(Debug, thiserror::Error)]
pub enum ModelStoreError {
    #[error(
        "model \"{model}\" is not installed in {dir}; re-run with --download-models yes, or place \
         {files} there (README, \"Models and downloads\")"
    )]
    NotInstalled {
        model: String,
        dir: String,
        files: String,
    },
    #[error("download of model \"{model}\" was declined")]
    Declined { model: String },
    #[error("cannot read the answer from the terminal: {source}")]
    Prompt {
        #[source]
        source: std::io::Error,
    },
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
    #[error(
        "{location} has {actual} bytes, expected {expected}; delete it or re-run with \
         --download-models yes"
    )]
    SizeMismatch {
        location: String,
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
    #[error("cannot write to the terminal: {reason}")]
    Report { reason: String },
}
