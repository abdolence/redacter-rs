use super::error::ModelStoreError;
use super::manifest::{ModelFile, ModelManifest};
use std::path::PathBuf;

/// What a download would fetch, shown to the user before anything is transferred.
#[derive(Debug)]
pub struct DownloadRequest {
    pub model: &'static ModelManifest,
    pub dir: PathBuf,
    /// Only the files that are missing or stale.
    pub files: Vec<&'static ModelFile>,
    pub total_bytes: u64,
}

/// Answers "may this be downloaded?": the terminal in production, a fixed answer in tests.
pub trait ConsentSource: Send + Sync {
    fn ask(&self, request: &DownloadRequest) -> Result<bool, ModelStoreError>;
}
