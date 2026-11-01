use crate::errors::AppError;
use crate::file_systems::gcs::GoogleCloudStorageFileSystem;
use crate::file_systems::local::LocalFileSystem;
use crate::file_systems::zip::ZipFileSystem;
use crate::AppResult;
use futures::Stream;
use gcloud_sdk::prost::bytes;
use gcloud_sdk::prost::bytes::Bytes;
use mime::Mime;
use rvstruct::ValueStruct;
use std::path::Path;

mod aws_s3;
mod gcs;
mod local;
mod zip;

#[cfg(feature = "clipboard")]
mod clipboard;

mod noop;

use crate::file_systems::aws_s3::AwsS3FileSystem;
use crate::file_tools::{FileMatcher, FileMatcherResult};
use crate::reporter::AppReporter;

#[derive(Debug, Clone, ValueStruct)]
pub struct RelativeFilePath(pub String);

impl RelativeFilePath {
    pub fn filename(&self) -> String {
        self.value()
            .split('/')
            .next_back()
            .map(|s| s.to_string())
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone)]
pub struct AbsoluteFilePath {
    pub file_path: String,
}

#[derive(Debug, Clone)]
pub struct FileSystemRef {
    pub relative_path: RelativeFilePath,
    pub media_type: Option<Mime>,
    pub file_size: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct ListFilesResult {
    pub files: Vec<FileSystemRef>,
    /// Files skipped by the size limit or the name filter.
    pub skipped: usize,
    /// Files ignored by the ignore rules of the matcher.
    pub ignored: usize,
}

impl ListFilesResult {
    pub const EMPTY: ListFilesResult = ListFilesResult {
        files: Vec::new(),
        skipped: 0,
        ignored: 0,
    };

    /// The files of `found` that `file_matcher` keeps, with the counts of those it skipped and
    /// of those it ignored.
    pub fn matching(
        found: impl IntoIterator<Item = FileSystemRef>,
        file_matcher: Option<&FileMatcher>,
    ) -> Self {
        let mut result = Self::EMPTY;
        for file_ref in found {
            result.add(file_ref, file_matcher);
        }
        result
    }

    /// Lists `file_ref` if `file_matcher` keeps it, and counts it by reason if not.
    pub fn add(&mut self, file_ref: FileSystemRef, file_matcher: Option<&FileMatcher>) {
        match file_matcher.map(|matcher| matcher.matches(&file_ref)) {
            None | Some(FileMatcherResult::Matched) => self.files.push(file_ref),
            Some(FileMatcherResult::SkippedDueToIgnore) => self.ignored += 1,
            Some(FileMatcherResult::SkippedDueToSize | FileMatcherResult::SkippedDueToName) => {
                self.skipped += 1
            }
        }
    }

    /// Appends the files and adds the counts of `other`.
    pub fn merge(&mut self, other: Self) {
        self.files.extend(other.files);
        self.skipped += other.skipped;
        self.ignored += other.ignored;
    }
}

pub trait FileSystemConnection<'a> {
    async fn download(
        &mut self,
        file_ref: Option<&FileSystemRef>,
    ) -> AppResult<(
        FileSystemRef,
        Box<dyn Stream<Item = AppResult<bytes::Bytes>> + Send + Sync + Unpin + 'static>,
    )>;

    async fn upload<S: Stream<Item = AppResult<bytes::Bytes>> + Send + Unpin + Sync + 'static>(
        &mut self,
        input: S,
        file_ref: Option<&FileSystemRef>,
    ) -> AppResult<()>;

    async fn list_files(
        &mut self,
        file_matcher: Option<&FileMatcher>,
        max_files_limit: Option<usize>,
    ) -> AppResult<ListFilesResult>;

    async fn close(self) -> AppResult<()>;

    async fn has_multiple_files(&self) -> AppResult<bool>;

    async fn accepts_multiple_files(&self) -> AppResult<bool>;

    fn resolve(&self, file_ref: Option<&FileSystemRef>) -> AbsoluteFilePath;

    /// The directory of the source when it is a local one.
    fn local_directory(&self) -> Option<&Path> {
        None
    }
}

pub enum DetectFileSystem<'a> {
    Local(LocalFileSystem<'a>),
    GoogleCloudStorage(GoogleCloudStorageFileSystem<'a>),
    AwsS3(AwsS3FileSystem<'a>),
    ZipFile(ZipFileSystem<'a>),
    #[cfg(feature = "clipboard")]
    Clipboard(clipboard::ClipboardFileSystem<'a>),
}

impl<'a> DetectFileSystem<'a> {
    pub async fn open(
        file_path: &str,
        reporter: &'a AppReporter<'a>,
    ) -> AppResult<impl FileSystemConnection<'a>> {
        if file_path.starts_with("file://") || !file_path.contains("://") {
            Ok(DetectFileSystem::Local(
                LocalFileSystem::new(file_path, reporter).await?,
            ))
        } else if file_path.starts_with("gs://") {
            Ok(DetectFileSystem::GoogleCloudStorage(
                GoogleCloudStorageFileSystem::new(file_path, reporter).await?,
            ))
        } else if file_path.starts_with("s3://") {
            Ok(DetectFileSystem::AwsS3(
                AwsS3FileSystem::new(file_path, reporter).await?,
            ))
        } else if file_path.starts_with("zip://") {
            Ok(DetectFileSystem::ZipFile(
                ZipFileSystem::new(file_path, reporter).await?,
            ))
        } else if file_path.starts_with("clipboard://") {
            #[cfg(feature = "clipboard")]
            {
                Ok(DetectFileSystem::Clipboard(
                    clipboard::ClipboardFileSystem::new(file_path, reporter).await?,
                ))
            }
            #[cfg(not(feature = "clipboard"))]
            {
                Err(AppError::UnknownFileSystem {
                    file_path: file_path.to_string(),
                })
            }
        } else {
            Err(AppError::UnknownFileSystem {
                file_path: file_path.to_string(),
            })
        }
    }
}

impl<'a> FileSystemConnection<'a> for DetectFileSystem<'a> {
    async fn download(
        &mut self,
        file_ref: Option<&FileSystemRef>,
    ) -> AppResult<(
        FileSystemRef,
        Box<dyn Stream<Item = AppResult<Bytes>> + Send + Sync + Unpin + 'static>,
    )> {
        match self {
            DetectFileSystem::Local(fs) => fs.download(file_ref).await,
            DetectFileSystem::GoogleCloudStorage(fs) => fs.download(file_ref).await,
            DetectFileSystem::AwsS3(fs) => fs.download(file_ref).await,
            DetectFileSystem::ZipFile(fs) => fs.download(file_ref).await,
            #[cfg(feature = "clipboard")]
            DetectFileSystem::Clipboard(fs) => fs.download(file_ref).await,
        }
    }

    async fn upload<S: Stream<Item = AppResult<Bytes>> + Send + Unpin + Sync + 'static>(
        &mut self,
        input: S,
        file_ref: Option<&FileSystemRef>,
    ) -> AppResult<()> {
        match self {
            DetectFileSystem::Local(fs) => fs.upload(input, file_ref).await,
            DetectFileSystem::GoogleCloudStorage(fs) => fs.upload(input, file_ref).await,
            DetectFileSystem::AwsS3(fs) => fs.upload(input, file_ref).await,
            DetectFileSystem::ZipFile(fs) => fs.upload(input, file_ref).await,
            #[cfg(feature = "clipboard")]
            DetectFileSystem::Clipboard(fs) => fs.upload(input, file_ref).await,
        }
    }

    async fn list_files(
        &mut self,
        file_matcher: Option<&FileMatcher>,
        max_files_limit: Option<usize>,
    ) -> AppResult<ListFilesResult> {
        match self {
            DetectFileSystem::Local(fs) => fs.list_files(file_matcher, max_files_limit).await,
            DetectFileSystem::GoogleCloudStorage(fs) => {
                fs.list_files(file_matcher, max_files_limit).await
            }
            DetectFileSystem::AwsS3(fs) => fs.list_files(file_matcher, max_files_limit).await,
            DetectFileSystem::ZipFile(fs) => fs.list_files(file_matcher, max_files_limit).await,
            #[cfg(feature = "clipboard")]
            DetectFileSystem::Clipboard(fs) => fs.list_files(file_matcher, max_files_limit).await,
        }
    }

    async fn close(self) -> AppResult<()> {
        match self {
            DetectFileSystem::Local(fs) => fs.close().await,
            DetectFileSystem::GoogleCloudStorage(fs) => fs.close().await,
            DetectFileSystem::AwsS3(fs) => fs.close().await,
            DetectFileSystem::ZipFile(fs) => fs.close().await,
            #[cfg(feature = "clipboard")]
            DetectFileSystem::Clipboard(fs) => fs.close().await,
        }
    }

    async fn has_multiple_files(&self) -> AppResult<bool> {
        match self {
            DetectFileSystem::Local(fs) => fs.has_multiple_files().await,
            DetectFileSystem::GoogleCloudStorage(fs) => fs.has_multiple_files().await,
            DetectFileSystem::AwsS3(fs) => fs.has_multiple_files().await,
            DetectFileSystem::ZipFile(fs) => fs.has_multiple_files().await,
            #[cfg(feature = "clipboard")]
            DetectFileSystem::Clipboard(fs) => fs.has_multiple_files().await,
        }
    }

    async fn accepts_multiple_files(&self) -> AppResult<bool> {
        match self {
            DetectFileSystem::Local(fs) => fs.accepts_multiple_files().await,
            DetectFileSystem::GoogleCloudStorage(fs) => fs.accepts_multiple_files().await,
            DetectFileSystem::AwsS3(fs) => fs.accepts_multiple_files().await,
            DetectFileSystem::ZipFile(fs) => fs.accepts_multiple_files().await,
            #[cfg(feature = "clipboard")]
            DetectFileSystem::Clipboard(fs) => fs.accepts_multiple_files().await,
        }
    }

    fn resolve(&self, file_ref: Option<&FileSystemRef>) -> AbsoluteFilePath {
        match self {
            DetectFileSystem::Local(fs) => fs.resolve(file_ref),
            DetectFileSystem::GoogleCloudStorage(fs) => fs.resolve(file_ref),
            DetectFileSystem::AwsS3(fs) => fs.resolve(file_ref),
            DetectFileSystem::ZipFile(fs) => fs.resolve(file_ref),
            #[cfg(feature = "clipboard")]
            DetectFileSystem::Clipboard(fs) => fs.resolve(file_ref),
        }
    }

    fn local_directory(&self) -> Option<&Path> {
        match self {
            DetectFileSystem::Local(fs) => fs.local_directory(),
            _ => None,
        }
    }
}
