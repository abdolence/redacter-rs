use crate::errors::AppError;
use crate::file_systems::{AbsoluteFilePath, FileSystemConnection, FileSystemRef, ListFilesResult};
use crate::file_tools::{FileMatcher, FileMatcherResult};
use crate::reporter::AppReporter;
use crate::AppResult;
use futures::{Stream, TryStreamExt};
use gcloud_sdk::prost::bytes;
use rvstruct::ValueStruct;
use std::path::PathBuf;
use tokio::fs::File;

pub struct LocalFileSystem<'a> {
    root_path: String,
    is_dir: bool,
    reporter: &'a AppReporter<'a>,
}

impl<'a> LocalFileSystem<'a> {
    pub async fn new(root_path: &str, reporter: &'a AppReporter<'a>) -> AppResult<Self> {
        let root_path_base_str = root_path.trim_start_matches("file://").to_string();
        let root_path_path = PathBuf::from(&root_path_base_str);
        let is_dir = root_path.ends_with('/') || root_path_path.is_dir();
        let root_path_str = if is_dir && !root_path_base_str.ends_with('/') {
            format!("{root_path_base_str}/")
        } else {
            root_path_base_str
        };
        Ok(LocalFileSystem {
            root_path: root_path_str,
            is_dir,
            reporter,
        })
    }

    #[async_recursion::async_recursion]
    pub async fn list_files_recursive(
        &self,
        dir_path: String,
        file_matcher: &Option<&FileMatcher>,
        max_files_limit: Option<usize>,
    ) -> AppResult<ListFilesResult> {
        if max_files_limit.iter().any(|v| *v == 0) {
            return Ok(ListFilesResult::EMPTY);
        }

        let mut entries = tokio::fs::read_dir(dir_path).await?;
        let mut files = Vec::new();
        let mut skipped: usize = 0;
        while let Some(entry) = entries.next_entry().await? {
            let file_type = entry.file_type().await?;
            if file_type.is_file() {
                let file_ref = FileSystemRef {
                    relative_path: entry
                        .path()
                        .to_string_lossy()
                        .to_string()
                        .replace(self.root_path.as_str(), "")
                        .into(),
                    media_type: mime_guess::from_path(entry.path()).first(),
                    file_size: Some(entry.metadata().await?.len() as usize),
                };
                if file_matcher
                    .iter()
                    .all(|matcher| matches!(matcher.matches(&file_ref), FileMatcherResult::Matched))
                {
                    files.push(file_ref);
                } else {
                    skipped += 1;
                }
            } else if file_type.is_dir() {
                let new_max_files_limit = max_files_limit.map(|v| v.saturating_sub(files.len()));
                let dir_files = self
                    .list_files_recursive(
                        entry.path().to_string_lossy().to_string(),
                        file_matcher,
                        new_max_files_limit,
                    )
                    .await?;
                skipped += dir_files.skipped;
                files.extend(dir_files.files);
            }

            if let Some(limit) = max_files_limit {
                if files.len() >= limit {
                    break;
                }
            }
        }
        Ok(ListFilesResult { files, skipped })
    }
}

impl<'a> FileSystemConnection<'a> for LocalFileSystem<'a> {
    async fn download(
        &mut self,
        file_ref: Option<&FileSystemRef>,
    ) -> AppResult<(
        FileSystemRef,
        Box<dyn Stream<Item = AppResult<bytes::Bytes>> + Send + Sync + Unpin + 'static>,
    )> {
        use futures::TryStreamExt;
        let file_path = PathBuf::from(self.resolve(file_ref).file_path);

        let file = tokio::fs::File::open(&file_path).await?;
        let stream = tokio_util::io::ReaderStream::new(file).map_err(AppError::from);
        let relative_file_path = file_path
            .file_name()
            .ok_or_else(|| AppError::SystemError {
                message: "Filename is empty".to_string(),
            })?
            .to_string_lossy()
            .to_string();
        let file_metadata = tokio::fs::metadata(&file_path).await?;
        let file_ref = FileSystemRef {
            relative_path: relative_file_path.into(),
            media_type: mime_guess::from_path(&file_path).first(),
            file_size: Some(file_metadata.len() as usize),
        };
        Ok((file_ref, Box::new(stream)))
    }

    async fn upload<S: Stream<Item = AppResult<bytes::Bytes>> + Send + Sync + Unpin + 'static>(
        &mut self,
        input: S,
        file_ref: Option<&FileSystemRef>,
    ) -> AppResult<()> {
        let file_path = PathBuf::from(self.resolve(file_ref).file_path);

        if let Some(parent) = file_path.parent() {
            if !parent.exists() {
                tokio::fs::create_dir_all(parent).await?;
            }
        }

        let mut file = File::create(file_path).await?;
        let mut reader = tokio_util::io::StreamReader::new(input.map_err(std::io::Error::other));
        tokio::io::copy(&mut reader, &mut file).await?;
        Ok(())
    }

    async fn list_files(
        &mut self,
        file_matcher: Option<&FileMatcher>,
        max_files_limit: Option<usize>,
    ) -> AppResult<ListFilesResult> {
        self.reporter
            .report(format!("Listing files in dir: {}", self.root_path.as_str()))?;
        let source = PathBuf::from(self.root_path.as_str());
        let source_str = source.to_string_lossy().to_string();
        self.list_files_recursive(source_str.clone(), &file_matcher, max_files_limit)
            .await
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
                format!(
                    "{}{}",
                    self.root_path,
                    file_ref
                        .map(|fr| fr.relative_path.value().clone())
                        .unwrap_or("".to_string())
                )
            } else {
                self.root_path.clone()
            },
        }
    }
}

#[allow(unused_imports)]
mod tests {
    use super::*;
    use crate::file_systems::DetectFileSystem;
    use console::Term;

    #[tokio::test]
    async fn download_test() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let term = Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let temp_dir = tempfile::TempDir::with_prefix("local_file_system_tests_download")?;
        let temp_dir_path = temp_dir.path();

        let fs = DetectFileSystem::open(
            &format!("file://{}", temp_dir_path.to_string_lossy()),
            &reporter,
        )
        .await?;
        // Create a temp file in the temp dir
        let temp_file = temp_dir_path.join("temp_file.txt");
        let temp_content = "test content";
        tokio::fs::write(&temp_file, temp_content).await?;

        let mut fs = fs;
        let (file_ref, stream) = fs
            .download(Some(&FileSystemRef {
                relative_path: "temp_file.txt".into(),
                media_type: None,
                file_size: None,
            }))
            .await?;

        let downloaded_bytes: Vec<bytes::Bytes> = stream.try_collect().await?;
        let flattened_bytes = downloaded_bytes.concat();
        let downloaded_content = std::str::from_utf8(&flattened_bytes)?;
        assert_eq!(downloaded_content, temp_content);
        assert_eq!(file_ref.relative_path.value(), "temp_file.txt");
        assert_eq!(file_ref.media_type, Some(mime::TEXT_PLAIN));
        assert_eq!(file_ref.file_size, Some(temp_content.len()));

        fs.close().await?;

        Ok(())
    }

    #[tokio::test]
    async fn upload_test() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let term = Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let temp_dir = tempfile::TempDir::with_prefix("local_file_system_tests_upload")?;
        let temp_dir_path = temp_dir.path();

        let fs = DetectFileSystem::open(
            &format!("file://{}", temp_dir_path.to_string_lossy()),
            &reporter,
        )
        .await?;

        let mut fs = fs;
        let content = "test content";
        let stream = futures::stream::iter(vec![Ok(bytes::Bytes::from(content))]);
        fs.upload(
            stream,
            Some(&FileSystemRef {
                relative_path: "temp_file.txt".into(),
                media_type: None,
                file_size: None,
            }),
        )
        .await?;

        let temp_file = temp_dir_path.join("temp_file.txt");
        let file_content = tokio::fs::read_to_string(&temp_file).await?;
        assert_eq!(file_content, content);

        fs.close().await?;

        Ok(())
    }

    #[tokio::test]
    async fn list_test() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let term = Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let temp_dir = tempfile::TempDir::with_prefix("local_file_system_tests_list")?;
        let temp_dir_path = temp_dir.path();

        let fs = DetectFileSystem::open(
            &format!("file://{}", temp_dir_path.to_string_lossy()),
            &reporter,
        )
        .await?;

        let mut fs = fs;
        let content = "test content";
        let stream = futures::stream::iter(vec![Ok(bytes::Bytes::from(content))]);
        fs.upload(
            stream,
            Some(&FileSystemRef {
                relative_path: "temp_file.txt".into(),
                media_type: None,
                file_size: None,
            }),
        )
        .await?;

        let list_files_result = fs.list_files(None, None).await?;
        assert_eq!(list_files_result.files.len(), 1);
        assert_eq!(
            list_files_result.files[0].relative_path.value(),
            "temp_file.txt"
        );

        fs.close().await?;

        Ok(())
    }
}
