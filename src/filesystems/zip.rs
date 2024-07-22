use crate::errors::AppError;
use crate::filesystems::local::LocalFileSystem;
use crate::filesystems::{
    AbsoluteFilePath, FileMatcher, FileSystemConnection, FileSystemRef, ListFilesResult,
};
use crate::reporter::AppReporter;
use crate::AppResult;
use futures::{Stream, TryStreamExt};
use gcloud_sdk::prost::bytes::Bytes;
use rvstruct::ValueStruct;
use std::io::Write;
use std::path::{Path, PathBuf};
use tempdir::TempDir;
use zip::*;

pub struct ZipFileSystem<'a> {
    zip_file_path: PathBuf,
    mode: Option<ZipFileSystemMode<'a>>,
    reporter: &'a AppReporter<'a>,
}

#[allow(clippy::large_enum_variant)]
enum ZipFileSystemMode<'a> {
    Read {
        _temp_dir: TempDir,
        temp_file_system: LocalFileSystem<'a>,
    },
    Write {
        zip_writer: ZipWriter<std::fs::File>,
    },
}

impl<'a> ZipFileSystem<'a> {
    pub async fn new(file_path: &str, reporter: &'a AppReporter<'a>) -> AppResult<Self> {
        let root_path_base_str = file_path.trim_start_matches("zip://").to_string();
        let root_path_path = PathBuf::from(&root_path_base_str);
        let is_dir = file_path.ends_with('/') || root_path_path.is_dir();
        if is_dir {
            return Err(AppError::SystemError {
                message: "ZipFileSystem does not support directories".into(),
            });
        }
        Ok(Self {
            zip_file_path: root_path_path,
            mode: None,
            reporter,
        })
    }

    async fn extract_zip_for_read(&mut self) -> Result<(), AppError> {
        if self.mode.is_none() {
            let file = std::fs::File::open(&self.zip_file_path)?;
            let mut archive = ZipArchive::new(file)?;
            let temp_dir = TempDir::new("redacter")?;
            archive.extract(temp_dir.path())?;
            let temp_dir_str = temp_dir.path().to_string_lossy();
            self.reporter
                .report(format!("Extracting files to temp dir: {}", temp_dir_str))?;
            let temp_file_system =
                LocalFileSystem::new(temp_dir_str.as_ref(), self.reporter).await?;
            self.mode = Some(ZipFileSystemMode::Read {
                _temp_dir: temp_dir,
                temp_file_system,
            });
        }
        Ok(())
    }
}

impl<'a> FileSystemConnection<'a> for ZipFileSystem<'a> {
    async fn download(
        &mut self,
        file_ref: Option<&FileSystemRef>,
    ) -> AppResult<(
        FileSystemRef,
        Box<dyn Stream<Item = AppResult<Bytes>> + Send + Sync + Unpin + 'static>,
    )> {
        self.extract_zip_for_read().await?;
        match self.mode {
            Some(ZipFileSystemMode::Read {
                _temp_dir: _,
                ref mut temp_file_system,
            }) => match file_ref {
                Some(file_ref) => temp_file_system.download(Some(file_ref)).await,
                None => Err(AppError::SystemError {
                    message: "FileSystemRef is required for ZipFileSystem".into(),
                }),
            },
            _ => Err(AppError::SystemError {
                message: "ZipFileSystem is not in read mode".into(),
            }),
        }
    }

    async fn upload<S: Stream<Item = AppResult<Bytes>> + Send + Unpin + Sync + 'static>(
        &mut self,
        mut input: S,
        file_ref: Option<&FileSystemRef>,
    ) -> AppResult<()> {
        if self.mode.is_none() {
            let zip_file = if self.zip_file_path.exists() {
                return Err(AppError::SystemError {
                    message: "Zip file already exists".into(),
                });
            } else {
                std::fs::File::create_new(&self.zip_file_path)?
            };

            let zip_writer = ZipWriter::new(zip_file);
            self.mode = Some(ZipFileSystemMode::Write { zip_writer });
        }
        match self.mode {
            Some(ZipFileSystemMode::Write { ref mut zip_writer }) => match file_ref {
                Some(file_ref) => {
                    let file_path = Path::new(file_ref.relative_path.value());
                    let file_path_str = file_path.to_string_lossy().to_string();
                    let file_options = zip::write::FullFileOptions::default();
                    zip_writer.start_file(file_path_str, file_options)?;
                    while let Some(chunk) = input.try_next().await? {
                        zip_writer.write_all(&chunk)?;
                    }
                    Ok(())
                }
                None => Err(AppError::SystemError {
                    message: "FileSystemRef is required for ZipFileSystem".into(),
                }),
            },
            _ => Err(AppError::SystemError {
                message: "ZipFileSystem is not in write mode".into(),
            }),
        }
    }

    async fn list_files(
        &mut self,
        file_matcher: Option<&FileMatcher>,
    ) -> AppResult<ListFilesResult> {
        self.extract_zip_for_read().await?;
        match self.mode {
            Some(ZipFileSystemMode::Read {
                _temp_dir: _,
                ref mut temp_file_system,
            }) => temp_file_system.list_files(file_matcher).await,
            _ => Err(AppError::SystemError {
                message: "ZipFileSystem is not in read mode".into(),
            }),
        }
    }

    async fn close(mut self) -> AppResult<()> {
        if let Some(ZipFileSystemMode::Write { zip_writer }) = self.mode {
            zip_writer.finish()?;
        }
        self.mode = None;
        Ok(())
    }

    async fn has_multiple_files(&self) -> AppResult<bool> {
        Ok(true)
    }

    async fn accepts_multiple_files(&self) -> AppResult<bool> {
        Ok(true)
    }

    fn resolve(&self, file_ref: Option<&FileSystemRef>) -> AbsoluteFilePath {
        AbsoluteFilePath {
            file_path: format!(
                "{}{}",
                self.zip_file_path.to_string_lossy(),
                file_ref
                    .map(|fr| format!(":{}", fr.relative_path.value()))
                    .unwrap_or("".to_string())
            ),
            scheme: "zip".to_string(),
        }
    }
}

mod tests {
    use super::*;
    use gcloud_sdk::prost::bytes;
    use std::io::Read;
    use tempdir::TempDir;

    #[tokio::test]
    async fn download_test() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let term = console::Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let temp_dir = TempDir::new("zip_file_system_tests_download")?;
        let temp_dir_path = temp_dir.path();
        let zip_file_path = temp_dir_path.join("test.zip");
        let mut zip = ZipWriter::new(std::fs::File::create(&zip_file_path)?);
        zip.start_file("file1.txt", zip::write::SimpleFileOptions::default())?;
        let test_content = b"test content";
        zip.write_all(test_content)?;
        zip.finish()?;

        let mut fs = ZipFileSystem::new(
            &format!("zip://{}", zip_file_path.to_string_lossy()),
            &reporter,
        )
        .await?;
        let (file_ref, stream) = fs
            .download(Some(&FileSystemRef {
                relative_path: "file1.txt".into(),
                media_type: None,
                file_size: None,
            }))
            .await?;
        let downloaded_bytes: Vec<bytes::Bytes> = stream.try_collect().await?;
        let flattened_bytes = downloaded_bytes.concat();
        let downloaded_content = std::str::from_utf8(&flattened_bytes)?;
        assert_eq!(downloaded_content, std::str::from_utf8(test_content)?);
        assert_eq!(file_ref.relative_path.value(), "file1.txt");
        assert_eq!(file_ref.media_type, Some(mime::TEXT_PLAIN));
        assert_eq!(file_ref.file_size, Some(test_content.len() as u64));

        fs.close().await?;

        Ok(())
    }

    #[tokio::test]
    async fn upload_test() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let term = console::Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let temp_dir = TempDir::new("zip_file_system_tests_upload")?;
        let temp_dir_path = temp_dir.path();
        let zip_file_path = temp_dir_path.join("test.zip");

        let mut fs = ZipFileSystem::new(
            &format!("zip://{}", zip_file_path.to_string_lossy()),
            &reporter,
        )
        .await?;

        let test_content = b"test content";
        let stream = futures::stream::iter(vec![Ok(bytes::Bytes::from(test_content.to_vec()))]);
        fs.upload(
            stream,
            Some(&FileSystemRef {
                relative_path: "file1.txt".into(),
                media_type: None,
                file_size: None,
            }),
        )
        .await?;

        fs.close().await?;

        let mut zip = ZipArchive::new(std::fs::File::open(&zip_file_path)?)?;
        let mut file = zip.by_index(0)?;
        let mut content = Vec::new();
        file.read_to_end(&mut content)?;
        assert_eq!(content, test_content);

        Ok(())
    }

    #[tokio::test]
    async fn list_files_test() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let term = console::Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let temp_dir = TempDir::new("zip_file_system_tests_list_files")?;
        let temp_dir_path = temp_dir.path();
        let zip_file_path = temp_dir_path.join("test.zip");
        let mut zip = ZipWriter::new(std::fs::File::create(&zip_file_path)?);
        zip.start_file("file1.txt", zip::write::SimpleFileOptions::default())?;
        zip.start_file("file2.txt", zip::write::SimpleFileOptions::default())?;
        zip.finish()?;

        let mut fs = ZipFileSystem::new(
            &format!("zip://{}", zip_file_path.to_string_lossy()),
            &reporter,
        )
        .await?;
        let list_files_result = fs.list_files(None).await?;
        assert_eq!(list_files_result.files.len(), 2);
        assert_eq!(list_files_result.skipped, 0);

        fs.close().await?;

        Ok(())
    }
}
