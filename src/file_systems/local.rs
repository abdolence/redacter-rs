use crate::errors::AppError;
use crate::file_systems::{AbsoluteFilePath, FileSystemConnection, FileSystemRef, ListFilesResult};
use crate::file_tools::FileMatcher;
use crate::reporter::AppReporter;
use crate::AppResult;
use futures::{Stream, TryStreamExt};
use gcloud_sdk::prost::bytes;
use rvstruct::ValueStruct;
use std::path::{Path, PathBuf};
use tempfile::TempPath;

/// The path a local source names. On Windows a `file://` URL puts a slash of its own before the
/// drive letter: `file:///C:/project` is `C:/project`. Elsewhere `/C:/project` is an absolute path.
fn local_path(source: &str) -> String {
    let path = source.trim_start_matches("file://");
    if cfg!(windows) {
        without_slash_before_drive_letter(path).to_string()
    } else {
        path.to_string()
    }
}

fn without_slash_before_drive_letter(path: &str) -> &str {
    match path.as_bytes() {
        [b'/', letter, b':', ..] if letter.is_ascii_alphabetic() => &path[1..],
        _ => path,
    }
}

pub struct LocalFileSystem<'a> {
    root_path: String,
    is_dir: bool,
    reporter: &'a AppReporter<'a>,
}

impl<'a> LocalFileSystem<'a> {
    pub async fn new(root_path: &str, reporter: &'a AppReporter<'a>) -> AppResult<Self> {
        let root_path_base_str = local_path(root_path);
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
        let mut listing = ListFilesResult::EMPTY;
        while let Some(entry) = entries.next_entry().await? {
            let file_type = entry.file_type().await?;
            if file_type.is_file() {
                let file_ref = FileSystemRef::with_guessed_media_type(
                    entry
                        .path()
                        .to_string_lossy()
                        .to_string()
                        .replace(self.root_path.as_str(), "")
                        .into(),
                    entry.metadata().await?.len() as usize,
                );
                listing.add(file_ref, *file_matcher);
            } else if file_type.is_dir() {
                let new_max_files_limit =
                    max_files_limit.map(|v| v.saturating_sub(listing.files.len()));
                let dir_files = self
                    .list_files_recursive(
                        entry.path().to_string_lossy().to_string(),
                        file_matcher,
                        new_max_files_limit,
                    )
                    .await?;
                listing.merge(dir_files);
            }

            if let Some(limit) = max_files_limit {
                if listing.files.len() >= limit {
                    break;
                }
            }
        }
        Ok(listing)
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
        // The name filter matches this path against the paths listings report, which are
        // relative to the source root, so a directory source keeps the listed path.
        let relative_file_path = match file_ref.filter(|_| self.is_dir) {
            Some(listed) => listed.relative_path.value().clone(),
            None => file_path
                .file_name()
                .ok_or_else(|| AppError::SystemError {
                    message: "Filename is empty".to_string(),
                })?
                .to_string_lossy()
                .to_string(),
        };
        let file_metadata = tokio::fs::metadata(&file_path).await?;
        let file_ref = FileSystemRef::with_guessed_media_type(
            relative_file_path.into(),
            file_metadata.len() as usize,
        );
        Ok((file_ref, Box::new(stream)))
    }

    async fn upload<S: Stream<Item = AppResult<bytes::Bytes>> + Send + Sync + Unpin + 'static>(
        &mut self,
        input: S,
        file_ref: Option<&FileSystemRef>,
    ) -> AppResult<()> {
        let requested_path = PathBuf::from(self.resolve(file_ref).file_path);

        if let Some(parent) = requested_path.parent() {
            if !parent.exists() {
                tokio::fs::create_dir_all(parent).await?;
            }
        }

        // The content is written beside the destination and renamed over it once complete, so a
        // failed copy leaves neither a partial file nor a truncated one it would have replaced.
        // A symlink at the destination is resolved first, because the rename would replace the
        // link itself and a plain write goes through it.
        let file_path = tokio::fs::canonicalize(&requested_path)
            .await
            .unwrap_or(requested_path);
        let file_name = file_path.file_name().ok_or_else(|| AppError::SystemError {
            message: format!("`{}` has no file name", file_path.to_string_lossy()),
        })?;
        let partial = TempPath::try_from_path(file_path.with_file_name(format!(
            ".{}.{:016x}.partial",
            file_name.to_string_lossy(),
            rand::random::<u64>()
        )))?;

        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&partial)
            .await?;
        if let Ok(replaced) = tokio::fs::metadata(&file_path).await {
            file.set_permissions(replaced.permissions()).await?;
        }
        let mut reader = tokio_util::io::StreamReader::new(input.map_err(std::io::Error::other));
        tokio::io::copy(&mut reader, &mut file).await?;
        drop(file);
        partial.persist(&file_path).map_err(std::io::Error::from)?;
        Ok(())
    }

    async fn list_files(
        &mut self,
        file_matcher: Option<&FileMatcher>,
        max_files_limit: Option<usize>,
    ) -> AppResult<ListFilesResult> {
        self.reporter
            .report_debug(format!("Listing files in dir: {}", self.root_path.as_str()));
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

    fn local_directory(&self) -> Option<&Path> {
        self.is_dir.then(|| Path::new(&self.root_path))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_systems::DetectFileSystem;
    use console::Term;

    #[test]
    fn a_file_url_with_a_drive_letter_names_the_drive_path() {
        assert_eq!(
            without_slash_before_drive_letter("/C:/project/"),
            "C:/project/"
        );
        assert_eq!(
            without_slash_before_drive_letter("/home/user/"),
            "/home/user/"
        );
        assert_eq!(
            without_slash_before_drive_letter("relative/dir"),
            "relative/dir"
        );
    }

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

    async fn open_directory<'a>(
        directory: &Path,
        reporter: &'a AppReporter<'a>,
    ) -> AppResult<LocalFileSystem<'a>> {
        LocalFileSystem::new(&directory.to_string_lossy(), reporter).await
    }

    fn file_named(relative_path: &str) -> FileSystemRef {
        FileSystemRef {
            relative_path: relative_path.into(),
            media_type: None,
            file_size: None,
        }
    }

    fn names_in(directory: &Path) -> Result<Vec<String>, std::io::Error> {
        let mut names = std::fs::read_dir(directory)?
            .map(|entry| entry.map(|entry| entry.file_name().to_string_lossy().into_owned()))
            .collect::<Result<Vec<_>, _>>()?;
        names.sort();
        Ok(names)
    }

    #[tokio::test]
    async fn a_failed_upload_leaves_no_file() -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    {
        let term = Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let temp_dir = tempfile::TempDir::with_prefix("local_file_system_tests_failed_upload")?;
        let mut fs = open_directory(temp_dir.path(), &reporter).await?;

        let result = fs
            .upload(
                crate::file_systems::stream_failing_after_first_chunk(),
                Some(&file_named("nested/temp_file.txt")),
            )
            .await;

        assert!(result.is_err());
        assert!(!temp_dir.path().join("nested/temp_file.txt").exists());
        assert_eq!(
            names_in(&temp_dir.path().join("nested"))?,
            Vec::<String>::new()
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_failed_upload_keeps_the_file_it_would_have_replaced(
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let term = Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let temp_dir = tempfile::TempDir::with_prefix("local_file_system_tests_keeps_existing")?;
        let existing = temp_dir.path().join("temp_file.txt");
        std::fs::write(&existing, "previous content")?;
        let mut fs = open_directory(temp_dir.path(), &reporter).await?;

        let result = fs
            .upload(
                crate::file_systems::stream_failing_after_first_chunk(),
                Some(&file_named("temp_file.txt")),
            )
            .await;

        assert!(result.is_err());
        assert_eq!(std::fs::read_to_string(&existing)?, "previous content");
        assert_eq!(names_in(temp_dir.path())?, vec!["temp_file.txt"]);
        Ok(())
    }

    #[tokio::test]
    async fn an_upload_replaces_an_existing_file(
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let term = Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let temp_dir = tempfile::TempDir::with_prefix("local_file_system_tests_replaces")?;
        let existing = temp_dir.path().join("temp_file.txt");
        std::fs::write(&existing, "previous content that is longer")?;
        let mut fs = open_directory(temp_dir.path(), &reporter).await?;

        fs.upload(
            futures::stream::iter(vec![Ok(bytes::Bytes::from("new content"))]),
            Some(&file_named("temp_file.txt")),
        )
        .await?;

        assert_eq!(std::fs::read_to_string(&existing)?, "new content");
        assert_eq!(names_in(temp_dir.path())?, vec!["temp_file.txt"]);
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_replaced_file_keeps_its_permissions_and_a_symlink_keeps_pointing_at_it(
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use std::os::unix::fs::PermissionsExt;
        let term = Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let temp_dir = tempfile::TempDir::with_prefix("local_file_system_tests_symlink")?;
        let target = temp_dir.path().join("target.txt");
        std::fs::write(&target, "previous content")?;
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o640))?;
        std::os::unix::fs::symlink(&target, temp_dir.path().join("link.txt"))?;
        let mut fs = open_directory(temp_dir.path(), &reporter).await?;

        fs.upload(
            futures::stream::iter(vec![Ok(bytes::Bytes::from("new content"))]),
            Some(&file_named("link.txt")),
        )
        .await?;

        assert!(std::fs::symlink_metadata(temp_dir.path().join("link.txt"))?
            .file_type()
            .is_symlink());
        assert_eq!(std::fs::read_to_string(&target)?, "new content");
        assert_eq!(
            std::fs::metadata(&target)?.permissions().mode() & 0o777,
            0o640
        );
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

    #[tokio::test]
    async fn list_sample_documents_fixtures_test(
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let term = Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);

        let mut fs = DetectFileSystem::open("test-fixtures/documents/", &reporter).await?;
        let list_files_result = fs.list_files(None, None).await?;

        let mut files_by_name: std::collections::HashMap<String, FileSystemRef> = list_files_result
            .files
            .into_iter()
            .map(|file_ref| (file_ref.relative_path.filename(), file_ref))
            .collect();
        assert_eq!(files_by_name.len(), 9);

        let expected_media_types = [
            ("customer-note.txt", mime::TEXT_PLAIN),
            ("multilingual.txt", mime::TEXT_PLAIN),
            ("customers.csv", mime::TEXT_CSV),
            ("customer.json", mime::APPLICATION_JSON),
            ("customer-profile.html", mime::TEXT_HTML),
            ("customer-form.pdf", mime::APPLICATION_PDF),
            ("dates-en.txt", mime::TEXT_PLAIN),
            ("false-positives-en.txt", mime::TEXT_PLAIN),
            ("contextual-en.txt", mime::TEXT_PLAIN),
        ];
        for (name, expected_media_type) in expected_media_types {
            let file_ref = files_by_name
                .remove(name)
                .unwrap_or_else(|| panic!("{name} should be listed"));
            assert_eq!(
                file_ref.media_type,
                Some(expected_media_type),
                "unexpected media type for {name}"
            );
            assert!(
                file_ref.file_size.unwrap_or(0) > 0,
                "{name} should have a non-zero size"
            );
        }

        fs.close().await?;

        Ok(())
    }

    #[tokio::test]
    async fn download_sample_note_fixture_test(
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let term = Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);

        let mut fs = DetectFileSystem::open("test-fixtures/documents/", &reporter).await?;
        let (file_ref, stream) = fs
            .download(Some(&FileSystemRef {
                relative_path: "customer-note.txt".into(),
                media_type: None,
                file_size: None,
            }))
            .await?;

        let downloaded_bytes: Vec<bytes::Bytes> = stream.try_collect().await?;
        let content = String::from_utf8(downloaded_bytes.concat())?;
        assert!(
            content.contains("john.smith@example.com"),
            "expected the note to carry the fake customer email"
        );
        assert_eq!(file_ref.media_type, Some(mime::TEXT_PLAIN));

        fs.close().await?;

        Ok(())
    }
}
