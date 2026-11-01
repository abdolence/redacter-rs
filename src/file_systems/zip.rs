use crate::errors::AppError;
use crate::file_systems::{
    AbsoluteFilePath, FileSystemConnection, FileSystemRef, ListFilesResult, RelativeFilePath,
};
use crate::file_tools::{FileMatcher, FileMatcherResult};
use crate::reporter::AppReporter;
use crate::AppResult;
use futures::{Stream, TryStreamExt};
use gcloud_sdk::prost::bytes::Bytes;
use rvstruct::ValueStruct;
use shared_file::SharedFile;
use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::io::{ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use zip::read::ZipFileEntry;
use zip::result::ZipError;
use zip::{ZipArchive, ZipWriter};

mod shared_file;

/// Size of the pieces an entry is decompressed into.
const DOWNLOAD_CHUNK_SIZE: usize = 64 * 1024;

/// Pieces buffered between the thread that decompresses or writes an entry and the async side,
/// so that a slow consumer holds the thread back instead of the entry piling up in memory.
const CHUNK_CHANNEL_DEPTH: usize = 4;

/// A zip archive read in place and written as a stream.
///
/// Reading never extracts. The central directory is read once, on first use, and each download
/// decompresses a single entry from a clone of the parsed archive, so downloads do not depend
/// on each other. Every blocking zip call runs on the blocking thread pool.
pub struct ZipFileSystem<'a> {
    zip_file_path: PathBuf,
    opened_archive: Option<OpenedArchive>,
    zip_writer: Option<ZipWriter<File>>,
    reporter: &'a AppReporter<'a>,
}

/// Why an archive entry is not offered to the caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SkipReason {
    /// The name is absolute, climbs out of the archive, or is not valid.
    UnsafePath,
    Symlink,
    Encrypted,
    /// An earlier entry has the same path once the name is normalised.
    DuplicatePath,
}

impl fmt::Display for SkipReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            SkipReason::UnsafePath => "its path is absolute, leaves the archive or is not valid",
            SkipReason::Symlink => "it is a symbolic link",
            SkipReason::Encrypted => "it is encrypted",
            SkipReason::DuplicatePath => "an earlier entry has the same path",
        })
    }
}

struct SkippedEntry {
    name: String,
    reason: SkipReason,
}

struct ReadableEntry {
    relative_path: RelativeFilePath,
    size: usize,
}

enum EntryKind {
    Directory,
    File(ReadableEntry),
    Skipped(SkippedEntry),
}

impl EntryKind {
    /// Classifies an entry from its central directory record alone, without reading its data.
    fn of(entry: &ZipFileEntry<'_>) -> Self {
        if entry.is_dir() {
            return EntryKind::Directory;
        }
        let skipped = |reason| {
            EntryKind::Skipped(SkippedEntry {
                name: String::from_utf8_lossy(entry.name_raw()).into_owned(),
                reason,
            })
        };
        let Some(relative_path) = entry
            .enclosed_name()
            .map(|path| RelativeFilePath::from(relative_path_string(&path)))
            .filter(|path| !path.value().is_empty())
        else {
            return skipped(SkipReason::UnsafePath);
        };
        if entry.is_symlink() {
            skipped(SkipReason::Symlink)
        } else if entry.encrypted() {
            skipped(SkipReason::Encrypted)
        } else {
            EntryKind::File(ReadableEntry {
                relative_path,
                // An entry larger than the address space is larger than any size limit.
                size: usize::try_from(entry.size()).unwrap_or(usize::MAX),
            })
        }
    }
}

fn relative_path_string(path: &Path) -> String {
    path.components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

/// What the central directory says about every entry, in archive order.
struct ZipIndex {
    entries: Vec<EntryKind>,
    /// Position of the first readable entry for each path.
    positions: HashMap<String, usize>,
    has_duplicate_names: bool,
}

struct ZipListing<'i> {
    result: ListFilesResult,
    skipped_entries: Vec<&'i SkippedEntry>,
}

impl ZipIndex {
    fn new(archive: &ZipArchive<SharedFile>) -> AppResult<Self> {
        let mut entries = Vec::with_capacity(archive.len());
        let mut positions = HashMap::new();
        for position in 0..archive.len() {
            let kind = match EntryKind::of(&archive.by_index_data(position)?) {
                EntryKind::File(readable)
                    if positions.contains_key(readable.relative_path.value()) =>
                {
                    EntryKind::Skipped(SkippedEntry {
                        name: readable.relative_path.value().clone(),
                        reason: SkipReason::DuplicatePath,
                    })
                }
                EntryKind::File(readable) => {
                    positions.insert(readable.relative_path.value().clone(), position);
                    EntryKind::File(readable)
                }
                other => other,
            };
            entries.push(kind);
        }
        Ok(Self {
            entries,
            positions,
            has_duplicate_names: archive.has_duplicate_names(),
        })
    }

    /// The entry that listing offers under `relative_path`, with its position in the archive.
    fn readable(&self, relative_path: &RelativeFilePath) -> Option<(usize, &ReadableEntry)> {
        let position = *self.positions.get(relative_path.value())?;
        match &self.entries[position] {
            EntryKind::File(readable) => Some((position, readable)),
            _ => None,
        }
    }

    fn list(
        &self,
        file_matcher: Option<&FileMatcher>,
        max_files_limit: Option<usize>,
    ) -> ZipListing<'_> {
        let mut listing = ZipListing {
            result: ListFilesResult::EMPTY,
            skipped_entries: Vec::new(),
        };
        if max_files_limit.iter().any(|limit| *limit == 0) {
            return listing;
        }
        for entry in &self.entries {
            match entry {
                EntryKind::Directory => {}
                EntryKind::Skipped(skipped) => {
                    listing.result.skipped += 1;
                    listing.skipped_entries.push(skipped);
                }
                EntryKind::File(readable) => {
                    let file_ref = FileSystemRef::with_guessed_media_type(
                        readable.relative_path.clone(),
                        readable.size,
                    );
                    if file_matcher.iter().all(|matcher| {
                        matches!(matcher.matches(&file_ref), FileMatcherResult::Matched)
                    }) {
                        listing.result.files.push(file_ref);
                    } else {
                        listing.result.skipped += 1;
                    }
                }
            }
            if max_files_limit.is_some_and(|limit| listing.result.files.len() >= limit) {
                break;
            }
        }
        listing
    }
}

/// A parsed archive. Clones are cheap and read independently of each other.
#[derive(Clone)]
struct OpenedArchive {
    archive: ZipArchive<SharedFile>,
    index: Arc<ZipIndex>,
}

impl OpenedArchive {
    fn open(zip_file_path: &Path) -> AppResult<Self> {
        let archive = ZipArchive::new(SharedFile::open(zip_file_path)?)?;
        let index = Arc::new(ZipIndex::new(&archive)?);
        Ok(Self { archive, index })
    }

    /// Runs on a blocking thread. Sends the content of the entry at `position`, and a failure
    /// in reading it as the last item.
    fn send_entry(mut self, position: usize, chunk_tx: &mpsc::Sender<AppResult<Bytes>>) {
        if let Err(error) = self.read_entry(position, chunk_tx) {
            let _ = chunk_tx.blocking_send(Err(error));
        }
    }

    fn read_entry(
        &mut self,
        position: usize,
        chunk_tx: &mpsc::Sender<AppResult<Bytes>>,
    ) -> AppResult<()> {
        let mut entry_reader = self.archive.by_index(position)?;
        let mut buffer = vec![0u8; DOWNLOAD_CHUNK_SIZE];
        loop {
            let read = match entry_reader.read(&mut buffer) {
                Ok(0) => return Ok(()),
                Ok(read) => read,
                Err(error) if error.kind() == ErrorKind::Interrupted => continue,
                Err(error) => return Err(error.into()),
            };
            if chunk_tx
                .blocking_send(Ok(Bytes::copy_from_slice(&buffer[..read])))
                .is_err()
            {
                return Ok(());
            }
        }
    }
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
            opened_archive: None,
            zip_writer: None,
            reporter,
        })
    }

    async fn opened_archive(&mut self) -> AppResult<OpenedArchive> {
        if let Some(opened_archive) = &self.opened_archive {
            return Ok(opened_archive.clone());
        }
        let zip_file_path = self.zip_file_path.clone();
        let opened_archive =
            tokio::task::spawn_blocking(move || OpenedArchive::open(&zip_file_path)).await??;
        self.opened_archive = Some(opened_archive.clone());
        Ok(opened_archive)
    }

    /// Ends with an error item if the thread behind `chunks` panicked, so that a stopped
    /// thread is never read as the end of the entry.
    fn chunk_stream(
        chunks: mpsc::Receiver<AppResult<Bytes>>,
        task: JoinHandle<()>,
    ) -> impl Stream<Item = AppResult<Bytes>> {
        futures::stream::unfold(Some((chunks, task)), |state| async move {
            let (mut chunks, task) = state?;
            match chunks.recv().await {
                Some(chunk) => Some((chunk, Some((chunks, task)))),
                None => task.await.err().map(|error| (Err(error.into()), None)),
            }
        })
    }

    fn write_entry(
        mut zip_writer: ZipWriter<File>,
        entry_name: String,
        mut chunks: mpsc::Receiver<Bytes>,
    ) -> (ZipWriter<File>, AppResult<()>) {
        let written = Self::write_chunks(&mut zip_writer, entry_name, &mut chunks);
        (zip_writer, written)
    }

    fn write_chunks(
        zip_writer: &mut ZipWriter<File>,
        entry_name: String,
        chunks: &mut mpsc::Receiver<Bytes>,
    ) -> AppResult<()> {
        zip_writer.start_file(entry_name, zip::write::FullFileOptions::default())?;
        while let Some(chunk) = chunks.blocking_recv() {
            zip_writer.write_all(&chunk)?;
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
        let Some(file_ref) = file_ref else {
            return Err(AppError::SystemError {
                message: "FileSystemRef is required for ZipFileSystem".into(),
            });
        };
        let opened_archive = self.opened_archive().await?;
        let Some((position, readable)) = opened_archive.index.readable(&file_ref.relative_path)
        else {
            return Err(ZipError::FileNotFound.into());
        };
        let downloaded_ref = FileSystemRef::with_guessed_media_type(
            readable.relative_path.filename().into(),
            readable.size,
        );
        let (chunk_tx, chunk_rx) = mpsc::channel(CHUNK_CHANNEL_DEPTH);
        let task = tokio::task::spawn_blocking(move || {
            opened_archive.send_entry(position, &chunk_tx);
        });
        Ok((
            downloaded_ref,
            Box::new(Box::pin(Self::chunk_stream(chunk_rx, task))),
        ))
    }

    async fn upload<S: Stream<Item = AppResult<Bytes>> + Send + Unpin + Sync + 'static>(
        &mut self,
        mut input: S,
        file_ref: Option<&FileSystemRef>,
    ) -> AppResult<()> {
        let Some(file_ref) = file_ref else {
            return Err(AppError::SystemError {
                message: "FileSystemRef is required for ZipFileSystem".into(),
            });
        };
        let zip_writer = match self.zip_writer.take() {
            Some(zip_writer) => zip_writer,
            None if self.zip_file_path.exists() => {
                return Err(AppError::SystemError {
                    message: "Zip file already exists".into(),
                });
            }
            None => ZipWriter::new(File::create_new(&self.zip_file_path)?),
        };
        let (chunk_tx, chunk_rx) = mpsc::channel(CHUNK_CHANNEL_DEPTH);
        let entry_name = file_ref.relative_path.value().clone();
        let task = tokio::task::spawn_blocking(move || {
            Self::write_entry(zip_writer, entry_name, chunk_rx)
        });
        let mut piped = Ok(());
        loop {
            match input.try_next().await {
                Ok(Some(chunk)) => {
                    if chunk_tx.send(chunk).await.is_err() {
                        break;
                    }
                }
                Ok(None) => break,
                Err(error) => {
                    piped = Err(error);
                    break;
                }
            }
        }
        drop(chunk_tx);
        let (zip_writer, written) = task.await?;
        self.zip_writer = Some(zip_writer);
        written?;
        piped
    }

    async fn list_files(
        &mut self,
        file_matcher: Option<&FileMatcher>,
        max_files_limit: Option<usize>,
    ) -> AppResult<ListFilesResult> {
        let opened_archive = self.opened_archive().await?;
        if opened_archive.index.has_duplicate_names {
            self.reporter.report(
                "The zip archive lists some entry names more than once; only the last entry of each name is read",
            )?;
        }
        let listing = opened_archive.index.list(file_matcher, max_files_limit);
        for skipped in &listing.skipped_entries {
            self.reporter.report(format!(
                "Skipping zip entry {:?}: {}",
                skipped.name, skipped.reason
            ))?;
        }
        Ok(listing.result)
    }

    async fn close(mut self) -> AppResult<()> {
        if let Some(zip_writer) = self.zip_writer.take() {
            tokio::task::spawn_blocking(move || zip_writer.finish()).await??;
        }
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gcloud_sdk::prost::bytes;
    use tempfile::TempDir;
    use zip::write::SimpleFileOptions;

    type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

    fn reference(relative_path: &str) -> FileSystemRef {
        FileSystemRef {
            relative_path: relative_path.into(),
            media_type: None,
            file_size: None,
        }
    }

    fn write_zip(
        zip_file_path: &Path,
        build: impl FnOnce(&mut ZipWriter<File>) -> zip::result::ZipResult<()>,
    ) -> TestResult {
        let mut writer = ZipWriter::new(File::create(zip_file_path)?);
        build(&mut writer)?;
        writer.finish()?;
        Ok(())
    }

    async fn open<'a>(
        zip_file_path: &Path,
        reporter: &'a AppReporter<'a>,
    ) -> AppResult<ZipFileSystem<'a>> {
        ZipFileSystem::new(
            &format!("zip://{}", zip_file_path.to_string_lossy()),
            reporter,
        )
        .await
    }

    async fn download_all(
        zip_file_system: &mut ZipFileSystem<'_>,
        relative_path: &str,
    ) -> AppResult<(FileSystemRef, Vec<u8>)> {
        let (file_ref, stream) = zip_file_system
            .download(Some(&reference(relative_path)))
            .await?;
        let chunks: Vec<bytes::Bytes> = stream.try_collect().await?;
        Ok((file_ref, chunks.concat()))
    }

    fn listed_paths(listing: &ListFilesResult) -> Vec<String> {
        listing
            .files
            .iter()
            .map(|file_ref| file_ref.relative_path.value().clone())
            .collect()
    }

    #[tokio::test]
    async fn download_test() -> TestResult {
        let term = console::Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let temp_dir = TempDir::with_prefix("zip_file_system_tests_download")?;
        let zip_file_path = temp_dir.path().join("test.zip");
        let test_content = b"test content";
        write_zip(&zip_file_path, |zip| {
            zip.start_file("file1.txt", SimpleFileOptions::default())?;
            zip.write_all(test_content)?;
            Ok(())
        })?;

        let mut fs = open(&zip_file_path, &reporter).await?;
        let (file_ref, content) = download_all(&mut fs, "file1.txt").await?;
        assert_eq!(content, test_content);
        assert_eq!(file_ref.relative_path.value(), "file1.txt");
        assert_eq!(file_ref.media_type, Some(mime::TEXT_PLAIN));
        assert_eq!(file_ref.file_size, Some(test_content.len()));

        fs.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn download_of_nested_entry_names_the_file_only() -> TestResult {
        let term = console::Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let temp_dir = TempDir::with_prefix("zip_file_system_tests_nested")?;
        let zip_file_path = temp_dir.path().join("test.zip");
        write_zip(&zip_file_path, |zip| {
            zip.start_file("docs/notes/file1.csv", SimpleFileOptions::default())?;
            zip.write_all(b"a,b")?;
            Ok(())
        })?;

        let mut fs = open(&zip_file_path, &reporter).await?;
        let listing = fs.list_files(None, None).await?;
        assert_eq!(listed_paths(&listing), ["docs/notes/file1.csv"]);

        let (file_ref, content) = download_all(&mut fs, "docs/notes/file1.csv").await?;
        assert_eq!(content, b"a,b");
        assert_eq!(file_ref.relative_path.value(), "file1.csv");
        assert_eq!(file_ref.media_type, Some(mime::TEXT_CSV));
        Ok(())
    }

    #[tokio::test]
    async fn download_of_unknown_entry_is_not_found() -> TestResult {
        let term = console::Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let temp_dir = TempDir::with_prefix("zip_file_system_tests_not_found")?;
        let zip_file_path = temp_dir.path().join("test.zip");
        write_zip(&zip_file_path, |zip| {
            zip.start_file("file1.txt", SimpleFileOptions::default())?;
            Ok(())
        })?;

        let mut fs = open(&zip_file_path, &reporter).await?;
        let error = download_all(&mut fs, "missing.txt").await.unwrap_err();
        assert!(matches!(error, AppError::ZipError(ZipError::FileNotFound)));
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_downloads_read_their_own_entry() -> TestResult {
        let term = console::Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let temp_dir = TempDir::with_prefix("zip_file_system_tests_concurrent")?;
        let zip_file_path = temp_dir.path().join("test.zip");
        let first_content: Vec<u8> = (0..2_000_000u32).map(|i| (i % 251) as u8).collect();
        let second_content: Vec<u8> = (0..2_000_000u32).map(|i| (i % 241) as u8).collect();
        write_zip(&zip_file_path, |zip| {
            let deflated =
                SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated);
            zip.start_file("first.bin", deflated)?;
            zip.write_all(&first_content)?;
            zip.start_file("second.bin", deflated)?;
            zip.write_all(&second_content)?;
            Ok(())
        })?;

        let mut fs = open(&zip_file_path, &reporter).await?;
        let (_, first_stream) = fs.download(Some(&reference("first.bin"))).await?;
        let (_, second_stream) = fs.download(Some(&reference("second.bin"))).await?;
        let (first, second): (Vec<bytes::Bytes>, Vec<bytes::Bytes>) = futures::future::try_join(
            first_stream.try_collect::<Vec<_>>(),
            second_stream.try_collect::<Vec<_>>(),
        )
        .await?;
        assert_eq!(first.concat(), first_content);
        assert_eq!(second.concat(), second_content);
        assert!(first.iter().all(|chunk| chunk.len() <= DOWNLOAD_CHUNK_SIZE));
        Ok(())
    }

    #[tokio::test]
    async fn list_files_test() -> TestResult {
        let term = console::Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let temp_dir = TempDir::with_prefix("zip_file_system_tests_list_files")?;
        let zip_file_path = temp_dir.path().join("test.zip");
        write_zip(&zip_file_path, |zip| {
            zip.start_file("file1.txt", SimpleFileOptions::default())?;
            zip.start_file("file2.txt", SimpleFileOptions::default())?;
            Ok(())
        })?;

        let mut fs = open(&zip_file_path, &reporter).await?;
        let list_files_result = fs.list_files(None, None).await?;
        assert_eq!(list_files_result.files.len(), 2);
        assert_eq!(list_files_result.skipped, 0);

        fs.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn directory_entries_are_not_listed() -> TestResult {
        let term = console::Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let temp_dir = TempDir::with_prefix("zip_file_system_tests_directories")?;
        let zip_file_path = temp_dir.path().join("test.zip");
        write_zip(&zip_file_path, |zip| {
            zip.add_directory("docs/", SimpleFileOptions::default())?;
            zip.start_file("docs/file1.txt", SimpleFileOptions::default())?;
            Ok(())
        })?;

        let mut fs = open(&zip_file_path, &reporter).await?;
        let listing = fs.list_files(None, None).await?;
        assert_eq!(listed_paths(&listing), ["docs/file1.txt"]);
        assert_eq!(listing.skipped, 0);
        Ok(())
    }

    #[tokio::test]
    async fn list_files_applies_matcher_and_limit() -> TestResult {
        let term = console::Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let temp_dir = TempDir::with_prefix("zip_file_system_tests_matcher")?;
        let zip_file_path = temp_dir.path().join("test.zip");
        write_zip(&zip_file_path, |zip| {
            zip.start_file("a.txt", SimpleFileOptions::default())?;
            zip.write_all(b"short")?;
            zip.start_file("b.csv", SimpleFileOptions::default())?;
            zip.write_all(b"short")?;
            zip.start_file("c.txt", SimpleFileOptions::default())?;
            zip.write_all(b"a much longer content")?;
            zip.start_file("d.txt", SimpleFileOptions::default())?;
            zip.write_all(b"short")?;
            Ok(())
        })?;
        let text_files_up_to_five_bytes = FileMatcher::new(
            Some(globset::Glob::new("*.txt")?.compile_matcher()),
            Some(5),
        );

        let mut fs = open(&zip_file_path, &reporter).await?;
        let matched = fs
            .list_files(Some(&text_files_up_to_five_bytes), None)
            .await?;
        assert_eq!(listed_paths(&matched), ["a.txt", "d.txt"]);
        assert_eq!(matched.skipped, 2);

        let limited = fs.list_files(None, Some(2)).await?;
        assert_eq!(listed_paths(&limited), ["a.txt", "b.csv"]);

        let none = fs.list_files(None, Some(0)).await?;
        assert!(none.files.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn filtered_out_entries_are_never_decompressed() -> TestResult {
        let term = console::Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let temp_dir = TempDir::with_prefix("zip_file_system_tests_corrupt")?;
        let zip_file_path = temp_dir.path().join("test.zip");
        let corrupt_data = b"CORRUPT-DATA-CORRUPT-DATA-CORRUPT-DATA";
        write_zip(&zip_file_path, |zip| {
            zip.start_file("good.txt", SimpleFileOptions::default())?;
            zip.write_all(b"good")?;
            zip.start_file(
                "bad.bin",
                SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored),
            )?;
            zip.write_all(corrupt_data)?;
            Ok(())
        })?;
        let mut archive_bytes = std::fs::read(&zip_file_path)?;
        let data_start = archive_bytes
            .windows(corrupt_data.len())
            .position(|window| window == corrupt_data)
            .expect("the stored entry data is in the archive");
        archive_bytes[data_start] ^= 0xff;
        std::fs::write(&zip_file_path, archive_bytes)?;
        let text_files =
            FileMatcher::new(Some(globset::Glob::new("*.txt")?.compile_matcher()), None);

        let mut fs = open(&zip_file_path, &reporter).await?;
        let listing = fs.list_files(Some(&text_files), None).await?;
        assert_eq!(listed_paths(&listing), ["good.txt"]);
        assert_eq!(listing.skipped, 1);
        assert_eq!(download_all(&mut fs, "good.txt").await?.1, b"good");

        assert!(download_all(&mut fs, "bad.bin").await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn entries_that_would_leave_the_archive_are_skipped() -> TestResult {
        let term = console::Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let temp_dir = TempDir::with_prefix("zip_file_system_tests_unsafe")?;
        let outside_file_path = temp_dir.path().join("outside-secret.txt");
        std::fs::write(&outside_file_path, "OUTSIDE-MARKER")?;
        let zip_file_path = temp_dir.path().join("evil.zip");
        write_zip(&zip_file_path, |zip| {
            let options = SimpleFileOptions::default();
            zip.start_file("ok.txt", options)?;
            zip.write_all(b"inside")?;
            zip.add_symlink(
                "link-absolute.txt",
                outside_file_path.to_string_lossy(),
                options,
            )?;
            zip.add_symlink("link-relative.txt", "../outside-secret.txt", options)?;
            zip.add_symlink("link-inside.txt", "ok.txt", options)?;
            zip.start_file("../escape.txt", options)?;
            zip.write_all(b"escaped")?;
            Ok(())
        })?;

        let mut fs = open(&zip_file_path, &reporter).await?;
        let listing = fs.list_files(None, None).await?;
        assert_eq!(listed_paths(&listing), ["ok.txt"]);
        assert_eq!(listing.skipped, 4);
        assert_eq!(download_all(&mut fs, "ok.txt").await?.1, b"inside");

        for skipped in [
            "link-absolute.txt",
            "link-relative.txt",
            "link-inside.txt",
            "../escape.txt",
        ] {
            assert!(download_all(&mut fs, skipped).await.is_err());
        }
        Ok(())
    }

    #[tokio::test]
    async fn duplicate_entry_names_are_read_once() -> TestResult {
        let term = console::Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let temp_dir = TempDir::with_prefix("zip_file_system_tests_duplicates")?;
        let zip_file_path = temp_dir.path().join("test.zip");
        write_zip(&zip_file_path, |zip| {
            zip.start_file("a.txt", SimpleFileOptions::default())?;
            zip.write_all(b"first")?;
            zip.start_file("b.txt", SimpleFileOptions::default())?;
            zip.write_all(b"second")?;
            zip.start_file("./a.txt", SimpleFileOptions::default())?;
            zip.write_all(b"third")?;
            Ok(())
        })?;
        // Both headers of the second entry get the first entry's name, which the writer refuses.
        let archive_bytes = std::fs::read(&zip_file_path)?;
        let mut patched = archive_bytes.clone();
        for (offset, window) in archive_bytes.windows(5).enumerate() {
            if window == b"b.txt" {
                patched[offset..offset + 5].copy_from_slice(b"a.txt");
            }
        }
        std::fs::write(&zip_file_path, patched)?;

        let mut fs = open(&zip_file_path, &reporter).await?;
        let listing = fs.list_files(None, None).await?;
        assert_eq!(listed_paths(&listing), ["a.txt"]);
        assert_eq!(listing.skipped, 1);
        assert_eq!(download_all(&mut fs, "a.txt").await?.1, b"second");
        Ok(())
    }

    #[tokio::test]
    async fn encrypted_entries_are_skipped() -> TestResult {
        let term = console::Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let temp_dir = TempDir::with_prefix("zip_file_system_tests_encrypted")?;
        let zip_file_path = temp_dir.path().join("test.zip");
        write_zip(&zip_file_path, |zip| {
            zip.start_file("open.txt", SimpleFileOptions::default())?;
            zip.write_all(b"open")?;
            zip.start_file(
                "locked.txt",
                SimpleFileOptions::default().with_aes_encryption(zip::AesMode::Aes256, "password"),
            )?;
            zip.write_all(b"locked")?;
            Ok(())
        })?;

        let mut fs = open(&zip_file_path, &reporter).await?;
        let listing = fs.list_files(None, None).await?;
        assert_eq!(listed_paths(&listing), ["open.txt"]);
        assert_eq!(listing.skipped, 1);

        assert!(download_all(&mut fs, "locked.txt").await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn upload_test() -> TestResult {
        let term = console::Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let temp_dir = TempDir::with_prefix("zip_file_system_tests_upload")?;
        let zip_file_path = temp_dir.path().join("test.zip");

        let mut fs = open(&zip_file_path, &reporter).await?;
        let test_content = b"test content";
        let stream = futures::stream::iter(vec![Ok(bytes::Bytes::from(test_content.to_vec()))]);
        fs.upload(stream, Some(&reference("file1.txt"))).await?;
        fs.close().await?;

        let mut zip = ZipArchive::new(File::open(&zip_file_path)?)?;
        let mut file = zip.by_index(0)?;
        let mut content = Vec::new();
        file.read_to_end(&mut content)?;
        assert_eq!(content, test_content);
        Ok(())
    }

    #[tokio::test]
    async fn upload_writes_every_entry_in_chunks() -> TestResult {
        let term = console::Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let temp_dir = TempDir::with_prefix("zip_file_system_tests_upload_chunks")?;
        let zip_file_path = temp_dir.path().join("test.zip");

        let mut fs = open(&zip_file_path, &reporter).await?;
        for (name, parts) in [
            ("one.txt", ["a", "b", "c"]),
            ("dir/two.txt", ["d", "e", "f"]),
        ] {
            let stream = futures::stream::iter(
                parts
                    .iter()
                    .map(|part| Ok(bytes::Bytes::from(part.to_string())))
                    .collect::<Vec<_>>(),
            );
            fs.upload(stream, Some(&reference(name))).await?;
        }
        fs.close().await?;

        let mut fs = open(&zip_file_path, &reporter).await?;
        assert_eq!(download_all(&mut fs, "one.txt").await?.1, b"abc");
        assert_eq!(download_all(&mut fs, "dir/two.txt").await?.1, b"def");
        Ok(())
    }

    #[tokio::test]
    async fn upload_to_existing_zip_is_refused() -> TestResult {
        let term = console::Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let temp_dir = TempDir::with_prefix("zip_file_system_tests_upload_existing")?;
        let zip_file_path = temp_dir.path().join("test.zip");
        std::fs::write(&zip_file_path, "existing")?;

        let mut fs = open(&zip_file_path, &reporter).await?;
        let stream = futures::stream::iter(Vec::<AppResult<bytes::Bytes>>::new());
        assert!(fs
            .upload(stream, Some(&reference("file1.txt")))
            .await
            .is_err());
        assert_eq!(std::fs::read(&zip_file_path)?, b"existing");
        Ok(())
    }

    #[tokio::test]
    async fn list_sample_documents_fixtures_test() -> TestResult {
        let term = console::Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let temp_dir = TempDir::with_prefix("zip_file_system_tests_sample_fixtures")?;
        let zip_file_path = temp_dir.path().join("documents.zip");

        let fixture_names = [
            "customer-note.txt",
            "customers.csv",
            "customer.json",
            "customer-profile.html",
            "customer-form.pdf",
        ];
        write_zip(&zip_file_path, |zip| {
            for name in fixture_names {
                let content = std::fs::read(Path::new("test-fixtures/documents").join(name))?;
                zip.start_file(name, SimpleFileOptions::default())?;
                zip.write_all(&content)?;
            }
            Ok(())
        })?;

        let mut fs = open(&zip_file_path, &reporter).await?;
        let list_files_result = fs.list_files(None, None).await?;
        let mut listed_names: Vec<String> = list_files_result
            .files
            .iter()
            .map(|file_ref| file_ref.relative_path.filename())
            .collect();
        listed_names.sort();
        let mut expected_names: Vec<String> =
            fixture_names.iter().map(|name| name.to_string()).collect();
        expected_names.sort();
        assert_eq!(listed_names, expected_names);

        fs.close().await?;
        Ok(())
    }
}
