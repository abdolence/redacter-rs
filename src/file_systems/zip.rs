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
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use zip::read::ZipFileEntry;
use zip::result::ZipError;
use zip::{ZipArchive, ZipWriter};

const DOWNLOAD_CHUNK_SIZE: usize = 64 * 1024;

/// Pieces buffered between the thread that decompresses an entry and the async side, so that a
/// slow consumer holds the thread back instead of the entry piling up in memory.
const CHUNK_CHANNEL_DEPTH: usize = 4;

pub struct ZipFileSystem<'a> {
    zip_file_path: PathBuf,
    mode: Option<ZipFileSystemMode>,
    reporter: &'a AppReporter<'a>,
}

enum ZipFileSystemMode {
    Read(OpenedArchive),
    Write(ArchiveWriter),
    /// The archive was removed after a failure; nothing more is written to it.
    Aborted,
}

/// The archive being written, with how many entries it holds. An archive with no entry left
/// cannot be finished: the bytes of an entry taken back out stay in the file, and a footer
/// written after them leaves a file that readers reject.
struct ArchiveWriter {
    zip: ZipWriter<File>,
    entries: usize,
}

/// Why an entry could not be written.
enum EntryFailure {
    /// The content did not arrive, and the entry was taken out of the archive's entries.
    Content(AppError),
    /// The archive itself can no longer be trusted.
    Archive(AppError),
}

impl From<ZipError> for EntryFailure {
    fn from(error: ZipError) -> Self {
        Self::Archive(error.into())
    }
}

impl From<io::Error> for EntryFailure {
    fn from(error: io::Error) -> Self {
        Self::Archive(error.into())
    }
}

struct SkippedEntry {
    name: String,
    reason: &'static str,
    /// The entry as a listing would show it, when its path is valid. The ignore rules apply to
    /// it, so that an entry the user ignored is counted as ignored and never reported as skipped.
    listed_as: Option<ReadableEntry>,
}

impl SkippedEntry {
    fn new(
        entry: &ZipFileEntry<'_>,
        reason: &'static str,
        listed_as: Option<ReadableEntry>,
    ) -> Self {
        Self {
            name: String::from_utf8_lossy(entry.name_raw()).into_owned(),
            reason,
            listed_as,
        }
    }

    fn is_ignored_by(&self, file_matcher: Option<&FileMatcher>) -> bool {
        match (&self.listed_as, file_matcher) {
            (Some(listed_as), Some(file_matcher)) => {
                file_matcher.matches(&listed_as.file_ref()) == FileMatcherResult::SkippedDueToIgnore
            }
            _ => false,
        }
    }
}

#[derive(Clone)]
struct ReadableEntry {
    relative_path: RelativeFilePath,
    size: usize,
}

impl ReadableEntry {
    fn file_ref(&self) -> FileSystemRef {
        FileSystemRef::with_guessed_media_type(self.relative_path.clone(), self.size)
    }
}

enum IndexedEntry {
    Directory,
    Readable(ReadableEntry),
    Skipped(SkippedEntry),
}

impl From<&ZipFileEntry<'_>> for IndexedEntry {
    /// Classifies an entry from its central directory record alone, without reading its data.
    fn from(entry: &ZipFileEntry<'_>) -> Self {
        if entry.is_dir() {
            return IndexedEntry::Directory;
        }
        let relative_path = entry
            .enclosed_name()
            .map(|path| {
                path.components()
                    .map(|component| component.as_os_str().to_string_lossy())
                    .collect::<Vec<_>>()
                    .join("/")
            })
            .filter(|path| !path.is_empty());
        let Some(relative_path) = relative_path else {
            return IndexedEntry::Skipped(SkippedEntry::new(
                entry,
                "its path leaves the archive or is not valid",
                None,
            ));
        };
        let readable = ReadableEntry {
            relative_path: relative_path.into(),
            // An entry larger than the address space is larger than any size limit.
            size: usize::try_from(entry.size()).unwrap_or(usize::MAX),
        };
        let unreadable = if entry.is_symlink() {
            Some("it is a symbolic link")
        } else if entry.encrypted() {
            Some("it is encrypted")
        } else {
            None
        };
        match unreadable {
            Some(reason) => IndexedEntry::Skipped(SkippedEntry::new(entry, reason, Some(readable))),
            None => IndexedEntry::Readable(readable),
        }
    }
}

/// What the central directory says about every entry, in archive order.
struct ZipIndex {
    entries: Vec<IndexedEntry>,
    /// Position of the first readable entry for each path.
    positions: HashMap<String, usize>,
}

impl ZipIndex {
    fn new(archive: &ZipArchive<SharedFile>) -> AppResult<Self> {
        let mut entries = Vec::with_capacity(archive.len());
        let mut positions = HashMap::new();
        for position in 0..archive.len() {
            let zip_entry = archive.by_index_data(position)?;
            let entry = match IndexedEntry::from(&zip_entry) {
                IndexedEntry::Readable(readable) => {
                    match positions.entry(readable.relative_path.value().clone()) {
                        Entry::Occupied(_) => IndexedEntry::Skipped(SkippedEntry::new(
                            &zip_entry,
                            "an earlier entry has the same path",
                            Some(readable),
                        )),
                        Entry::Vacant(vacant) => {
                            vacant.insert(position);
                            IndexedEntry::Readable(readable)
                        }
                    }
                }
                other => other,
            };
            entries.push(entry);
        }
        Ok(Self { entries, positions })
    }

    fn readable(&self, relative_path: &RelativeFilePath) -> Option<(usize, &ReadableEntry)> {
        let position = *self.positions.get(relative_path.value())?;
        match &self.entries[position] {
            IndexedEntry::Readable(readable) => Some((position, readable)),
            _ => None,
        }
    }
}

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

    /// Runs on a blocking thread. A failure in reading the entry is sent as the last item.
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

/// A read-only view of an open file that has a cursor of its own.
///
/// Clones share the file handle and read at explicit offsets, so reading through one clone
/// never moves another. That is what lets a parsed `ZipArchive` be cloned for each concurrent
/// download instead of parsing the central directory again.
///
/// On Windows `seek_read` also moves the cursor of the shared handle. Nothing reads through that
/// cursor and every read passes its own offset, so clones still do not affect each other.
#[derive(Clone)]
struct SharedFile {
    file: Arc<File>,
    length: u64,
    position: u64,
}

impl SharedFile {
    fn open(path: &Path) -> io::Result<Self> {
        let file = File::open(path)?;
        let length = file.metadata()?.len();
        Ok(Self {
            file: Arc::new(file),
            length,
            position: 0,
        })
    }
}

impl Read for SharedFile {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        #[cfg(unix)]
        let read = std::os::unix::fs::FileExt::read_at(&*self.file, buffer, self.position)?;
        #[cfg(windows)]
        let read = std::os::windows::fs::FileExt::seek_read(&*self.file, buffer, self.position)?;
        self.position += read as u64;
        Ok(read)
    }
}

impl Seek for SharedFile {
    fn seek(&mut self, target: SeekFrom) -> io::Result<u64> {
        let position = match target {
            SeekFrom::Start(offset) => Some(offset),
            SeekFrom::End(offset) => self.length.checked_add_signed(offset),
            SeekFrom::Current(offset) => self.position.checked_add_signed(offset),
        };
        self.position = position.ok_or_else(|| {
            io::Error::new(
                ErrorKind::InvalidInput,
                "seek to a position before the start of the file",
            )
        })?;
        Ok(self.position)
    }
}

impl Drop for ZipFileSystem<'_> {
    /// A copy that fails before `close` drops the archive unfinished.
    fn drop(&mut self) {
        self.abort_archive();
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
            mode: None,
            reporter,
        })
    }

    /// Removes the archive being written, when the writer itself has failed: a finished archive
    /// with a cut-off entry looks like a complete copy, so nothing is left. A no-op unless this
    /// system created the archive and has not closed it.
    fn abort_archive(&mut self) {
        if matches!(self.mode, Some(ZipFileSystemMode::Write(_))) {
            // Dropping the writer closes the file, which has to happen before it is removed.
            self.mode = Some(ZipFileSystemMode::Aborted);
            self.remove_archive_file();
        }
    }

    fn remove_archive_file(&self) {
        if let Err(error) = std::fs::remove_file(&self.zip_file_path) {
            tracing::warn!(
                path = %self.zip_file_path.display(),
                %error,
                "could not remove the incomplete zip archive"
            );
        }
    }

    async fn opened_archive(&mut self) -> AppResult<OpenedArchive> {
        match &self.mode {
            Some(ZipFileSystemMode::Read(opened_archive)) => Ok(opened_archive.clone()),
            Some(ZipFileSystemMode::Write(_) | ZipFileSystemMode::Aborted) => {
                Err(AppError::SystemError {
                    message: "ZipFileSystem is not in read mode".into(),
                })
            }
            None => {
                let zip_file_path = self.zip_file_path.clone();
                let opened_archive =
                    tokio::task::spawn_blocking(move || OpenedArchive::open(&zip_file_path))
                        .await??;
                self.mode = Some(ZipFileSystemMode::Read(opened_archive.clone()));
                Ok(opened_archive)
            }
        }
    }

    /// Writes `input` as the entry `name`. An entry whose content fails to arrive is taken back
    /// out of the archive, which stays readable without it: a copy of many files goes on past one
    /// that cannot be read. The compressed bytes already written stay in the file as unused
    /// space until a later entry overwrites them.
    async fn write_entry<S: Stream<Item = AppResult<Bytes>> + Unpin>(
        zip_writer: &mut ZipWriter<File>,
        name: String,
        mut input: S,
    ) -> Result<(), EntryFailure> {
        zip_writer.start_file(name, zip::write::FullFileOptions::default())?;
        loop {
            match input.try_next().await {
                Ok(Some(chunk)) => zip_writer.write_all(&chunk)?,
                Ok(None) => return Ok(()),
                Err(error) => {
                    if let Err(abort_error) = zip_writer.abort_file() {
                        tracing::warn!(error = %abort_error, "could not take the entry out of the zip archive");
                        return Err(EntryFailure::Archive(error));
                    }
                    return Err(EntryFailure::Content(error));
                }
            }
        }
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
        // The listed path, not the file name: the name filter matches the returned reference
        // against the same relative paths the listing reports.
        let downloaded_ref = readable.file_ref();
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
        input: S,
        file_ref: Option<&FileSystemRef>,
    ) -> AppResult<()> {
        if self.mode.is_none() {
            let zip_file = if self.zip_file_path.exists() {
                return Err(AppError::SystemError {
                    message: "Zip file already exists".into(),
                });
            } else {
                File::create_new(&self.zip_file_path)?
            };

            self.mode = Some(ZipFileSystemMode::Write(ArchiveWriter {
                zip: ZipWriter::new(zip_file),
                entries: 0,
            }));
        }
        let written = match self.mode {
            Some(ZipFileSystemMode::Write(ref mut writer)) => match file_ref {
                Some(file_ref) => {
                    let name = Path::new(file_ref.relative_path.value())
                        .to_string_lossy()
                        .to_string();
                    let written = Self::write_entry(&mut writer.zip, name, input).await;
                    if written.is_ok() {
                        writer.entries += 1;
                    }
                    written
                }
                None => Err(EntryFailure::Archive(AppError::SystemError {
                    message: "FileSystemRef is required for ZipFileSystem".into(),
                })),
            },
            _ => Err(EntryFailure::Archive(AppError::SystemError {
                message: "ZipFileSystem is not in write mode".into(),
            })),
        };
        match written {
            Ok(()) => Ok(()),
            Err(EntryFailure::Content(error)) => Err(error),
            Err(EntryFailure::Archive(error)) => {
                self.abort_archive();
                Err(error)
            }
        }
    }

    async fn list_files(
        &mut self,
        file_matcher: Option<&FileMatcher>,
        max_files_limit: Option<usize>,
    ) -> AppResult<ListFilesResult> {
        let opened_archive = self.opened_archive().await?;
        if opened_archive.archive.has_duplicate_names() {
            self.reporter.report(
                "The zip archive repeats some entry names; only the last entry with each name is read, and the earlier ones are neither listed nor counted as skipped",
            )?;
        }
        let limit = max_files_limit.unwrap_or(usize::MAX);
        let mut listing = ListFilesResult::EMPTY;
        for entry in &opened_archive.index.entries {
            if listing.files.len() >= limit {
                break;
            }
            match entry {
                IndexedEntry::Directory => {}
                IndexedEntry::Skipped(skipped) if skipped.is_ignored_by(file_matcher) => {
                    listing.ignored += 1;
                }
                IndexedEntry::Skipped(skipped) => {
                    listing.skipped += 1;
                    self.reporter.report(format!(
                        "Skipping zip entry {:?}: {}",
                        skipped.name, skipped.reason
                    ))?;
                }
                IndexedEntry::Readable(readable) => listing.add(readable.file_ref(), file_matcher),
            }
        }
        Ok(listing)
    }

    async fn close(mut self) -> AppResult<()> {
        if let Some(ZipFileSystemMode::Write(writer)) = self.mode.take() {
            if writer.entries == 0 {
                drop(writer);
                self.remove_archive_file();
                return Ok(());
            }
            if let Err(error) = writer.zip.finish() {
                self.remove_archive_file();
                return Err(error.into());
            }
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
    use crate::file_tools::IgnoreFileChoice;
    use gcloud_sdk::prost::bytes;
    use tempfile::TempDir;
    use zip::write::SimpleFileOptions;

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

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
            zip.start_file("docs/a/notes.csv", SimpleFileOptions::default())?;
            zip.write_all(b"a,b")?;
            zip.start_file("docs/b/notes.csv", SimpleFileOptions::default())?;
            zip.write_all(b"c,d")?;
            Ok(())
        })?;

        let mut fs = open(&zip_file_path, &reporter).await?;
        let (file_ref, content) = download_all(&mut fs, "file1.txt").await?;
        assert_eq!(content, test_content);
        assert_eq!(file_ref.relative_path.value(), "file1.txt");
        assert_eq!(file_ref.media_type, Some(mime::TEXT_PLAIN));
        assert_eq!(file_ref.file_size, Some(test_content.len()));

        let listing = fs.list_files(None, None).await?;
        assert_eq!(
            listed_paths(&listing),
            ["file1.txt", "docs/a/notes.csv", "docs/b/notes.csv"]
        );
        let (nested_ref, nested_content) = download_all(&mut fs, "docs/b/notes.csv").await?;
        assert_eq!(nested_content, b"c,d");
        assert_eq!(nested_ref.relative_path.value(), "docs/b/notes.csv");
        assert_eq!(nested_ref.media_type, Some(mime::TEXT_CSV));

        fs.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn entry_names_are_listed_without_dot_or_root_components() -> TestResult {
        let term = console::Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let temp_dir = TempDir::with_prefix("zip_file_system_tests_dot_slash")?;
        let zip_file_path = temp_dir.path().join("test.zip");
        write_zip(&zip_file_path, |zip| {
            zip.start_file("./dir/file1.txt", SimpleFileOptions::default())?;
            zip.write_all(b"content")?;
            zip.start_file("/abs.txt", SimpleFileOptions::default())?;
            zip.write_all(b"absolute")?;
            Ok(())
        })?;

        let mut fs = open(&zip_file_path, &reporter).await?;
        let listing = fs.list_files(None, None).await?;
        assert_eq!(listed_paths(&listing), ["dir/file1.txt", "abs.txt"]);
        assert_eq!(download_all(&mut fs, "abs.txt").await?.1, b"absolute");
        assert_eq!(download_all(&mut fs, "dir/file1.txt").await?.1, b"content");
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
            zip.add_directory("docs/", SimpleFileOptions::default())?;
            zip.start_file("docs/file2.txt", SimpleFileOptions::default())?;
            Ok(())
        })?;

        let mut fs = open(&zip_file_path, &reporter).await?;
        let list_files_result = fs.list_files(None, None).await?;
        assert_eq!(
            listed_paths(&list_files_result),
            ["file1.txt", "docs/file2.txt"]
        );
        assert_eq!(list_files_result.skipped, 0);

        fs.close().await?;
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

        let matched_and_limited = fs
            .list_files(Some(&text_files_up_to_five_bytes), Some(1))
            .await?;
        assert_eq!(listed_paths(&matched_and_limited), ["a.txt"]);
        assert_eq!(matched_and_limited.skipped, 0);

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
    async fn symlinks_and_entries_leaving_the_archive_are_skipped() -> TestResult {
        let term = console::Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let temp_dir = TempDir::with_prefix("zip_file_system_tests_unsafe")?;
        let zip_file_path = temp_dir.path().join("evil.zip");
        write_zip(&zip_file_path, |zip| {
            let options = SimpleFileOptions::default();
            zip.start_file("ok.txt", options)?;
            zip.write_all(b"inside")?;
            zip.add_symlink("link.txt", "ok.txt", options)?;
            zip.start_file("../escape.txt", options)?;
            zip.write_all(b"escaped")?;
            Ok(())
        })?;

        let mut fs = open(&zip_file_path, &reporter).await?;
        let listing = fs.list_files(None, None).await?;
        assert_eq!(listed_paths(&listing), ["ok.txt"]);
        assert_eq!(listing.skipped, 2);
        assert_eq!(download_all(&mut fs, "ok.txt").await?.1, b"inside");

        for skipped in ["link.txt", "../escape.txt"] {
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
    async fn entries_matched_by_the_ignore_rules_count_as_ignored() -> TestResult {
        let term = console::Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let temp_dir = TempDir::with_prefix("zip_file_system_tests_ignore")?;
        let zip_file_path = temp_dir.path().join("test.zip");
        write_zip(&zip_file_path, |zip| {
            zip.start_file("kept.txt", SimpleFileOptions::default())?;
            zip.start_file("mocks/a.txt", SimpleFileOptions::default())?;
            zip.start_file("b.mock", SimpleFileOptions::default())?;
            zip.add_symlink("link.txt", "kept.txt", SimpleFileOptions::default())?;
            zip.add_symlink("ignored.mock", "kept.txt", SimpleFileOptions::default())?;
            Ok(())
        })?;
        let ignore_file_path = temp_dir.path().join("ignore");
        std::fs::write(&ignore_file_path, "mocks/\n*.mock\n")?;
        let ignore_rules = IgnoreFileChoice::Explicit(ignore_file_path).load(None)?;
        let matcher = FileMatcher::new(None, None).with_ignore(ignore_rules);

        let mut fs = open(&zip_file_path, &reporter).await?;
        let listing = fs.list_files(Some(&matcher), None).await?;
        assert_eq!(listed_paths(&listing), ["kept.txt"]);
        assert_eq!((listing.ignored, listing.skipped), (3, 1));
        Ok(())
    }

    #[test]
    fn an_entry_that_repeats_a_path_is_skipped_under_its_own_name() -> TestResult {
        let temp_dir = TempDir::with_prefix("zip_file_system_tests_duplicate_name")?;
        let zip_file_path = temp_dir.path().join("test.zip");
        write_zip(&zip_file_path, |zip| {
            zip.start_file("a.txt", SimpleFileOptions::default())?;
            zip.start_file("./a.txt", SimpleFileOptions::default())?;
            Ok(())
        })?;

        let archive = OpenedArchive::open(&zip_file_path)?;
        let skipped_names: Vec<&str> = archive
            .index
            .entries
            .iter()
            .filter_map(|entry| match entry {
                IndexedEntry::Skipped(skipped) => Some(skipped.name.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(skipped_names, ["./a.txt"]);
        Ok(())
    }

    #[tokio::test]
    async fn an_entry_larger_than_its_declared_size_fails_the_download() -> TestResult {
        let term = console::Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let temp_dir = TempDir::with_prefix("zip_file_system_tests_declared_size")?;
        let zip_file_path = temp_dir.path().join("test.zip");
        write_zip(&zip_file_path, |zip| {
            zip.start_file(
                "zeros.bin",
                SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated),
            )?;
            zip.write_all(&vec![0u8; 1_000_000])?;
            Ok(())
        })?;
        // The uncompressed size sits 24 bytes into the central directory header.
        let mut archive_bytes = std::fs::read(&zip_file_path)?;
        let central_header = archive_bytes
            .windows(4)
            .rposition(|window| window == b"PK\x01\x02")
            .expect("the archive has a central directory header");
        archive_bytes[central_header + 24..central_header + 28]
            .copy_from_slice(&1000u32.to_le_bytes());
        std::fs::write(&zip_file_path, archive_bytes)?;

        let mut fs = open(&zip_file_path, &reporter).await?;
        let listing = fs.list_files(None, None).await?;
        assert_eq!(listing.files[0].file_size, Some(1000));
        assert!(download_all(&mut fs, "zeros.bin").await.is_err());
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

    fn archive_contents(zip_file_path: &Path) -> TestResult<Vec<(String, Vec<u8>)>> {
        let mut archive = ZipArchive::new(File::open(zip_file_path)?)?;
        (0..archive.len())
            .map(|position| {
                let mut entry = archive.by_index(position)?;
                let mut content = Vec::new();
                entry.read_to_end(&mut content)?;
                Ok((
                    String::from_utf8_lossy(entry.name_raw()).into_owned(),
                    content,
                ))
            })
            .collect()
    }

    #[tokio::test]
    async fn an_entry_whose_content_fails_is_left_out_and_the_others_are_kept() -> TestResult {
        let term = console::Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let temp_dir = TempDir::with_prefix("zip_file_system_tests_failed_entry")?;
        let zip_file_path = temp_dir.path().join("test.zip");

        let mut fs = open(&zip_file_path, &reporter).await?;
        let complete = futures::stream::iter(vec![Ok(bytes::Bytes::from_static(b"complete"))]);
        fs.upload(complete, Some(&reference("complete.txt")))
            .await?;
        let failed = fs
            .upload(
                crate::file_systems::stream_failing_after_first_chunk(),
                Some(&reference("broken.txt")),
            )
            .await;
        let later = futures::stream::iter(vec![Ok(bytes::Bytes::from_static(b"later"))]);
        fs.upload(later, Some(&reference("later.txt"))).await?;
        fs.close().await?;

        assert!(
            matches!(failed, Err(AppError::SystemError { ref message }) if message == "the stream broke"),
            "{failed:?}"
        );
        assert_eq!(
            archive_contents(&zip_file_path)?,
            vec![
                ("complete.txt".to_string(), b"complete".to_vec()),
                ("later.txt".to_string(), b"later".to_vec()),
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_failed_last_entry_with_much_content_leaves_a_readable_archive() -> TestResult {
        let term = console::Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let temp_dir = TempDir::with_prefix("zip_file_system_tests_failed_last_entry")?;
        let zip_file_path = temp_dir.path().join("test.zip");
        let incompressible: Vec<u8> = (0..300_000u32)
            .map(|index| (index.wrapping_mul(2_654_435_761) >> 24) as u8)
            .collect();
        let chunks: Vec<AppResult<bytes::Bytes>> = incompressible
            .chunks(64 * 1024)
            .map(|chunk| Ok(bytes::Bytes::copy_from_slice(chunk)))
            .chain([Err(AppError::SystemError {
                message: "the stream broke".to_string(),
            })])
            .collect();

        let mut fs = open(&zip_file_path, &reporter).await?;
        let complete = futures::stream::iter(vec![Ok(bytes::Bytes::from_static(b"complete"))]);
        fs.upload(complete, Some(&reference("complete.txt")))
            .await?;
        assert!(fs
            .upload(
                futures::stream::iter(chunks),
                Some(&reference("broken.bin"))
            )
            .await
            .is_err());
        fs.close().await?;

        assert_eq!(
            archive_contents(&zip_file_path)?,
            vec![("complete.txt".to_string(), b"complete".to_vec())]
        );
        Ok(())
    }

    #[tokio::test]
    async fn an_archive_without_a_surviving_entry_is_not_left_behind() -> TestResult {
        let term = console::Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let temp_dir = TempDir::with_prefix("zip_file_system_tests_no_entry_left")?;
        let zip_file_path = temp_dir.path().join("test.zip");

        let mut fs = open(&zip_file_path, &reporter).await?;
        let failed = fs
            .upload(
                crate::file_systems::stream_failing_after_first_chunk(),
                Some(&reference("broken.txt")),
            )
            .await;
        fs.close().await?;

        assert!(failed.is_err());
        assert!(!zip_file_path.exists());

        let mut again = open(&zip_file_path, &reporter).await?;
        let content = futures::stream::iter(vec![Ok(bytes::Bytes::from_static(b"content"))]);
        again.upload(content, Some(&reference("file1.txt"))).await?;
        again.close().await?;
        assert_eq!(
            archive_contents(&zip_file_path)?,
            vec![("file1.txt".to_string(), b"content".to_vec())]
        );
        Ok(())
    }

    #[tokio::test]
    async fn an_archive_that_is_dropped_before_it_is_closed_is_removed() -> TestResult {
        let term = console::Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let temp_dir = TempDir::with_prefix("zip_file_system_tests_dropped")?;
        let zip_file_path = temp_dir.path().join("test.zip");

        let mut fs = open(&zip_file_path, &reporter).await?;
        let complete = futures::stream::iter(vec![Ok(bytes::Bytes::from_static(b"complete"))]);
        fs.upload(complete, Some(&reference("complete.txt")))
            .await?;
        assert!(zip_file_path.exists());
        drop(fs);

        assert!(!zip_file_path.exists());
        Ok(())
    }

    #[tokio::test]
    async fn an_existing_archive_is_refused_and_left_alone() -> TestResult {
        let term = console::Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);
        let temp_dir = TempDir::with_prefix("zip_file_system_tests_existing")?;
        let zip_file_path = temp_dir.path().join("test.zip");
        std::fs::write(&zip_file_path, b"not replaced")?;

        let mut fs = open(&zip_file_path, &reporter).await?;
        let stream = futures::stream::iter(vec![Ok(bytes::Bytes::from_static(b"content"))]);
        assert!(fs
            .upload(stream, Some(&reference("file1.txt")))
            .await
            .is_err());
        drop(fs);

        assert_eq!(std::fs::read(&zip_file_path)?, b"not replaced");
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
