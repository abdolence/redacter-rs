use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::sync::Arc;

/// A read-only view of an open file that has a cursor of its own.
///
/// Clones share the file handle and read at explicit offsets, so reading through one clone
/// never moves another. That is what lets a parsed `ZipArchive` be cloned for each concurrent
/// download instead of parsing the central directory again.
#[derive(Debug, Clone)]
pub struct SharedFile {
    file: Arc<File>,
    length: u64,
    position: u64,
}

impl SharedFile {
    pub fn open(path: &std::path::Path) -> io::Result<Self> {
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
                io::ErrorKind::InvalidInput,
                "seek to a position before the start of the file",
            )
        })?;
        Ok(self.position)
    }
}
