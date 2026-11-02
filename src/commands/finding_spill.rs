//! The findings of a `scan`, kept in a temporary file so that the memory a scan needs does not
//! grow with how much it finds. Each finding is one line of JSON; a file's findings are a range
//! of lines that the writers of the reports, and the terminal, read back one at a time.

use crate::commands::scan_output::FindingReport;
use crate::redacters::DetectionLabel;
use crate::{AppError, AppResult};
use serde::ser::{Error as _, SerializeSeq};
use serde::{Serialize, Serializer};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, ErrorKind, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

/// The temporary file of a run. It is created by its first write, so a run without findings
/// touches no disk.
///
/// The file has no name: it is private to the process and the system removes it when the last
/// handle is gone, even if the process is killed. It holds the value of a finding as the
/// report does, masked unless `--show-values` was given.
#[derive(Debug, Clone, Default)]
pub struct FindingSpill(Arc<Mutex<SpillFile>>);

#[derive(Debug, Default)]
struct SpillFile {
    file: Option<File>,
    len: u64,
}

impl FindingSpill {
    fn locked(&self) -> MutexGuard<'_, SpillFile> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The findings of one file: a range of the spill, and the counts the summary and the row of
/// the file need. They are in the order of the windows the file was read in, and sorted by
/// location within a window.
///
/// Every `append` of one file must follow the previous one directly, so no other file's
/// findings are written in between.
#[derive(Debug, Clone, Default)]
pub struct SpilledFindings {
    spill: FindingSpill,
    range: Range<u64>,
    count: usize,
    by_label: BTreeMap<DetectionLabel, usize>,
}

impl SpilledFindings {
    /// No findings yet, to be followed by `append`s to `spill`.
    pub fn new(spill: &FindingSpill) -> Self {
        let end = spill.locked().len;
        Self {
            spill: spill.clone(),
            range: end..end,
            count: 0,
            by_label: BTreeMap::new(),
        }
    }

    /// Writes `findings` after the ones written before. A failure is the spill's, not the
    /// file's: it names the directory the temporary file lives in.
    pub fn append(&mut self, findings: &[FindingReport]) -> AppResult<()> {
        if findings.is_empty() {
            return Ok(());
        }
        self.write(findings)
            .map_err(|source| AppError::FindingSpillFailed {
                directory: std::env::temp_dir().display().to_string(),
                source,
            })?;
        self.count += findings.len();
        for finding in findings {
            match self.by_label.get_mut(finding.label()) {
                Some(count) => *count += 1,
                None => {
                    self.by_label.insert(finding.label().clone(), 1);
                }
            }
        }
        Ok(())
    }

    fn write(&mut self, findings: &[FindingReport]) -> std::io::Result<()> {
        let mut guard = self.spill.locked();
        let spill = &mut *guard;
        let file = match &mut spill.file {
            Some(file) => file,
            unopened => unopened.insert(tempfile::tempfile()?),
        };
        debug_assert_eq!(spill.len, self.range.end, "another file wrote in between");
        file.seek(SeekFrom::Start(spill.len))?;
        let mut out = BufWriter::new(&mut *file);
        for finding in findings {
            finding.write_spilled(&mut out)?;
            out.write_all(b"\n")?;
        }
        out.flush()?;
        drop(out);
        spill.len = file.stream_position()?;
        self.range.end = spill.len;
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// How many findings each label has.
    pub fn by_label(&self) -> &BTreeMap<DetectionLabel, usize> {
        &self.by_label
    }

    /// Reads the findings back, one at a time. Readers are independent of one another and of
    /// later `append`s.
    pub fn iter(&self) -> SpilledFindingsIter {
        SpilledFindingsIter {
            lines: BufReader::new(SpillRead {
                spill: self.spill.clone(),
                position: self.range.start,
                end: self.range.end,
            }),
            line: Vec::new(),
            failed: false,
        }
    }
}

/// Serialises as the list of findings it holds, reading them from the spill, so a report that
/// contains it is written without holding them. A finding that cannot be read back is an error
/// of the report.
impl Serialize for SpilledFindings {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.count))?;
        for finding in self.iter() {
            seq.serialize_element(&finding.map_err(S::Error::custom)?)?;
        }
        seq.end()
    }
}

/// Ends after the first error: the position of a failed read does not move, so reading on would
/// fail again forever.
pub struct SpilledFindingsIter {
    lines: BufReader<SpillRead>,
    line: Vec<u8>,
    failed: bool,
}

impl Iterator for SpilledFindingsIter {
    type Item = AppResult<FindingReport>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        self.line.clear();
        match self.lines.read_until(b'\n', &mut self.line) {
            Ok(0) => None,
            Ok(_) => Some(FindingReport::read_spilled(&self.line)),
            Err(error) => {
                self.failed = true;
                Some(Err(error.into()))
            }
        }
    }
}

/// A position in the spill that reads without moving anyone else's.
struct SpillRead {
    spill: FindingSpill,
    position: u64,
    end: u64,
}

impl Read for SpillRead {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let left = self.end - self.position;
        let wanted = buf.len().min(usize::try_from(left).unwrap_or(usize::MAX));
        if wanted == 0 {
            return Ok(0);
        }
        let mut spill = self.spill.locked();
        let file = spill.file.as_mut().ok_or(ErrorKind::UnexpectedEof)?;
        file.seek(SeekFrom::Start(self.position))?;
        let read = file.read(&mut buf[..wanted])?;
        if read == 0 {
            // The range was written whole, so a shorter file means the spill was cut short.
            return Err(ErrorKind::UnexpectedEof.into());
        }
        self.position += read as u64;
        Ok(read)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::args::RedacterType;
    use crate::redacters::{Detection, DetectionLocation};
    use crate::ValueDisplay;

    fn email_finding() -> FindingReport {
        FindingReport::new(
            RedacterType::LocalRules,
            &Detection {
                label: DetectionLabel::new("email"),
                location: DetectionLocation::File,
                score: None,
                value: "john.smith@example.com".to_string(),
            },
            ValueDisplay::Masked,
        )
    }

    #[test]
    fn a_spill_cut_short_is_an_error_not_a_shorter_report() {
        let spill = FindingSpill::default();
        let mut findings = SpilledFindings::new(&spill);
        findings
            .append(&[email_finding(), email_finding()])
            .unwrap();
        let spilled_len = spill.locked().len;
        spill
            .locked()
            .file
            .as_ref()
            .unwrap()
            .set_len(spilled_len / 2)
            .unwrap();

        let read: Vec<_> = findings.iter().collect();

        let last = read.last().unwrap();
        assert!(
            matches!(last, Err(AppError::InputOutputError(error)) if error.kind() == ErrorKind::UnexpectedEof),
            "{read:?}"
        );
    }
}
