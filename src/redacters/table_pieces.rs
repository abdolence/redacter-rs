//! Reads a CSV table in batches of rows, so `cp` and `scan` hold a few batches of it and not the
//! file.
//!
//! A row is the unit. The local redacters read a table one cell at a time, with the header of its
//! column, so a batch of rows gives the findings and the redacted cells that the same rows give
//! in the whole table.

use crate::args::RedacterType;
use crate::errors::AppError;
use crate::file_systems::FileSystemRef;
use crate::redacters::piece_redaction::end_with_error;
use crate::redacters::{
    Detection, DetectionLocation, RecordRedactions, RedacterBaseOptions, RedacterDataItem,
    RedacterDataItemContent, Redacters, StreamRedactions,
};
use crate::AppResult;
use bytes::Bytes;
use futures::{Stream, TryStreamExt};
use tokio::sync::mpsc;
use tokio_util::io::StreamReader;

/// How many bytes of cells a batch holds before the next row starts a new one, each cell counted
/// with [`CELL_OVERHEAD_BYTES`]. A memory bound, not a limit of any redacter.
pub(crate) const TABLE_BATCH_BYTES: usize = 4 * 1024 * 1024;

/// What a cell costs besides its text: the `String` that holds it. Without it, a table of
/// one-byte cells would hold dozens of times `TABLE_BATCH_BYTES` in a batch.
const CELL_OVERHEAD_BYTES: usize = std::mem::size_of::<String>();

type TableInput<S> =
    StreamReader<futures::stream::MapErr<S, fn(AppError) -> std::io::Error>, Bytes>;

/// A CSV table read a batch of rows at a time.
pub(crate) struct TableBatches<S> {
    reader: csv_async::AsyncReader<TableInput<S>>,
    headers: Vec<String>,
    batch_bytes: usize,
    rows_before: usize,
    /// The row that did not fit in the batch before it.
    carried: Option<csv_async::StringRecord>,
}

/// Some rows of a table, with its headers.
pub(crate) struct TableBatch {
    headers: Vec<String>,
    pub rows: Vec<Vec<String>>,
    /// How many rows of the table come before the first of these.
    pub rows_before: usize,
    /// Whether these are the last rows of the table.
    pub reaches_end: bool,
}

impl TableBatch {
    /// Whether the batch is the whole table.
    pub fn is_whole_table(&self) -> bool {
        self.rows_before == 0 && self.reaches_end
    }

    pub fn into_item(self, file_ref: &FileSystemRef) -> RedacterDataItem {
        RedacterDataItem {
            content: RedacterDataItemContent::Table {
                headers: self.headers,
                rows: self.rows,
            },
            file_ref: file_ref.clone(),
        }
    }
}

impl<S: Stream<Item = AppResult<Bytes>> + Send + Unpin> TableBatches<S> {
    /// Reads the headers of the table `input` holds, and cuts its rows in batches of about
    /// `batch_bytes` of cells, see [`TABLE_BATCH_BYTES`].
    pub async fn new(
        input: S,
        options: &RedacterBaseOptions,
        batch_bytes: usize,
    ) -> AppResult<Self> {
        let into_io_error: fn(AppError) -> std::io::Error = std::io::Error::other;
        let mut reader = csv_async::AsyncReaderBuilder::default()
            .has_headers(!options.csv_headers_disable)
            .delimiter(options.table_delimiter())
            .create_reader(StreamReader::new(input.map_err(into_io_error)));
        let headers = if options.csv_headers_disable {
            Vec::new()
        } else {
            reader.headers().await?.iter().map(str::to_string).collect()
        };
        Ok(Self {
            reader,
            headers,
            batch_bytes,
            rows_before: 0,
            carried: None,
        })
    }

    /// The next rows of the table. The batch that reaches the end of the table is the last one:
    /// a table with no rows is one batch of none.
    pub async fn next_batch(&mut self) -> AppResult<TableBatch> {
        let mut rows = Vec::new();
        let mut cell_bytes = 0;
        let mut reaches_end = false;
        loop {
            let record = match self.carried.take() {
                Some(record) => record,
                None => {
                    let mut record = csv_async::StringRecord::new();
                    if !self.reader.read_record(&mut record).await? {
                        reaches_end = true;
                        break;
                    }
                    record
                }
            };
            if !rows.is_empty() && cell_bytes >= self.batch_bytes {
                self.carried = Some(record);
                break;
            }
            cell_bytes += record.as_slice().len() + record.len() * CELL_OVERHEAD_BYTES;
            rows.push(record.iter().map(str::to_string).collect());
        }
        let rows_before = self.rows_before;
        self.rows_before += rows.len();
        Ok(TableBatch {
            headers: self.headers.clone(),
            rows,
            rows_before,
            reaches_end,
        })
    }
}

/// The chain of redacters that read the batches of one table. The redactions are placed in the
/// table for the report when `options` ask for a list.
pub(crate) struct TableRedaction<'a> {
    redacters: Vec<&'a Redacters<'a>>,
    options: &'a RedacterBaseOptions,
    file_ref: FileSystemRef,
}

impl<'a> TableRedaction<'a> {
    /// The chain of `redacters`.
    pub fn new(
        redacters: &[&'a Redacters<'a>],
        options: &'a RedacterBaseOptions,
        file_ref: &FileSystemRef,
    ) -> Self {
        Self {
            redacters: redacters.to_vec(),
            options,
            file_ref: file_ref.clone(),
        }
    }

    /// Runs the chain on `batch`, each redacter reading the rows as the ones before it left them.
    /// Returns the batch as CSV, with the headers when it starts the table, and what the
    /// redacters redacted in it, placed in the table.
    async fn redact_batch(
        &self,
        batch: TableBatch,
    ) -> AppResult<(Bytes, Vec<(RedacterType, Detection)>)> {
        let rows_before = batch.rows_before;
        let mut item = batch.into_item(&self.file_ref);
        let mut recorder = StreamRedactions::new(self.options.list_redactions);
        for redacter in &self.redacters {
            item = recorder
                .redact(redacter, None, item, self.options.categories.as_ref())
                .await?;
        }
        let RedacterDataItemContent::Table { headers, rows } = item.content else {
            return Err(AppError::SystemError {
                message: "A redacter changed a table into another kind of content".to_string(),
            });
        };
        let csv = table_csv(self.options, headers, rows, rows_before == 0).await?;

        let mut redactions = recorder.into_records();
        for (_, detection) in &mut redactions {
            detection.location = detection.location.after_rows(rows_before);
        }
        Ok((csv, redactions))
    }
}

impl DetectionLocation {
    /// The location of a table cell found in a batch that starts after `rows_before` rows of the
    /// table, in the table.
    pub(crate) fn after_rows(self, rows_before: usize) -> Self {
        match self {
            Self::TableCell {
                row,
                column,
                start,
                end,
            } => Self::TableCell {
                row: row + rows_before,
                column,
                start,
                end,
            },
            other => other,
        }
    }
}

/// Redacts the batches of a table one after the other and hands the CSV of each to `written`,
/// handing what the redacters redacted in each batch to `record`. `first` is the batch `batches`
/// read first.
///
/// When a batch cannot be read or redacted, the stream ends with the error, so that the
/// destination does not keep the part of the table it was given.
pub(crate) async fn redact_batches<S: Stream<Item = AppResult<Bytes>> + Send + Unpin>(
    redaction: TableRedaction<'_>,
    mut batches: TableBatches<S>,
    first: TableBatch,
    written: mpsc::Sender<AppResult<Bytes>>,
    record: &mut RecordRedactions<'_>,
) -> AppResult<()> {
    let outcome: AppResult<()> = async {
        let mut batch = first;
        loop {
            let reaches_end = batch.reaches_end;
            let (csv, redacted) = redaction.redact_batch(batch).await?;
            record(redacted)?;
            if written.send(Ok(csv)).await.is_err() || reaches_end {
                return Ok(());
            }
            batch = batches.next_batch().await?;
        }
    }
    .await;
    end_with_error(outcome, &written).await
}

/// The CSV of a table, written with the delimiter the table is read with. The header record is
/// written when `starts_table`, unless the options disable the headers: a table read without
/// headers has none to write.
pub(crate) async fn table_csv(
    options: &RedacterBaseOptions,
    headers: Vec<String>,
    rows: Vec<Vec<String>>,
    starts_table: bool,
) -> AppResult<Bytes> {
    let mut writer = csv_async::AsyncWriterBuilder::new()
        .delimiter(options.table_delimiter())
        .create_writer(vec![]);
    if starts_table && !options.csv_headers_disable {
        writer.write_record(headers).await?;
    }
    for row in rows {
        writer.write_record(row).await?;
    }
    writer.flush().await?;
    Ok(Bytes::from(writer.into_inner().await?))
}
