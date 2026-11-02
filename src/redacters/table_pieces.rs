//! Reads a CSV table in batches of rows, so `cp` and `scan` hold a few batches of it and not the
//! file.
//!
//! A row is the unit. The local redacters read a table one cell at a time, with the header of its
//! column, so a batch of rows gives the findings and the redacted cells that the same rows give
//! in the whole table.

use crate::args::RedacterType;
use crate::errors::AppError;
use crate::file_systems::FileSystemRef;
use crate::redacters::{
    Categories, Detection, DetectionLocation, Redacter, RedacterBaseOptions, RedacterDataItem,
    RedacterDataItemContent, Redacters, StreamRedactions,
};
use crate::AppResult;
use bytes::Bytes;
use futures::{Stream, TryStreamExt};
use tokio::sync::mpsc;
use tokio_util::io::StreamReader;

/// How many bytes of cells a batch holds before the next row starts a new one. A memory bound,
/// not a limit of any redacter.
pub(crate) const TABLE_BATCH_BYTES: usize = 4 * 1024 * 1024;

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
    /// `batch_bytes` of cells.
    pub async fn new(
        input: S,
        options: &RedacterBaseOptions,
        batch_bytes: usize,
    ) -> AppResult<Self> {
        let into_io_error: fn(AppError) -> std::io::Error = std::io::Error::other;
        let mut reader = csv_async::AsyncReaderBuilder::default()
            .has_headers(!options.csv_headers_disable)
            .delimiter(options.csv_delimiter.unwrap_or(b','))
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
            cell_bytes += record.as_slice().len();
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

/// The chain of redacters that read the batches of one table.
pub(crate) struct TableRedaction<'a> {
    redacters: Vec<&'a Redacters<'a>>,
    categories: Option<&'a Categories>,
    file_ref: FileSystemRef,
    recording: bool,
}

impl<'a> TableRedaction<'a> {
    /// The chain of `redacters`. With `recording`, the redactions are placed in the table for
    /// the report.
    pub fn new(
        redacters: &[&'a Redacters<'a>],
        categories: Option<&'a Categories>,
        file_ref: &FileSystemRef,
        recording: bool,
    ) -> Self {
        Self {
            redacters: redacters.to_vec(),
            categories,
            file_ref: file_ref.clone(),
            recording,
        }
    }

    /// The redacters of the chain, in order.
    pub fn redacters(&self) -> Vec<RedacterType> {
        self.redacters
            .iter()
            .map(|redacter| redacter.redacter_type())
            .collect()
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
        let mut recorder = StreamRedactions::new(self.recording);
        for redacter in &self.redacters {
            item = recorder
                .redact(redacter, None, item, self.categories)
                .await?;
        }
        let RedacterDataItemContent::Table { headers, rows } = item.content else {
            return Err(AppError::SystemError {
                message: "A redacter changed a table into another kind of content".to_string(),
            });
        };
        let mut writer = csv_async::AsyncWriter::from_writer(vec![]);
        if rows_before == 0 {
            writer.write_record(headers).await?;
        }
        for row in rows {
            writer.write_record(row).await?;
        }
        writer.flush().await?;
        let csv = Bytes::from(writer.into_inner().await?);

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
/// then returns what the redacters redacted in the table, a batch after another. `first` is the batch
/// `batches` read first.
///
/// When a batch cannot be read or redacted, the stream ends with the error, so that the
/// destination does not keep the part of the table it was given.
pub(crate) async fn redact_batches<S: Stream<Item = AppResult<Bytes>> + Send + Unpin>(
    redaction: TableRedaction<'_>,
    mut batches: TableBatches<S>,
    first: TableBatch,
    written: mpsc::Sender<AppResult<Bytes>>,
) -> AppResult<Vec<(RedacterType, Detection)>> {
    let mut redactions = Vec::new();
    let outcome: AppResult<()> = async {
        let mut batch = first;
        loop {
            let reaches_end = batch.reaches_end;
            let (csv, redacted) = redaction.redact_batch(batch).await?;
            redactions.extend(redacted);
            if written.send(Ok(csv)).await.is_err() || reaches_end {
                return Ok(());
            }
            batch = batches.next_batch().await?;
        }
    }
    .await;
    match outcome {
        Ok(()) => Ok(redactions),
        Err(error) => {
            // The receiver may be gone already, and then there is nobody to tell.
            let _ = written
                .send(Err(AppError::SystemError {
                    message: error.to_string(),
                }))
                .await;
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_converters::FileConverters;
    use crate::redacters::{
        DetectionLabel, LocalRulesRedacter, LocalRulesRedacterOptions, RuleGroup, StreamRedacter,
    };
    use crate::reporter::AppReporter;
    use std::collections::BTreeSet;

    fn base_options() -> RedacterBaseOptions {
        RedacterBaseOptions {
            allow_unsupported_copies: false,
            unsupported_as_text: false,
            csv_headers_disable: false,
            csv_delimiter: None,
            sampling_size: None,
            limit_dlp_requests: None,
            categories: None,
            list_redactions: true,
        }
    }

    fn file_ref() -> FileSystemRef {
        FileSystemRef {
            relative_path: "people.csv".into(),
            media_type: Some(mime::TEXT_CSV),
            file_size: None,
        }
    }

    fn reporter() -> &'static AppReporter<'static> {
        // Leaked so the reporter, and the terminal it borrows, outlive the redacters built
        // from it in a short-lived test process.
        let term: &'static console::Term = Box::leak(Box::new(console::Term::stdout()));
        Box::leak(Box::new(AppReporter::from(term)))
    }

    async fn rules(group: RuleGroup) -> Redacters<'static> {
        let options = LocalRulesRedacterOptions {
            groups: BTreeSet::from([group]),
            user_rules: Vec::new(),
        };
        Redacters::LocalRules(
            LocalRulesRedacter::new(options, reporter())
                .await
                .expect("valid rule set"),
        )
    }

    /// `data` in chunks of `size` bytes, so that no chunk ends where a row does.
    fn in_chunks(
        data: &str,
        size: usize,
    ) -> impl Stream<Item = AppResult<Bytes>> + Send + Sync + Unpin + 'static {
        futures::stream::iter(
            data.as_bytes()
                .chunks(size)
                .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
                .collect::<Vec<_>>(),
        )
    }

    /// Rows with an email, an address and cells that hold a comma, quotes and a line break, which
    /// a batch boundary must not cut.
    fn people(rows: usize) -> String {
        let mut table = String::from("id,contact,note\r\n");
        for row in 0..rows {
            table.push_str(&format!(
                "{row},user{row}@example.com,\"host 10.1.{row}.3, \"\"main\"\"\nthen mail ops{row}@example.com\"\r\n"
            ));
        }
        table
    }

    async fn batches_of(table: &str, batch_bytes: usize, chunk: usize) -> Vec<TableBatch> {
        let mut batches = TableBatches::new(in_chunks(table, chunk), &base_options(), batch_bytes)
            .await
            .unwrap();
        let mut all = Vec::new();
        loop {
            let batch = batches.next_batch().await.unwrap();
            let reaches_end = batch.reaches_end;
            all.push(batch);
            if reaches_end {
                return all;
            }
        }
    }

    #[tokio::test]
    async fn batches_hold_whole_rows_and_count_them_from_the_start_of_the_table() {
        let batches = batches_of("a,b\n1,x\n2,y\n3,z\n", 2, 5).await;

        assert_eq!(
            batches
                .iter()
                .map(|batch| (batch.rows_before, batch.rows.len(), batch.reaches_end))
                .collect::<Vec<_>>(),
            [(0, 1, false), (1, 1, false), (2, 1, true)]
        );
        assert_eq!(batches[2].rows, [["3", "z"]]);
        assert!(batches.iter().all(|batch| batch.headers == ["a", "b"]));
    }

    #[tokio::test]
    async fn a_quoted_cell_with_a_comma_and_a_line_break_is_one_cell_across_chunks_and_batches() {
        let table = "id,note\r\n1,\"a,\r\nb\"\r\n2,\"c\nd\"\r\n";

        let batches = batches_of(table, 1, 3).await;

        let rows: Vec<_> = batches
            .iter()
            .flat_map(|batch| batch.rows.clone())
            .collect();
        assert_eq!(rows, [["1", "a,\r\nb"], ["2", "c\nd"]]);
        assert_eq!(batches.len(), 2);
    }

    #[tokio::test]
    async fn a_table_with_no_rows_is_one_batch_of_none() {
        let batches = batches_of("a,b\n", 2, 5).await;

        assert_eq!(batches.len(), 1);
        assert!(batches[0].is_whole_table());
        assert!(batches[0].rows.is_empty());
    }

    /// The way a report orders what it lists: by place, and by redacter within a place.
    fn by_location(mut found: Vec<(RedacterType, Detection)>) -> Vec<(RedacterType, Detection)> {
        found.sort_by(|(_, a), (_, b)| a.location.partial_cmp(&b.location).unwrap());
        found
    }

    /// What a copy writes, in how many chunks, and lists.
    struct Copied {
        output: Vec<u8>,
        chunks: usize,
        redactions: Vec<(RedacterType, Detection)>,
    }

    /// Copies `table` in chunks of 7 bytes, read in batches of `batch_bytes` or, for `None`,
    /// whole. The stream and the redactions are polled together, as `cp` does.
    async fn copy(
        redacters: &[Redacters<'static>],
        table: &str,
        batch_bytes: Option<usize>,
    ) -> AppResult<(
        AppResult<Vec<Bytes>>,
        AppResult<Vec<(RedacterType, Detection)>>,
    )> {
        let converters = FileConverters::new();
        let options = base_options();
        let stream_redacter = StreamRedacter::new(&options, &converters);
        let mut plan = stream_redacter
            .create_redact_plan(redacters, &file_ref())
            .await?;
        plan.table_batch_bytes = batch_bytes;
        let result = stream_redacter
            .redact_stream(in_chunks(table, 7), plan, &file_ref())
            .await?;
        Ok(result
            .output
            .drive(async |stream| stream.try_collect().await)
            .await)
    }

    async fn copied(
        redacters: &[Redacters<'static>],
        table: &str,
        batch_bytes: Option<usize>,
    ) -> Copied {
        let (stream, redactions) = copy(redacters, table, batch_bytes).await.unwrap();
        let chunks = stream.unwrap();
        Copied {
            chunks: chunks.len(),
            output: chunks.concat(),
            redactions: redactions.unwrap(),
        }
    }

    #[tokio::test]
    async fn a_table_copied_in_batches_is_the_table_copied_whole() {
        let redacters = vec![
            rules(RuleGroup::Email).await,
            rules(RuleGroup::Network).await,
        ];
        let table = people(40);

        let whole = copied(&redacters, &table, None).await;
        let in_batches = copied(&redacters, &table, Some(300)).await;

        assert_eq!(whole.chunks, 1);
        assert!(in_batches.chunks > 1, "{} chunks", in_batches.chunks);
        assert_eq!(
            String::from_utf8_lossy(&in_batches.output),
            String::from_utf8_lossy(&whole.output)
        );
        assert_eq!(whole.redactions.len(), 40 * 3);
        assert_eq!(
            by_location(in_batches.redactions),
            by_location(whole.redactions)
        );
    }

    #[tokio::test]
    async fn a_row_of_the_wrong_length_after_the_first_batch_fails_the_copy() {
        let redacters = vec![rules(RuleGroup::Email).await];
        let table = format!("{}9,only one cell\r\n", people(10));

        let (stream, redactions) = copy(&redacters, &table, Some(300)).await.unwrap();

        assert!(stream.is_err());
        assert!(redactions.is_err());
    }

    async fn detected(
        redacters: &[Redacters<'static>],
        table: &str,
        batch_bytes: Option<usize>,
    ) -> crate::redacters::DetectStreamResult {
        let converters = FileConverters::new();
        let options = base_options();
        let stream_redacter = StreamRedacter::new(&options, &converters);
        let mut plan = stream_redacter
            .create_detect_plan(redacters, &file_ref())
            .await
            .unwrap();
        plan.table_batch_bytes = batch_bytes;
        stream_redacter
            .detect_stream(in_chunks(table, 7), &plan, &file_ref())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn a_scan_in_batches_finds_what_a_scan_of_the_whole_table_finds() {
        let redacters = vec![
            rules(RuleGroup::Email).await,
            rules(RuleGroup::Network).await,
        ];
        let table = format!("{}x,\"eve@example.com redacter:allow\",n\r\n", people(40));

        let whole = detected(&redacters, &table, None).await;
        let in_batches = detected(&redacters, &table, Some(300)).await;

        assert_eq!(whole.detections.len(), 40 * 3);
        assert_eq!(whole.ignored.len(), 1);
        let eve = &in_batches.ignored[0].1;
        assert_eq!(eve.label, DetectionLabel::new("email"));
        assert_eq!(
            eve.location,
            DetectionLocation::TableCell {
                row: 41,
                column: 2,
                start: 0,
                end: 15
            }
        );
        assert_eq!(
            by_location(in_batches.detections),
            by_location(whole.detections)
        );
        assert_eq!(by_location(in_batches.ignored), by_location(whole.ignored));
        assert_eq!(in_batches.unlocated, whole.unlocated);
    }

    #[tokio::test]
    async fn a_table_is_read_in_batches_only_when_every_redacter_reads_cells() {
        let converters = FileConverters::new();
        let options = base_options();
        let stream_redacter = StreamRedacter::new(&options, &converters);
        let redacters = vec![rules(RuleGroup::Email).await];

        let table = stream_redacter
            .create_redact_plan(&redacters, &file_ref())
            .await
            .unwrap();
        let scan = stream_redacter
            .create_detect_plan(&redacters, &file_ref())
            .await
            .unwrap();
        let text = stream_redacter
            .create_redact_plan(
                &redacters,
                &FileSystemRef {
                    relative_path: "people.txt".into(),
                    media_type: Some(mime::TEXT_PLAIN),
                    file_size: None,
                },
            )
            .await
            .unwrap();

        assert_eq!(table.table_batch_bytes, Some(TABLE_BATCH_BYTES));
        assert_eq!(scan.table_batch_bytes, Some(TABLE_BATCH_BYTES));
        assert_eq!(text.table_batch_bytes, None);
    }
}
