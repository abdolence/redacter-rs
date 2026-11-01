//! Where a copy redacted a file: the records the stream builds from what each redaction step
//! changed in the file as it was read.

use crate::args::RedacterType;
use crate::errors::AppError;
use crate::redacters::stream_redacter::OcrText;
use crate::redacters::text_changes::{TextChain, TEXT_DIFF_TIME_LIMIT};
use crate::redacters::{
    Detection, DetectionLabel, DetectionLocation, Redacted, Redacter, RedacterDataItem,
    RedacterDataItemContent, Redacters,
};
use crate::AppResult;
use std::collections::BTreeMap;
use std::io::Cursor;
use std::ops::Range;
use std::time::Instant;

/// What the stream keeps of one file across the steps that redact it, so that each step is
/// reported where it sits in the file as first read. A step that cannot be placed is reported
/// as a whole instead, and never fails the copy.
#[derive(Default)]
pub(crate) struct StreamRedactions {
    text: Option<TextChain>,
    cells: BTreeMap<CellPosition, TextChain>,
    /// Set once a step returned a table whose cells no longer pair up with the original's.
    table_shape_changed: bool,
    /// The pixel size of each image as first seen, by PDF page; `None` is an image file.
    image_sizes: BTreeMap<Option<usize>, ImageSize>,
    records: Vec<(RedacterType, Detection)>,
}

/// A cell of a table, counting from 0 the way the rows are indexed in memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct CellPosition {
    row: usize,
    column: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ImageSize {
    width: u32,
    height: u32,
}

impl StreamRedactions {
    /// Runs `redacter` on `item` and records what it changed, as part of `page` of a PDF when
    /// the item is a rendered page.
    pub(crate) async fn redact(
        &mut self,
        redacter: &Redacters<'_>,
        page: Option<usize>,
        item: RedacterDataItem,
    ) -> AppResult<RedacterDataItem> {
        let input = item.content.clone();
        let Redacted { item, replaced } = redacter.redact(item).await?;
        self.record(
            redacter.redacter_type(),
            page,
            &input,
            &item.content,
            replaced,
        );
        Ok(item)
    }

    /// Adds records built elsewhere, such as those of one OCR page.
    pub(crate) fn extend(&mut self, records: Vec<(RedacterType, Detection)>) {
        self.records.extend(records);
    }

    pub(crate) fn into_records(self) -> Vec<(RedacterType, Detection)> {
        self.records
    }

    fn record(
        &mut self,
        redacter: RedacterType,
        page: Option<usize>,
        input: &RedacterDataItemContent,
        output: &RedacterDataItemContent,
        replaced: Option<Vec<Detection>>,
    ) {
        match self.place(page, input, output, replaced) {
            Ok(placed) => self
                .records
                .extend(placed.into_iter().map(|detection| (redacter, detection))),
            Err(error) => {
                tracing::debug!(%redacter, %error, "reporting a redaction step as a whole");
                self.records.push((redacter, Detection::whole(page)));
            }
        }
    }

    fn place(
        &mut self,
        page: Option<usize>,
        input: &RedacterDataItemContent,
        output: &RedacterDataItemContent,
        replaced: Option<Vec<Detection>>,
    ) -> AppResult<Vec<Detection>> {
        let deadline = Instant::now() + TEXT_DIFF_TIME_LIMIT;
        match (input, output) {
            (RedacterDataItemContent::Value(before), RedacterDataItemContent::Value(after)) => {
                self.place_text(before, after, &replaced.unwrap_or_default(), deadline)
            }
            (
                RedacterDataItemContent::Table { rows: before, .. },
                RedacterDataItemContent::Table { rows: after, .. },
            ) => self.place_table(page, before, after, replaced, deadline),
            (
                RedacterDataItemContent::Image { data: before, .. },
                RedacterDataItemContent::Image { data: after, .. },
            ) => self.place_image(page, before, after, replaced),
            _ => Err(AppError::SystemError {
                message: "A redaction step changed the kind of the content".to_string(),
            }),
        }
    }

    fn place_text(
        &mut self,
        before: &str,
        after: &str,
        found: &[Detection],
        deadline: Instant,
    ) -> AppResult<Vec<Detection>> {
        if before == after {
            return Ok(Vec::new());
        }
        self.text
            .get_or_insert_with(|| TextChain::new(before.to_string()))
            .step(before, after, found, deadline)
    }

    /// Every changed cell is placed in its original text. A table whose cells no longer pair up
    /// with the original's is one record for the step, from then on.
    fn place_table(
        &mut self,
        page: Option<usize>,
        before: &[Vec<String>],
        after: &[Vec<String>],
        replaced: Option<Vec<Detection>>,
        deadline: Instant,
    ) -> AppResult<Vec<Detection>> {
        if before == after {
            return Ok(Vec::new());
        }
        if self.table_shape_changed || !pairs_up(before, after) {
            self.table_shape_changed = true;
            return Ok(vec![Detection::whole(page)]);
        }

        let mut found_by_cell: BTreeMap<CellPosition, Vec<Detection>> = BTreeMap::new();
        for detection in replaced.unwrap_or_default() {
            if let Some(position) = CellPosition::of(&detection.location) {
                found_by_cell.entry(position).or_default().push(detection);
            }
        }

        let mut placed = Vec::new();
        let mut failure = None;
        for (row, (cells_before, cells_after)) in before.iter().zip(after).enumerate() {
            for (column, (cell_before, cell_after)) in
                cells_before.iter().zip(cells_after).enumerate()
            {
                if cell_before == cell_after {
                    continue;
                }
                let position = CellPosition { row, column };
                let found = found_by_cell.remove(&position).unwrap_or_default();
                // Every changed cell takes its step, even after a failure, or the cell's
                // later steps would be mapped through a chain that skipped one.
                let step = self
                    .cells
                    .entry(position)
                    .or_insert_with(|| TextChain::new(cell_before.clone()))
                    .step(cell_before, cell_after, &found, deadline);
                match step {
                    Ok(in_cell) => placed.extend(
                        in_cell
                            .into_iter()
                            .map(|detection| position.locate(detection)),
                    ),
                    Err(error) => failure = failure.or(Some(error)),
                }
            }
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(placed),
        }
    }

    /// The boxes a redacter returned, when they are in the pixels of the image as first seen;
    /// otherwise the same detections without a place on the image. A redacter that returned no
    /// boxes is reported when it changed a pixel, without saying where.
    fn place_image(
        &mut self,
        page: Option<usize>,
        before: &[u8],
        after: &[u8],
        replaced: Option<Vec<Detection>>,
    ) -> AppResult<Vec<Detection>> {
        let size = ImageSize::of(before)?;
        let first_seen = *self.image_sizes.entry(page).or_insert(size);
        match replaced {
            Some(boxes) if size == first_seen => Ok(boxes
                .into_iter()
                .map(|detection| detection.on_page(page))
                .collect()),
            Some(boxes) => Ok(boxes
                .into_iter()
                .map(|detection| Detection {
                    location: whole_location(page),
                    ..detection
                })
                .collect()),
            None => {
                let (before, after) = (
                    image::load_from_memory(before)?.to_rgba8(),
                    image::load_from_memory(after)?.to_rgba8(),
                );
                Ok(if before == after {
                    Vec::new()
                } else {
                    vec![Detection::whole(page)]
                })
            }
        }
    }
}

/// Whether `after` has the rows and the cells per row of `before`.
fn pairs_up(before: &[Vec<String>], after: &[Vec<String>]) -> bool {
    before.len() == after.len()
        && before
            .iter()
            .zip(after)
            .all(|(cells_before, cells_after)| cells_before.len() == cells_after.len())
}

/// The coarsest place a redaction can have: a page of a PDF, or the file.
fn whole_location(page: Option<usize>) -> DetectionLocation {
    match page {
        Some(page) => DetectionLocation::Page { page },
        None => DetectionLocation::File,
    }
}

impl CellPosition {
    /// The cell a located detection is in; `None` when it is not in a cell.
    fn of(location: &DetectionLocation) -> Option<Self> {
        match *location {
            DetectionLocation::TableCell { row, column, .. } => Some(Self {
                row: row.checked_sub(1)?,
                column: column.checked_sub(1)?,
            }),
            _ => None,
        }
    }

    /// `detection`, a text range of this cell's text, as a range of the cell.
    fn locate(self, detection: Detection) -> Detection {
        match detection.location {
            DetectionLocation::Text { start, end, .. } => Detection {
                location: DetectionLocation::TableCell {
                    row: self.row + 1,
                    column: self.column + 1,
                    start,
                    end,
                },
                ..detection
            },
            _ => detection,
        }
    }
}

impl ImageSize {
    /// The size the header of the encoded image gives, without decoding its pixels.
    fn of(encoded: &[u8]) -> AppResult<Self> {
        let (width, height) = image::ImageReader::new(Cursor::new(encoded))
            .with_guessed_format()?
            .into_dimensions()?;
        Ok(Self { width, height })
    }
}

impl Detection {
    /// A redaction in a PDF page or a file, where on it is not known.
    fn whole(page: Option<usize>) -> Self {
        Self {
            label: DetectionLabel::unclassified(),
            location: whole_location(page),
            score: None,
            value: String::new(),
        }
    }

    /// This detection with its image box on `page`.
    fn on_page(mut self, page: Option<usize>) -> Self {
        if let DetectionLocation::Image {
            page: ref mut box_page,
            ..
        } = self.location
        {
            *box_page = page;
        }
        self
    }
}

impl OcrText {
    /// `records`, text ranges of this text, placed on the words that were drawn over. A record
    /// is boxed by the drawn words it overlaps, and dropped when it overlaps none. A drawn word
    /// no record overlaps is reported, with the drawn words next to it on its row, as data
    /// `last_redacter` redacted without saying what it was. `drawn` holds one flag per word.
    pub(crate) fn drawn_records(
        &self,
        records: Vec<(RedacterType, Detection)>,
        drawn: &[bool],
        page: Option<usize>,
        last_redacter: RedacterType,
    ) -> Vec<(RedacterType, Detection)> {
        let mut covered = vec![false; self.words.len()];
        let mut placed = Vec::with_capacity(records.len());
        for (redacter, detection) in records {
            let DetectionLocation::Text { start, end, .. } = detection.location else {
                placed.push((redacter, detection));
                continue;
            };
            let first = self.words.partition_point(|word| word.span.end <= start);
            let overlapped: Vec<usize> = (first..self.words.len())
                .take_while(|&index| self.words[index].span.start < end)
                .filter(|&index| drawn[index])
                .collect();
            let location =
                Self::box_around(overlapped.iter().map(|&index| &self.words[index]), page);
            match location {
                Some(location) => {
                    for index in overlapped {
                        covered[index] = true;
                    }
                    placed.push((
                        redacter,
                        Detection {
                            location,
                            ..detection
                        },
                    ));
                }
                None => tracing::debug!(
                    %redacter,
                    start,
                    end,
                    "dropping a redaction that overlaps no drawn word"
                ),
            }
        }

        let uncovered = (0..self.words.len()).filter(|&index| drawn[index] && !covered[index]);
        for run in self.rows_of(uncovered) {
            let words = &self.words[run.clone()];
            let (Some(first), Some(last)) = (words.first(), words.last()) else {
                continue;
            };
            if let Some(location) = Self::box_around(words.iter(), page) {
                placed.push((
                    last_redacter,
                    Detection {
                        label: DetectionLabel::unclassified(),
                        location,
                        score: None,
                        value: self.text[first.span.start..last.span.end].to_string(),
                    },
                ));
            }
        }
        placed
    }

    /// The runs of consecutive words, from the word `indices` in order, that follow each other
    /// on one row of the text.
    fn rows_of(&self, indices: impl Iterator<Item = usize>) -> Vec<Range<usize>> {
        let mut runs: Vec<Range<usize>> = Vec::new();
        for index in indices {
            match runs.last_mut() {
                Some(run)
                    if run.end == index
                        && !self.text
                            [self.words[index - 1].span.end..self.words[index].span.start]
                            .contains('\n') =>
                {
                    run.end = index + 1;
                }
                _ => runs.push(index..index + 1),
            }
        }
        runs
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common_types::TextImageCoords;
    use crate::redacters::{Score, TextLines};
    use image::ImageFormat;

    fn text(content: &str) -> RedacterDataItemContent {
        RedacterDataItemContent::Value(content.to_string())
    }

    fn table(rows: &[&[&str]]) -> RedacterDataItemContent {
        RedacterDataItemContent::Table {
            headers: Vec::new(),
            rows: rows
                .iter()
                .map(|row| row.iter().map(|cell| cell.to_string()).collect())
                .collect(),
        }
    }

    fn png(width: u32, height: u32, shade: u8) -> RedacterDataItemContent {
        let image = image::RgbImage::from_pixel(width, height, image::Rgb([shade; 3]));
        let mut encoded = Cursor::new(Vec::new());
        image
            .write_to(&mut encoded, ImageFormat::Png)
            .expect("encodes");
        RedacterDataItemContent::Image {
            mime_type: mime::IMAGE_PNG,
            data: encoded.into_inner().into(),
        }
    }

    fn span(content: &str, needle: &str, label: &str) -> Detection {
        let start = content.find(needle).expect("needle in content");
        TextLines::new(content)
            .detection(
                start..start + needle.len(),
                DetectionLabel::new(label),
                None,
            )
            .unwrap()
    }

    fn image_box(label: &str, value: &str) -> Detection {
        Detection {
            label: DetectionLabel::new(label),
            location: DetectionLocation::Image {
                page: None,
                x1: 1.0,
                y1: 2.0,
                x2: 30.0,
                y2: 40.0,
            },
            score: Score::try_from(0.5).ok(),
            value: value.to_string(),
        }
    }

    /// The location, label and value of each record, in order.
    fn summary(records: &[(RedacterType, Detection)]) -> Vec<(DetectionLocation, String, &str)> {
        records
            .iter()
            .map(|(_, detection)| {
                (
                    detection.location,
                    detection.label.to_string(),
                    detection.value.as_str(),
                )
            })
            .collect()
    }

    fn text_at(start: usize, end: usize, line: usize, column: usize) -> DetectionLocation {
        DetectionLocation::Text {
            start,
            end,
            line,
            column,
        }
    }

    #[test]
    fn a_rewrite_that_names_no_location_is_placed_by_what_changed() {
        let input = r#"{"user": "Ana Lima", "phone": "+44 20 7946 0958"}"#;
        let output = "{\n  \"user\": \"[REDACTED]\", \"phone\": \"[REDACTED]\"}";
        let mut recorder = StreamRedactions::default();

        recorder.record(
            RedacterType::GeminiLlm,
            None,
            &text(input),
            &text(output),
            None,
        );

        let records = recorder.into_records();
        assert!(records
            .iter()
            .all(|(redacter, _)| *redacter == RedacterType::GeminiLlm));
        assert_eq!(
            summary(&records),
            vec![
                (text_at(10, 18, 1, 11), "sensitive_data".into(), "Ana Lima"),
                (
                    text_at(31, 47, 1, 32),
                    "sensitive_data".into(),
                    "+44 20 7946 0958"
                ),
            ]
        );
    }

    #[test]
    fn a_later_step_is_placed_in_the_text_as_first_read() {
        let original = "Mail ana@x.io, ask Ana Lima.";
        let after_rules = "Mail [REDACTED], ask Ana Lima.";
        let after_model = "Mail [REDACTED], ask [REDACTED].";
        let mut recorder = StreamRedactions::default();

        recorder.record(
            RedacterType::LocalRules,
            None,
            &text(original),
            &text(after_rules),
            Some(vec![span(original, "ana@x.io", "email")]),
        );
        recorder.record(
            RedacterType::GeminiLlm,
            None,
            &text(after_rules),
            &text(after_model),
            None,
        );

        let records = recorder.into_records();
        assert_eq!(
            records
                .iter()
                .map(|(redacter, _)| *redacter)
                .collect::<Vec<_>>(),
            vec![RedacterType::LocalRules, RedacterType::GeminiLlm]
        );
        assert_eq!(
            summary(&records),
            vec![
                (text_at(5, 13, 1, 6), "email".into(), "ana@x.io"),
                (text_at(19, 27, 1, 20), "sensitive_data".into(), "Ana Lima"),
            ]
        );
    }

    #[test]
    fn a_step_that_changes_nothing_records_nothing() {
        let mut recorder = StreamRedactions::default();

        recorder.record(
            RedacterType::LocalRules,
            None,
            &text("nothing here"),
            &text("nothing here"),
            Some(vec![]),
        );

        assert!(recorder.into_records().is_empty());
    }

    #[test]
    fn a_table_is_placed_by_cell_and_keeps_the_label_of_the_detection() {
        let input = table(&[&["id", "contact"], &["John Smith", "j@x.io"]]);
        let output = table(&[&["id", "contact"], &["[REDACTED]", "[REDACTED]"]]);
        let email =
            Detection::in_table_cell(1, 1, "j@x.io", 0..6, DetectionLabel::new("email"), None)
                .unwrap();
        let mut recorder = StreamRedactions::default();

        recorder.record(
            RedacterType::GcpDlp,
            None,
            &input,
            &output,
            Some(vec![email]),
        );

        let cell = |row, column, end| DetectionLocation::TableCell {
            row,
            column,
            start: 0,
            end,
        };
        assert_eq!(
            summary(&recorder.into_records()),
            vec![
                (cell(2, 1, 10), "sensitive_data".into(), "John Smith"),
                (cell(2, 2, 6), "email".into(), "j@x.io"),
            ]
        );
    }

    #[test]
    fn a_table_whose_shape_changed_is_reported_whole_from_then_on() {
        let original = table(&[&["a@x.io", "b"], &["c", "d"]]);
        let one_row = table(&[&["[REDACTED]", "b"]]);
        let mut recorder = StreamRedactions::default();

        recorder.record(RedacterType::GcpDlp, None, &original, &one_row, None);
        recorder.record(
            RedacterType::LocalRules,
            None,
            &one_row,
            &table(&[&["[REDACTED]", "[REDACTED]"]]),
            None,
        );
        recorder.record(
            RedacterType::LocalNer,
            None,
            &table(&[&["[REDACTED]", "[REDACTED]"]]),
            &table(&[&["[REDACTED]", "[REDACTED]"]]),
            None,
        );

        let records = recorder.into_records();
        assert_eq!(
            records
                .iter()
                .map(|(redacter, detection)| (*redacter, detection.location))
                .collect::<Vec<_>>(),
            vec![
                (RedacterType::GcpDlp, DetectionLocation::File),
                (RedacterType::LocalRules, DetectionLocation::File),
            ]
        );
    }

    #[test]
    fn a_detection_outside_the_text_reports_the_step_as_a_whole() {
        let mut recorder = StreamRedactions::default();
        let outside = TextLines::new(&"x".repeat(100))
            .detection(90..95, DetectionLabel::new("email"), None)
            .unwrap();

        recorder.record(
            RedacterType::LocalRules,
            None,
            &text("mail a@b.io"),
            &text("mail [REDACTED]"),
            Some(vec![outside]),
        );

        assert_eq!(
            summary(&recorder.into_records()),
            vec![(DetectionLocation::File, "sensitive_data".into(), "")]
        );
    }

    #[test]
    fn an_image_that_cannot_be_read_reports_its_page_as_a_whole() {
        let mut recorder = StreamRedactions::default();
        let garbage = RedacterDataItemContent::Image {
            mime_type: mime::IMAGE_PNG,
            data: bytes::Bytes::from_static(b"not an image"),
        };

        recorder.record(RedacterType::GeminiLlm, Some(3), &garbage, &garbage, None);

        assert_eq!(
            summary(&recorder.into_records()),
            vec![(
                DetectionLocation::Page { page: 3 },
                "sensitive_data".into(),
                ""
            )]
        );
    }

    #[test]
    fn boxes_are_kept_at_the_size_the_image_was_first_seen_and_move_to_the_page_at_another() {
        let mut recorder = StreamRedactions::default();

        recorder.record(
            RedacterType::GeminiLlm,
            Some(2),
            &png(40, 50, 255),
            &png(20, 25, 255),
            Some(vec![image_box("sensitive_data", "Ana")]),
        );
        recorder.record(
            RedacterType::OpenAiLlm,
            Some(2),
            &png(20, 25, 255),
            &png(20, 25, 255),
            Some(vec![image_box("sensitive_data", "Lima")]),
        );

        let records = recorder.into_records();
        assert_eq!(
            summary(&records),
            vec![
                (
                    DetectionLocation::Image {
                        page: Some(2),
                        x1: 1.0,
                        y1: 2.0,
                        x2: 30.0,
                        y2: 40.0
                    },
                    "sensitive_data".into(),
                    "Ana"
                ),
                (
                    DetectionLocation::Page { page: 2 },
                    "sensitive_data".into(),
                    "Lima"
                ),
            ]
        );
        assert_eq!(records[1].1.score, Score::try_from(0.5).ok());
    }

    #[test]
    fn a_coordinate_pass_that_found_nothing_reports_nothing_though_the_copy_is_smaller() {
        let mut recorder = StreamRedactions::default();

        recorder.record(
            RedacterType::GeminiLlm,
            None,
            &png(40, 50, 255),
            &png(20, 25, 255),
            Some(vec![]),
        );

        assert!(recorder.into_records().is_empty());
    }

    #[test]
    fn an_image_edit_without_boxes_is_reported_only_when_a_pixel_changed() {
        let mut recorder = StreamRedactions::default();

        recorder.record(
            RedacterType::Presidio,
            None,
            &png(8, 8, 255),
            &png(8, 8, 255),
            None,
        );
        assert!(recorder.records.is_empty());

        recorder.record(
            RedacterType::Presidio,
            None,
            &png(8, 8, 255),
            &png(8, 8, 0),
            None,
        );
        recorder.record(
            RedacterType::Presidio,
            Some(4),
            &png(8, 8, 255),
            &png(8, 8, 0),
            None,
        );

        assert_eq!(
            summary(&recorder.into_records()),
            vec![
                (DetectionLocation::File, "sensitive_data".into(), ""),
                (
                    DetectionLocation::Page { page: 4 },
                    "sensitive_data".into(),
                    ""
                ),
            ]
        );
    }

    fn word(text: &str, line: usize, x1: f32, y1: f32) -> TextImageCoords {
        TextImageCoords {
            x1,
            y1,
            x2: x1 + 10.0,
            y2: y1 + 10.0,
            text: Some(text.to_string()),
            line,
        }
    }

    fn box_of(x1: f32, y1: f32, x2: f32, y2: f32, page: Option<usize>) -> DetectionLocation {
        DetectionLocation::Image {
            page,
            x1,
            y1,
            x2,
            y2,
        }
    }

    #[test]
    fn a_record_is_boxed_by_the_drawn_words_and_an_unexplained_drawn_word_is_reported() {
        let coords = [
            word("Pay", 0, 0.0, 0.0),
            word("Ana", 0, 20.0, 0.0),
            word("Lima", 0, 40.0, 0.0),
            word("now", 0, 60.0, 0.0),
            word("Call", 1, 0.0, 30.0),
            word("Ana", 1, 20.0, 30.0),
        ];
        let ocr = OcrText::from(coords.as_slice());
        let record = span(&ocr.text, "Ana Lima", "name");
        // The alignment drew over "Lima" and over the second "Ana", which no record names.
        let drawn = [false, false, true, false, false, true];

        let records = ocr.drawn_records(
            vec![(RedacterType::LocalNer, record)],
            &drawn,
            Some(2),
            RedacterType::LocalRules,
        );

        assert_eq!(
            records
                .iter()
                .map(|(redacter, _)| *redacter)
                .collect::<Vec<_>>(),
            vec![RedacterType::LocalNer, RedacterType::LocalRules]
        );
        assert_eq!(
            summary(&records),
            vec![
                (
                    box_of(40.0, 0.0, 50.0, 10.0, Some(2)),
                    "name".into(),
                    "Ana Lima"
                ),
                (
                    box_of(20.0, 30.0, 30.0, 40.0, Some(2)),
                    "sensitive_data".into(),
                    "Ana"
                ),
            ]
        );
    }

    #[test]
    fn a_record_that_overlaps_no_drawn_word_is_dropped() {
        let coords = [word("Ana", 0, 0.0, 0.0), word("Lima", 0, 20.0, 0.0)];
        let ocr = OcrText::from(coords.as_slice());
        let record = span(&ocr.text, "Ana", "name");

        let records = ocr.drawn_records(
            vec![(RedacterType::LocalNer, record)],
            &[false, false],
            None,
            RedacterType::LocalNer,
        );

        assert!(records.is_empty());
    }

    #[test]
    fn drawn_words_on_one_row_are_one_record_and_a_new_row_starts_another() {
        let coords = [
            word("one", 0, 0.0, 0.0),
            word("two", 0, 20.0, 0.0),
            word("three", 1, 0.0, 30.0),
        ];
        let ocr = OcrText::from(coords.as_slice());

        let records = ocr.drawn_records(
            Vec::new(),
            &[true, true, true],
            None,
            RedacterType::LocalRules,
        );

        assert_eq!(
            summary(&records),
            vec![
                (
                    box_of(0.0, 0.0, 30.0, 10.0, None),
                    "sensitive_data".into(),
                    "one two"
                ),
                (
                    box_of(0.0, 30.0, 10.0, 40.0, None),
                    "sensitive_data".into(),
                    "three"
                ),
            ]
        );
    }
}
