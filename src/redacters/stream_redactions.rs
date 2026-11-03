//! Where a copy redacted a file: the records the stream builds from what each redaction step
//! changed in the file as it was read.

use crate::args::RedacterType;
use crate::errors::AppError;
use crate::redacters::stream_redacter::OcrText;
use crate::redacters::text_changes::TextChain;
use crate::redacters::{
    Categories, Detection, DetectionLabel, DetectionLocation, Redacted, Redacter, RedacterDataItem,
    RedacterDataItemContent, Redacters, TextMask,
};
use crate::AppResult;
use std::collections::BTreeMap;
use std::io::Cursor;
use std::ops::Range;

/// What the stream keeps of one file across the steps that redact it, so that each step is
/// reported where it sits in the file as first read. A step that cannot be placed is reported
/// as a whole instead, and never fails the copy.
pub(crate) struct StreamRedactions {
    recording: bool,
    text: Placement<Option<TextChain>>,
    cells: Placement<BTreeMap<CellPosition, TextChain>>,
    /// The pixel size of each image as first seen, by PDF page; `None` is an image file.
    image_sizes: BTreeMap<Option<usize>, ImageSize>,
    records: Vec<(RedacterType, Detection)>,
}

/// The chains that place the steps of one kind of content, until a step cannot be placed: a
/// redacter that does not say where it replaced, or that replaced more than it said. A later
/// step cannot be mapped through that one, so from then on every step is the whole.
enum Placement<Tracked> {
    Chains(Tracked),
    Whole,
}

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
    pub(crate) fn new(recording: bool) -> Self {
        Self {
            recording,
            text: Placement::Chains(None),
            cells: Placement::Chains(BTreeMap::new()),
            image_sizes: BTreeMap::new(),
            records: Vec::new(),
        }
    }

    /// Runs `redacter` on `item`, limited to `categories`, and records what it changed, as part
    /// of `page` of a PDF when the item is a rendered page.
    pub(crate) async fn redact(
        &mut self,
        redacter: &Redacters<'_>,
        page: Option<usize>,
        item: RedacterDataItem,
        categories: Option<&Categories>,
    ) -> AppResult<RedacterDataItem> {
        if !self.recording {
            return Ok(redacter.redact_in(item, categories).await?.item);
        }
        let input = item.content.clone();
        let Redacted { item, replaced } = redacter.redact_in(item, categories).await?;
        self.record(
            redacter.redacter_type(),
            redacter.text_mask(),
            page,
            &input,
            &item.content,
            replaced,
        );
        Ok(item)
    }

    pub(crate) fn extend(&mut self, records: Vec<(RedacterType, Detection)>) {
        self.records.extend(records);
    }

    pub(crate) fn into_records(self) -> Vec<(RedacterType, Detection)> {
        self.records
    }

    fn record(
        &mut self,
        redacter: RedacterType,
        mask: TextMask,
        page: Option<usize>,
        input: &RedacterDataItemContent,
        output: &RedacterDataItemContent,
        replaced: Option<Vec<Detection>>,
    ) {
        match self.place(mask, page, input, output, replaced) {
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
        mask: TextMask,
        page: Option<usize>,
        input: &RedacterDataItemContent,
        output: &RedacterDataItemContent,
        replaced: Option<Vec<Detection>>,
    ) -> AppResult<Vec<Detection>> {
        match (input, output) {
            (RedacterDataItemContent::Value(before), RedacterDataItemContent::Value(after)) => {
                self.place_text(mask, page, before, after, replaced.as_deref())
            }
            (
                RedacterDataItemContent::Table { rows: before, .. },
                RedacterDataItemContent::Table { rows: after, .. },
            ) => self.place_table(mask, page, before, after, replaced),
            (
                RedacterDataItemContent::Image { data: before, .. },
                RedacterDataItemContent::Image { data: after, .. },
            ) => self.place_image(page, before, after, replaced),
            _ => Err(AppError::SystemError {
                message: "A redaction step changed the kind of the content".to_string(),
            }),
        }
    }

    /// A step without locations that only moves whitespace (a model dropping the final newline)
    /// is not a redaction and is not reported, but the text can no longer be mapped through it.
    fn place_text(
        &mut self,
        mask: TextMask,
        page: Option<usize>,
        before: &str,
        after: &str,
        replaced: Option<&[Detection]>,
    ) -> AppResult<Vec<Detection>> {
        if before == after {
            return Ok(Vec::new());
        }
        let placed = match (&mut self.text, replaced) {
            (Placement::Chains(chain), Some(found)) => chain
                .get_or_insert_with(|| TextChain::new(before.to_string()))
                .step(before, after, found, mask),
            _ => {
                self.text = Placement::Whole;
                let only_whitespace = before.split_whitespace().eq(after.split_whitespace());
                return Ok(if replaced.is_none() && only_whitespace {
                    Vec::new()
                } else {
                    vec![Detection::whole(page)]
                });
            }
        };
        if placed.is_err() {
            self.text = Placement::Whole;
        }
        placed
    }

    /// Every changed cell is placed in its original text. A table whose cells no longer pair up
    /// with the original's is one record for the step, from then on.
    fn place_table(
        &mut self,
        mask: TextMask,
        page: Option<usize>,
        before: &[Vec<String>],
        after: &[Vec<String>],
        replaced: Option<Vec<Detection>>,
    ) -> AppResult<Vec<Detection>> {
        if before == after {
            return Ok(Vec::new());
        }
        let placed = match (&mut self.cells, replaced) {
            (Placement::Chains(chains), Some(replaced)) if pairs_up(before, after) => {
                Self::place_cells(chains, mask, before, after, replaced)
            }
            _ => {
                self.cells = Placement::Whole;
                return Ok(vec![Detection::whole(page)]);
            }
        };
        if placed.is_err() {
            self.cells = Placement::Whole;
        }
        placed
    }

    fn place_cells(
        chains: &mut BTreeMap<CellPosition, TextChain>,
        mask: TextMask,
        before: &[Vec<String>],
        after: &[Vec<String>],
        replaced: Vec<Detection>,
    ) -> AppResult<Vec<Detection>> {
        let mut found_by_cell: BTreeMap<CellPosition, Vec<Detection>> = BTreeMap::new();
        for detection in replaced {
            if let Some(position) = CellPosition::of(&detection.location) {
                found_by_cell.entry(position).or_default().push(detection);
            }
        }

        let mut placed = Vec::new();
        for (row, (cells_before, cells_after)) in before.iter().zip(after).enumerate() {
            for (column, (cell_before, cell_after)) in
                cells_before.iter().zip(cells_after).enumerate()
            {
                if cell_before == cell_after {
                    continue;
                }
                let position = CellPosition { row, column };
                let found = found_by_cell.remove(&position).unwrap_or_default();
                let in_cell = chains
                    .entry(position)
                    .or_insert_with(|| TextChain::new(cell_before.clone()))
                    .step(cell_before, cell_after, &found, mask)?;
                placed.extend(
                    in_cell
                        .into_iter()
                        .map(|detection| position.locate(detection)),
                );
            }
        }
        Ok(placed)
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
                    location: DetectionLocation::whole(page),
                    ..detection
                })
                .collect()),
            None if before == after => Ok(Vec::new()),
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

fn pairs_up(before: &[Vec<String>], after: &[Vec<String>]) -> bool {
    before.len() == after.len()
        && before
            .iter()
            .zip(after)
            .all(|(cells_before, cells_after)| cells_before.len() == cells_after.len())
}

impl DetectionLocation {
    /// The coarsest place a redaction can have: a page of a PDF, or the file.
    fn whole(page: Option<usize>) -> Self {
        match page {
            Some(page) => Self::Page { page },
            None => Self::File,
        }
    }
}

impl CellPosition {
    fn of(location: &DetectionLocation) -> Option<Self> {
        match *location {
            DetectionLocation::TableCell { row, column, .. } => Some(Self {
                row: row.checked_sub(1)?,
                column: column.checked_sub(1)?,
            }),
            _ => None,
        }
    }

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
    fn whole(page: Option<usize>) -> Self {
        Self {
            label: DetectionLabel::unclassified(),
            location: DetectionLocation::whole(page),
            score: None,
            value: String::new(),
        }
    }

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
    fn a_later_step_is_placed_in_the_text_as_first_read() {
        let original = "Mail ana@x.io, ask Ana Lima.";
        let after_rules = "Mail [REDACTED], ask Ana Lima.";
        let after_model = "Mail [REDACTED], ask [REDACTED].";
        let mut recorder = StreamRedactions::new(true);

        recorder.record(
            RedacterType::LocalRules,
            TextMask::Token,
            None,
            &text(original),
            &text(after_rules),
            Some(vec![span(original, "ana@x.io", "email")]),
        );
        recorder.record(
            RedacterType::LocalNer,
            TextMask::Token,
            None,
            &text(after_rules),
            &text(after_model),
            Some(vec![span(after_rules, "Ana Lima", "name")]),
        );

        let records = recorder.into_records();
        assert_eq!(
            records
                .iter()
                .map(|(redacter, _)| *redacter)
                .collect::<Vec<_>>(),
            vec![RedacterType::LocalRules, RedacterType::LocalNer]
        );
        assert_eq!(
            summary(&records),
            vec![
                (text_at(5, 13, 1, 6), "email".into(), "ana@x.io"),
                (text_at(19, 27, 1, 20), "name".into(), "Ana Lima"),
            ]
        );
    }

    #[test]
    fn a_step_without_locations_is_the_file_and_so_is_every_step_after_it() {
        let original = "Mail ana@x.io, ask Ana Lima.";
        let after_model = "Mail [REDACTED], ask Ana Lima.";
        let after_rules = "Mail [REDACTED], ask [REDACTED].";
        let mut recorder = StreamRedactions::new(true);

        recorder.record(
            RedacterType::GeminiLlm,
            TextMask::Token,
            None,
            &text(original),
            &text(after_model),
            None,
        );
        recorder.record(
            RedacterType::LocalRules,
            TextMask::Token,
            None,
            &text(after_model),
            &text(after_rules),
            Some(vec![span(after_model, "Ana Lima", "name")]),
        );

        assert_eq!(
            recorder
                .into_records()
                .into_iter()
                .map(|(redacter, detection)| (redacter, detection.location))
                .collect::<Vec<_>>(),
            vec![
                (RedacterType::GeminiLlm, DetectionLocation::File),
                (RedacterType::LocalRules, DetectionLocation::File),
            ]
        );
    }

    #[test]
    fn a_step_without_locations_that_changes_only_whitespace_reports_nothing() {
        let mut recorder = StreamRedactions::new(true);

        recorder.record(
            RedacterType::GcpVertexAi,
            TextMask::Token,
            None,
            &text("Meet on 12 May 2024.\n"),
            &text("Meet on 12 May 2024."),
            None,
        );

        assert!(recorder.into_records().is_empty());
    }

    #[test]
    fn values_a_step_masked_are_recorded_where_it_found_them() {
        let block = "-----BEGIN PRIVATE KEY-----\n-----END PRIVATE KEY-----";
        let original = format!("{block}\n{block}");
        let found = vec![
            TextLines::new(&original)
                .detection(
                    0..block.len(),
                    DetectionLabel::new("private-key-block"),
                    None,
                )
                .unwrap(),
            TextLines::new(&original)
                .detection(
                    block.len() + 1..original.len(),
                    DetectionLabel::new("private-key-block"),
                    None,
                )
                .unwrap(),
        ];
        let masked = Detection::mask(&original, &found, TextMask::Token).unwrap();
        let mut recorder = StreamRedactions::new(true);

        recorder.record(
            RedacterType::LocalRules,
            TextMask::Token,
            None,
            &text(&original),
            &text(&masked),
            Some(found.clone()),
        );

        let records = recorder.into_records();
        assert_eq!(
            records
                .iter()
                .map(|(_, detection)| detection.location)
                .collect::<Vec<_>>(),
            found
                .iter()
                .map(|detection| detection.location)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn a_step_whose_output_is_not_its_masked_values_is_the_file() {
        let original = "Mail ana@x.io, ask Ana Lima.";
        let rewritten = "Mail [REDACTED], ask Ana Lima.";
        let mut recorder = StreamRedactions::new(true);

        recorder.record(
            RedacterType::LocalRules,
            TextMask::EveryCharacter,
            None,
            &text(original),
            &text(rewritten),
            Some(vec![span(original, "ana@x.io", "email")]),
        );

        assert_eq!(
            summary(&recorder.into_records()),
            vec![(DetectionLocation::File, "sensitive_data".into(), "")]
        );
    }

    #[test]
    fn a_table_is_placed_by_cell_and_keeps_the_label_of_the_detection() {
        let input = table(&[&["id", "contact"], &["John Smith", "j@x.io"]]);
        let output = table(&[&["id", "contact"], &["[REDACTED]", "[REDACTED]"]]);
        let person = Detection::in_table_cell(
            1,
            0,
            "John Smith",
            0..10,
            DetectionLabel::new("person"),
            None,
        )
        .unwrap();
        let email =
            Detection::in_table_cell(1, 1, "j@x.io", 0..6, DetectionLabel::new("email"), None)
                .unwrap();
        let mut recorder = StreamRedactions::new(true);

        recorder.record(
            RedacterType::GcpDlp,
            TextMask::Token,
            None,
            &input,
            &output,
            Some(vec![person, email]),
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
                (cell(2, 1, 10), "person".into(), "John Smith"),
                (cell(2, 2, 6), "email".into(), "j@x.io"),
            ]
        );
    }

    #[test]
    fn a_table_step_without_locations_is_the_file() {
        let mut recorder = StreamRedactions::new(true);

        recorder.record(
            RedacterType::GcpDlp,
            TextMask::Token,
            Some(2),
            &table(&[&["id", "contact"], &["John Smith", "j@x.io"]]),
            &table(&[&["id", "contact"], &["[REDACTED]", "[REDACTED]"]]),
            None,
        );

        assert_eq!(
            summary(&recorder.into_records()),
            vec![(
                DetectionLocation::Page { page: 2 },
                "sensitive_data".into(),
                ""
            )]
        );
    }

    #[test]
    fn a_table_whose_shape_changed_is_reported_whole_from_then_on() {
        let original = table(&[&["a@x.io", "b"], &["c", "d"]]);
        let one_row = table(&[&["[REDACTED]", "b"]]);
        let mut recorder = StreamRedactions::new(true);

        recorder.record(
            RedacterType::GcpDlp,
            TextMask::Token,
            None,
            &original,
            &one_row,
            None,
        );
        recorder.record(
            RedacterType::LocalRules,
            TextMask::Token,
            None,
            &one_row,
            &table(&[&["[REDACTED]", "[REDACTED]"]]),
            None,
        );
        recorder.record(
            RedacterType::LocalNer,
            TextMask::Token,
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
        let mut recorder = StreamRedactions::new(true);
        let outside = TextLines::new(&"x".repeat(100))
            .detection(90..95, DetectionLabel::new("email"), None)
            .unwrap();

        recorder.record(
            RedacterType::LocalRules,
            TextMask::Token,
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
        let mut recorder = StreamRedactions::new(true);
        let garbage = RedacterDataItemContent::Image {
            mime_type: mime::IMAGE_PNG,
            data: bytes::Bytes::from_static(b"not an image"),
        };

        recorder.record(
            RedacterType::GeminiLlm,
            TextMask::Token,
            Some(3),
            &garbage,
            &garbage,
            None,
        );

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
        let mut recorder = StreamRedactions::new(true);

        recorder.record(
            RedacterType::GeminiLlm,
            TextMask::Token,
            Some(2),
            &png(40, 50, 255),
            &png(20, 25, 255),
            Some(vec![image_box("sensitive_data", "Ana")]),
        );
        recorder.record(
            RedacterType::OpenAiLlm,
            TextMask::Token,
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
        let mut recorder = StreamRedactions::new(true);

        recorder.record(
            RedacterType::GeminiLlm,
            TextMask::Token,
            None,
            &png(40, 50, 255),
            &png(20, 25, 255),
            Some(vec![]),
        );

        assert!(recorder.into_records().is_empty());
    }

    #[test]
    fn an_image_edit_without_boxes_is_reported_only_when_a_pixel_changed() {
        let mut recorder = StreamRedactions::new(true);

        recorder.record(
            RedacterType::Presidio,
            TextMask::Token,
            None,
            &png(8, 8, 255),
            &png(8, 8, 255),
            None,
        );
        assert!(recorder.records.is_empty());

        recorder.record(
            RedacterType::Presidio,
            TextMask::Token,
            None,
            &png(8, 8, 255),
            &png(8, 8, 0),
            None,
        );
        recorder.record(
            RedacterType::Presidio,
            TextMask::Token,
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

    #[test]
    fn an_image_returned_byte_for_byte_is_unchanged_without_decoding_it() {
        let RedacterDataItemContent::Image { mime_type, data } = png(8, 8, 255) else {
            unreachable!("png makes an image");
        };
        let header_only = RedacterDataItemContent::Image {
            mime_type,
            data: data.slice(..data.len() - 14),
        };
        let mut recorder = StreamRedactions::new(true);

        recorder.record(
            RedacterType::Presidio,
            TextMask::Token,
            None,
            &header_only,
            &header_only,
            None,
        );

        assert!(recorder.into_records().is_empty());
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
