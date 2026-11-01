//! Where a redaction chain changed a text, found by diffing each step's input against its
//! output, and mapped back to the text as first read.

use crate::errors::AppError;
use crate::redacters::{Detection, DetectionLabel, Score, TextLines};
use crate::AppResult;
use similar::{capture_diff_slices_deadline, Algorithm, DiffTag};
use std::ops::Range;
use std::time::{Duration, Instant};

/// How long one step's diff may run before it settles for coarser changes.
pub(crate) const TEXT_DIFF_TIME_LIMIT: Duration = Duration::from_secs(5);

/// One change a step made to a text: `input` of what it read became `output` of what it
/// returned. Byte ranges on character boundaries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TextChange {
    pub input: Range<usize>,
    pub output: Range<usize>,
}

/// Every change one step made to a text, in order on both sides.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TextChanges(Vec<TextChange>);

impl TextChanges {
    /// The changes from `input` to `output`. Both are cut into runs of letters and digits, runs
    /// of whitespace and single other characters, so that two values in one minified line, or
    /// in one URL, are two changes. Each change is then narrowed by the characters its two
    /// sides share at both ends, which keeps `hunter2` out of `password=hunter2` and finds a
    /// number inside a run of Japanese text.
    ///
    /// Past `deadline` the diff is coarser, and still covers everything that changed.
    pub(crate) fn between(input: &str, output: &str, deadline: Instant) -> Self {
        if input == output {
            return Self(Vec::new());
        }
        let old = Tokens::new(input);
        let new = Tokens::new(output);
        let operations = capture_diff_slices_deadline(
            Algorithm::Myers,
            &old.pieces,
            &new.pieces,
            Some(deadline),
        );

        let mut unequal: Vec<(Range<usize>, Range<usize>)> = Vec::new();
        for operation in operations {
            let (tag, old_tokens, new_tokens) = operation.as_tag_tuple();
            if tag == DiffTag::Equal {
                continue;
            }
            let (old_range, new_range) = (old.byte_range(old_tokens), new.byte_range(new_tokens));
            match unequal.last_mut() {
                Some((last_old, last_new))
                    if last_old.end == old_range.start && last_new.end == new_range.start =>
                {
                    last_old.end = old_range.end;
                    last_new.end = new_range.end;
                }
                _ => unequal.push((old_range, new_range)),
            }
        }

        Self(
            unequal
                .into_iter()
                .filter_map(|(old_range, new_range)| {
                    TextChange::narrowed(input, output, old_range, new_range)
                })
                .collect(),
        )
    }

    /// `range` of the output as the range of the input it came from. An end inside a change
    /// widens to that change's whole input range; a position outside every change shifts by
    /// the length difference of the changes before it. The result is empty only when `range`
    /// was.
    pub(crate) fn to_input(&self, range: Range<usize>) -> Range<usize> {
        let start = {
            let before = self
                .0
                .partition_point(|change| change.output.end <= range.start);
            match self.0.get(before) {
                Some(change) if change.output.start < range.start => change.input.start,
                _ => self.shifted(before, range.start),
            }
        };
        let end = {
            let before = self
                .0
                .partition_point(|change| change.output.end < range.end);
            match self.0.get(before) {
                Some(change) if change.output.start < range.end => change.input.end,
                _ => self.shifted(before, range.end),
            }
        };
        start..end.max(start)
    }

    /// The input position of output position `position`, which lies after the first `before`
    /// changes and outside the rest.
    fn shifted(&self, before: usize, position: usize) -> usize {
        match before.checked_sub(1).map(|last| &self.0[last]) {
            Some(change) => position - change.output.end + change.input.end,
            None => position,
        }
    }

    /// The input range of every change that replaced something other than whitespace, trimmed
    /// of the whitespace at both ends. A reflow and a pure insertion are changes, and are not
    /// redactions.
    fn redactions<'a>(&'a self, input: &'a str) -> impl Iterator<Item = Range<usize>> + 'a {
        self.0.iter().filter_map(|change| {
            let replaced = &input[change.input.clone()];
            let trimmed = replaced.trim();
            let start = change.input.start + (replaced.len() - replaced.trim_start().len());
            (!trimmed.is_empty()).then(|| start..start + trimmed.len())
        })
    }
}

impl TextChange {
    /// The change from `input[old]` to `output[new]` without the characters both sides share
    /// at the start and at the end; `None` when nothing is left.
    fn narrowed(input: &str, output: &str, old: Range<usize>, new: Range<usize>) -> Option<Self> {
        let (replaced, replacement) = (&input[old.clone()], &output[new.clone()]);
        let same_start = shared_length(replaced.chars(), replacement.chars());
        let same_end = shared_length(
            replaced[same_start..].chars().rev(),
            replacement[same_start..].chars().rev(),
        );
        let change = Self {
            input: old.start + same_start..old.end - same_end,
            output: new.start + same_start..new.end - same_end,
        };
        (!change.input.is_empty() || !change.output.is_empty()).then_some(change)
    }
}

/// The byte length of the characters `a` and `b` agree on from their start.
fn shared_length(a: impl Iterator<Item = char>, b: impl Iterator<Item = char>) -> usize {
    a.zip(b)
        .take_while(|(a, b)| a == b)
        .map(|(character, _)| character.len_utf8())
        .sum()
}

/// A text cut into the pieces the diff compares.
struct Tokens<'a> {
    pieces: Vec<&'a str>,
    /// The byte offset of every piece, then the length of the text.
    starts: Vec<usize>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TokenClass {
    Word,
    Space,
    Other,
}

impl TokenClass {
    fn of(character: char) -> Self {
        if character.is_alphanumeric() {
            Self::Word
        } else if character.is_whitespace() {
            Self::Space
        } else {
            Self::Other
        }
    }
}

impl<'a> Tokens<'a> {
    fn new(text: &'a str) -> Self {
        let mut pieces = Vec::new();
        let mut starts = Vec::new();
        let mut characters = text.char_indices().peekable();
        while let Some((start, first)) = characters.next() {
            let class = TokenClass::of(first);
            let mut end = start + first.len_utf8();
            if class != TokenClass::Other {
                while let Some(&(next, character)) = characters.peek() {
                    if TokenClass::of(character) != class {
                        break;
                    }
                    end = next + character.len_utf8();
                    characters.next();
                }
            }
            pieces.push(&text[start..end]);
            starts.push(start);
        }
        starts.push(text.len());
        Self { pieces, starts }
    }

    fn byte_range(&self, tokens: Range<usize>) -> Range<usize> {
        self.starts[tokens.start]..self.starts[tokens.end]
    }
}

/// The steps one text went through in a redaction chain, so each step's changes are placed in
/// the text as first read.
pub(crate) struct TextChain {
    original: String,
    steps: Vec<TextChanges>,
}

/// A detection of one step, with its range in the original text.
struct Located<'a> {
    range: Range<usize>,
    detection: &'a Detection,
}

impl TextChain {
    pub(crate) fn new(original: String) -> Self {
        Self {
            original,
            steps: Vec::new(),
        }
    }

    /// What one step replaced, placed in the original text and labelled. `found` holds the
    /// step's detections located in `input`: text ranges, or the ranges of the one cell this
    /// chain covers. Every returned detection has a `DetectionLocation::Text` in the original
    /// and its value copied from it.
    ///
    /// A change takes the label and score of the detection that overlaps it most, and widens
    /// to the union with every detection that overlaps it, so a mask that shares a character
    /// with its value, or a placeholder that splits a value in two, still reports the whole
    /// value. A change no detection overlaps is [`DetectionLabel::unclassified`]; a detection
    /// that overlaps no change is dropped, since the copy does not differ there.
    ///
    /// `output` must be the `input` of the next call. The step counts in the chain also when
    /// it returns an error.
    pub(crate) fn step(
        &mut self,
        input: &str,
        output: &str,
        found: &[Detection],
        deadline: Instant,
    ) -> AppResult<Vec<Detection>> {
        let changes = TextChanges::between(input, output, deadline);
        let placed = self.place(input, &changes, found);
        self.steps.push(changes);
        placed
    }

    fn place(
        &self,
        input: &str,
        changes: &TextChanges,
        found: &[Detection],
    ) -> AppResult<Vec<Detection>> {
        let located = found
            .iter()
            .filter_map(|detection| Some((detection.location.byte_range()?, detection)))
            .map(|(range, detection)| {
                if input.get(range.clone()).is_none() {
                    return Err(AppError::DetectionOutsideContent {
                        start: range.start,
                        end: range.end,
                    });
                }
                Ok(Located {
                    range: self.to_original(range),
                    detection,
                })
            })
            .collect::<AppResult<Vec<_>>>()?;

        let mut spans: Vec<Span> = changes
            .redactions(input)
            .map(|range| self.to_original(range))
            .filter(|range| !range.is_empty())
            .map(|change| Span::widened_by(change, &located))
            .collect();
        spans.sort_by_key(|span| (span.range.start, span.range.end));

        let mut merged: Vec<Span> = Vec::new();
        for span in spans {
            match merged.last_mut() {
                Some(last) if span.range.start < last.range.end => last.absorb(span),
                _ => merged.push(span),
            }
        }

        let lines = TextLines::new(&self.original);
        merged
            .into_iter()
            .map(|span| {
                let (label, score) = span.label(&located);
                lines.detection(span.range, label, score)
            })
            .collect()
    }

    /// `range` of the text a step returned as the range of the original it came from.
    fn to_original(&self, range: Range<usize>) -> Range<usize> {
        self.steps
            .iter()
            .rev()
            .fold(range, |range, changes| changes.to_input(range))
    }
}

/// One redaction in the original text: the ranges the changes cover, and the range they
/// widen to with the detections that overlap them.
struct Span {
    range: Range<usize>,
    changes: Vec<Range<usize>>,
}

impl Span {
    fn widened_by(change: Range<usize>, located: &[Located]) -> Self {
        let range = located
            .iter()
            .filter(|candidate| overlap(&candidate.range, &change) > 0)
            .fold(change.clone(), |range, candidate| {
                range.start.min(candidate.range.start)..range.end.max(candidate.range.end)
            });
        Self {
            range,
            changes: vec![change],
        }
    }

    fn absorb(&mut self, other: Self) {
        self.range.end = self.range.end.max(other.range.end);
        self.changes.extend(other.changes);
    }

    /// The label and score of the detection that overlaps the most: the change when the span
    /// is one change, the whole span when changes were joined. The first detection wins a tie.
    fn label(&self, located: &[Located]) -> (DetectionLabel, Option<Score>) {
        let measured = match self.changes.as_slice() {
            [change] => change,
            _ => &self.range,
        };
        let mut best: Option<(usize, &Detection)> = None;
        for candidate in located {
            let overlapped = overlap(&candidate.range, measured);
            if overlapped > best.map_or(0, |(most, _)| most) {
                best = Some((overlapped, candidate.detection));
            }
        }
        best.map_or((DetectionLabel::unclassified(), None), |(_, detection)| {
            (detection.label.clone(), detection.score)
        })
    }
}

/// How many bytes `a` and `b` have in common.
fn overlap(a: &Range<usize>, b: &Range<usize>) -> usize {
    a.end.min(b.end).saturating_sub(a.start.max(b.start))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::AppError;
    use crate::redacters::{DetectionLabel, DetectionLocation, Score, TextLines};

    fn deadline() -> Instant {
        Instant::now() + TEXT_DIFF_TIME_LIMIT
    }

    fn score(value: f32) -> Score {
        Score::try_from(value).unwrap()
    }

    /// A detection of `range` in `text` as a redacter located it.
    fn found(text: &str, range: Range<usize>, label: &str, score: Option<Score>) -> Detection {
        TextLines::new(text)
            .detection(range, DetectionLabel::new(label), score)
            .unwrap()
    }

    /// The range of the first `needle` in `text`.
    fn range_of(text: &str, needle: &str) -> Range<usize> {
        let start = text.find(needle).unwrap();
        start..start + needle.len()
    }

    /// The text range, label and value of each detection, in order.
    fn placed(detections: &[Detection]) -> Vec<(Range<usize>, String, &str)> {
        detections
            .iter()
            .map(|detection| match detection.location {
                DetectionLocation::Text { start, end, .. } => (
                    start..end,
                    detection.label.to_string(),
                    detection.value.as_str(),
                ),
                other => panic!("a text location is expected, got {other:?}"),
            })
            .collect()
    }

    fn step_once(input: &str, output: &str, found: &[Detection]) -> AppResult<Vec<Detection>> {
        TextChain::new(input.to_string()).step(input, output, found, deadline())
    }

    #[test]
    fn a_replaced_value_is_placed_at_its_exact_span() {
        let input = "Hi team,\nMail john.smith@example.com or call +1 (555) 123-4567.";
        let output = "Hi team,\nMail [REDACTED] or call [REDACTED].";
        let email = range_of(input, "john.smith@example.com");
        let phone = range_of(input, "+1 (555) 123-4567");
        let detections = [
            found(input, email.clone(), "email", None),
            found(input, phone.clone(), "phone-international", None),
        ];

        let records = step_once(input, output, &detections).unwrap();

        assert_eq!(
            placed(&records),
            vec![
                (email, "email".to_string(), "john.smith@example.com"),
                (
                    phone,
                    "phone-international".to_string(),
                    "+1 (555) 123-4567"
                ),
            ]
        );
        assert_eq!(
            records[0].location,
            DetectionLocation::Text {
                start: 14,
                end: 36,
                line: 2,
                column: 6
            }
        );
    }

    #[test]
    fn a_value_inside_a_longer_token_is_narrowed_by_the_shared_prefix_and_suffix() {
        let input = "password=hunter2";
        let output = "password=[REDACTED]";

        let records = step_once(input, output, &[]).unwrap();

        assert_eq!(
            placed(&records),
            vec![(9..16, "sensitive_data".to_string(), "hunter2")]
        );
    }

    #[test]
    fn a_run_without_separators_is_narrowed_to_the_changed_characters() {
        let input = "電話は09012345678です";
        let output = "電話は[REDACTED]です";

        let records = step_once(input, output, &[]).unwrap();

        assert_eq!(
            placed(&records),
            vec![(
                range_of(input, "09012345678"),
                "sensitive_data".to_string(),
                "09012345678"
            )]
        );
    }

    #[test]
    fn ranges_stay_on_character_boundaries_and_columns_count_characters() {
        let input = "Grüße 😀 ana@x.io ÜberAnna";
        let output = "Grüße 😀 [REDACTED] Über[REDACTED]";

        let records = step_once(input, output, &[]).unwrap();

        assert_eq!(
            placed(&records),
            vec![
                (
                    range_of(input, "ana@x.io"),
                    "sensitive_data".to_string(),
                    "ana@x.io"
                ),
                (
                    range_of(input, "Anna"),
                    "sensitive_data".to_string(),
                    "Anna"
                ),
            ]
        );
        assert!(matches!(
            records[0].location,
            DetectionLocation::Text { column: 9, .. }
        ));
    }

    #[test]
    fn a_whitespace_only_change_is_not_a_redaction_but_still_shifts_later_positions() {
        let input = r#"{"user": "Ana Lima", "phone": "+44 20 7946 0958"}"#;
        let output = "{\n  \"user\": \"[REDACTED]\", \"phone\": \"[REDACTED]\"}";

        let records = step_once(input, output, &[]).unwrap();

        assert_eq!(
            placed(&records),
            vec![
                (10..18, "sensitive_data".to_string(), "Ana Lima"),
                (31..47, "sensitive_data".to_string(), "+44 20 7946 0958"),
            ]
        );
    }

    #[test]
    fn two_values_in_one_minified_json_line_are_placed_separately() {
        let input = r#"{"name":"John Smith","email":"j@x.io"}"#;
        let output = r#"{"name":"[REDACTED]","email":"[REDACTED]"}"#;

        let records = step_once(input, output, &[]).unwrap();

        assert_eq!(
            placed(&records),
            vec![
                (
                    range_of(input, "John Smith"),
                    "sensitive_data".to_string(),
                    "John Smith"
                ),
                (
                    range_of(input, "j@x.io"),
                    "sensitive_data".to_string(),
                    "j@x.io"
                ),
            ]
        );
    }

    #[test]
    fn a_step_that_changes_nothing_reports_nothing() {
        let text = "nothing to hide";
        let detections = [found(text, 0..7, "word", None)];

        assert!(step_once(text, text, &detections).unwrap().is_empty());
    }

    #[test]
    fn a_later_step_is_placed_in_the_original_past_an_earlier_replacement() {
        let original = "Mail ana@x.io, ask Ana Lima.";
        let after_rules = "Mail [REDACTED], ask Ana Lima.";
        let after_model = "Mail [REDACTED], ask [REDACTED].";
        let mut chain = TextChain::new(original.to_string());

        let first = chain
            .step(
                original,
                after_rules,
                &[found(
                    original,
                    range_of(original, "ana@x.io"),
                    "email",
                    None,
                )],
                deadline(),
            )
            .unwrap();
        let second = chain
            .step(after_rules, after_model, &[], deadline())
            .unwrap();

        assert_eq!(
            placed(&first),
            vec![(5..13, "email".to_string(), "ana@x.io")]
        );
        assert_eq!(
            placed(&second),
            vec![(19..27, "sensitive_data".to_string(), "Ana Lima")]
        );
        assert!(matches!(
            second[0].location,
            DetectionLocation::Text {
                line: 1,
                column: 20,
                ..
            }
        ));
    }

    #[test]
    fn a_later_change_covering_an_earlier_replacement_widens_to_its_original_range() {
        let original = "Ask Dr. Jane Roe now";
        let after_rules = "Ask Dr. Jane [REDACTED] now";
        let after_ner = "Ask Dr. [REDACTED] now";
        let mut chain = TextChain::new(original.to_string());

        chain
            .step(
                original,
                after_rules,
                &[found(original, range_of(original, "Roe"), "surname", None)],
                deadline(),
            )
            .unwrap();
        let second = chain
            .step(
                after_rules,
                after_ner,
                &[found(after_rules, 8..23, "PER", Some(score(0.9)))],
                deadline(),
            )
            .unwrap();

        assert_eq!(
            placed(&second),
            vec![(8..16, "PER".to_string(), "Jane Roe")]
        );
    }

    #[test]
    fn a_change_takes_the_label_and_score_of_the_detection_it_overlaps_most() {
        let input = "call Ana Lima now";
        let output = "call [REDACTED] now";
        let detections = [
            found(input, 5..8, "first-name", Some(score(0.5))),
            found(input, 5..13, "person", Some(score(0.9))),
        ];

        let records = step_once(input, output, &detections).unwrap();

        assert_eq!(
            placed(&records),
            vec![(5..13, "person".to_string(), "Ana Lima")]
        );
        assert_eq!(records[0].score, Some(score(0.9)));
    }

    #[test]
    fn a_change_no_detection_overlaps_is_unclassified_and_unscored() {
        let input = "call Ana Lima now";
        let output = "call [REDACTED] now";

        let records = step_once(input, output, &[]).unwrap();

        assert_eq!(records[0].label, DetectionLabel::unclassified());
        assert_eq!(records[0].score, None);
    }

    #[test]
    fn a_detection_the_output_does_not_reflect_is_not_reported() {
        let input = "Ana Lima and Bo Li";
        let output = "[REDACTED] and Bo Li";
        let detections = [
            found(input, range_of(input, "Ana Lima"), "person", None),
            found(input, range_of(input, "Bo Li"), "person", None),
        ];

        let records = step_once(input, output, &detections).unwrap();

        assert_eq!(
            placed(&records),
            vec![(0..8, "person".to_string(), "Ana Lima")]
        );
    }

    #[test]
    fn a_mask_sharing_the_values_first_character_is_widened_back_by_the_detection() {
        let input = "Contact Xavier Lee at x.lee@example.org";
        let output = "Contact XXXXXXXXXX at XXXXXXXXXXXXXXXXX";
        let detections = [
            found(input, 8..18, "NAME", Some(score(0.99))),
            found(input, 22..39, "EMAIL", Some(score(0.95))),
        ];

        let records = step_once(input, output, &detections).unwrap();

        assert_eq!(
            placed(&records),
            vec![
                (8..18, "NAME".to_string(), "Xavier Lee"),
                (22..39, "EMAIL".to_string(), "x.lee@example.org"),
            ]
        );
        assert_eq!(records[0].score, Some(score(0.99)));
    }

    #[test]
    fn changes_split_at_punctuation_the_placeholder_shares_are_joined_by_the_detection() {
        let input = "Order AB-1234 for John Müller.";
        let output = "Order {order-id} for {NAME}.";
        let detections = [
            found(input, range_of(input, "AB-1234"), "order-id", None),
            found(input, range_of(input, "John Müller"), "NAME", None),
        ];

        let records = step_once(input, output, &detections).unwrap();

        assert_eq!(
            placed(&records),
            vec![
                (6..13, "order-id".to_string(), "AB-1234"),
                (18..30, "NAME".to_string(), "John Müller"),
            ]
        );
        assert!(matches!(
            records[1].location,
            DetectionLocation::Text { column: 19, .. }
        ));
    }

    #[test]
    fn detections_in_a_table_cell_label_the_changes_of_that_cell() {
        let cell = "John Smith";
        let detection =
            Detection::in_table_cell(0, 1, cell, 0..10, DetectionLabel::new("person"), None)
                .unwrap();

        let records = step_once(cell, "[REDACTED]", &[detection]).unwrap();

        assert_eq!(
            placed(&records),
            vec![(0..10, "person".to_string(), "John Smith")]
        );
    }

    #[test]
    fn a_detection_outside_the_input_is_an_error() {
        let input = "short";
        let detection = Detection {
            label: DetectionLabel::new("person"),
            location: DetectionLocation::Text {
                start: 2,
                end: 40,
                line: 1,
                column: 3,
            },
            score: None,
            value: String::new(),
        };

        let result = step_once(input, "sh[REDACTED]", &[detection]);

        assert!(matches!(
            result,
            Err(AppError::DetectionOutsideContent { start: 2, end: 40 })
        ));
    }

    #[test]
    fn with_a_deadline_already_past_every_change_is_still_covered() {
        let values: Vec<String> = (0..3000)
            .map(|number| format!("user{number}@x.io"))
            .collect();
        let input = values
            .iter()
            .map(|value| format!("id={value};"))
            .collect::<String>();
        let output = values
            .iter()
            .map(|_| "id=[REDACTED];".to_string())
            .collect::<String>();

        let records = TextChain::new(input.clone())
            .step(&input, &output, &[], Instant::now())
            .unwrap();

        let spans = placed(&records);
        let mut offset = 0;
        for value in &values {
            let start = offset + "id=".len();
            let end = start + value.len();
            offset = end + ";".len();
            assert!(
                spans
                    .iter()
                    .any(|(span, _, _)| span.start <= start && end <= span.end),
                "{value} is not covered"
            );
        }
    }

    #[test]
    fn applying_the_changes_to_the_input_gives_the_output() {
        let pairs = [
            ("", "added"),
            ("removed", ""),
            ("a b c", "a  c"),
            ("Grüße 😀 ana", "Grüße [X]"),
            ("x=1;y=2;z=3", "x=[R];y=2;z=[R]"),
            ("same", "same"),
        ];
        for (input, output) in pairs {
            let changes = TextChanges::between(input, output, deadline());

            let mut rebuilt = String::new();
            let mut cursor = 0;
            for change in &changes.0 {
                rebuilt.push_str(&input[cursor..change.input.start]);
                rebuilt.push_str(&output[change.output.clone()]);
                cursor = change.input.end;
            }
            rebuilt.push_str(&input[cursor..]);

            assert_eq!(rebuilt, output, "{input:?} to {output:?}");
        }
    }

    #[test]
    fn a_range_ending_inside_a_change_widens_to_the_changes_whole_input() {
        let input = "key=secret-value end";
        let output = "key=[REDACTED] end";
        let changes = TextChanges::between(input, output, deadline());

        let start_inside = changes.to_input(7..output.len());
        let end_inside = changes.to_input(0..8);

        assert_eq!(start_inside, 4..input.len());
        assert_eq!(end_inside, 0..input.find(" end").unwrap());
    }
}
