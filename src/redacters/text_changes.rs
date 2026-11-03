//! Where a redaction chain changed a text, mapped back to the text as first read.

use crate::errors::AppError;
use crate::redacters::piece_redaction::{mask_ranges, merge, StepFinding};
use crate::redacters::{Detection, TextLines, TextMask};
use crate::AppResult;
use std::ops::Range;

/// One change a step made to a text: `input` of what it read became `output` of what it
/// returned. Byte ranges on character boundaries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TextChange {
    pub input: Range<usize>,
    pub output: Range<usize>,
}

/// Where a position of a step's input is in its output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OutputPosition {
    /// At this position of the output.
    At(usize),
    /// Inside a change, after its start and before its end, where the output has no position of
    /// its own: the range of the output the change wrote.
    Inside(Range<usize>),
}

/// Every change one step made to a text, in order on both sides.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TextChanges(Vec<TextChange>);

impl TextChanges {
    /// The changes of a step whose replacements are all known. Both ranges of every change are
    /// on character boundaries, and the changes are in order and apart on both sides.
    pub(crate) fn new(changes: Vec<TextChange>) -> Self {
        debug_assert!(
            changes.windows(2).all(|pair| {
                pair[0].input.end <= pair[1].input.start
                    && pair[0].output.end <= pair[1].output.start
            }),
            "changes must be in order and apart"
        );
        Self(changes)
    }

    /// Where `position` of the input is in the output.
    pub(crate) fn in_output(&self, position: usize) -> OutputPosition {
        let before = self
            .0
            .partition_point(|change| change.input.end <= position);
        match self.0.get(before) {
            Some(change) if change.input.start < position => {
                OutputPosition::Inside(change.output.clone())
            }
            _ => OutputPosition::At(match before.checked_sub(1).map(|last| &self.0[last]) {
                Some(change) => position - change.input.end + change.output.end,
                None => position,
            }),
        }
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

    fn shifted(&self, before: usize, position: usize) -> usize {
        match before.checked_sub(1).map(|last| &self.0[last]) {
            Some(change) => position - change.output.end + change.input.end,
            None => position,
        }
    }
}

/// The steps one text went through in a redaction chain, so each step's redactions are placed
/// in the text as first read.
pub(crate) struct TextChain {
    original: String,
    steps: Vec<TextChanges>,
}

impl TextChain {
    pub(crate) fn new(original: String) -> Self {
        Self {
            original,
            steps: Vec::new(),
        }
    }

    /// The values `found` in `input`, which a redacter masked as `mask` says to give `output`,
    /// placed in the original text, merged where they overlap and labelled as the mask was.
    /// A value that starts inside an earlier mask, or ends inside one, takes in the whole of
    /// the value that mask replaced. Every returned detection has a `DetectionLocation::Text`
    /// in the original and its value copied from it.
    ///
    /// `output` must be the `input` of the next call. Fails, leaving the chain as it was, when
    /// masking `found` does not give `output`, so the changes of the step are not known, or
    /// when a detection does not fit `input`.
    pub(crate) fn step(
        &mut self,
        input: &str,
        output: &str,
        found: &[Detection],
        mask: TextMask,
    ) -> AppResult<Vec<Detection>> {
        let findings = Detection::findings(found, mask);
        let (masked, changes) = mask_ranges(input, 0, &merge(&findings), &findings)?;
        if masked != output {
            return Err(AppError::SystemError {
                message: "A redaction step is not the masking of the values it found".to_string(),
            });
        }

        let in_original: Vec<StepFinding> = findings
            .into_iter()
            .map(|finding| StepFinding {
                range: self.to_original(finding.range.clone()),
                ..finding
            })
            .collect();
        let lines = TextLines::new(&self.original);
        let placed = merge(&in_original)
            .into_iter()
            .map(|united| {
                let representative = &in_original[united.representative];
                lines.detection(
                    united.range,
                    representative.label.clone(),
                    representative.score,
                )
            })
            .collect::<AppResult<Vec<_>>>()?;
        self.steps.push(changes);
        Ok(placed)
    }

    fn to_original(&self, range: Range<usize>) -> Range<usize> {
        self.steps
            .iter()
            .rev()
            .fold(range, |range, changes| changes.to_input(range))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::redacters::{DetectionLabel, DetectionLocation, Score};

    fn score(value: f32) -> Score {
        Score::try_from(value).unwrap()
    }

    fn found(text: &str, range: Range<usize>, label: &str, score: Option<Score>) -> Detection {
        TextLines::new(text)
            .detection(range, DetectionLabel::new(label), score)
            .unwrap()
    }

    fn range_of(text: &str, needle: &str) -> Range<usize> {
        let start = text.find(needle).unwrap();
        start..start + needle.len()
    }

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

    fn step_once(
        input: &str,
        output: &str,
        found: &[Detection],
        mask: TextMask,
    ) -> AppResult<Vec<Detection>> {
        TextChain::new(input.to_string()).step(input, output, found, mask)
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

        let records = step_once(input, output, &detections, TextMask::Token).unwrap();

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
    fn a_later_value_covering_an_earlier_mask_widens_to_the_value_the_mask_replaced() {
        let original = "Ask Dr. Jane Roe now";
        let after_rules = "Ask Dr. Jane [REDACTED] now";
        let after_ner = "Ask Dr. [REDACTED] now";
        let mut chain = TextChain::new(original.to_string());

        chain
            .step(
                original,
                after_rules,
                &[found(original, range_of(original, "Roe"), "surname", None)],
                TextMask::Token,
            )
            .unwrap();
        let second = chain
            .step(
                after_rules,
                after_ner,
                &[found(after_rules, 8..23, "PER", Some(score(0.9)))],
                TextMask::Token,
            )
            .unwrap();

        assert_eq!(
            placed(&second),
            vec![(8..16, "PER".to_string(), "Jane Roe")]
        );
    }

    #[test]
    fn overlapping_values_are_one_record_with_the_label_and_score_of_the_first_and_longest() {
        let input = "call Ana Lima now";
        let output = "call [REDACTED] now";
        let detections = [
            found(input, 9..13, "last-name", None),
            found(input, 5..9, "first-name", None),
            found(input, 5..13, "person", Some(score(0.9))),
        ];

        let records = step_once(input, output, &detections, TextMask::Token).unwrap();

        assert_eq!(
            placed(&records),
            vec![(5..13, "person".to_string(), "Ana Lima")]
        );
        assert_eq!(records[0].score, Some(score(0.9)));
    }

    #[test]
    fn a_step_whose_output_is_not_its_masked_values_fails_without_joining_the_chain() {
        let input = "Ana Lima and Bo Li";
        let mut chain = TextChain::new(input.to_string());

        let result = chain.step(
            input,
            "[REDACTED] and Bo Li",
            &[
                found(input, range_of(input, "Ana Lima"), "person", None),
                found(input, range_of(input, "Bo Li"), "person", None),
            ],
            TextMask::Token,
        );

        assert!(matches!(result, Err(AppError::SystemError { .. })));
        assert!(chain.steps.is_empty());
    }

    #[test]
    fn a_label_mask_is_placed_with_columns_counted_in_characters() {
        let input = "Order AB-1234 for John Müller.";
        let output = "Order {order-id} for {NAME}.";
        let detections = [
            found(input, range_of(input, "AB-1234"), "order-id", None),
            found(input, range_of(input, "John Müller"), "NAME", None),
        ];

        let records = step_once(input, output, &detections, TextMask::Label).unwrap();

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

        let result = step_once(input, "sh[REDACTED]", &[detection], TextMask::Token);

        assert!(matches!(
            result,
            Err(AppError::DetectionOutsideContent { start: 2, end: 40 })
        ));
    }

    #[test]
    fn a_position_of_the_input_is_shifted_by_the_changes_before_it() {
        let changes = TextChanges::new(vec![
            TextChange {
                input: 4..16,
                output: 4..14,
            },
            TextChange {
                input: 21..25,
                output: 19..29,
            },
        ]);

        assert_eq!(changes.in_output(0), OutputPosition::At(0));
        assert_eq!(changes.in_output(4), OutputPosition::At(4));
        assert_eq!(changes.in_output(16), OutputPosition::At(14));
        assert_eq!(changes.in_output(17), OutputPosition::At(15));
        assert_eq!(changes.in_output(21), OutputPosition::At(19));
        assert_eq!(changes.in_output(25), OutputPosition::At(29));
        assert_eq!(changes.in_output(26), OutputPosition::At(30));
    }

    #[test]
    fn a_position_inside_a_change_has_no_place_in_the_output() {
        let changes = TextChanges::new(vec![TextChange {
            input: 4..16,
            output: 4..14,
        }]);

        assert_eq!(changes.in_output(5), OutputPosition::Inside(4..14));
        assert_eq!(changes.in_output(15), OutputPosition::Inside(4..14));
    }

    #[test]
    fn a_range_ending_inside_a_change_widens_to_the_changes_whole_input() {
        let changes = TextChanges::new(vec![TextChange {
            input: 4..16,
            output: 4..14,
        }]);

        assert_eq!(changes.to_input(7..18), 4..20);
        assert_eq!(changes.to_input(0..8), 0..16);
    }
}
