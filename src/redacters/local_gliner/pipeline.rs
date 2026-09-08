//! Pure windowing and span-decoding logic for the local GLiNER redacter: no model and no
//! tokenizer types, so every rule is tested with hand-built token ids and fake logits.

use super::error::LocalGlinerError;
use crate::redacters::text_spans::{Finding, RuleName};
use regex::Regex;
use std::sync::OnceLock;

/// What the redacter looks for and how sure the model must be about a span.
#[derive(Clone, Debug, PartialEq)]
pub struct GlinerOptions {
    pub labels: Vec<String>,
    pub min_score: f32,
}

/// A whitespace-split word: GLiNER's `WhitespaceTokenSplitter`
/// (`gliner/data_processing/tokenizer.py`) matches `\w+(?:[-_]\w+)*|\S`, i.e. runs of word
/// characters optionally hyphen/underscore-joined, or any single other non-space character, so
/// punctuation becomes its own word. Byte offsets, not character offsets, are what redaction
/// needs to slice the original text.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Word<'a> {
    pub text: &'a str,
    pub start: usize,
    pub end: usize,
}

fn word_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| Regex::new(r"\w+(?:[-_]\w+)*|\S").expect("static pattern is valid"))
}

pub fn split_words(text: &str) -> Vec<Word<'_>> {
    word_pattern()
        .find_iter(text)
        .map(|m| Word {
            text: m.as_str(),
            start: m.start(),
            end: m.end(),
        })
        .collect()
}

/// Tokens shared by two neighbouring windows, in words. Long documents cross a window
/// boundary; a word near that boundary is scored from whichever window gives it more
/// context on both sides, the same idea as the token overlap in `local_ner`.
pub const OVERLAP_WORDS: usize = 32;

/// A window over word indices: the model sees `start..end`; only `keep_from..keep_to` is
/// taken from this window, the rest from the neighbour that has more context for it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WordWindow {
    pub start: usize,
    pub end: usize,
    pub keep_from: usize,
    pub keep_to: usize,
}

/// The token budget left for text words in one forward pass: `max_len` minus `[CLS]`/`[SEP]`
/// minus the longest label-batch prompt, so every batch's window fits under `max_len` even
/// though batches share the same window plan.
pub fn window_budget(max_len: usize, prompt_lens: &[usize]) -> Result<usize, LocalGlinerError> {
    let longest_prompt = prompt_lens.iter().copied().max().unwrap_or(0);
    let frame = 2 + longest_prompt;
    max_len
        .checked_sub(frame)
        .filter(|budget| *budget > 0)
        .ok_or(LocalGlinerError::PromptTooLong {
            prompt_tokens: longest_prompt,
            max_len,
        })
}

/// Cuts `word_token_counts.len()` words into windows whose subtoken budget never exceeds
/// `budget`, overlapping by roughly `overlap` words. A single word wider than `budget` still
/// gets its own window rather than being split or dropped. The keep ranges tile every word
/// exactly once, splitting each overlap near the middle.
pub fn plan_word_windows(
    word_token_counts: &[usize],
    budget: usize,
    overlap: usize,
) -> Vec<WordWindow> {
    let n = word_token_counts.len();
    if n == 0 || budget == 0 {
        return Vec::new();
    }
    // Pass 1: greedily pack words into windows of at most `budget` tokens, each starting
    // `overlap` words before the previous window's end. Always take at least one word, even
    // if it alone exceeds the budget, so a single oversized word gets its own window rather
    // than stalling the loop.
    let mut spans: Vec<(usize, usize)> = Vec::new();
    let mut start = 0usize;
    loop {
        let mut end = start + 1;
        let mut used = word_token_counts[start];
        while end < n {
            let next = word_token_counts[end];
            if used + next > budget {
                break;
            }
            used += next;
            end += 1;
        }
        spans.push((start, end));
        if end == n {
            break;
        }
        let stride = (end - start).saturating_sub(overlap).max(1);
        start += stride;
    }
    // Pass 2: split each shared region between neighbours at its midpoint. Two adjacent
    // windows compute the same split point from their shared boundary, so the keep ranges
    // tile `0..n` exactly once regardless of how uneven the window sizes are; a pair of
    // windows with no shared region (an oversized word left no room for overlap) still
    // splits at their touching boundary, so neither drops a word.
    let mut windows = Vec::with_capacity(spans.len());
    for (i, &(start, end)) in spans.iter().enumerate() {
        let keep_from = if i == 0 {
            0
        } else {
            let prev_end = spans[i - 1].1;
            start + prev_end.saturating_sub(start) / 2
        };
        let keep_to = if i + 1 == spans.len() {
            n
        } else {
            let next_start = spans[i + 1].0;
            next_start + end.saturating_sub(next_start) / 2
        };
        windows.push(WordWindow {
            start,
            end,
            keep_from,
            keep_to,
        });
    }
    windows
}

/// All possible `(start_word, end_word)` pairs for `num_words` words and the model's
/// `max_width`, inclusive on both ends, in the row-major `[word][width]` order the graph's
/// `span_idx` input expects (`gliner/data_processing/utils.py::prepare_span_idx`).
pub fn span_candidates(num_words: usize, max_width: usize) -> Vec<[i32; 2]> {
    let mut spans = Vec::with_capacity(num_words * max_width);
    for start in 0..num_words {
        for width in 0..max_width {
            spans.push([start as i32, (start + width) as i32]);
        }
    }
    spans
}

/// Builds the prompt token sequence `<<ENT>> label_1 ... <<ENT>> label_L <<SEP>>` for one
/// label batch, without `[CLS]`/`[SEP]`: those frame the whole window.
pub fn prompt_ids(ent: u32, gliner_sep: u32, label_ids: &[Vec<u32>]) -> Vec<u32> {
    let mut ids = Vec::new();
    for label in label_ids {
        ids.push(ent);
        ids.extend_from_slice(label);
    }
    ids.push(gliner_sep);
    ids
}

/// One forward pass worth of tensors, already shaped `[1, ...]` (batch size 1).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WindowInputs {
    /// Token ids: `[CLS] prompt... word_subtokens... [SEP]`.
    pub ids: Vec<i32>,
    /// Same length as `ids`; 0 everywhere except the first subtoken of each text word, which
    /// carries that word's 1-based index within this window.
    pub words_mask: Vec<i32>,
    /// Number of text words in this window (`span_idx` end positions are checked against it).
    pub num_words: usize,
    /// `(start_word, end_word)` pairs, inclusive, in row-major `[word][width]` order.
    pub span_idx: Vec<[i32; 2]>,
    /// 1 where the span's end word is within `num_words`, 0 otherwise (padding candidates).
    pub span_mask: Vec<i32>,
}

/// Assembles one window's token/mask/span tensors from already-tokenized words and prompt.
pub fn window_inputs(
    cls: u32,
    sep: u32,
    prompt: &[u32],
    word_ids: &[&[u32]],
    max_width: usize,
) -> WindowInputs {
    let num_words = word_ids.len();
    let mut ids = Vec::with_capacity(1 + prompt.len() + num_words * 2 + 1);
    let mut words_mask = Vec::with_capacity(ids.capacity());

    ids.push(cls as i32);
    words_mask.push(0);
    for &id in prompt {
        ids.push(id as i32);
        words_mask.push(0);
    }
    for (word_index, subtokens) in word_ids.iter().enumerate() {
        for (sub_index, &id) in subtokens.iter().enumerate() {
            ids.push(id as i32);
            words_mask.push(if sub_index == 0 {
                (word_index + 1) as i32
            } else {
                0
            });
        }
    }
    ids.push(sep as i32);
    words_mask.push(0);

    let span_idx = span_candidates(num_words, max_width);
    let span_mask = span_idx
        .iter()
        .map(|[_, end]| i32::from((*end as usize) < num_words))
        .collect();

    WindowInputs {
        ids,
        words_mask,
        num_words,
        span_idx,
        span_mask,
    }
}

/// `logits[word][width][label]`, already flattened; call sites index with the strides this
/// struct exposes rather than reshaping, since only one window is scored per call.
#[derive(Clone, Debug, PartialEq)]
pub struct Logits {
    pub num_words: usize,
    pub max_width: usize,
    pub num_labels: usize,
    pub data: Vec<f32>,
}

impl Logits {
    pub fn at(&self, word: usize, width: usize, label: usize) -> f32 {
        let idx = (word * self.max_width + width) * self.num_labels + label;
        self.data[idx]
    }
}

/// Scores one window of already-built tensors against `num_labels` labels.
pub trait WindowScorer {
    fn score_window(
        &self,
        inputs: &WindowInputs,
        num_labels: usize,
    ) -> Result<Logits, LocalGlinerError>;
}

fn sigmoid(x: f32) -> f32 {
    1.0 / (1.0 + (-x).exp())
}

/// One candidate entity span, in both word and byte coordinates. Byte coordinates are what
/// redaction needs; word coordinates are what non-overlap decoding needs, since two spans
/// can share a boundary byte only when they share no word.
#[derive(Clone, Debug, PartialEq)]
pub struct Candidate {
    pub start_word: usize,
    pub end_word: usize,
    pub start_byte: usize,
    pub end_byte: usize,
    pub label: String,
    pub score: f32,
}

fn collect_candidates(
    logits: &Logits,
    window_words: &[Word],
    word_offset: usize,
    labels: &[String],
    threshold: f32,
    out: &mut Vec<Candidate>,
) {
    for start in 0..logits.num_words {
        for width in 0..logits.max_width {
            let end = start + width;
            if end >= logits.num_words {
                continue;
            }
            for (label_index, label) in labels.iter().enumerate() {
                let score = sigmoid(logits.at(start, width, label_index));
                if score >= threshold {
                    out.push(Candidate {
                        start_word: word_offset + start,
                        end_word: word_offset + end,
                        start_byte: window_words[start].start,
                        end_byte: window_words[end].end,
                        label: label.clone(),
                        score,
                    });
                }
            }
        }
    }
}

/// Scores every window of `words` against every label batch and returns every candidate span
/// at or above `threshold`, unresolved (overlapping candidates from different labels or
/// batches are expected; [`greedy_decode`] resolves them). Every batch shares one window
/// plan, built from the tightest budget across batches, so a word is scored at the same
/// window position for every label.
#[allow(clippy::too_many_arguments)]
pub fn score_document(
    scorer: &dyn WindowScorer,
    cls: u32,
    sep: u32,
    ent: u32,
    gliner_sep: u32,
    words: &[Word],
    word_ids: &[Vec<u32>],
    label_batches: &[&[String]],
    label_ids: &[Vec<Vec<u32>>],
    max_len: usize,
    max_width: usize,
    overlap: usize,
    threshold: f32,
) -> Result<Vec<Candidate>, LocalGlinerError> {
    if words.is_empty() {
        return Ok(Vec::new());
    }
    let prompts: Vec<Vec<u32>> = label_ids
        .iter()
        .map(|ids| prompt_ids(ent, gliner_sep, ids))
        .collect();
    let prompt_lens: Vec<usize> = prompts.iter().map(Vec::len).collect();
    let budget = window_budget(max_len, &prompt_lens)?;

    let word_token_counts: Vec<usize> = word_ids.iter().map(Vec::len).collect();
    let windows = plan_word_windows(&word_token_counts, budget, overlap);

    let mut candidates = Vec::new();
    for (batch_index, labels) in label_batches.iter().enumerate() {
        let prompt = &prompts[batch_index];
        for window in &windows {
            let window_word_ids: Vec<&[u32]> = word_ids[window.start..window.end]
                .iter()
                .map(Vec::as_slice)
                .collect();
            let inputs = window_inputs(cls, sep, prompt, &window_word_ids, max_width);
            let logits = scorer.score_window(&inputs, labels.len())?;
            let mut window_candidates = Vec::new();
            collect_candidates(
                &logits,
                &words[window.start..window.end],
                window.start,
                labels,
                threshold,
                &mut window_candidates,
            );
            candidates.extend(
                window_candidates
                    .into_iter()
                    .filter(|c| c.start_word >= window.keep_from && c.start_word < window.keep_to),
            );
        }
    }
    Ok(candidates)
}

/// Sorts candidates by descending score and keeps each one whose word range does not
/// overlap a previously accepted span, highest-confidence spans winning ties for a word.
/// Returned in document order.
pub fn greedy_decode(mut candidates: Vec<Candidate>) -> Vec<Candidate> {
    candidates.sort_by(|a, b| b.score.total_cmp(&a.score));
    let mut accepted: Vec<Candidate> = Vec::new();
    for candidate in candidates {
        let overlaps = accepted.iter().any(|kept| {
            candidate.start_word <= kept.end_word && kept.start_word <= candidate.end_word
        });
        if !overlaps {
            accepted.push(candidate);
        }
    }
    accepted.sort_by_key(|c| c.start_byte);
    accepted
}

/// Maps accepted, non-overlapping candidates to byte-range findings named by their label.
pub fn candidates_to_findings(candidates: Vec<Candidate>) -> Vec<Finding> {
    candidates
        .into_iter()
        .map(|candidate| Finding {
            start: candidate.start_byte,
            end: candidate.end_byte,
            rule: RuleName::new(&candidate.label),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    #[test]
    fn splits_on_whitespace_and_keeps_hyphenated_words_together() {
        let words = split_words("Jean-Paul lives in São Paulo.");
        let texts: Vec<&str> = words.iter().map(|w| w.text).collect();
        assert_eq!(texts, vec!["Jean-Paul", "lives", "in", "São", "Paulo", "."]);
    }

    #[test]
    fn punctuation_becomes_its_own_word() {
        let words = split_words("a, b!");
        let texts: Vec<&str> = words.iter().map(|w| w.text).collect();
        assert_eq!(texts, vec!["a", ",", "b", "!"]);
    }

    #[test]
    fn offsets_are_byte_offsets_that_round_trip() {
        let text = "call 555-1234 now, José";
        for word in split_words(text) {
            assert_eq!(&text[word.start..word.end], word.text);
        }
    }

    #[test]
    fn empty_text_has_no_words() {
        assert!(split_words("").is_empty());
        assert!(split_words("   ").is_empty());
    }

    #[test]
    fn window_budget_is_max_len_minus_special_tokens_and_the_longest_prompt() {
        assert_eq!(window_budget(384, &[10, 25, 5]).unwrap(), 384 - 2 - 25);
        assert_eq!(window_budget(20, &[0]).unwrap(), 18);
    }

    #[test]
    fn window_budget_rejects_a_prompt_that_leaves_no_room() {
        let err = window_budget(10, &[9]).unwrap_err();
        assert!(
            matches!(err, LocalGlinerError::PromptTooLong { .. }),
            "{err}"
        );
        let err = window_budget(10, &[8]).unwrap_err();
        assert!(
            matches!(err, LocalGlinerError::PromptTooLong { .. }),
            "{err}"
        );
    }

    #[test]
    fn plan_word_windows_edge_cases() {
        assert!(plan_word_windows(&[], 10, 4).is_empty());
        assert_eq!(
            plan_word_windows(&[3], 10, 4),
            vec![WordWindow {
                start: 0,
                end: 1,
                keep_from: 0,
                keep_to: 1
            }]
        );
        // Every word fits in one window.
        assert_eq!(
            plan_word_windows(&[1, 1, 1, 1, 1], 10, 4),
            vec![WordWindow {
                start: 0,
                end: 5,
                keep_from: 0,
                keep_to: 5
            }]
        );
    }

    #[test]
    fn plan_word_windows_keep_ranges_tile_every_word_once() {
        for n in [0usize, 1, 9, 10, 11, 13, 25, 100, 500] {
            let counts = vec![1usize; n];
            let covered: Vec<usize> = plan_word_windows(&counts, 10, 4)
                .iter()
                .flat_map(|w| w.keep_from..w.keep_to)
                .collect();
            assert_eq!(covered, (0..n).collect::<Vec<_>>(), "n = {n}");
        }
    }

    #[test]
    fn plan_word_windows_gives_an_oversized_word_its_own_window() {
        let windows = plan_word_windows(&[1, 20, 1], 5, 2);
        let covered: Vec<usize> = windows
            .iter()
            .flat_map(|w| w.keep_from..w.keep_to)
            .collect();
        assert_eq!(covered, vec![0, 1, 2]);
        assert!(
            windows.iter().any(|w| w.start == 1 && w.end == 2),
            "{windows:?}"
        );
    }

    #[test]
    fn span_candidates_cover_every_word_and_width_in_row_major_order() {
        let spans = span_candidates(3, 2);
        assert_eq!(spans, vec![[0, 0], [0, 1], [1, 1], [1, 2], [2, 2], [2, 3]]);
    }

    #[test]
    fn prompt_ids_frames_each_label_with_ent_and_ends_with_gliner_sep() {
        let ids = prompt_ids(100, 200, &[vec![1, 2], vec![3]]);
        assert_eq!(ids, vec![100, 1, 2, 100, 3, 200]);
    }

    #[test]
    fn window_inputs_builds_ids_words_mask_and_span_tensors() {
        let word_ids: Vec<&[u32]> = vec![&[10, 11], &[12]];
        let inputs = window_inputs(1, 2, &[50, 51], &word_ids, 2);
        assert_eq!(inputs.ids, vec![1, 50, 51, 10, 11, 12, 2]);
        assert_eq!(inputs.words_mask, vec![0, 0, 0, 1, 0, 2, 0]);
        assert_eq!(inputs.num_words, 2);
        assert_eq!(inputs.span_idx, vec![[0, 0], [0, 1], [1, 1], [1, 2]]);
        assert_eq!(inputs.span_mask, vec![1, 1, 1, 0]);
    }

    #[test]
    fn logits_at_indexes_the_flattened_word_width_label_layout() {
        let logits = Logits {
            num_words: 2,
            max_width: 3,
            num_labels: 2,
            data: (0..12).map(|v| v as f32).collect(),
        };
        assert_eq!(logits.at(0, 0, 0), 0.0);
        assert_eq!(logits.at(0, 0, 1), 1.0);
        assert_eq!(logits.at(1, 2, 1), 11.0);
    }

    struct FakeScorer {
        /// `(start_word, end_word, label_index) -> logit`; anything else scores far below 0.
        hits: Vec<(usize, usize, usize, f32)>,
        calls: RefCell<Vec<WindowInputs>>,
    }

    impl WindowScorer for FakeScorer {
        fn score_window(
            &self,
            inputs: &WindowInputs,
            num_labels: usize,
        ) -> Result<Logits, LocalGlinerError> {
            self.calls.borrow_mut().push(inputs.clone());
            let max_width = 12;
            let mut data = vec![-10.0f32; inputs.num_words * max_width * num_labels];
            for &(start, end, label, logit) in &self.hits {
                if start < inputs.num_words {
                    let width = end - start;
                    if width < max_width {
                        let idx = (start * max_width + width) * num_labels + label;
                        if idx < data.len() {
                            data[idx] = logit;
                        }
                    }
                }
            }
            Ok(Logits {
                num_words: inputs.num_words,
                max_width,
                num_labels,
                data,
            })
        }
    }

    fn word(text: &'static str, start: usize, end: usize) -> Word<'static> {
        Word { text, start, end }
    }

    #[test]
    fn score_document_finds_a_span_above_threshold_and_ignores_one_below() {
        let words = vec![word("Alice", 0, 5), word("called", 6, 12)];
        let word_ids = vec![vec![10u32], vec![11u32]];
        let labels = vec!["person".to_string()];
        let label_batches: Vec<&[String]> = vec![&labels];
        let label_ids = vec![vec![vec![99u32]]];
        let scorer = FakeScorer {
            // "Alice" (word 0, width 0) scores high for label 0; "called" scores low.
            hits: vec![(0, 0, 0, 10.0), (1, 1, 0, -10.0)],
            calls: RefCell::new(Vec::new()),
        };
        let candidates = score_document(
            &scorer,
            1,
            2,
            100,
            200,
            &words,
            &word_ids,
            &label_batches,
            &label_ids,
            384,
            12,
            32,
            0.5,
        )
        .unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].start_byte, 0);
        assert_eq!(candidates[0].end_byte, 5);
        assert_eq!(candidates[0].label, "person");
        assert!(candidates[0].score > 0.99, "{}", candidates[0].score);
    }

    fn candidate(
        start_word: usize,
        end_word: usize,
        start_byte: usize,
        end_byte: usize,
        score: f32,
    ) -> Candidate {
        Candidate {
            start_word,
            end_word,
            start_byte,
            end_byte,
            label: "x".to_string(),
            score,
        }
    }

    #[test]
    fn greedy_decode_drops_a_lower_score_overlap() {
        let candidates = vec![candidate(0, 2, 0, 10, 0.6), candidate(1, 1, 4, 6, 0.9)];
        let accepted = greedy_decode(candidates);
        assert_eq!(accepted.len(), 1);
        assert_eq!((accepted[0].start_byte, accepted[0].end_byte), (4, 6));
    }

    #[test]
    fn greedy_decode_keeps_non_overlapping_spans_in_document_order() {
        let candidates = vec![candidate(1, 1, 5, 9, 0.5), candidate(0, 0, 0, 4, 0.5)];
        let accepted = greedy_decode(candidates);
        assert_eq!(accepted.len(), 2);
        assert_eq!(accepted[0].start_byte, 0);
        assert_eq!(accepted[1].start_byte, 5);
    }

    #[test]
    fn candidates_to_findings_names_the_rule_after_the_label() {
        let findings = candidates_to_findings(vec![candidate(0, 0, 0, 5, 0.9)]);
        assert_eq!(
            findings,
            vec![Finding {
                start: 0,
                end: 5,
                rule: RuleName::new("x")
            }]
        );
    }

    /// Scores whichever local word carries `sentinel_id`, wherever a window places it; this
    /// is what lets the boundary test below assert on a global word index without knowing in
    /// advance which window keeps it.
    struct SentinelScorer {
        sentinel_id: i32,
        calls: RefCell<usize>,
    }

    impl WindowScorer for SentinelScorer {
        fn score_window(
            &self,
            inputs: &WindowInputs,
            num_labels: usize,
        ) -> Result<Logits, LocalGlinerError> {
            *self.calls.borrow_mut() += 1;
            let max_width = 12;
            let mut data = vec![-10.0f32; inputs.num_words * max_width * num_labels];
            if let Some(position) = inputs.ids.iter().position(|&id| id == self.sentinel_id) {
                let local_word = inputs.words_mask[position] - 1;
                if local_word >= 0 {
                    let idx = (local_word as usize * max_width) * num_labels;
                    data[idx] = 10.0;
                }
            }
            Ok(Logits {
                num_words: inputs.num_words,
                max_width,
                num_labels,
                data,
            })
        }
    }

    #[test]
    fn a_span_crossing_a_window_boundary_is_still_found_once() {
        // 40 one-token words, a small per-window budget forces several windows; word 20
        // carries a sentinel token id so the assertion holds regardless of which window's
        // keep range ends up covering it.
        let mut text = String::new();
        let mut words: Vec<Word> = Vec::new();
        for i in 0..40 {
            let start = text.len();
            let w = format!("w{i}");
            text.push_str(&w);
            text.push(' ');
            words.push(Word {
                text: Box::leak(w.into_boxed_str()),
                start,
                end: start + text[start..].trim_end().len(),
            });
        }
        let word_ids: Vec<Vec<u32>> = (0..40u32)
            .map(|i| vec![if i == 20 { 9000 } else { i }])
            .collect();
        let labels = vec!["thing".to_string()];
        let label_batches: Vec<&[String]> = vec![&labels];
        let label_ids = vec![vec![vec![999u32]]];
        let scorer = SentinelScorer {
            sentinel_id: 9000,
            calls: RefCell::new(0),
        };
        let candidates = score_document(
            &scorer,
            1,
            2,
            100,
            200,
            &words,
            &word_ids,
            &label_batches,
            &label_ids,
            14,
            12,
            4,
            0.5,
        )
        .unwrap();
        assert_eq!(candidates.len(), 1, "{candidates:?}");
        assert_eq!(candidates[0].start_word, 20);
        assert_eq!(candidates[0].end_word, 20);
        assert!(*scorer.calls.borrow() > 1, "expected several windows");
    }
}
