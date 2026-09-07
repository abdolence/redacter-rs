//! Pure tagging logic for the local NER redacter: no model and no tokenizer types, so every
//! rule is tested with hand-built tokens and fake logits.

use super::error::LocalNerError;
use crate::redacters::text_spans::{Finding, RuleName};
use std::collections::{BTreeMap, BTreeSet};

/// Tokens shared by two neighbouring windows.
pub const OVERLAP: usize = 64;

/// Entity types the model can mark and the user can select.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, clap::ValueEnum)]
pub enum Entity {
    Per,
    Org,
    Loc,
}

impl Entity {
    pub fn all() -> BTreeSet<Entity> {
        [Entity::Per, Entity::Org, Entity::Loc]
            .into_iter()
            .collect()
    }

    /// The name used in findings and in `--local-ner-entities`.
    pub fn as_str(self) -> &'static str {
        match self {
            Entity::Per => "per",
            Entity::Org => "org",
            Entity::Loc => "loc",
        }
    }
}

/// A BIO label. `B-DATE` and `I-DATE` map to `Outside`: the model config lists them, the
/// model never emits them, and dates are not redacted here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tag {
    Outside,
    Begin(Entity),
    Inside(Entity),
}

/// The model's label columns, in logit order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LabelSet(Vec<Tag>);

impl LabelSet {
    /// Builds the set from the `id2label` table of `config.json`. Ids must be `0..n`.
    pub fn from_id2label(map: &BTreeMap<usize, String>) -> Result<Self, LocalNerError> {
        if map.is_empty() {
            return Err(LocalNerError::InvalidLabels {
                reason: "id2label is empty".to_string(),
            });
        }
        let mut tags = Vec::with_capacity(map.len());
        for (expected, (id, label)) in map.iter().enumerate() {
            if *id != expected {
                return Err(LocalNerError::InvalidLabels {
                    reason: format!("ids are not contiguous: expected {expected}, found {id}"),
                });
            }
            tags.push(parse_label(label)?);
        }
        Ok(Self(tags))
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn tag(&self, id: usize) -> Option<Tag> {
        self.0.get(id).copied()
    }
}

fn parse_label(label: &str) -> Result<Tag, LocalNerError> {
    if label == "O" {
        return Ok(Tag::Outside);
    }
    let unknown = || LocalNerError::UnknownLabel {
        label: label.to_string(),
    };
    let (prefix, name) = label.split_at_checked(2).ok_or_else(unknown)?;
    let entity = match name {
        "PER" => Some(Entity::Per),
        "ORG" => Some(Entity::Org),
        "LOC" => Some(Entity::Loc),
        "DATE" => None,
        _ => return Err(unknown()),
    };
    match (prefix, entity) {
        ("B-", Some(entity)) => Ok(Tag::Begin(entity)),
        ("I-", Some(entity)) => Ok(Tag::Inside(entity)),
        ("B-" | "I-", None) => Ok(Tag::Outside),
        _ => Err(unknown()),
    }
}

/// A tokenized text as three parallel arrays, without special tokens.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TokenizedText {
    pub ids: Vec<u32>,
    /// Byte offsets into the original text, half-open.
    pub offsets: Vec<(usize, usize)>,
    /// Index of the pre-tokenizer word each token belongs to.
    pub word_ids: Vec<Option<u32>>,
}

/// One window's logits, row-major `[rows][cols]`: one row per input token including
/// `[CLS]` and `[SEP]`, one column per label.
#[derive(Clone, Debug, PartialEq)]
pub struct Logits {
    pub rows: usize,
    pub cols: usize,
    pub data: Vec<f32>,
}

impl Logits {
    fn row(&self, index: usize) -> &[f32] {
        &self.data[index * self.cols..(index + 1) * self.cols]
    }
}

/// Scores one window of token ids already framed by `[CLS]` and `[SEP]`.
pub trait WindowScorer {
    fn score_window(&self, ids_with_cls_sep: &[u32]) -> Result<Logits, LocalNerError>;
}

/// A window over the token indexes: the model sees `start..end`; only `keep_from..keep_to`
/// is taken from this window, the rest from the neighbour that has more context for it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Window {
    pub start: usize,
    pub end: usize,
    pub keep_from: usize,
    pub keep_to: usize,
}

/// Cuts `n_tokens` into windows of at most `body` tokens overlapping by `overlap`. The keep
/// ranges tile `0..n_tokens` exactly once, splitting each overlap in the middle so every
/// token is taken from the window in which it is farther from an edge.
pub fn plan_windows(n_tokens: usize, body: usize, overlap: usize) -> Vec<Window> {
    let mut windows = Vec::new();
    if n_tokens == 0 || body == 0 {
        return windows;
    }
    let stride = body.saturating_sub(overlap).max(1);
    // The earlier window keeps `half` of the shared tokens, the later one the rest, so the
    // two keep ranges meet exactly even when `overlap` is odd.
    let half = overlap / 2;
    let rest = overlap - half;
    let mut start = 0;
    loop {
        let end = (start + body).min(n_tokens);
        let keep_from = if start == 0 {
            0
        } else {
            (start + half).min(end)
        };
        let keep_to = if end == n_tokens {
            n_tokens
        } else {
            end.saturating_sub(rest).max(keep_from)
        };
        windows.push(Window {
            start,
            end,
            keep_from,
            keep_to,
        });
        if end == n_tokens {
            break;
        }
        start += stride;
    }
    windows
}

/// Index of the largest logit and its softmax probability; the first maximum wins a tie.
/// Computed as `1 / sum(exp(x - max))`, which cannot overflow.
pub fn argmax_softmax(row: &[f32]) -> (usize, f32) {
    let mut best = 0;
    let mut max = f32::NEG_INFINITY;
    for (index, &value) in row.iter().enumerate() {
        if value > max {
            max = value;
            best = index;
        }
    }
    if row.is_empty() {
        return (0, 0.0);
    }
    let sum: f32 = row.iter().map(|value| (value - max).exp()).sum();
    (best, 1.0 / sum)
}

/// The label id and confidence chosen for one token.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TokenTag {
    pub label: usize,
    pub score: f32,
}

/// Scores every token exactly once. Each window is framed with `cls` and `sep`; token `k` of
/// a window is row `k + 1` of its logits.
pub fn tag_tokens(
    tokens: &TokenizedText,
    scorer: &dyn WindowScorer,
    cls: u32,
    sep: u32,
    body: usize,
    overlap: usize,
    n_labels: usize,
) -> Result<Vec<TokenTag>, LocalNerError> {
    let n = tokens.ids.len();
    let mut tags: Vec<Option<TokenTag>> = vec![None; n];
    for window in plan_windows(n, body, overlap) {
        let mut ids = Vec::with_capacity(window.end - window.start + 2);
        ids.push(cls);
        ids.extend_from_slice(&tokens.ids[window.start..window.end]);
        ids.push(sep);
        let logits = scorer.score_window(&ids)?;
        if logits.rows != ids.len()
            || logits.cols != n_labels
            || logits.data.len() != logits.rows * logits.cols
        {
            return Err(LocalNerError::OutputShape {
                expected: format!("[{}, {n_labels}]", ids.len()),
                actual: format!(
                    "[{}, {}] with {} values",
                    logits.rows,
                    logits.cols,
                    logits.data.len()
                ),
            });
        }
        // Row 0 is `[CLS]`, so the first kept token sits at `keep_from - start + 1`.
        let first_row = window.keep_from - window.start + 1;
        for (offset, slot) in tags[window.keep_from..window.keep_to]
            .iter_mut()
            .enumerate()
        {
            let (label, score) = argmax_softmax(logits.row(first_row + offset));
            *slot = Some(TokenTag { label, score });
        }
    }
    tags.into_iter()
        .enumerate()
        .map(|(index, tag)| {
            tag.ok_or_else(|| LocalNerError::Inference {
                reason: format!("token {index} was not covered by any window"),
            })
        })
        .collect()
}

/// A word with the label of its first subword and byte offsets on char boundaries.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Word {
    pub start: usize,
    pub end: usize,
    pub tag: Tag,
    pub score: f32,
}

/// Collapses subword tokens onto words: consecutive tokens with the same word id form one
/// word spanning them all, and the first subword's label and score win (how
/// token-classification heads are trained). Zero-length tokens are skipped; a token without
/// a word id is a word of its own; offsets are widened to char boundaries.
pub fn collapse_words(
    text: &str,
    tokens: &TokenizedText,
    tags: &[TokenTag],
    labels: &LabelSet,
) -> Vec<Word> {
    let mut words: Vec<Word> = Vec::new();
    let mut previous_word: Option<u32> = None;
    for (index, tag) in tags.iter().enumerate() {
        let Some(&(start, end)) = tokens.offsets.get(index) else {
            break;
        };
        if start == end {
            continue;
        }
        let end = text.ceil_char_boundary(end.min(text.len()));
        let start = text.floor_char_boundary(start.min(end));
        let word_id = tokens.word_ids.get(index).copied().flatten();
        let same_word = word_id.is_some() && word_id == previous_word;
        previous_word = word_id;
        if same_word {
            if let Some(last) = words.last_mut() {
                last.end = last.end.max(end);
                continue;
            }
        }
        words.push(Word {
            start,
            end,
            tag: labels.tag(tag.label).unwrap_or(Tag::Outside),
            score: tag.score,
        });
    }
    words
}

/// What the redacter removes and how sure the model must be about a word.
#[derive(Clone, Debug, PartialEq)]
pub struct NerOptions {
    pub entities: BTreeSet<Entity>,
    pub min_score: f32,
}

/// Merges BIO-tagged words into findings. A word tagged `O`, of an unselected entity, or
/// below the score floor closes the open span. `I-` continues the open span of the same
/// entity when only whitespace separates them, otherwise it starts a new span.
pub fn merge_spans(text: &str, words: &[Word], options: &NerOptions) -> Vec<Finding> {
    let mut spans: Vec<(Entity, usize, usize)> = Vec::new();
    let mut open: Option<usize> = None;
    for word in words {
        let selected =
            |entity: Entity| options.entities.contains(&entity) && word.score >= options.min_score;
        match word.tag {
            Tag::Begin(entity) if selected(entity) => {
                spans.push((entity, word.start, word.end));
                open = Some(spans.len() - 1);
            }
            Tag::Inside(entity) if selected(entity) => {
                let continuing = open.filter(|index| {
                    spans.get(*index).is_some_and(|span| {
                        span.0 == entity
                            && text
                                .get(span.2..word.start)
                                .is_some_and(|gap| gap.trim().is_empty())
                    })
                });
                match continuing.and_then(|index| spans.get_mut(index)) {
                    Some(span) => span.2 = word.end,
                    None => {
                        spans.push((entity, word.start, word.end));
                        open = Some(spans.len() - 1);
                    }
                }
            }
            _ => open = None,
        }
    }
    spans
        .into_iter()
        .map(|(entity, start, end)| Finding {
            start,
            end,
            rule: RuleName::new(entity.as_str()),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::collections::HashMap;

    const CLS: u32 = 1000;
    const SEP: u32 = 1001;
    const O: usize = 0;
    const B_PER: usize = 3;
    const I_PER: usize = 4;
    const B_ORG: usize = 5;
    const I_ORG: usize = 6;
    const B_LOC: usize = 7;
    const I_LOC: usize = 8;
    /// Against eight zero logits: 10.0 gives softmax 0.9996, 0.5 gives 0.171.
    const SURE: f32 = 10.0;
    const WEAK: f32 = 0.5;

    fn real_labels() -> LabelSet {
        let map: BTreeMap<usize, String> = [
            "O", "B-DATE", "I-DATE", "B-PER", "I-PER", "B-ORG", "I-ORG", "B-LOC", "I-LOC",
        ]
        .iter()
        .enumerate()
        .map(|(id, label)| (id, label.to_string()))
        .collect();
        LabelSet::from_id2label(&map).unwrap()
    }

    /// Token ids equal token positions, so the fake knows which token each row is. Every
    /// position gets `token_labels[position]` at logit `SURE`, positions in `weak` at `WEAK`;
    /// `overrides` replace the label for a (call index, position) pair, to tell apart the two
    /// windows that see an overlapped token.
    struct FakeScorer {
        token_labels: Vec<usize>,
        weak: Vec<usize>,
        overrides: HashMap<(usize, usize), usize>,
        cols: usize,
        calls: RefCell<Vec<Vec<u32>>>,
    }

    impl FakeScorer {
        fn new(token_labels: Vec<usize>) -> Self {
            Self {
                token_labels,
                weak: Vec::new(),
                overrides: HashMap::new(),
                cols: 9,
                calls: RefCell::new(Vec::new()),
            }
        }
    }

    impl WindowScorer for FakeScorer {
        fn score_window(&self, ids: &[u32]) -> Result<Logits, LocalNerError> {
            let call = self.calls.borrow().len();
            self.calls.borrow_mut().push(ids.to_vec());
            let mut data = vec![0.0; ids.len() * self.cols];
            for (row, &id) in ids.iter().enumerate() {
                if id == CLS || id == SEP {
                    continue;
                }
                let position = id as usize;
                let label = self
                    .overrides
                    .get(&(call, position))
                    .copied()
                    .unwrap_or(self.token_labels[position]);
                let logit = if self.weak.contains(&position) {
                    WEAK
                } else {
                    SURE
                };
                data[row * self.cols + label] = logit;
            }
            Ok(Logits {
                rows: ids.len(),
                cols: self.cols,
                data,
            })
        }
    }

    fn positions(n: usize) -> TokenizedText {
        TokenizedText {
            ids: (0..n as u32).collect(),
            offsets: (0..n).map(|i| (i * 2, i * 2 + 1)).collect(),
            word_ids: (0..n as u32).map(Some).collect(),
        }
    }

    fn sure(label: usize) -> TokenTag {
        TokenTag { label, score: 0.99 }
    }

    fn word(start: usize, end: usize, tag: Tag, score: f32) -> Word {
        Word {
            start,
            end,
            tag,
            score,
        }
    }

    fn all_entities() -> NerOptions {
        NerOptions {
            entities: Entity::all(),
            min_score: 0.5,
        }
    }

    fn spans(text: &str, findings: &[Finding]) -> Vec<(String, String)> {
        findings
            .iter()
            .map(|f| {
                (
                    f.rule.as_str().to_string(),
                    text[f.start..f.end].to_string(),
                )
            })
            .collect()
    }

    #[test]
    fn labels_parse_the_real_table_and_map_dates_to_outside() {
        let labels = real_labels();
        assert_eq!(labels.len(), 9);
        assert_eq!(labels.tag(0), Some(Tag::Outside));
        assert_eq!(labels.tag(1), Some(Tag::Outside));
        assert_eq!(labels.tag(2), Some(Tag::Outside));
        assert_eq!(labels.tag(3), Some(Tag::Begin(Entity::Per)));
        assert_eq!(labels.tag(6), Some(Tag::Inside(Entity::Org)));
        assert_eq!(labels.tag(8), Some(Tag::Inside(Entity::Loc)));
        assert_eq!(labels.tag(9), None);
    }

    #[test]
    fn labels_reject_unknown_and_non_contiguous_tables() {
        let map: BTreeMap<usize, String> = [(0, "O"), (1, "B-MISC")]
            .into_iter()
            .map(|(id, label)| (id, label.to_string()))
            .collect();
        let err = LabelSet::from_id2label(&map).unwrap_err();
        assert!(
            matches!(err, LocalNerError::UnknownLabel { ref label } if label == "B-MISC"),
            "{err}"
        );

        let map: BTreeMap<usize, String> = [(0, "O"), (2, "B-PER")]
            .into_iter()
            .map(|(id, label)| (id, label.to_string()))
            .collect();
        let err = LabelSet::from_id2label(&map).unwrap_err();
        assert!(matches!(err, LocalNerError::InvalidLabels { .. }), "{err}");

        let err = LabelSet::from_id2label(&BTreeMap::new()).unwrap_err();
        assert!(matches!(err, LocalNerError::InvalidLabels { .. }), "{err}");
        for bad in ["B", "X-PER", "PER", "b-per"] {
            let map: BTreeMap<usize, String> = [(0, bad.to_string())].into_iter().collect();
            assert!(LabelSet::from_id2label(&map).is_err(), "{bad}");
        }
    }

    #[test]
    fn entity_names_round_trip_through_the_cli_values() {
        use clap::ValueEnum;
        for entity in Entity::value_variants() {
            let cli = entity.to_possible_value().unwrap();
            assert_eq!(entity.as_str(), cli.get_name());
        }
        assert_eq!(Entity::all().len(), 3);
    }

    #[test]
    fn plan_windows_edge_cases() {
        assert!(plan_windows(0, 10, 4).is_empty());
        assert_eq!(
            plan_windows(1, 10, 4),
            vec![Window {
                start: 0,
                end: 1,
                keep_from: 0,
                keep_to: 1
            }]
        );
        assert_eq!(
            plan_windows(10, 10, 4),
            vec![Window {
                start: 0,
                end: 10,
                keep_from: 0,
                keep_to: 10
            }]
        );
        assert_eq!(
            plan_windows(11, 10, 4),
            vec![
                Window {
                    start: 0,
                    end: 10,
                    keep_from: 0,
                    keep_to: 8
                },
                Window {
                    start: 6,
                    end: 11,
                    keep_from: 8,
                    keep_to: 11
                },
            ]
        );
    }

    #[test]
    fn plan_windows_keep_ranges_tile_the_tokens_once() {
        let windows = plan_windows(25, 10, 4);
        assert_eq!(
            windows,
            vec![
                Window {
                    start: 0,
                    end: 10,
                    keep_from: 0,
                    keep_to: 8
                },
                Window {
                    start: 6,
                    end: 16,
                    keep_from: 8,
                    keep_to: 14
                },
                Window {
                    start: 12,
                    end: 22,
                    keep_from: 14,
                    keep_to: 20
                },
                Window {
                    start: 18,
                    end: 25,
                    keep_from: 20,
                    keep_to: 25
                },
            ]
        );
        for n in [0usize, 1, 9, 10, 11, 13, 25, 100, 1234] {
            let covered: Vec<usize> = plan_windows(n, 10, 4)
                .iter()
                .flat_map(|w| w.keep_from..w.keep_to)
                .collect();
            assert_eq!(covered, (0..n).collect::<Vec<_>>(), "n = {n}");
        }
        let covered: Vec<usize> = plan_windows(2000, 510, OVERLAP)
            .iter()
            .flat_map(|w| w.keep_from..w.keep_to)
            .collect();
        assert_eq!(covered, (0..2000).collect::<Vec<_>>());
    }

    #[test]
    fn argmax_softmax_picks_the_first_maximum_and_never_overflows() {
        let (label, score) = argmax_softmax(&[0.0, 0.0, 0.0]);
        assert_eq!(label, 0);
        assert!((score - 1.0 / 3.0).abs() < 1e-6, "{score}");
        let (label, score) = argmax_softmax(&[0.0, SURE, 0.0]);
        assert_eq!(label, 1);
        assert!(score > 0.999, "{score}");
        let (label, score) = argmax_softmax(&[1000.0, 999.0]);
        assert_eq!(label, 0);
        assert!(
            score.is_finite() && (score - 0.7310586).abs() < 1e-5,
            "{score}"
        );
        assert_eq!(argmax_softmax(&[]), (0, 0.0));
    }

    #[test]
    fn tag_tokens_frames_every_window_and_splits_overlaps_in_the_middle() {
        let tokens = positions(25);
        let mut scorer = FakeScorer::new(vec![O; 25]);
        // token 7 is kept from window 0, token 9 from window 1
        scorer.overrides.insert((0, 7), B_PER);
        scorer.overrides.insert((1, 7), B_LOC);
        scorer.overrides.insert((0, 9), B_PER);
        scorer.overrides.insert((1, 9), B_LOC);
        let tags = tag_tokens(&tokens, &scorer, CLS, SEP, 10, 4, 9).unwrap();
        let calls = scorer.calls.borrow();
        assert_eq!(calls.len(), 4);
        let expected: Vec<u32> = std::iter::once(CLS)
            .chain(6..16)
            .chain(std::iter::once(SEP))
            .collect();
        assert_eq!(calls[1], expected);
        assert_eq!(tags.len(), 25);
        assert_eq!(tags[7].label, B_PER);
        assert_eq!(tags[9].label, B_LOC);
        assert!(tags.iter().all(|t| t.score > 0.999), "{tags:?}");
    }

    #[test]
    fn tag_tokens_reports_a_weak_token_with_its_low_score() {
        let tokens = positions(3);
        let mut scorer = FakeScorer::new(vec![B_PER, I_PER, O]);
        scorer.weak.push(1);
        let tags = tag_tokens(&tokens, &scorer, CLS, SEP, 10, 4, 9).unwrap();
        assert_eq!(tags[1].label, I_PER);
        assert!(tags[1].score < 0.2, "{}", tags[1].score);
    }

    #[test]
    fn tag_tokens_rejects_the_wrong_output_shape() {
        let tokens = positions(3);
        let mut scorer = FakeScorer::new(vec![O; 3]);
        scorer.cols = 8;
        let err = tag_tokens(&tokens, &scorer, CLS, SEP, 10, 4, 9).unwrap_err();
        assert!(matches!(err, LocalNerError::OutputShape { .. }), "{err}");
    }

    #[test]
    fn tag_tokens_on_no_tokens_calls_nothing() {
        let scorer = FakeScorer::new(Vec::new());
        let tags = tag_tokens(&TokenizedText::default(), &scorer, CLS, SEP, 10, 4, 9).unwrap();
        assert!(tags.is_empty());
        assert!(scorer.calls.borrow().is_empty());
    }

    #[test]
    fn collapse_words_joins_subwords_and_keeps_the_first_label() {
        let text = "Müller AG";
        let tokens = TokenizedText {
            ids: vec![1, 2, 3],
            offsets: vec![(0, 3), (3, 7), (8, 10)],
            word_ids: vec![Some(0), Some(0), Some(1)],
        };
        let words = collapse_words(
            text,
            &tokens,
            &[sure(B_PER), sure(O), sure(I_ORG)],
            &real_labels(),
        );
        assert_eq!(
            words,
            vec![
                word(0, 7, Tag::Begin(Entity::Per), 0.99),
                word(8, 10, Tag::Inside(Entity::Org), 0.99),
            ]
        );
        assert_eq!(&text[words[0].start..words[0].end], "Müller");
    }

    #[test]
    fn collapse_words_skips_empty_tokens_and_keeps_wordless_tokens_apart() {
        let text = "a b";
        let tokens = TokenizedText {
            ids: vec![1, 2, 3, 4],
            offsets: vec![(0, 1), (1, 1), (2, 3), (2, 3)],
            word_ids: vec![Some(0), None, None, None],
        };
        let tags = [sure(B_PER), sure(I_PER), sure(B_LOC), sure(I_LOC)];
        let words = collapse_words(text, &tokens, &tags, &real_labels());
        assert_eq!(
            words,
            vec![
                word(0, 1, Tag::Begin(Entity::Per), 0.99),
                word(2, 3, Tag::Begin(Entity::Loc), 0.99),
                word(2, 3, Tag::Inside(Entity::Loc), 0.99),
            ]
        );
    }

    #[test]
    fn collapse_words_widens_offsets_to_char_boundaries() {
        let text = "aé b";
        let tokens = TokenizedText {
            ids: vec![1, 2],
            offsets: vec![(0, 2), (4, 5)],
            word_ids: vec![Some(0), Some(1)],
        };
        let words = collapse_words(text, &tokens, &[sure(B_PER), sure(O)], &real_labels());
        assert_eq!(words[0].start, 0);
        assert_eq!(words[0].end, 3);
        assert!(text.is_char_boundary(words[0].end));
        assert_eq!(&text[words[1].start..words[1].end], "b");
    }

    #[test]
    fn merge_spans_joins_b_and_i_of_the_same_entity() {
        let text = "John Michael Smith lives in London";
        let words = [
            word(0, 4, Tag::Begin(Entity::Per), 0.99),
            word(5, 12, Tag::Inside(Entity::Per), 0.99),
            word(13, 18, Tag::Inside(Entity::Per), 0.99),
            word(19, 24, Tag::Outside, 0.99),
            word(25, 27, Tag::Outside, 0.99),
            word(28, 34, Tag::Begin(Entity::Loc), 0.99),
        ];
        let findings = merge_spans(text, &words, &all_entities());
        assert_eq!(
            spans(text, &findings),
            vec![
                ("per".to_string(), "John Michael Smith".to_string()),
                ("loc".to_string(), "London".to_string()),
            ]
        );
    }

    #[test]
    fn merge_spans_starts_a_span_on_an_inside_tag_without_begin_or_with_another_entity() {
        let text = "Smith Siemens";
        let words = [
            word(0, 5, Tag::Inside(Entity::Per), 0.99),
            word(6, 13, Tag::Inside(Entity::Org), 0.99),
        ];
        assert_eq!(
            spans(text, &merge_spans(text, &words, &all_entities())),
            vec![
                ("per".to_string(), "Smith".to_string()),
                ("org".to_string(), "Siemens".to_string()),
            ]
        );
    }

    #[test]
    fn merge_spans_continues_across_whitespace_only() {
        let text = "New York, Paris France";
        let words = [
            word(0, 3, Tag::Begin(Entity::Loc), 0.99),
            word(4, 8, Tag::Inside(Entity::Loc), 0.99),
            word(10, 15, Tag::Begin(Entity::Loc), 0.99),
            word(16, 22, Tag::Inside(Entity::Loc), 0.99),
        ];
        assert_eq!(
            spans(text, &merge_spans(text, &words, &all_entities())),
            vec![
                ("loc".to_string(), "New York".to_string()),
                ("loc".to_string(), "Paris France".to_string()),
            ]
        );
        let words = [
            word(0, 8, Tag::Begin(Entity::Loc), 0.99),
            word(10, 15, Tag::Inside(Entity::Loc), 0.99),
        ];
        assert_eq!(
            spans(text, &merge_spans(text, &words, &all_entities())),
            vec![
                ("loc".to_string(), "New York".to_string()),
                ("loc".to_string(), "Paris".to_string()),
            ]
        );
    }

    #[test]
    fn merge_spans_drops_words_below_the_score_floor_per_word() {
        let text = "Jean Claude Van Damme";
        let words = [
            word(0, 4, Tag::Begin(Entity::Per), 0.99),
            word(5, 11, Tag::Inside(Entity::Per), 0.99),
            word(12, 15, Tag::Inside(Entity::Per), 0.2),
            word(16, 21, Tag::Inside(Entity::Per), 0.99),
        ];
        assert_eq!(
            spans(text, &merge_spans(text, &words, &all_entities())),
            vec![
                ("per".to_string(), "Jean Claude".to_string()),
                ("per".to_string(), "Damme".to_string()),
            ]
        );
        let lenient = NerOptions {
            entities: Entity::all(),
            min_score: 0.1,
        };
        assert_eq!(
            spans(text, &merge_spans(text, &words, &lenient)),
            vec![("per".to_string(), "Jean Claude Van Damme".to_string())]
        );
    }

    #[test]
    fn merge_spans_ignores_unselected_entities() {
        let text = "Smith at Siemens in Berlin";
        let words = [
            word(0, 5, Tag::Begin(Entity::Per), 0.99),
            word(6, 8, Tag::Outside, 0.99),
            word(9, 16, Tag::Begin(Entity::Org), 0.99),
            word(17, 19, Tag::Outside, 0.99),
            word(20, 26, Tag::Begin(Entity::Loc), 0.99),
        ];
        let only_loc = NerOptions {
            entities: [Entity::Loc].into_iter().collect(),
            min_score: 0.5,
        };
        assert_eq!(
            spans(text, &merge_spans(text, &words, &only_loc)),
            vec![("loc".to_string(), "Berlin".to_string())]
        );
    }
}
