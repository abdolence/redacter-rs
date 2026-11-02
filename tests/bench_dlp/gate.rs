//! The pieces gate of the model redacters: the same dataset read whole, twice for the spread of
//! the run, and in pieces small enough that every file is cut, compared by what each run finds.

use crate::scoring::{FileTruth, Recall, Tally, Target};
use serde::Serialize;
use std::str::FromStr;

/// The most bytes a window can hold beyond its context on one side, because a cut moves out to
/// the start of a character.
const CUT_ROUNDING_BYTES: usize = 3;

/// A share, in points, within which two runs count as equal when the whole runs agree exactly.
const ZERO_SPREAD_SLACK_POINTS: f64 = 1.0;

/// A size of piece and of context to force on the redacter, `piece:context` in bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct PiecesArm {
    pub piece_bytes: usize,
    pub context_bytes: usize,
}

impl FromStr for PiecesArm {
    type Err = String;

    fn from_str(arm: &str) -> Result<Self, Self::Err> {
        let (piece, context) = arm
            .split_once(':')
            .ok_or_else(|| format!("pieces arm {arm:?} is not piece:context"))?;
        let number = |value: &str| {
            value
                .trim()
                .parse::<usize>()
                .map_err(|e| format!("pieces arm {arm:?}: {e}"))
        };
        let arm = PiecesArm {
            piece_bytes: number(piece)?,
            context_bytes: number(context)?,
        };
        if arm.piece_bytes == 0 {
            return Err("a piece of 0 bytes".to_string());
        }
        Ok(arm)
    }
}

impl std::fmt::Display for PiecesArm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.piece_bytes, self.context_bytes)
    }
}

impl PiecesArm {
    /// The environment that makes the binary lay its windows out this way. It reads it when it is
    /// built with the `bench-pieces` feature. The window holds the piece and the context on both
    /// sides, as the binary lays it out.
    pub fn environment(&self) -> [(&'static str, String); 2] {
        let window = self.piece_bytes + 2 * (self.context_bytes + CUT_ROUNDING_BYTES);
        [
            ("REDACTER_BENCH_WINDOW_BYTES", window.to_string()),
            (
                "REDACTER_BENCH_CONTEXT_BYTES",
                self.context_bytes.to_string(),
            ),
        ]
    }

    /// Where the pieces of `text` end, but the last: a piece ends after the last line break
    /// within its size, and inside a line, at a character, when it holds none.
    pub fn cuts(&self, text: &str) -> Vec<usize> {
        let bytes = text.as_bytes();
        let (mut cuts, mut start) = (Vec::new(), 0);
        while start + self.piece_bytes < bytes.len() {
            let target = start + self.piece_bytes;
            let end = match bytes[start..target].iter().rposition(|&byte| byte == b'\n') {
                Some(newline) => start + newline + 1,
                None => (start + 1..=target)
                    .rev()
                    .find(|&at| text.is_char_boundary(at))
                    .unwrap_or(target),
            };
            cuts.push(end);
            start = end;
        }
        cuts
    }

    /// The sensitive entries of a file that a cut puts at risk: a value that occurs once in
    /// `text`, and lies within the context of a cut or across it. Named by index into
    /// `truth.sensitive`.
    pub fn entries_near_cuts(&self, truth: &FileTruth, text: &str) -> Vec<usize> {
        let cuts = self.cuts(text);
        truth
            .sensitive
            .iter()
            .enumerate()
            .filter_map(|(index, sensitive)| {
                let Target::Text(value) = &sensitive.value else {
                    return None;
                };
                let mut places = text.match_indices(value.as_str());
                let (start, _) = places.next()?;
                let end = start + value.len();
                let near = places.next().is_none()
                    && cuts.iter().any(|&cut| {
                        start < cut + self.context_bytes && end + self.context_bytes > cut
                    });
                near.then_some(index)
            })
            .collect()
    }
}

/// What one run found, reduced to what the gate compares.
#[derive(Debug, Clone, Serialize)]
pub struct Run {
    pub tally: Tally,
    pub files_failed: usize,
    /// Recall over the entries near a cut of the arm being judged.
    pub near_cuts: Recall,
    pub wall_ms: u128,
}

/// One pieces arm and the three runs it is judged by.
#[derive(Debug, Serialize)]
pub struct ArmResult {
    pub arm: PiecesArm,
    pub whole: [Run; 2],
    pub pieces: Run,
    /// What the pieces run lost; empty when it passes.
    pub losses: Vec<String>,
}

fn points(share: Option<f64>) -> f64 {
    share.map_or(0.0, |share| share * 100.0)
}

/// Whether `pieces`, a share in points, is below the lower of the two `whole` shares by more than
/// the spread between them, or by more than one point when they agree exactly. A run that finds
/// more than the whole one is not a loss.
fn is_loss(whole: [f64; 2], pieces: f64) -> bool {
    let (low, high) = (whole[0].min(whole[1]), whole[0].max(whole[1]));
    let slack = if high > low {
        0.0
    } else {
        ZERO_SPREAD_SLACK_POINTS
    };
    pieces < low - slack
}

impl ArmResult {
    pub fn judge(arm: PiecesArm, whole: [Run; 2], pieces: Run) -> Self {
        let mut losses = Vec::new();
        let categories: Vec<_> = whole[0].tally.recall.keys().copied().collect();
        for category in categories {
            let share = |tally: &Tally| {
                tally
                    .recall
                    .get(&category)
                    .filter(|recall| recall.total > 0)
                    .map(|recall| recall.found as f64 * 100.0 / recall.total as f64)
            };
            if let Some(in_pieces) = share(&pieces.tally) {
                let (a, b) = (share(&whole[0].tally), share(&whole[1].tally));
                if let (Some(a), Some(b)) = (a, b) {
                    if is_loss([a, b], in_pieces) {
                        losses.push(format!(
                            "{category} recall {in_pieces:.1}% against {a:.1}% and {b:.1}% whole"
                        ));
                    }
                }
            }
        }
        let precision = |run: &Run| points(run.tally.precision());
        if is_loss(
            [precision(&whole[0]), precision(&whole[1])],
            precision(&pieces),
        ) {
            losses.push(format!(
                "precision {:.1}% against {:.1}% and {:.1}% whole",
                precision(&pieces),
                precision(&whole[0]),
                precision(&whole[1])
            ));
        }
        let near = |run: &Run| {
            (run.near_cuts.total > 0)
                .then(|| run.near_cuts.found as f64 * 100.0 / run.near_cuts.total as f64)
        };
        if let (Some(a), Some(b), Some(in_pieces)) =
            (near(&whole[0]), near(&whole[1]), near(&pieces))
        {
            if is_loss([a, b], in_pieces) {
                losses.push(format!(
                    "recall near a cut {in_pieces:.1}% against {a:.1}% and {b:.1}% whole"
                ));
            }
        }
        let failed_whole = whole[0].files_failed.max(whole[1].files_failed);
        if pieces.files_failed > failed_whole {
            losses.push(format!(
                "{} files failed against {failed_whole} whole",
                pieces.files_failed
            ));
        }
        Self {
            arm,
            whole,
            pieces,
            losses,
        }
    }

    pub fn passes(&self) -> bool {
        self.losses.is_empty()
    }
}

fn share(recall: Recall) -> String {
    format!("{}/{}", recall.found, recall.total)
}

fn percent(share: Option<f64>) -> String {
    share.map_or("-".to_string(), |share| format!("{:.1}%", share * 100.0))
}

fn run_cells(run: &Run, near_cuts: String) -> Vec<String> {
    vec![
        percent(run.tally.overall_recall()),
        percent(run.tally.precision()),
        near_cuts,
        run.files_failed.to_string(),
        format!("{:.1} s", run.wall_ms as f64 / 1000.0),
    ]
}

/// A markdown table of the arms of one redacter on one dataset. The entries near a cut depend on
/// the arm, so each arm shows what its pieces run found of them beside what the whole runs did.
pub fn table(arms: &[ArmResult]) -> String {
    let mut lines = vec![
        "| Run | Recall | Precision | Near a cut | Failed files | Wall | Verdict |".to_string(),
        "|---|---|---|---|---|---|---|".to_string(),
    ];
    let mut push = |name: String, cells: Vec<String>, verdict: String| {
        let mut row = vec![name];
        row.extend(cells);
        row.push(verdict);
        lines.push(format!("| {} |", row.join(" | ")));
    };
    if let Some(first) = arms.first() {
        push(
            "whole".to_string(),
            run_cells(&first.whole[0], "-".to_string()),
            String::new(),
        );
        push(
            "whole, again".to_string(),
            run_cells(&first.whole[1], "-".to_string()),
            String::new(),
        );
    }
    for arm in arms {
        let near_cuts = format!(
            "{} (whole {}, {})",
            share(arm.pieces.near_cuts),
            share(arm.whole[0].near_cuts),
            share(arm.whole[1].near_cuts)
        );
        let verdict = if arm.passes() {
            "pass".to_string()
        } else {
            format!("FAIL: {}", arm.losses.join("; "))
        };
        push(
            format!("pieces {}", arm.arm),
            run_cells(&arm.pieces, near_cuts),
            verdict,
        );
    }
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::category::Category;
    use crate::scoring::Sensitive;

    fn truth_of(values: &[&str]) -> FileTruth {
        FileTruth {
            sensitive: values
                .iter()
                .map(|value| Sensitive {
                    category: Category::Person,
                    value: Target::Text(value.to_string()),
                })
                .collect(),
            ..FileTruth::default()
        }
    }

    fn run(found: usize, total: usize, near: (usize, usize), failed: usize) -> Run {
        let mut tally = Tally::default();
        tally
            .recall
            .insert(Category::Person, Recall { found, total });
        tally.true_findings = found;
        Run {
            tally,
            files_failed: failed,
            near_cuts: Recall {
                found: near.0,
                total: near.1,
            },
            wall_ms: 1000,
        }
    }

    #[test]
    fn arm_is_piece_and_context() {
        assert_eq!(
            "128:1024".parse::<PiecesArm>().unwrap(),
            PiecesArm {
                piece_bytes: 128,
                context_bytes: 1024
            }
        );
        assert!("128".parse::<PiecesArm>().is_err());
        assert!("0:10".parse::<PiecesArm>().is_err());
    }

    #[test]
    fn pieces_end_after_a_line_break_or_inside_a_long_line() {
        let arm = PiecesArm {
            piece_bytes: 8,
            context_bytes: 0,
        };

        assert_eq!(arm.cuts("abc\ndefg\nhij\nk"), vec![4, 9]);
        assert_eq!(arm.cuts("0123456789abcdefghij"), vec![8, 16]);
        assert_eq!(arm.cuts("ééééééé"), vec![8]);
        assert_eq!(arm.cuts("short"), Vec::<usize>::new());
    }

    #[test]
    fn only_values_found_once_near_a_cut_are_at_risk() {
        let arm = PiecesArm {
            piece_bytes: 20,
            context_bytes: 4,
        };
        let text = "aaaa bbbb cccc dddd Zed Smith eeee ffff Zed Smith";
        let truth = truth_of(&["Zed Smith", "Zed", "dddd", "aaaa", "ffff"]);

        let near = arm.entries_near_cuts(&truth, text);

        assert_eq!(near, vec![2, 4]);
    }

    #[test]
    fn a_run_within_the_spread_passes_and_one_outside_it_fails() {
        let arm = PiecesArm {
            piece_bytes: 1,
            context_bytes: 1,
        };
        let whole = || [run(90, 100, (9, 10), 0), run(91, 100, (9, 10), 0)];

        let inside = ArmResult::judge(arm, whole(), run(90, 100, (9, 10), 0));
        let below = ArmResult::judge(arm, whole(), run(88, 100, (9, 10), 0));
        let unplaced = ArmResult::judge(arm, whole(), run(90, 100, (9, 10), 1));
        let near = ArmResult::judge(arm, whole(), run(90, 100, (6, 10), 0));

        assert!(inside.passes(), "{:?}", inside.losses);
        assert!(!below.passes());
        assert!(!unplaced.passes());
        assert!(!near.passes());
    }

    #[test]
    fn identical_whole_runs_allow_one_point_and_more_is_no_loss() {
        assert!(!is_loss([90.0, 90.0], 89.2));
        assert!(is_loss([90.0, 90.0], 88.5));
        assert!(is_loss([90.0, 91.0], 89.5));
        assert!(!is_loss([90.0, 91.0], 95.0));
    }
}
