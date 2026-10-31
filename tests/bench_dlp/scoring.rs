//! Scoring of a `scan` report against a dataset's truth. Everything here is a pure function of
//! the report and the truth, so it is unit tested without running a redacter.

use crate::categories::Category;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// The share of a target a finding, or the findings of a box together, must cover.
const MIN_COVERED: f32 = 0.6;
/// How much longer, or larger in area, than its target a finding may be. It keeps a finding
/// that spans a whole sentence or page from counting as every target in it.
const MAX_SIZE_FACTOR: f32 = 3.0;

/// A box in pixels, `x1 <= x2` and `y1 <= y2`.
#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
#[serde(from = "[f32; 4]")]
pub struct Rect {
    x1: f32,
    y1: f32,
    x2: f32,
    y2: f32,
}

impl From<[f32; 4]> for Rect {
    fn from([x1, y1, x2, y2]: [f32; 4]) -> Self {
        Self { x1, y1, x2, y2 }
    }
}

impl Rect {
    fn area(&self) -> f32 {
        (self.x2 - self.x1) * (self.y2 - self.y1)
    }

    fn intersection(&self, other: &Rect) -> Option<Rect> {
        let overlap = Rect {
            x1: self.x1.max(other.x1),
            y1: self.y1.max(other.y1),
            x2: self.x2.min(other.x2),
            y2: self.y2.min(other.y2),
        };
        (overlap.x2 > overlap.x1 && overlap.y2 > overlap.y1).then_some(overlap)
    }
}

fn union_area(rects: &[Rect]) -> f32 {
    let mut edges: Vec<f32> = rects.iter().flat_map(|rect| [rect.x1, rect.x2]).collect();
    edges.sort_by(f32::total_cmp);
    edges.dedup();
    edges
        .windows(2)
        .map(|strip| {
            let mut spans: Vec<(f32, f32)> = rects
                .iter()
                .filter(|rect| rect.x1 <= strip[0] && rect.x2 >= strip[1])
                .map(|rect| (rect.y1, rect.y2))
                .collect();
            spans.sort_by(|a, b| a.0.total_cmp(&b.0));
            let (mut covered, mut reach) = (0.0, f32::MIN);
            for (top, bottom) in spans {
                covered += (bottom - top.max(reach)).max(0.0);
                reach = reach.max(bottom);
            }
            covered * (strip[1] - strip[0])
        })
        .sum()
}

/// Where a finding sits in its file.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Place {
    Text {
        start: usize,
        end: usize,
    },
    Cell {
        row: usize,
        column: usize,
        start: usize,
        end: usize,
    },
    Region(Rect),
    /// A location kind scoring does not know.
    Unplaced,
}

/// What a redacter found, reduced to what scoring compares: the matched text and where it is.
#[derive(Debug, Clone, PartialEq)]
pub struct Finding {
    pub value: String,
    pub place: Place,
}

impl Finding {
    /// One finding reported twice, by two redacters of a chain for instance, sits at one place
    /// and counts once. Findings of an unknown kind of place are never taken for one.
    fn is_same_finding(&self, other: &Finding) -> bool {
        self.place != Place::Unplaced && self.place == other.place
    }

    fn region(&self) -> Option<Rect> {
        match self.place {
            Place::Region(region) => Some(region),
            _ => None,
        }
    }
}

/// One thing a file holds: a value of a text, or a box of an image.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum Target {
    Text(String),
    Region(Rect),
}

impl Target {
    /// Whether `finding` is on this target. A text finding must contain at least 60% of the
    /// target, or be at least 60% of it inside it, and be at most 3 times its length, ignoring
    /// case. A box finding must overlap the target and be at most 3 times its area.
    fn is_touched_by(&self, finding: &Finding) -> bool {
        match (self, finding.region()) {
            (Target::Text(expected), _) => {
                let (expected, found) = (
                    expected.trim().to_lowercase(),
                    finding.value.trim().to_lowercase(),
                );
                let (expected_len, found_len) = (expected.chars().count(), found.chars().count());
                let covered = if found.contains(&expected) {
                    expected_len
                } else if expected.contains(&found) {
                    found_len
                } else {
                    0
                };
                covered as f32 >= MIN_COVERED * expected_len as f32
                    && found_len as f32 <= MAX_SIZE_FACTOR * expected_len as f32
            }
            (Target::Region(expected), Some(found)) => {
                expected.intersection(&found).is_some()
                    && found.area() <= MAX_SIZE_FACTOR * expected.area()
            }
            (Target::Region(_), None) => false,
        }
    }

    /// Whether `finding` lies inside this target: a text finding is a part of the value, a box
    /// finding has at least 60% of its area in the target. Such a finding is on sensitive text
    /// without finding the value, so it is not a false alarm.
    fn holds(&self, finding: &Finding) -> bool {
        match (self, finding.region()) {
            (Target::Text(expected), _) => {
                let found = finding.value.trim().to_lowercase();
                !found.is_empty() && expected.trim().to_lowercase().contains(&found)
            }
            (Target::Region(expected), Some(found)) => expected
                .intersection(&found)
                .is_some_and(|overlap| overlap.area() >= MIN_COVERED * found.area()),
            (Target::Region(_), None) => false,
        }
    }

    /// Whether `findings` together find this target. A box is often reported as one box per
    /// word, so the boxes on it count by the area they cover together.
    fn is_found_by(&self, findings: &[Finding]) -> bool {
        match self {
            Target::Text(_) => findings.iter().any(|finding| self.is_touched_by(finding)),
            Target::Region(expected) => {
                let covered: Vec<Rect> = findings
                    .iter()
                    .filter(|finding| self.is_touched_by(finding))
                    .filter_map(|finding| finding.region()?.intersection(expected))
                    .collect();
                union_area(&covered) >= MIN_COVERED * expected.area()
            }
        }
    }
}

/// A value or box a redacter should find, and the category recall counts it in.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Sensitive {
    pub category: Category,
    pub value: Target,
}

/// What one file holds, from the truth of its dataset.
#[derive(Debug, Clone, PartialEq, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileTruth {
    /// Sensitive values a redacter should find.
    pub sensitive: Vec<Sensitive>,
    /// Values whose redaction is a configuration choice (cities, organisations): finding one
    /// is neither right nor wrong.
    #[serde(default)]
    pub neutral: Vec<Target>,
    /// Values that look sensitive and are not: finding one is a false alarm.
    #[serde(default)]
    pub keep: Vec<Target>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Recall {
    pub found: usize,
    pub total: usize,
}

/// The counts of one scored file, or of many added together.
#[derive(Debug, Default, Clone, PartialEq, Serialize)]
pub struct Tally {
    pub recall: BTreeMap<Category, Recall>,
    /// Distinct findings on a sensitive target.
    pub true_findings: usize,
    /// Distinct findings on no sensitive and no neutral target.
    pub false_findings: usize,
    pub keep_hit: usize,
    pub keep_total: usize,
}

impl Tally {
    pub fn merge(&mut self, other: &Tally) {
        for (category, recall) in &other.recall {
            let sum = self.recall.entry(*category).or_default();
            sum.found += recall.found;
            sum.total += recall.total;
        }
        self.true_findings += other.true_findings;
        self.false_findings += other.false_findings;
        self.keep_hit += other.keep_hit;
        self.keep_total += other.keep_total;
    }

    /// The share of sensitive targets found, `None` for a dataset without any.
    pub fn overall_recall(&self) -> Option<f64> {
        let found: usize = self.recall.values().map(|recall| recall.found).sum();
        let total: usize = self.recall.values().map(|recall| recall.total).sum();
        (total > 0).then(|| found as f64 / total as f64)
    }

    /// The share of findings that were on a sensitive target, `None` without findings.
    pub fn precision(&self) -> Option<f64> {
        let findings = self.true_findings + self.false_findings;
        (findings > 0).then(|| self.true_findings as f64 / findings as f64)
    }
}

impl FileTruth {
    /// Scores the findings of one file. A finding is true when it is on a sensitive target the
    /// findings find, and a box target is one true finding however many boxes it is found by.
    /// A finding left out of the count is one on a neutral target, and one inside a sensitive
    /// target that the findings do not find.
    pub fn score(&self, findings: &[Finding]) -> Tally {
        let mut distinct: Vec<Finding> = Vec::new();
        for finding in findings {
            if !distinct.iter().any(|seen| seen.is_same_finding(finding)) {
                distinct.push(finding.clone());
            }
        }
        let mut tally = self.unscored();
        let found: Vec<bool> = self
            .sensitive
            .iter()
            .map(|sensitive| sensitive.value.is_found_by(&distinct))
            .collect();
        for (sensitive, found) in self.sensitive.iter().zip(&found) {
            tally.recall.entry(sensitive.category).or_default().found += usize::from(*found);
        }
        tally.keep_hit = self
            .keep
            .iter()
            .filter(|target| target.is_found_by(&distinct))
            .count();
        let mut box_counted = vec![false; self.sensitive.len()];
        for finding in &distinct {
            let on_found_target = self
                .sensitive
                .iter()
                .zip(&found)
                .position(|(sensitive, found)| *found && sensitive.value.is_touched_by(finding));
            match on_found_target {
                Some(index) => {
                    let repeats_box = matches!(self.sensitive[index].value, Target::Region(_))
                        && std::mem::replace(&mut box_counted[index], true);
                    tally.true_findings += usize::from(!repeats_box);
                }
                None if self
                    .sensitive
                    .iter()
                    .any(|sensitive| sensitive.value.holds(finding))
                    || self
                        .neutral
                        .iter()
                        .any(|target| target.is_touched_by(finding)) => {}
                None => tally.false_findings += 1,
            }
        }
        tally
    }

    /// What a file the scan did not read adds: every target missed.
    pub fn unscored(&self) -> Tally {
        let mut tally = Tally {
            keep_total: self.keep.len(),
            ..Tally::default()
        };
        for sensitive in &self.sensitive {
            tally.recall.entry(sensitive.category).or_default().total += 1;
        }
        tally
    }
}

/// The counters and files of a `scan` JSON report that scoring reads.
#[derive(Debug, Deserialize)]
pub struct ScanReport {
    pub files_failed: usize,
    pub files_blocked: usize,
    pub files_unsupported: usize,
    files: Vec<ReportFile>,
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum FileStatus {
    Scanned,
    #[serde(other)]
    NotScanned,
}

#[derive(Debug, Deserialize)]
struct ReportFile {
    path: String,
    status: FileStatus,
    findings: Vec<ReportFinding>,
}

#[derive(Debug, Deserialize)]
struct ReportFinding {
    value: String,
    location: ReportLocation,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ReportLocation {
    Text {
        start: usize,
        end: usize,
    },
    TableCell {
        row: usize,
        column: usize,
        start: usize,
        end: usize,
    },
    Image {
        x1: f32,
        y1: f32,
        x2: f32,
        y2: f32,
    },
    #[serde(other)]
    Other,
}

impl From<ReportFinding> for Finding {
    fn from(finding: ReportFinding) -> Self {
        let place = match finding.location {
            ReportLocation::Text { start, end } => Place::Text { start, end },
            ReportLocation::TableCell {
                row,
                column,
                start,
                end,
            } => Place::Cell {
                row,
                column,
                start,
                end,
            },
            ReportLocation::Image { x1, y1, x2, y2 } => Place::Region(Rect::from([x1, y1, x2, y2])),
            ReportLocation::Other => Place::Unplaced,
        };
        Finding {
            value: finding.value,
            place,
        }
    }
}

impl ScanReport {
    /// Scores every file of `truth`. A file the report lacks, or did not scan, counts as missed.
    pub fn score(self, truth: &BTreeMap<String, FileTruth>) -> Tally {
        let mut scanned: BTreeMap<String, Vec<Finding>> = self
            .files
            .into_iter()
            .filter(|file| file.status == FileStatus::Scanned)
            .map(|file| {
                let findings = file.findings.into_iter().map(Finding::from).collect();
                (file.path, findings)
            })
            .collect();
        let mut tally = Tally::default();
        for (path, file_truth) in truth {
            tally.merge(&match scanned.remove(path) {
                Some(findings) => file_truth.score(&findings),
                None => file_truth.unscored(),
            });
        }
        tally
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(value: &str) -> Finding {
        Finding {
            value: value.to_string(),
            place: Place::Unplaced,
        }
    }

    fn text_at(start: usize, value: &str) -> Finding {
        Finding {
            value: value.to_string(),
            place: Place::Text {
                start,
                end: start + value.len(),
            },
        }
    }

    fn boxed(x1: f32, y1: f32, x2: f32, y2: f32) -> Finding {
        Finding {
            value: String::new(),
            place: Place::Region(Rect::from([x1, y1, x2, y2])),
        }
    }

    fn truth_of(sensitive: &[(Category, &str)], neutral: &[&str], keep: &[&str]) -> FileTruth {
        let targets =
            |values: &[&str]| values.iter().map(|v| Target::Text(v.to_string())).collect();
        FileTruth {
            sensitive: sensitive
                .iter()
                .map(|(category, value)| Sensitive {
                    category: *category,
                    value: Target::Text(value.to_string()),
                })
                .collect(),
            neutral: targets(neutral),
            keep: targets(keep),
        }
    }

    fn person_box(x1: f32, y1: f32, x2: f32, y2: f32) -> FileTruth {
        FileTruth {
            sensitive: vec![Sensitive {
                category: Category::Person,
                value: Target::Region(Rect::from([x1, y1, x2, y2])),
            }],
            ..FileTruth::default()
        }
    }

    fn found(tally: &Tally, category: Category) -> (usize, usize) {
        (tally.recall[&category].found, tally.recall[&category].total)
    }

    fn counted(tally: &Tally) -> (usize, usize) {
        (tally.true_findings, tally.false_findings)
    }

    /// Whether a finding is on `target`, judged as a sensitive value and as a keep string,
    /// which must agree.
    fn text_hit(target: &str, finding: &str) -> bool {
        let sensitive = truth_of(&[(Category::Other, target)], &[], &[]).score(&[text(finding)]);
        let keep = truth_of(&[], &[], &[target]).score(&[text(finding)]);
        let hit = found(&sensitive, Category::Other).0 == 1;
        assert_eq!(hit, keep.keep_hit == 1, "{finding:?} on {target:?}");
        hit
    }

    #[test]
    fn text_finding_must_cover_most_of_the_target_without_being_much_longer() {
        let address = "221B Baker Street, London NW1 6XE";

        assert!(text_hit(address, "221b baker street, london"));
        assert!(text_hit("alice@example.com", "Email: ALICE@example.com"));
        assert!(!text_hit("VA", "Vanessa"));
        assert!(!text_hit(address, "London"));
        assert!(!text_hit(
            "alice@example.com",
            "Please write to alice@example.com about the delivery before Friday"
        ));
    }

    #[test]
    fn findings_are_true_neutral_or_false_and_keep_hits_are_counted() {
        let truth = truth_of(
            &[
                (Category::Contact, "alice@example.com"),
                (Category::Person, "Alice Example"),
            ],
            &["London"],
            &["Order#1"],
        );

        let tally = truth.score(&[
            text("alice@example.com"),
            text("London"),
            text("Order#1"),
            text("Monday"),
        ]);

        assert_eq!(found(&tally, Category::Contact), (1, 1));
        assert_eq!(found(&tally, Category::Person), (0, 1));
        assert_eq!(counted(&tally), (1, 2));
        assert_eq!((tally.keep_hit, tally.keep_total), (1, 1));
        assert_eq!(tally.precision(), Some(1.0 / 3.0));
        assert_eq!(tally.overall_recall(), Some(0.5));
    }

    #[test]
    fn a_finding_reported_twice_at_one_place_counts_once() {
        let truth = truth_of(&[(Category::Contact, "alice@example.com")], &[], &[]);

        let tally = truth.score(&[
            text_at(0, "alice@example.com"),
            text_at(0, "ALICE@example.com"),
            text_at(10, "Monday"),
            text_at(10, "Monday"),
        ]);

        assert_eq!(counted(&tally), (1, 1));
    }

    #[test]
    fn identical_findings_at_different_places_count_separately() {
        let truth = truth_of(&[], &[], &[]);

        let tally = truth.score(&[text_at(0, "Monday"), text_at(40, "Monday")]);

        assert_eq!(counted(&tally), (0, 2));
    }

    #[test]
    fn a_finding_inside_a_sensitive_value_that_is_not_found_is_not_a_false_alarm() {
        let truth = truth_of(
            &[(Category::Address, "221B Baker Street, London NW1 6XE")],
            &[],
            &[],
        );

        let tally = truth.score(&[text("Baker Street")]);

        assert_eq!(found(&tally, Category::Address), (0, 1));
        assert_eq!(counted(&tally), (0, 0));
    }

    #[test]
    fn box_target_is_found_by_the_union_of_its_word_boxes_and_counts_once() {
        let truth = person_box(100.0, 100.0, 300.0, 120.0);

        let words = truth.score(&[
            boxed(98.0, 98.0, 160.0, 122.0),
            boxed(170.0, 98.0, 305.0, 122.0),
        ]);
        let whole_page = truth.score(&[boxed(0.0, 0.0, 1000.0, 720.0)]);

        assert_eq!(found(&words, Category::Person), (1, 1));
        assert_eq!(counted(&words), (1, 0));
        assert_eq!(found(&whole_page, Category::Person), (0, 1));
        assert_eq!(counted(&whole_page), (0, 1));
    }

    #[test]
    fn a_box_mostly_inside_a_box_target_that_is_not_found_is_not_a_false_alarm() {
        let truth = person_box(100.0, 100.0, 300.0, 120.0);

        let one_word = truth.score(&[boxed(100.0, 100.0, 160.0, 120.0)]);
        let mostly_outside = truth.score(&[boxed(250.0, 100.0, 400.0, 120.0)]);

        assert_eq!(found(&one_word, Category::Person), (0, 1));
        assert_eq!(counted(&one_word), (0, 0));
        assert_eq!(counted(&mostly_outside), (0, 1));
    }

    const REPORT: &str = r#"{
        "files_scanned": 3, "files_failed": 1, "files_blocked": 0, "files_unsupported": 0,
        "files": [
            {"path": "a.txt", "status": "scanned", "findings": [
                {"redacter": "local-rules", "label": "email", "score": null,
                 "location": {"kind": "text", "start": 0, "end": 17, "line": 1, "column": 1},
                 "value": "alice@example.com"},
                {"redacter": "local-ner", "label": "EMAIL", "score": 0.8,
                 "location": {"kind": "text", "start": 0, "end": 17, "line": 1, "column": 1},
                 "value": "alice@example.com"}]},
            {"path": "b.png", "status": "scanned", "findings": [
                {"redacter": "gcp-dlp", "label": "PERSON_NAME", "score": 0.9,
                 "location": {"kind": "image", "page": null, "y1": 1.0, "x1": 2.0, "y2": 3.0, "x2": 4.0},
                 "value": "Alice"}]},
            {"path": "c.txt", "status": "failed", "findings": []}
        ]
    }"#;

    #[test]
    fn report_is_scored_per_file_and_unread_files_count_as_missed() {
        let email = || truth_of(&[(Category::Contact, "alice@example.com")], &[], &[]);
        let truth = BTreeMap::from([
            ("a.txt".to_string(), email()),
            ("c.txt".to_string(), email()),
            ("missing.txt".to_string(), email()),
            ("b.png".to_string(), person_box(2.0, 1.0, 4.0, 3.0)),
        ]);
        let report: ScanReport = serde_json::from_str(REPORT).unwrap();

        assert_eq!(report.files_failed, 1);
        let tally = report.score(&truth);

        assert_eq!(found(&tally, Category::Contact), (1, 3));
        assert_eq!(found(&tally, Category::Person), (1, 1));
        assert_eq!(tally.true_findings, 2);
    }
}
