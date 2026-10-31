//! Scoring of a `scan` report against a dataset's truth. Everything here is a pure function of
//! the report and the truth, so it is unit tested without running a redacter.

use crate::categories::Category;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

const REGION_MIN_COVERED: f32 = 0.6;
const REGION_MAX_AREA_FACTOR: f32 = 3.0;

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

    fn intersection_area(&self, other: &Rect) -> f32 {
        let width = self.x2.min(other.x2) - self.x1.max(other.x1);
        let height = self.y2.min(other.y2) - self.y1.max(other.y1);
        width.max(0.0) * height.max(0.0)
    }
}

/// What a redacter found, reduced to what scoring compares: the matched text and, for images,
/// where it is.
#[derive(Debug, Clone, PartialEq)]
pub struct Finding {
    pub value: String,
    pub region: Option<Rect>,
}

/// One thing a file holds: a value of a text, or a box of an image.
#[derive(Debug, Clone, PartialEq)]
pub enum Target {
    Text(String),
    Region(Rect),
}

impl Target {
    /// Whether `finding` is on this target. Text matches when either value contains the other,
    /// ignoring case, so a redacter that finds only the postcode of an address, or a whole
    /// line around an email, is on the target. A region is on target when the finding covers
    /// at least 60% of it and is at most 3 times its area, which keeps a finding that boxes a
    /// whole page from counting as every target on it.
    pub fn is_hit_by(&self, finding: &Finding) -> bool {
        match (self, &finding.region) {
            (Target::Text(expected), _) => {
                let (expected, found) = (expected.to_lowercase(), finding.value.to_lowercase());
                !found.is_empty() && (found.contains(&expected) || expected.contains(&found))
            }
            (Target::Region(expected), Some(found)) => {
                expected.intersection_area(found) >= REGION_MIN_COVERED * expected.area()
                    && found.area() <= REGION_MAX_AREA_FACTOR * expected.area()
            }
            (Target::Region(_), None) => false,
        }
    }
}

/// What one file holds, from the truth of its dataset.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct FileTruth {
    /// Sensitive values a redacter should find.
    pub sensitive: Vec<(Category, Target)>,
    /// Values whose redaction is a configuration choice (cities, organisations): finding one
    /// is neither right nor wrong.
    pub neutral: Vec<Target>,
    /// Values that look sensitive and are not: finding one is a false alarm.
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
    /// Findings on a sensitive target.
    pub true_findings: usize,
    /// Findings on no sensitive and no neutral target.
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
    /// Scores the findings of one file.
    pub fn score(&self, findings: &[Finding]) -> Tally {
        let mut tally = self.unscored();
        for (category, target) in &self.sensitive {
            if findings.iter().any(|finding| target.is_hit_by(finding)) {
                tally.recall.entry(*category).or_default().found += 1;
            }
        }
        tally.keep_hit = self
            .keep
            .iter()
            .filter(|target| findings.iter().any(|finding| target.is_hit_by(finding)))
            .count();
        for finding in findings {
            let on = |target: &Target| target.is_hit_by(finding);
            if self.sensitive.iter().any(|(_, target)| on(target)) {
                tally.true_findings += 1;
            } else if !self.neutral.iter().any(on) {
                tally.false_findings += 1;
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
        for (category, _) in &self.sensitive {
            tally.recall.entry(*category).or_default().total += 1;
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

#[derive(Debug, Deserialize)]
struct ReportFile {
    path: String,
    status: String,
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
        let region = match finding.location {
            ReportLocation::Image { x1, y1, x2, y2 } => Some(Rect::from([x1, y1, x2, y2])),
            ReportLocation::Other => None,
        };
        Finding {
            value: finding.value,
            region,
        }
    }
}

impl ScanReport {
    /// Scores every file of `truth`. A file the report lacks, or did not scan, counts as missed.
    pub fn score(self, truth: &BTreeMap<String, FileTruth>) -> Tally {
        let mut scanned: BTreeMap<String, Vec<Finding>> = self
            .files
            .into_iter()
            .filter(|file| file.status == "scanned")
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
            region: None,
        }
    }

    fn boxed(x1: f32, y1: f32, x2: f32, y2: f32) -> Finding {
        Finding {
            value: String::new(),
            region: Some(Rect::from([x1, y1, x2, y2])),
        }
    }

    fn email_truth() -> FileTruth {
        FileTruth {
            sensitive: vec![(
                Category::Contact,
                Target::Text("alice@example.com".to_string()),
            )],
            neutral: vec![Target::Text("London".to_string())],
            keep: vec![Target::Text("Order#1".to_string())],
        }
    }

    #[test]
    fn text_target_is_hit_by_a_value_containing_it_or_inside_it() {
        let target = Target::Text("221B Baker Street, London NW1 6XE".to_string());

        assert!(target.is_hit_by(&text("NW1 6XE")));
        assert!(target.is_hit_by(&text("Address: 221B Baker Street, London NW1 6XE.")));
        assert!(!target.is_hit_by(&text("Paris")));
    }

    #[test]
    fn text_match_ignores_case_and_never_matches_an_empty_value() {
        let target = Target::Text("jonas petersen".to_string());

        assert!(target.is_hit_by(&text("Jonas Petersen")));
        assert!(!target.is_hit_by(&text("")));
    }

    #[test]
    fn region_target_needs_most_of_the_box_and_not_much_more() {
        let target = Target::Region(Rect::from([310.0, 195.0, 620.0, 217.0]));

        assert!(target.is_hit_by(&boxed(305.0, 190.0, 625.0, 222.0)));
        assert!(!target.is_hit_by(&boxed(310.0, 195.0, 400.0, 217.0)));
        assert!(!target.is_hit_by(&boxed(0.0, 0.0, 1000.0, 720.0)));
        assert!(!target.is_hit_by(&text("john.smith@example.com")));
    }

    #[test]
    fn scoring_counts_found_missed_neutral_and_false_findings() {
        let truth = FileTruth {
            sensitive: vec![
                (
                    Category::Contact,
                    Target::Text("alice@example.com".to_string()),
                ),
                (Category::Person, Target::Text("Alice Example".to_string())),
            ],
            ..email_truth()
        };

        let tally = truth.score(&[
            text("alice@example.com"),
            text("London"),
            text("Order#1"),
            text("Monday"),
        ]);

        assert_eq!(
            tally.recall[&Category::Contact],
            Recall { found: 1, total: 1 }
        );
        assert_eq!(
            tally.recall[&Category::Person],
            Recall { found: 0, total: 1 }
        );
        assert_eq!(tally.true_findings, 1);
        assert_eq!(tally.false_findings, 2);
        assert_eq!(tally.keep_hit, 1);
        assert_eq!(tally.keep_total, 1);
        assert_eq!(tally.precision(), Some(1.0 / 3.0));
        assert_eq!(tally.overall_recall(), Some(0.5));
    }

    #[test]
    fn unscored_file_misses_every_target() {
        let tally = email_truth().unscored();

        assert_eq!(
            tally.recall[&Category::Contact],
            Recall { found: 0, total: 1 }
        );
        assert_eq!(tally.keep_total, 1);
        assert_eq!(tally.keep_hit, 0);
        assert_eq!(tally.precision(), None);
    }

    #[test]
    fn merged_tallies_add_up() {
        let mut total = email_truth().score(&[text("alice@example.com")]);
        total.merge(&email_truth().unscored());

        assert_eq!(
            total.recall[&Category::Contact],
            Recall { found: 1, total: 2 }
        );
        assert_eq!(total.keep_total, 2);
        assert_eq!(total.overall_recall(), Some(0.5));
    }

    const REPORT: &str = r#"{
        "files_scanned": 2, "files_failed": 1, "files_blocked": 0, "files_unsupported": 0,
        "files": [
            {"path": "a.txt", "status": "scanned", "findings": [
                {"redacter": "local-rules", "label": "email", "score": null,
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
    fn report_is_scored_per_file_and_failed_files_count_as_missed() {
        let truth = BTreeMap::from([
            ("a.txt".to_string(), email_truth()),
            ("c.txt".to_string(), email_truth()),
            ("missing.txt".to_string(), email_truth()),
        ]);
        let report: ScanReport = serde_json::from_str(REPORT).unwrap();

        assert_eq!(report.files_failed, 1);
        let tally = report.score(&truth);

        assert_eq!(
            tally.recall[&Category::Contact],
            Recall { found: 1, total: 3 }
        );
        assert_eq!(tally.true_findings, 1);
    }

    #[test]
    fn image_findings_carry_their_box() {
        let report: ScanReport = serde_json::from_str(REPORT).unwrap();
        let truth = BTreeMap::from([(
            "b.png".to_string(),
            FileTruth {
                sensitive: vec![(
                    Category::Person,
                    Target::Region(Rect::from([2.0, 1.0, 4.0, 3.0])),
                )],
                ..FileTruth::default()
            },
        )]);

        let tally = report.score(&truth);

        assert_eq!(
            tally.recall[&Category::Person],
            Recall { found: 1, total: 1 }
        );
    }
}
