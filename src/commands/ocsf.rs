//! OCSF 1.9.0 reports for SIEMs and security data lakes.
//!
//! OCSF, the Open Cybersecurity Schema Framework, names the attributes of security events so
//! that tools can exchange them. The writer takes what a command found as [`ReportedFile`]s and
//! returns one Data Security Finding event (class 2006) for each file that holds findings or
//! may hold some. A file that was clean, left out, unsupported, blocked or failed has no event:
//! the report carries findings only, and the JSON report tells what else happened.
//!
//! The events go to a data lake with a long retention and wide read access, so a value is
//! always masked here, whatever the caller passes, and no hash of a value is written.

use crate::commands::findings::{FileOutcome, PixelBox, Place, ReportedFile, ReportedFinding};
use crate::commands::sarif::ArtifactLocator;
use crate::redacters::{DetectionLabel, Score};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, SystemTimeError, UNIX_EPOCH};

const OCSF_VERSION: &str = "1.9.0";
const CLASS_UID: u32 = 2006;
const CATEGORY_UID: u32 = 2;
const ACTIVITY_ID_CREATE: u32 = 1;

/// "Data Discovery & Classification" in the `detection_system_id` enum of `data_security`.
const DETECTION_SYSTEM_ID: u8 = 4;
const DETECTION_SYSTEM: &str = "Data Discovery & Classification";

/// A time as OCSF writes it: milliseconds since the Unix epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct EpochMillis(u64);

impl TryFrom<SystemTime> for EpochMillis {
    type Error = SystemTimeError;

    fn try_from(time: SystemTime) -> Result<Self, Self::Error> {
        let since_epoch = time.duration_since(UNIX_EPOCH)?;
        Ok(Self(
            since_epoch.as_secs() * 1000 + u64::from(since_epoch.subsec_millis()),
        ))
    }
}

/// How serious an event is, by the `severity_id` enum of OCSF.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Severity {
    /// Values the redacters named could not be found in the file.
    Informational,
    /// Sensitive data was found.
    Medium,
    /// A credential was found.
    High,
}

impl Severity {
    fn id(self) -> u8 {
        match self {
            Self::Informational => 1,
            Self::Medium => 3,
            Self::High => 4,
        }
    }

    fn caption(self) -> &'static str {
        match self {
            Self::Informational => "Informational",
            Self::Medium => "Medium",
            Self::High => "High",
        }
    }
}

/// A redacter's score as a whole percentage, which is what `confidence_score` holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
struct ConfidenceScore(u8);

impl From<Score> for ConfidenceScore {
    fn from(score: Score) -> Self {
        // A score prints with two decimals, the precision of a whole percent.
        let score: f32 = score
            .to_string()
            .parse()
            .expect("a score prints as a decimal number");
        Self((score * 100.0).round() as u8)
    }
}

/// How sure the analytic is, by the `confidence_id` enum of OCSF.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Confidence {
    Unknown,
    Low,
    Medium,
    High,
}

impl Confidence {
    fn id(self) -> u8 {
        match self {
            Self::Unknown => 0,
            Self::Low => 1,
            Self::Medium => 2,
            Self::High => 3,
        }
    }

    fn caption(self) -> &'static str {
        match self {
            Self::Unknown => "Unknown",
            Self::Low => "Low",
            Self::Medium => "Medium",
            Self::High => "High",
        }
    }
}

impl From<ConfidenceScore> for Confidence {
    fn from(ConfidenceScore(percent): ConfidenceScore) -> Self {
        match percent {
            0..=49 => Self::Low,
            50..=79 => Self::Medium,
            _ => Self::High,
        }
    }
}

/// The events of a run, in the order of its files.
#[derive(Debug)]
pub struct OcsfEvents(Vec<Event>);

impl OcsfEvents {
    /// `time` is when the scan ran. `locator` tells where the files are.
    pub fn new(files: &[ReportedFile], locator: &ArtifactLocator, time: EpochMillis) -> Self {
        Self(
            files
                .iter()
                .filter_map(|file| Event::of(file, &locator.full_uri(&file.path), time))
                .collect(),
        )
    }

    /// The events as NDJSON (newline-delimited JSON): one event on each line, each line ended by
    /// a newline, and nothing for a run without events.
    pub fn to_ndjson(&self) -> serde_json::Result<String> {
        let mut ndjson = String::new();
        for event in &self.0 {
            ndjson.push_str(&serde_json::to_string(event)?);
            ndjson.push('\n');
        }
        Ok(ndjson)
    }
}

#[derive(Debug, Serialize)]
struct Event {
    class_uid: u32,
    class_name: &'static str,
    category_uid: u32,
    category_name: &'static str,
    activity_id: u32,
    activity_name: &'static str,
    type_uid: u32,
    type_name: &'static str,
    time: EpochMillis,
    severity_id: u8,
    severity: &'static str,
    status_id: u8,
    status: &'static str,
    confidence_id: u8,
    confidence: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    confidence_score: Option<ConfidenceScore>,
    message: String,
    metadata: Metadata,
    finding_info: FindingInfo,
    #[serde(skip_serializing_if = "Option::is_none")]
    data_security: Option<DataSecurity>,
    file: File,
    unmapped: Unmapped,
}

impl Event {
    /// The event of a file, or `None` for a file with nothing to report.
    fn of(file: &ReportedFile, uri: &str, time: EpochMillis) -> Option<Self> {
        let summary = match &file.outcome {
            FileOutcome::Findings(findings) if !findings.is_empty() => Summary::found(findings),
            FileOutcome::Unconfirmed { unlocated_values } => {
                Summary::unconfirmed(*unlocated_values)
            }
            FileOutcome::Findings(_)
            | FileOutcome::Failed { .. }
            | FileOutcome::Blocked { .. }
            | FileOutcome::Unsupported { .. } => return None,
        };
        let confidence = summary
            .confidence
            .map_or(Confidence::Unknown, Confidence::from);
        Some(Self {
            class_uid: CLASS_UID,
            class_name: "Data Security Finding",
            category_uid: CATEGORY_UID,
            category_name: "Findings",
            activity_id: ACTIVITY_ID_CREATE,
            activity_name: "Create",
            type_uid: CLASS_UID * 100 + ACTIVITY_ID_CREATE,
            type_name: "Data Security Finding: Create",
            time,
            severity_id: summary.severity.id(),
            severity: summary.severity.caption(),
            status_id: 1,
            status: "New",
            confidence_id: confidence.id(),
            confidence: confidence.caption(),
            confidence_score: summary.confidence,
            message: format!("{} in {}", summary.message, file.path),
            metadata: Metadata::new(),
            finding_info: FindingInfo::new(uri, summary.title, summary.types, time),
            data_security: summary.data_security,
            file: File::new(&file.path, uri),
            unmapped: summary.unmapped,
        })
    }
}

/// What an event says about the findings of a file, apart from the file itself.
struct Summary {
    severity: Severity,
    /// The highest score of the findings; `None` when none has a score.
    confidence: Option<ConfidenceScore>,
    message: String,
    title: &'static str,
    types: [&'static str; 1],
    data_security: Option<DataSecurity>,
    unmapped: Unmapped,
}

impl Summary {
    /// The findings of a file are grouped by redacter, label and masked value: a group is one
    /// discovery detail, with a place for each finding in it.
    fn found(findings: &[ReportedFinding]) -> Self {
        let mut details: Vec<DiscoveryDetail> = Vec::new();
        let mut detail_of_group: BTreeMap<(String, &DetectionLabel, &str), usize> = BTreeMap::new();
        let mut redacter_extras = Vec::new();
        for finding in findings {
            let group = (
                finding.redacter.to_string(),
                &finding.label,
                finding.masked_value.as_str(),
            );
            let index = *detail_of_group.entry(group).or_insert_with(|| {
                details.push(DiscoveryDetail::new(finding));
                details.len() - 1
            });
            let detail = &mut details[index];
            detail.count += 1;
            let occurrence = finding
                .place
                .as_ref()
                .and_then(Place::occurrence)
                .map(|occurrence| {
                    detail.occurrences.push(occurrence);
                    detail.occurrences.len() - 1
                });
            let location = finding.place.as_ref().and_then(Place::location);
            if finding.score.is_some() || location.is_some() {
                redacter_extras.push(FindingExtras {
                    discovery_detail: index,
                    occurrence,
                    score: finding.score,
                    location,
                });
            }
        }
        let redacters: BTreeSet<String> = findings
            .iter()
            .map(|finding| finding.redacter.to_string())
            .collect();
        Self {
            severity: if findings.iter().any(ReportedFinding::is_secret) {
                Severity::High
            } else {
                Severity::Medium
            },
            confidence: findings
                .iter()
                .filter_map(|finding| finding.score)
                .map(ConfidenceScore::from)
                .max(),
            message: format!(
                "{} sensitive {} found",
                findings.len(),
                if findings.len() == 1 {
                    "value"
                } else {
                    "values"
                }
            ),
            title: "Sensitive data found",
            types: ["sensitive_data"],
            data_security: Some(DataSecurity {
                detection_system_id: DETECTION_SYSTEM_ID,
                detection_system: DETECTION_SYSTEM,
                detection_pattern: redacters.into_iter().collect::<Vec<_>>().join(", "),
                discovery_details: details,
            }),
            unmapped: Unmapped {
                redacter: RedacterExtras {
                    findings: redacter_extras,
                    unlocated_values: None,
                },
            },
        }
    }

    fn unconfirmed(unlocated_values: usize) -> Self {
        Self {
            severity: Severity::Informational,
            confidence: None,
            message: format!(
                "{unlocated_values} {} named by the redacters could not be located, so the file \
                 may hold sensitive data",
                if unlocated_values == 1 {
                    "value"
                } else {
                    "values"
                }
            ),
            title: "Sensitive data may be present",
            types: ["unconfirmed"],
            data_security: None,
            unmapped: Unmapped {
                redacter: RedacterExtras {
                    findings: Vec::new(),
                    unlocated_values: Some(unlocated_values),
                },
            },
        }
    }
}

#[derive(Debug, Serialize)]
struct Metadata {
    version: &'static str,
    product: Product,
}

impl Metadata {
    fn new() -> Self {
        Self {
            version: OCSF_VERSION,
            product: Product {
                name: "redacter",
                version: env!("CARGO_PKG_VERSION"),
                url_string: env!("CARGO_PKG_REPOSITORY"),
            },
        }
    }
}

#[derive(Debug, Serialize)]
struct Product {
    name: &'static str,
    version: &'static str,
    url_string: &'static str,
}

#[derive(Debug, Serialize)]
struct FindingInfo {
    /// Stable across runs: the same file has the same identifier in the next scan. It is
    /// derived from where the file is and never from what it holds.
    uid: String,
    title: &'static str,
    types: [&'static str; 1],
    created_time: EpochMillis,
}

impl FindingInfo {
    fn new(
        uri: &str,
        title: &'static str,
        types: [&'static str; 1],
        created_time: EpochMillis,
    ) -> Self {
        Self {
            uid: Sha256::digest(uri.as_bytes())
                .iter()
                .take(16)
                .map(|byte| format!("{byte:02x}"))
                .collect(),
            title,
            types,
            created_time,
        }
    }
}

#[derive(Debug, Serialize)]
struct DataSecurity {
    detection_system_id: u8,
    detection_system: &'static str,
    /// The redacters that found something, as `--redact` names them.
    detection_pattern: String,
    discovery_details: Vec<DiscoveryDetail>,
}

/// The findings of one redacter with one label and one masked value in a file.
#[derive(Debug, Serialize)]
struct DiscoveryDetail {
    #[serde(rename = "type")]
    label: String,
    count: usize,
    rule: Rule,
    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    occurrences: Vec<Occurrence>,
}

impl DiscoveryDetail {
    fn new(finding: &ReportedFinding) -> Self {
        Self {
            label: finding.label.to_string(),
            count: 0,
            rule: Rule {
                name: finding.redacter.to_string(),
            },
            value: Some(finding.masked_value.clone()).filter(|value| !value.is_empty()),
            occurrences: Vec::new(),
        }
    }
}

#[derive(Debug, Serialize)]
struct Rule {
    name: String,
}

/// The places of `occurrence_details` that a finding fills. Lines, columns, rows and pages
/// count from 1, and the column of a text is a character column.
#[derive(Debug, Default, Serialize)]
struct Occurrence {
    #[serde(skip_serializing_if = "Option::is_none")]
    start_line: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    end_line: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    column_number: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    row_number: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    page_number: Option<usize>,
}

impl Place {
    /// The place in the terms of OCSF; `None` when it names none of them, such as an image
    /// box on an image that is not a page.
    fn occurrence(&self) -> Option<Occurrence> {
        match *self {
            Self::Text {
                line,
                column,
                end_line,
                ..
            } => Some(Occurrence {
                start_line: Some(line),
                end_line: Some(end_line),
                column_number: Some(column),
                ..Occurrence::default()
            }),
            Self::TableCell { row, column, .. } => Some(Occurrence {
                row_number: Some(row),
                column_number: Some(column),
                ..Occurrence::default()
            }),
            Self::Image { page, .. } => page.map(|page| Occurrence {
                page_number: Some(page),
                ..Occurrence::default()
            }),
        }
    }

    /// What of the place OCSF has no attribute for.
    fn location(&self) -> Option<Location> {
        match *self {
            Self::Text {
                end_column,
                byte_start,
                byte_end,
                ..
            } => Some(Location::Text {
                byte_start,
                byte_end,
                end_column,
            }),
            Self::TableCell {
                byte_start,
                byte_end,
                ..
            } => Some(Location::TableCell {
                byte_start,
                byte_end,
            }),
            Self::Image { area, .. } => area.map(|area| Location::Image { area }),
        }
    }
}

#[derive(Debug, Serialize)]
struct File {
    name: String,
    path: String,
    /// 1, a regular file.
    type_id: u8,
    #[serde(rename = "type")]
    type_: &'static str,
    uri: String,
}

impl File {
    fn new(path: &str, uri: &str) -> Self {
        Self {
            name: path.rsplit('/').next().unwrap_or(path).to_string(),
            path: path.to_string(),
            type_id: 1,
            type_: "Regular File",
            uri: uri.to_string(),
        }
    }
}

/// What OCSF has no attribute for, under the namespace of the product.
#[derive(Debug, Serialize)]
struct Unmapped {
    redacter: RedacterExtras,
}

#[derive(Debug, Serialize)]
struct RedacterExtras {
    /// What a finding has beyond what its discovery detail and occurrence say: its own score
    /// and the parts of its place OCSF cannot hold.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    findings: Vec<FindingExtras>,
    #[serde(skip_serializing_if = "Option::is_none")]
    unlocated_values: Option<usize>,
}

#[derive(Debug, Serialize)]
struct FindingExtras {
    /// The index in `data_security.discovery_details`.
    discovery_detail: usize,
    /// The index in `occurrences` of that discovery detail.
    #[serde(skip_serializing_if = "Option::is_none")]
    occurrence: Option<usize>,
    /// The score of the redacter as it gave it, from 0 to 1.
    #[serde(skip_serializing_if = "Option::is_none")]
    score: Option<Score>,
    #[serde(skip_serializing_if = "Option::is_none")]
    location: Option<Location>,
}

/// The parts of a place that OCSF has no attribute for. Byte ranges are half-open and count
/// from 0, in the text of the file or of the cell.
#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Location {
    Text {
        byte_start: usize,
        byte_end: usize,
        /// The character column after the last character of the range.
        end_column: usize,
    },
    TableCell {
        byte_start: usize,
        byte_end: usize,
    },
    Image {
        #[serde(rename = "box")]
        area: PixelBox,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::args::RedacterType;
    use serde_json::{json, Value};
    use std::path::Path;
    use std::time::Duration;

    const SCHEMA_PATH: &str = "test-fixtures/ocsf/data_security_finding-1.9.0.schema.json";

    fn scan_time() -> EpochMillis {
        EpochMillis::try_from(UNIX_EPOCH + Duration::from_millis(1_761_990_000_123)).unwrap()
    }

    fn text_at(line: usize, column: usize, end_column: usize) -> Option<Place> {
        Some(Place::Text {
            line,
            column,
            end_line: line,
            end_column,
            byte_start: 40,
            byte_end: 62,
        })
    }

    fn finding(
        redacter: RedacterType,
        label: &str,
        score: Option<f32>,
        place: Option<Place>,
        value: &str,
    ) -> ReportedFinding {
        ReportedFinding::new(
            redacter,
            DetectionLabel::new(label),
            score.map(|score| Score::try_from(score).unwrap()),
            place,
            value,
        )
    }

    fn email(place: Option<Place>, value: &str) -> ReportedFinding {
        finding(RedacterType::LocalRules, "email", None, place, value)
    }

    fn file(path: &str, outcome: FileOutcome) -> ReportedFile {
        ReportedFile {
            path: path.to_string(),
            outcome,
        }
    }

    fn found(path: &str, findings: Vec<ReportedFinding>) -> ReportedFile {
        file(path, FileOutcome::Findings(findings))
    }

    fn ndjson_of(files: &[ReportedFile]) -> String {
        let locator = ArtifactLocator::new("./", Path::new("/work"));
        OcsfEvents::new(files, &locator, scan_time())
            .to_ndjson()
            .unwrap()
    }

    fn events_of(files: &[ReportedFile]) -> Vec<Value> {
        ndjson_of(files)
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    fn event_of(file: ReportedFile) -> Value {
        let mut events = events_of(&[file]);
        assert_eq!(events.len(), 1, "{events:?}");
        events.remove(0)
    }

    fn details_of(event: &Value) -> &Value {
        &event["data_security"]["discovery_details"]
    }

    #[test]
    fn a_text_finding_is_an_event_with_an_occurrence_and_the_masked_value() {
        let event = event_of(found(
            "docs/note.txt",
            vec![finding(
                RedacterType::LocalNer,
                "per",
                Some(0.75),
                text_at(3, 15, 37),
                "john.smith@example.com",
            )],
        ));

        assert_eq!(
            event,
            json!({
                "class_uid": 2006,
                "class_name": "Data Security Finding",
                "category_uid": 2,
                "category_name": "Findings",
                "activity_id": 1,
                "activity_name": "Create",
                "type_uid": 200601,
                "type_name": "Data Security Finding: Create",
                "time": 1_761_990_000_123u64,
                "severity_id": 3,
                "severity": "Medium",
                "status_id": 1,
                "status": "New",
                "confidence_id": 2,
                "confidence": "Medium",
                "confidence_score": 75,
                "message": "1 sensitive value found in docs/note.txt",
                "metadata": {
                    "version": "1.9.0",
                    "product": {
                        "name": "redacter",
                        "version": env!("CARGO_PKG_VERSION"),
                        "url_string": "https://github.com/abdolence/redacter-rs"
                    }
                },
                "finding_info": {
                    "uid": event["finding_info"]["uid"],
                    "title": "Sensitive data found",
                    "types": ["sensitive_data"],
                    "created_time": 1_761_990_000_123u64
                },
                "data_security": {
                    "detection_system_id": 4,
                    "detection_system": "Data Discovery & Classification",
                    "detection_pattern": "local-ner",
                    "discovery_details": [{
                        "type": "per",
                        "count": 1,
                        "rule": {"name": "local-ner"},
                        "value": "j********************m",
                        "occurrences": [{"start_line": 3, "end_line": 3, "column_number": 15}]
                    }]
                },
                "file": {
                    "name": "note.txt",
                    "path": "docs/note.txt",
                    "type_id": 1,
                    "type": "Regular File",
                    "uri": "file:///work/docs/note.txt"
                },
                "unmapped": {"redacter": {"findings": [{
                    "discovery_detail": 0,
                    "occurrence": 0,
                    "score": 0.75,
                    "location": {"kind": "text", "byte_start": 40, "byte_end": 62, "end_column": 37}
                }]}}
            })
        );
    }

    #[test]
    fn a_table_cell_is_a_row_and_a_column_and_keeps_its_byte_range_in_unmapped() {
        let cell = Place::TableCell {
            row: 4,
            column: 2,
            byte_start: 0,
            byte_end: 9,
        };

        let event = event_of(found("people.csv", vec![email(Some(cell), "a@b.io")]));

        assert_eq!(
            details_of(&event)[0]["occurrences"],
            json!([{"row_number": 4, "column_number": 2}])
        );
        assert_eq!(
            event["unmapped"]["redacter"]["findings"],
            json!([{
                "discovery_detail": 0,
                "occurrence": 0,
                "location": {"kind": "table_cell", "byte_start": 0, "byte_end": 9}
            }])
        );
    }

    #[test]
    fn an_image_box_is_in_unmapped_and_its_page_is_an_occurrence() {
        let area = PixelBox {
            x1: 10.5,
            y1: 20.0,
            x2: 110.5,
            y2: 40.0,
        };
        let on_page = Place::Image {
            page: Some(2),
            area: Some(area),
        };
        let on_image = Place::Image {
            page: None,
            area: Some(area),
        };

        let event = event_of(found(
            "scan.pdf",
            vec![
                email(Some(on_page), "a@b.io"),
                email(Some(on_image), "c@d.io"),
            ],
        ));

        assert_eq!(
            details_of(&event)[0]["occurrences"],
            json!([{"page_number": 2}])
        );
        assert_eq!(
            details_of(&event)[1].get("occurrences"),
            None,
            "an image is not a page"
        );
        assert_eq!(
            event["unmapped"]["redacter"]["findings"],
            json!([
                {
                    "discovery_detail": 0,
                    "occurrence": 0,
                    "location": {"kind": "image", "box": {"x1": 10.5, "y1": 20.0, "x2": 110.5, "y2": 40.0}}
                },
                {
                    "discovery_detail": 1,
                    "location": {"kind": "image", "box": {"x1": 10.5, "y1": 20.0, "x2": 110.5, "y2": 40.0}}
                }
            ])
        );
    }

    #[test]
    fn a_finding_known_only_to_the_file_is_counted_without_an_occurrence() {
        let page_only = Place::Image {
            page: Some(3),
            area: None,
        };

        let event = event_of(found(
            "scan.pdf",
            vec![email(None, "a@b.io"), email(Some(page_only), "c@d.io")],
        ));

        assert_eq!(
            *details_of(&event),
            json!([
                {"type": "email", "count": 1, "rule": {"name": "local-rules"}, "value": "a****o"},
                {
                    "type": "email",
                    "count": 1,
                    "rule": {"name": "local-rules"},
                    "value": "c****o",
                    "occurrences": [{"page_number": 3}]
                }
            ])
        );
        assert_eq!(event["unmapped"]["redacter"].get("findings"), None);
    }

    #[test]
    fn findings_of_one_redacter_label_and_masked_value_share_a_discovery_detail() {
        let event = event_of(found(
            "a.txt",
            vec![
                email(text_at(1, 1, 7), "a@b.io"),
                finding(
                    RedacterType::LocalNer,
                    "email",
                    None,
                    text_at(2, 1, 7),
                    "a@b.io",
                ),
                email(text_at(5, 3, 9), "a@b.io"),
                email(text_at(6, 1, 7), "c@d.io"),
            ],
        ));

        let details = details_of(&event).as_array().unwrap();
        let counts: Vec<(&str, &str, u64)> = details
            .iter()
            .map(|detail| {
                (
                    detail["rule"]["name"].as_str().unwrap(),
                    detail["value"].as_str().unwrap(),
                    detail["count"].as_u64().unwrap(),
                )
            })
            .collect();
        assert_eq!(
            counts,
            [
                ("local-rules", "a****o", 2),
                ("local-ner", "a****o", 1),
                ("local-rules", "c****o", 1)
            ]
        );
        assert_eq!(
            details[0]["occurrences"],
            json!([
                {"start_line": 1, "end_line": 1, "column_number": 1},
                {"start_line": 5, "end_line": 5, "column_number": 3}
            ])
        );
        assert_eq!(
            event["data_security"]["detection_pattern"],
            "local-ner, local-rules"
        );
    }

    #[test]
    fn a_value_over_several_lines_ends_on_its_last_line() {
        let place = Place::Text {
            line: 2,
            column: 5,
            end_line: 4,
            end_column: 12,
            byte_start: 0,
            byte_end: 30,
        };

        let event = event_of(found(
            "key.pem",
            vec![email(Some(place), "-----BEGIN-----")],
        ));

        assert_eq!(
            details_of(&event)[0]["occurrences"],
            json!([{"start_line": 2, "end_line": 4, "column_number": 5}])
        );
    }

    #[test]
    fn an_unconfirmed_file_is_an_informational_event_with_no_data_security() {
        let event = event_of(file(
            "odd.txt",
            FileOutcome::Unconfirmed {
                unlocated_values: 2,
            },
        ));

        assert_eq!(event["severity_id"], 1);
        assert_eq!(event["severity"], "Informational");
        assert_eq!(event["confidence_id"], 0);
        assert_eq!(event["confidence"], "Unknown");
        assert_eq!(event.get("confidence_score"), None);
        assert_eq!(event["finding_info"]["types"], json!(["unconfirmed"]));
        assert_eq!(event.get("data_security"), None);
        assert_eq!(
            event["unmapped"],
            json!({"redacter": {"unlocated_values": 2}})
        );
        assert_eq!(
            event["message"],
            "2 values named by the redacters could not be located, so the file may hold sensitive data in odd.txt"
        );
    }

    #[test]
    fn severity_is_high_for_a_credential_and_medium_for_other_findings() {
        let credential = finding(
            RedacterType::LocalRules,
            "aws-access-key",
            None,
            None,
            "AKIAIOSFODNN7EXAMPLE",
        );
        let learned = finding(
            RedacterType::LocalCredentialMl,
            "credential",
            Some(0.9),
            None,
            "hunter2hunter2",
        );
        let personal = email(None, "a@b.io");

        let severity_of = |findings: Vec<ReportedFinding>| {
            let event = event_of(found("a.txt", findings));
            (event["severity_id"].clone(), event["severity"].clone())
        };

        assert_eq!(
            severity_of(vec![personal.clone()]),
            (json!(3), json!("Medium"))
        );
        assert_eq!(
            severity_of(vec![credential.clone()]),
            (json!(4), json!("High"))
        );
        assert_eq!(severity_of(vec![learned]), (json!(4), json!("High")));
        assert_eq!(
            severity_of(vec![personal, credential]),
            (json!(4), json!("High"))
        );
    }

    #[test]
    fn confidence_follows_the_highest_score_of_the_file() {
        let confidence_of = |scores: &[Option<f32>]| {
            let findings = scores
                .iter()
                .map(|score| finding(RedacterType::LocalNer, "per", *score, None, "Jane Doe"))
                .collect();
            let event = event_of(found("a.txt", findings));
            (
                event["confidence_id"].as_u64().unwrap(),
                event.get("confidence_score").and_then(Value::as_u64),
            )
        };

        assert_eq!(confidence_of(&[None]), (0, None));
        assert_eq!(confidence_of(&[Some(0.0)]), (1, Some(0)));
        assert_eq!(confidence_of(&[Some(0.49)]), (1, Some(49)));
        assert_eq!(confidence_of(&[Some(0.5)]), (2, Some(50)));
        assert_eq!(confidence_of(&[Some(0.79)]), (2, Some(79)));
        assert_eq!(confidence_of(&[Some(0.8)]), (3, Some(80)));
        assert_eq!(confidence_of(&[Some(1.0)]), (3, Some(100)));
        assert_eq!(
            confidence_of(&[Some(0.3), None, Some(0.9), Some(0.6)]),
            (3, Some(90))
        );
        assert_eq!(confidence_of(&[None, Some(0.3)]), (1, Some(30)));
    }

    #[test]
    fn a_clean_file_and_a_file_that_was_not_read_have_no_event() {
        let events = events_of(&[
            found("clean.txt", Vec::new()),
            file(
                "broken.txt",
                FileOutcome::Failed {
                    reason: "bad".to_string(),
                },
            ),
            file(
                "scan.png",
                FileOutcome::Blocked {
                    reason: "no OCR".to_string(),
                },
            ),
            file(
                "archive.bin",
                FileOutcome::Unsupported {
                    reason: "binary".to_string(),
                },
            ),
        ]);

        assert_eq!(events, Vec::<Value>::new());
    }

    #[test]
    fn the_report_is_one_event_on_each_line_and_empty_without_events() {
        let ndjson = ndjson_of(&[
            found("a.txt", vec![email(None, "a@b.io")]),
            found("clean.txt", Vec::new()),
            found("b.txt", vec![email(None, "c@d.io")]),
            file(
                "odd.txt",
                FileOutcome::Unconfirmed {
                    unlocated_values: 1,
                },
            ),
        ]);

        let paths: Vec<String> = ndjson
            .lines()
            .map(|line| {
                let event: Value = serde_json::from_str(line).unwrap();
                event["file"]["path"].as_str().unwrap().to_string()
            })
            .collect();
        assert_eq!(paths, ["a.txt", "b.txt", "odd.txt"]);
        assert!(ndjson.ends_with("}\n"));
        assert_eq!(ndjson.lines().count(), ndjson.matches('\n').count());
        assert_eq!(ndjson_of(&[found("clean.txt", Vec::new())]), "");
    }

    #[test]
    fn a_file_has_the_same_finding_identifier_in_every_run_and_no_other_file_shares_it() {
        let uid_of = |path: &str, value: &str| {
            let event = event_of(found(path, vec![email(text_at(1, 1, 7), value)]));
            event["finding_info"]["uid"].as_str().unwrap().to_string()
        };

        assert_eq!(
            uid_of("a.txt", "a@b.io"),
            uid_of("a.txt", "someone@else.org")
        );
        assert_ne!(uid_of("a.txt", "a@b.io"), uid_of("b.txt", "a@b.io"));
    }

    #[test]
    fn the_file_uri_of_a_remote_source_is_the_uri_of_the_object() {
        let locator = ArtifactLocator::new("s3://bucket/dir/", Path::new("/work"));
        let files = [found("sub/a.csv", vec![email(None, "a@b.io")])];

        let ndjson = OcsfEvents::new(&files, &locator, scan_time())
            .to_ndjson()
            .unwrap();

        let event: Value = serde_json::from_str(&ndjson).unwrap();
        assert_eq!(event["file"]["uri"], "s3://bucket/dir/sub/a.csv");
        assert_eq!(event["file"]["name"], "a.csv");
        assert_eq!(event["file"]["path"], "sub/a.csv");
    }

    #[test]
    fn every_kind_of_event_is_valid_against_the_ocsf_schema() {
        let schema: Value =
            serde_json::from_str(&std::fs::read_to_string(SCHEMA_PATH).unwrap()).unwrap();
        let validator = jsonschema::draft7::new(&schema).unwrap();
        let area = PixelBox {
            x1: 1.0,
            y1: 2.0,
            x2: 3.0,
            y2: 4.0,
        };
        let events = events_of(&[
            found(
                "text.txt",
                vec![
                    finding(
                        RedacterType::LocalNer,
                        "per",
                        Some(0.75),
                        text_at(3, 15, 37),
                        "Jane Doe",
                    ),
                    finding(
                        RedacterType::LocalRules,
                        "aws-access-key",
                        None,
                        text_at(4, 1, 21),
                        "AKIAIOSFODNN7EXAMPLE",
                    ),
                ],
            ),
            found(
                "table.csv",
                vec![email(
                    Some(Place::TableCell {
                        row: 1,
                        column: 2,
                        byte_start: 0,
                        byte_end: 6,
                    }),
                    "a@b.io",
                )],
            ),
            found(
                "image.png",
                vec![email(
                    Some(Place::Image {
                        page: None,
                        area: Some(area),
                    }),
                    "a@b.io",
                )],
            ),
            found(
                "form.pdf",
                vec![
                    email(
                        Some(Place::Image {
                            page: Some(2),
                            area: Some(area),
                        }),
                        "a@b.io",
                    ),
                    email(
                        Some(Place::Image {
                            page: Some(2),
                            area: None,
                        }),
                        "c@d.io",
                    ),
                ],
            ),
            found("whole.bin", vec![email(None, "a@b.io")]),
            file(
                "odd.txt",
                FileOutcome::Unconfirmed {
                    unlocated_values: 3,
                },
            ),
        ]);
        assert_eq!(events.len(), 6);

        for event in &events {
            let errors: Vec<String> = validator
                .iter_errors(event)
                .map(|error| error.to_string())
                .collect();
            assert!(errors.is_empty(), "{errors:?} in {event}");
        }

        let mut with_unknown_attribute = events[0].clone();
        with_unknown_attribute["not_an_attribute"] = json!(true);
        let mut without_time = events[0].clone();
        without_time.as_object_mut().unwrap().remove("time");
        assert!(!validator.is_valid(&with_unknown_attribute));
        assert!(!validator.is_valid(&without_time));
    }
}
