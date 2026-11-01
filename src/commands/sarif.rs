//! SARIF 2.1.0 reports for GitHub code scanning.
//!
//! The writer takes what a command found as [`SarifArtifact`]s, at whatever precision the
//! command knows, and returns the log to serialise. It does not know the command that
//! produced them.
//!
//! GitHub shows a result under the file and line of its first location, and does not show a
//! result without a location, so every result has one; a region is added only when the
//! finding has an exact place in a text. SARIF ends up in GitHub's UI and API, so a value is
//! always masked here, whatever the caller passes.

use crate::args::RedacterType;
use crate::commands::copy_output::escaped;
use crate::commands::scan_output::mask_value;
use crate::redacters::{DetectionLabel, DetectionLocation, Score};
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};

const SARIF_SCHEMA: &str =
    "https://docs.oasis-open.org/sarif/sarif/v2.1.0/errata01/os/schemas/sarif-schema-2.1.0.json";

/// The base GitHub resolves a relative `uri` against: the root of the checkout.
const SOURCE_ROOT_ID: &str = "%SRCROOT%";

/// Characters that cannot appear in a URI path segment as they are.
const PATH_SEGMENT: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'<')
    .add(b'>')
    .add(b'?')
    .add(b'[')
    .add(b'\\')
    .add(b']')
    .add(b'^')
    .add(b'`')
    .add(b'{')
    .add(b'|')
    .add(b'}');

/// A file of a run and what the command found in it.
#[derive(Debug, Clone, PartialEq)]
pub struct SarifArtifact {
    /// The path as the source's file system names it, relative to the source.
    pub path: String,
    pub outcome: ArtifactOutcome,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ArtifactOutcome {
    /// The file was read. A clean file has no findings.
    Findings(Vec<SarifFinding>),
    /// The file was read and has no findings, but the redacters named values they could not
    /// place in it, so it is not known to be clean.
    Unconfirmed { unlocated_values: usize },
    /// The file could not be read or processed.
    Failed { reason: String },
    /// The file needs a converter the run lacks.
    Blocked { reason: String },
    /// No selected redacter reads the file.
    Unsupported { reason: String },
}

/// One piece of sensitive data in a file.
#[derive(Debug, Clone, PartialEq)]
pub struct SarifFinding {
    redacter: RedacterType,
    label: DetectionLabel,
    score: Option<Score>,
    /// `None` when only the file is known to hold it.
    place: Option<Place>,
    masked_value: String,
}

impl SarifFinding {
    /// `value` may be the value as found: it is masked here.
    pub fn new(
        redacter: RedacterType,
        label: DetectionLabel,
        score: Option<Score>,
        place: Option<Place>,
        value: &str,
    ) -> Self {
        Self {
            redacter,
            label,
            score,
            place,
            masked_value: mask_value(value),
        }
    }

    fn rule_id(&self) -> RuleId {
        RuleId(format!(
            "{}/{}",
            self.redacter,
            escaped(&self.label.to_string())
        ))
    }

    fn message(&self) -> String {
        let mut message = format!(
            "{} found by {}.",
            escaped(&self.label.to_string()),
            self.redacter
        );
        if let Some(score) = self.score {
            message.push_str(&format!(" Score {score}."));
        }
        if let Some(place) = &self.place {
            message.push_str(&place.description());
        }
        message.push_str(&format!(" Value: {}", escaped(&self.masked_value)));
        message
    }
}

/// Where in a file something is, at the precision a command knows. Line, column, row and page
/// numbers count from 1.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Place {
    /// A range of a text. The column counts characters, and `end_column` is the one after the
    /// last character of the range.
    Text {
        line: usize,
        column: usize,
        end_line: usize,
        end_column: usize,
    },
    /// A cell of a table, the header row not counted. The line of a cell in its file is not
    /// known.
    TableCell { row: usize, column: usize },
    /// An image, or a page of a PDF, and the box on it when the position is known.
    Image {
        page: Option<usize>,
        area: Option<PixelBox>,
    },
}

/// A box in pixels, `x1 <= x2` and `y1 <= y2`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PixelBox {
    pub x1: f32,
    pub y1: f32,
    pub x2: f32,
    pub y2: f32,
}

impl Place {
    /// The place of a detection; `value` is the text it matched, which gives the end of a
    /// range that spans lines.
    pub fn new(location: DetectionLocation, value: &str) -> Self {
        match location {
            DetectionLocation::Text { line, column, .. } => {
                let (end_line, end_column) = match value.rsplit_once('\n') {
                    Some((before, last_line)) => (
                        line + before.matches('\n').count() + 1,
                        last_line.chars().count() + 1,
                    ),
                    None => (line, column + value.chars().count()),
                };
                Self::Text {
                    line,
                    column,
                    end_line,
                    end_column,
                }
            }
            DetectionLocation::TableCell { row, column, .. } => Self::TableCell { row, column },
            DetectionLocation::Image {
                page,
                x1,
                y1,
                x2,
                y2,
            } => Self::Image {
                page,
                area: Some(PixelBox { x1, y1, x2, y2 }),
            },
        }
    }

    fn region(&self) -> Option<Region> {
        match *self {
            Self::Text {
                line,
                column,
                end_line,
                end_column,
            } => Some(Region {
                start_line: line,
                start_column: column,
                end_line,
                end_column,
            }),
            Self::TableCell { .. } | Self::Image { .. } => None,
        }
    }

    /// What a message says for a place the region does not carry, with a leading space.
    fn description(&self) -> String {
        match *self {
            Self::Text { .. } => String::new(),
            Self::TableCell { row, column } => {
                format!(" Table cell: row {row}, column {column}, the header row not counted.")
            }
            Self::Image { page, area } => {
                let page = page.map(|page| format!("page {page}"));
                let area = area.map(|PixelBox { x1, y1, x2, y2 }| {
                    format!(
                        "box [{},{},{},{}] in pixels",
                        x1.round(),
                        y1.round(),
                        x2.round(),
                        y2.round()
                    )
                });
                match (page, area) {
                    (Some(page), Some(area)) => format!(" Image: {page}, {area}."),
                    (Some(only), None) | (None, Some(only)) => format!(" Image: {only}."),
                    (None, None) => String::new(),
                }
            }
        }
    }
}

/// Where the files of a source are, as GitHub can link them.
///
/// A local path is relative to the working directory with the base `%SRCROOT%`, which is the
/// checkout root when the command runs in a workflow. A path outside the working directory, and
/// a file of a remote source, are written as a full URI.
#[derive(Debug, Clone)]
pub struct ArtifactLocator {
    base: LocatorBase,
}

#[derive(Debug, Clone)]
enum LocatorBase {
    Local {
        source: PathBuf,
        is_dir: bool,
        working_dir: PathBuf,
    },
    Remote {
        /// Without a trailing slash.
        source: String,
        /// Whether the paths of the files are relative to the source. The path of the one
        /// object of a `gs://` or `s3://` source is its file name only, and the source names it.
        joins_paths: bool,
    },
}

impl ArtifactLocator {
    /// `source` is the argument the user gave. A local directory is told as the local file
    /// system tells it: a trailing `/` or a directory on disk.
    pub fn new(source: &str, working_dir: &Path) -> Self {
        let base = match source.strip_prefix("file://") {
            Some(path) => Some(path),
            None if !source.contains("://") => Some(source),
            None => None,
        };
        Self {
            base: match base {
                Some(path) => LocatorBase::Local {
                    is_dir: path.ends_with('/') || Path::new(path).is_dir(),
                    source: PathBuf::from(path),
                    working_dir: working_dir.to_path_buf(),
                },
                None => LocatorBase::Remote {
                    joins_paths: source.starts_with("zip://") || source.ends_with('/'),
                    source: source.trim_end_matches('/').to_string(),
                },
            },
        }
    }

    fn locate(&self, path: &str) -> ArtifactLocation {
        match &self.base {
            LocatorBase::Local {
                source,
                is_dir,
                working_dir,
            } => {
                let full = if *is_dir {
                    source.join(path)
                } else {
                    source.clone()
                };
                let full = lexically_normal(&full);
                let from_working_dir = if full.is_absolute() {
                    full.strip_prefix(working_dir).ok().map(Path::to_path_buf)
                } else {
                    Some(full.clone())
                };
                match from_working_dir {
                    Some(relative) if !relative.starts_with("..") => ArtifactLocation {
                        uri: encoded_path(&relative),
                        uri_base_id: Some(SOURCE_ROOT_ID),
                    },
                    _ => {
                        let absolute = lexically_normal(&working_dir.join(&full));
                        ArtifactLocation {
                            uri: format!("file:///{}", encoded_path(&absolute)),
                            uri_base_id: None,
                        }
                    }
                }
            }
            LocatorBase::Remote {
                source,
                joins_paths,
            } => ArtifactLocation {
                uri: if *joins_paths {
                    format!("{source}/{}", encoded_path(Path::new(path)))
                } else {
                    source.clone()
                },
                uri_base_id: None,
            },
        }
    }
}

/// Drops `.` and resolves `..` against the segment before it, without touching the disk.
fn lexically_normal(path: &Path) -> PathBuf {
    let mut normal = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir if normal.file_name().is_some() => {
                normal.pop();
            }
            other => normal.push(other),
        }
    }
    normal
}

/// The segments of a path joined by `/` and percent-encoded, without a root.
fn encoded_path(path: &Path) -> String {
    path.components()
        .filter_map(|component| match component {
            Component::Normal(segment) => {
                Some(utf8_percent_encode(&segment.to_string_lossy(), PATH_SEGMENT).to_string())
            }
            Component::ParentDir => Some("..".to_string()),
            Component::Prefix(prefix) => Some(prefix.as_os_str().to_string_lossy().to_string()),
            Component::RootDir | Component::CurDir => None,
        })
        .collect::<Vec<_>>()
        .join("/")
}

/// The identifier of a rule: `<redacter>/<label>`, or the rule of unconfirmed files.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
struct RuleId(String);

impl RuleId {
    fn unconfirmed() -> Self {
        Self("redacter/unconfirmed".to_string())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum Level {
    Error,
    Warning,
    Note,
}

#[derive(Debug, Serialize)]
struct Text {
    text: String,
}

impl From<String> for Text {
    fn from(text: String) -> Self {
        Self { text }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SarifLog {
    #[serde(rename = "$schema")]
    schema: &'static str,
    version: &'static str,
    runs: Vec<Run>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Run {
    tool: Tool,
    /// The columns of a region count Unicode code points, as the columns of a finding do. The
    /// SARIF default is UTF-16 code units, which differ for characters outside the Basic
    /// Multilingual Plane.
    column_kind: &'static str,
    invocations: Vec<Invocation>,
    results: Vec<SarifResult>,
}

#[derive(Debug, Serialize)]
struct Tool {
    driver: Driver,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Driver {
    name: &'static str,
    version: &'static str,
    information_uri: &'static str,
    rules: Vec<Rule>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Rule {
    id: RuleId,
    short_description: Text,
    full_description: Text,
    help: Help,
    default_configuration: RuleConfiguration,
    properties: RuleProperties,
}

#[derive(Debug, Serialize)]
struct Help {
    text: String,
    markdown: String,
}

#[derive(Debug, Serialize)]
struct RuleConfiguration {
    level: Level,
}

#[derive(Debug, Serialize)]
struct RuleProperties {
    tags: [&'static str; 2],
}

impl Rule {
    fn for_finding(finding: &SarifFinding) -> Self {
        let label = escaped(&finding.label.to_string());
        let help = format!(
            "The {} redacter reported data labelled `{label}`. Remove the value from the file, or \
             keep it out of the repository, for example in a secret store. A redacter names data \
             with its own labels, see https://github.com/abdolence/redacter-rs#detection-per-redacter.",
            finding.redacter
        );
        Self {
            id: finding.rule_id(),
            short_description: format!("Sensitive data: {label}").into(),
            full_description: format!(
                "The {} redacter found data it labels `{label}`, such as personal information or a secret.",
                finding.redacter
            )
            .into(),
            help: Help {
                text: help.clone(),
                markdown: help,
            },
            default_configuration: RuleConfiguration { level: Level::Error },
            properties: RuleProperties::default(),
        }
    }

    fn unconfirmed() -> Self {
        let help = "A redacter named values that it could not find in the file as written, such \
                    as a value a model reformatted, so the file may still hold sensitive data. \
                    Review the file by hand.";
        Self {
            id: RuleId::unconfirmed(),
            short_description: "Sensitive data may be present".to_string().into(),
            full_description: "The redacters named values they could not locate in the file, and \
                               found nothing else in it."
                .to_string()
                .into(),
            help: Help {
                text: help.to_string(),
                markdown: help.to_string(),
            },
            default_configuration: RuleConfiguration {
                level: Level::Warning,
            },
            properties: RuleProperties::default(),
        }
    }
}

impl Default for RuleProperties {
    fn default() -> Self {
        Self {
            tags: ["security", "sensitive-data"],
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Invocation {
    execution_successful: bool,
    tool_execution_notifications: Vec<Notification>,
}

#[derive(Debug, Serialize)]
struct Notification {
    level: Level,
    message: Text,
    locations: Vec<Location>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SarifResult {
    rule_id: RuleId,
    rule_index: usize,
    level: Level,
    message: Text,
    locations: Vec<Location>,
    partial_fingerprints: Fingerprints,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Location {
    physical_location: PhysicalLocation,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PhysicalLocation {
    artifact_location: ArtifactLocation,
    #[serde(skip_serializing_if = "Option::is_none")]
    region: Option<Region>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct ArtifactLocation {
    uri: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    uri_base_id: Option<&'static str>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Region {
    start_line: usize,
    start_column: usize,
    end_line: usize,
    end_column: usize,
}

/// What GitHub tells one alert from another across uploads. It is the only fingerprint GitHub
/// reads, and it is derived from the rule, the file and the masked value, never from the
/// value itself: a hash of a secret is reversed by trying every candidate. It does not change
/// when lines are added above the finding. The part after the colon counts the findings of
/// the same rule, file and masked value before this one.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Fingerprints {
    primary_location_line_hash: String,
}

impl Fingerprints {
    fn new(rule_id: &RuleId, uri: &str, masked_value: &str, occurrence: usize) -> Self {
        let mut hasher = Sha256::new();
        for part in [rule_id.0.as_str(), uri, masked_value] {
            hasher.update(part.as_bytes());
            hasher.update([0]);
        }
        let hash: String = hasher
            .finalize()
            .iter()
            .take(8)
            .map(|byte| format!("{byte:02x}"))
            .collect();
        Self {
            primary_location_line_hash: format!("{hash}:{occurrence}"),
        }
    }
}

impl Location {
    fn of(artifact_location: ArtifactLocation, region: Option<Region>) -> Self {
        Self {
            physical_location: PhysicalLocation {
                artifact_location,
                region,
            },
        }
    }
}

impl SarifLog {
    /// The log of one run over `artifacts`. Rules are sorted by id, so the log does not
    /// depend on the order the files were read in.
    pub fn new(artifacts: &[SarifArtifact], locator: &ArtifactLocator) -> Self {
        let mut rules: BTreeMap<RuleId, Rule> = BTreeMap::new();
        for artifact in artifacts {
            match &artifact.outcome {
                ArtifactOutcome::Findings(findings) => {
                    for finding in findings {
                        rules
                            .entry(finding.rule_id())
                            .or_insert_with(|| Rule::for_finding(finding));
                    }
                }
                ArtifactOutcome::Unconfirmed { .. } => {
                    rules
                        .entry(RuleId::unconfirmed())
                        .or_insert_with(Rule::unconfirmed);
                }
                ArtifactOutcome::Failed { .. }
                | ArtifactOutcome::Blocked { .. }
                | ArtifactOutcome::Unsupported { .. } => {}
            }
        }
        let rule_indexes: BTreeMap<&RuleId, usize> = rules
            .keys()
            .enumerate()
            .map(|(index, id)| (id, index))
            .collect();

        let mut results = Vec::new();
        let mut notifications = Vec::new();
        for artifact in artifacts {
            let location = locator.locate(&artifact.path);
            match &artifact.outcome {
                ArtifactOutcome::Findings(findings) => {
                    let mut seen: BTreeMap<(RuleId, &str), usize> = BTreeMap::new();
                    for finding in findings {
                        let rule_id = finding.rule_id();
                        let occurrence = seen
                            .entry((rule_id.clone(), finding.masked_value.as_str()))
                            .or_default();
                        results.push(SarifResult {
                            rule_index: rule_indexes[&rule_id],
                            level: Level::Error,
                            message: finding.message().into(),
                            locations: vec![Location::of(
                                location.clone(),
                                finding.place.as_ref().and_then(Place::region),
                            )],
                            partial_fingerprints: Fingerprints::new(
                                &rule_id,
                                &location.uri,
                                &finding.masked_value,
                                *occurrence,
                            ),
                            rule_id,
                        });
                        *occurrence += 1;
                    }
                }
                ArtifactOutcome::Unconfirmed { unlocated_values } => {
                    let rule_id = RuleId::unconfirmed();
                    results.push(SarifResult {
                        rule_index: rule_indexes[&rule_id],
                        level: Level::Warning,
                        message: format!(
                            "{unlocated_values} {} named by the redacters could not be located in \
                             the file, so it may still hold sensitive data.",
                            if *unlocated_values == 1 {
                                "value"
                            } else {
                                "values"
                            }
                        )
                        .into(),
                        partial_fingerprints: Fingerprints::new(&rule_id, &location.uri, "", 0),
                        locations: vec![Location::of(location, None)],
                        rule_id,
                    });
                }
                ArtifactOutcome::Failed { reason } | ArtifactOutcome::Blocked { reason } => {
                    notifications.push(Notification::new(Level::Error, artifact, reason, location));
                }
                ArtifactOutcome::Unsupported { reason } => {
                    notifications.push(Notification::new(Level::Note, artifact, reason, location));
                }
            }
        }

        let execution_successful = !artifacts.iter().any(|artifact| {
            matches!(
                artifact.outcome,
                ArtifactOutcome::Failed { .. } | ArtifactOutcome::Blocked { .. }
            )
        });
        Self {
            schema: SARIF_SCHEMA,
            version: "2.1.0",
            runs: vec![Run {
                tool: Tool {
                    driver: Driver {
                        name: "redacter",
                        version: env!("CARGO_PKG_VERSION"),
                        information_uri: env!("CARGO_PKG_REPOSITORY"),
                        rules: rules.into_values().collect(),
                    },
                },
                column_kind: "unicodeCodePoints",
                invocations: vec![Invocation {
                    execution_successful,
                    tool_execution_notifications: notifications,
                }],
                results,
            }],
        }
    }
}

impl Notification {
    fn new(
        level: Level,
        artifact: &SarifArtifact,
        reason: &str,
        location: ArtifactLocation,
    ) -> Self {
        Self {
            level,
            message: format!("{}: {reason}", artifact.path).into(),
            locations: vec![Location::of(location, None)],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::redacters::TextLines;
    use serde_json::{json, Value};

    fn text_at(line: usize, column: usize, end_column: usize) -> Option<Place> {
        Some(Place::Text {
            line,
            column,
            end_line: line,
            end_column,
        })
    }

    fn email(place: Option<Place>, value: &str) -> SarifFinding {
        SarifFinding::new(
            RedacterType::LocalRules,
            DetectionLabel::new("email"),
            None,
            place,
            value,
        )
    }

    fn artifact(path: &str, outcome: ArtifactOutcome) -> SarifArtifact {
        SarifArtifact {
            path: path.to_string(),
            outcome,
        }
    }

    fn log_of(artifacts: &[SarifArtifact]) -> Value {
        let locator = ArtifactLocator::new("./", Path::new("/work"));
        serde_json::to_value(SarifLog::new(artifacts, &locator)).unwrap()
    }

    fn results(artifacts: &[SarifArtifact]) -> Vec<Value> {
        log_of(artifacts)["runs"][0]["results"]
            .as_array()
            .cloned()
            .unwrap_or_default()
    }

    fn fingerprint(result: &Value) -> String {
        result["partialFingerprints"]["primaryLocationLineHash"]
            .as_str()
            .unwrap()
            .to_string()
    }

    #[test]
    fn a_text_finding_is_a_region_with_the_masked_value() {
        let log = log_of(&[artifact(
            "docs/note.txt",
            ArtifactOutcome::Findings(vec![SarifFinding::new(
                RedacterType::LocalNer,
                DetectionLabel::new("per"),
                Some(Score::from_static(0.75)),
                text_at(3, 15, 37),
                "john.smith@example.com",
            )]),
        )]);

        assert_eq!(
            log,
            json!({
                "$schema": SARIF_SCHEMA,
                "version": "2.1.0",
                "runs": [{
                    "tool": {"driver": {
                        "name": "redacter",
                        "version": env!("CARGO_PKG_VERSION"),
                        "informationUri": "https://github.com/abdolence/redacter-rs",
                        "rules": [{
                            "id": "local-ner/per",
                            "shortDescription": {"text": "Sensitive data: per"},
                            "fullDescription": {"text": "The local-ner redacter found data it labels `per`, such as personal information or a secret."},
                            "help": {
                                "text": "The local-ner redacter reported data labelled `per`. Remove the value from the file, or keep it out of the repository, for example in a secret store. A redacter names data with its own labels, see https://github.com/abdolence/redacter-rs#detection-per-redacter.",
                                "markdown": "The local-ner redacter reported data labelled `per`. Remove the value from the file, or keep it out of the repository, for example in a secret store. A redacter names data with its own labels, see https://github.com/abdolence/redacter-rs#detection-per-redacter."
                            },
                            "defaultConfiguration": {"level": "error"},
                            "properties": {"tags": ["security", "sensitive-data"]}
                        }]
                    }},
                    "columnKind": "unicodeCodePoints",
                    "invocations": [{"executionSuccessful": true, "toolExecutionNotifications": []}],
                    "results": [{
                        "ruleId": "local-ner/per",
                        "ruleIndex": 0,
                        "level": "error",
                        "message": {"text": "per found by local-ner. Score 0.75. Value: j********************m"},
                        "locations": [{"physicalLocation": {
                            "artifactLocation": {"uri": "docs/note.txt", "uriBaseId": "%SRCROOT%"},
                            "region": {"startLine": 3, "startColumn": 15, "endLine": 3, "endColumn": 37}
                        }}],
                        "partialFingerprints": {"primaryLocationLineHash": "a3b6615200f194aa:0"}
                    }]
                }]
            })
        );
    }

    #[test]
    fn columns_count_code_points_not_bytes_or_utf16_units() {
        let text = "Grüße 😀 j.smith@example.com";
        let start = text.find("j.smith").unwrap();
        let detection = TextLines::new(text)
            .detection(start..text.len(), DetectionLabel::new("email"), None)
            .unwrap();
        let finding = email(
            Some(Place::new(detection.location, &detection.value)),
            &detection.value,
        );

        let region = &results(&[artifact("a.txt", ArtifactOutcome::Findings(vec![finding]))])[0]
            ["locations"][0]["physicalLocation"]["region"];

        // "Grüße 😀 " is 8 characters, 9 bytes more in UTF-8 and one more in UTF-16.
        assert_eq!(
            *region,
            json!({"startLine": 1, "startColumn": 9, "endLine": 1, "endColumn": 28})
        );
    }

    #[test]
    fn a_value_over_several_lines_ends_on_its_last_line() {
        let text = "key:\n-----BEGIN-----\nabc\n-----END-----\nnext";
        let start = text.find("-----BEGIN").unwrap();
        let end = text.find("\nnext").unwrap();
        let detection = TextLines::new(text)
            .detection(start..end, DetectionLabel::new("private-key"), None)
            .unwrap();
        let finding = email(
            Some(Place::new(detection.location, &detection.value)),
            &detection.value,
        );

        let region = &results(&[artifact("a.txt", ArtifactOutcome::Findings(vec![finding]))])[0]
            ["locations"][0]["physicalLocation"]["region"];

        assert_eq!(
            *region,
            json!({"startLine": 2, "startColumn": 1, "endLine": 4, "endColumn": 14})
        );
    }

    #[test]
    fn a_table_cell_has_no_region_and_the_message_names_the_cell() {
        let finding = email(Some(Place::TableCell { row: 3, column: 2 }), "a@b.io");

        let result = &results(&[artifact(
            "people.csv",
            ArtifactOutcome::Findings(vec![finding]),
        )])[0];

        assert_eq!(
            result["message"]["text"],
            "email found by local-rules. Table cell: row 3, column 2, the header row not counted. Value: a****o"
        );
        assert_eq!(
            result["locations"][0]["physicalLocation"],
            json!({"artifactLocation": {"uri": "people.csv", "uriBaseId": "%SRCROOT%"}})
        );
    }

    #[test]
    fn an_image_or_page_has_no_region_and_the_message_gives_the_box() {
        let boxed = Place::Image {
            page: Some(2),
            area: Some(PixelBox {
                x1: 310.2,
                y1: 193.0,
                x2: 621.6,
                y2: 218.0,
            }),
        };
        let page_only = Place::Image {
            page: Some(2),
            area: None,
        };
        let file_only = None;

        let messages: Vec<String> = [Some(boxed), Some(page_only), file_only]
            .into_iter()
            .flat_map(|place| {
                results(&[artifact(
                    "scan.pdf",
                    ArtifactOutcome::Findings(vec![email(place, "a@b.io")]),
                )])
            })
            .map(|result| {
                assert!(result["locations"][0]["physicalLocation"]["region"].is_null());
                result["message"]["text"].as_str().unwrap().to_string()
            })
            .collect();

        assert_eq!(
            messages,
            [
                "email found by local-rules. Image: page 2, box [310,193,622,218] in pixels. Value: a****o",
                "email found by local-rules. Image: page 2. Value: a****o",
                "email found by local-rules. Value: a****o",
            ]
        );
    }

    #[test]
    fn an_unconfirmed_file_is_a_warning_on_the_file() {
        let log = log_of(&[artifact(
            "reformatted.txt",
            ArtifactOutcome::Unconfirmed {
                unlocated_values: 2,
            },
        )]);

        assert_eq!(
            log["runs"][0]["tool"]["driver"]["rules"][0]["id"],
            "redacter/unconfirmed"
        );
        let result = &log["runs"][0]["results"][0];
        assert_eq!(
            [&result["ruleId"], &result["level"]],
            ["redacter/unconfirmed", "warning"]
        );
        assert_eq!(
            result["locations"][0]["physicalLocation"],
            json!({"artifactLocation": {"uri": "reformatted.txt", "uriBaseId": "%SRCROOT%"}})
        );
    }

    #[test]
    fn a_file_that_failed_or_is_blocked_makes_the_invocation_unsuccessful() {
        let invocation = |outcomes: Vec<ArtifactOutcome>| {
            let artifacts: Vec<SarifArtifact> = outcomes
                .into_iter()
                .enumerate()
                .map(|(index, outcome)| artifact(&format!("f{index}.bin"), outcome))
                .collect();
            log_of(&artifacts)["runs"][0]["invocations"][0].clone()
        };

        let unsupported_only = invocation(vec![ArtifactOutcome::Unsupported {
            reason: "type not supported".to_string(),
        }]);
        assert_eq!(unsupported_only["executionSuccessful"], true);

        let all = invocation(vec![
            ArtifactOutcome::Failed {
                reason: "provider unavailable".to_string(),
            },
            ArtifactOutcome::Blocked {
                reason: "ocr unavailable".to_string(),
            },
            ArtifactOutcome::Unsupported {
                reason: "type not supported".to_string(),
            },
        ]);
        assert_eq!(all["executionSuccessful"], false);
        assert_eq!(
            all["toolExecutionNotifications"],
            json!([
                {"level": "error", "message": {"text": "f0.bin: provider unavailable"},
                 "locations": [{"physicalLocation": {"artifactLocation": {"uri": "f0.bin", "uriBaseId": "%SRCROOT%"}}}]},
                {"level": "error", "message": {"text": "f1.bin: ocr unavailable"},
                 "locations": [{"physicalLocation": {"artifactLocation": {"uri": "f1.bin", "uriBaseId": "%SRCROOT%"}}}]},
                {"level": "note", "message": {"text": "f2.bin: type not supported"},
                 "locations": [{"physicalLocation": {"artifactLocation": {"uri": "f2.bin", "uriBaseId": "%SRCROOT%"}}}]},
            ])
        );
    }

    #[test]
    fn a_value_as_found_never_reaches_the_log() {
        let raw = "john.smith@example.com";
        let log = log_of(&[
            artifact(
                "a.txt",
                ArtifactOutcome::Findings(vec![email(text_at(1, 1, 23), raw)]),
            ),
            artifact(
                "b.csv",
                ArtifactOutcome::Findings(vec![email(
                    Some(Place::TableCell { row: 1, column: 1 }),
                    raw,
                )]),
            ),
        ])
        .to_string();

        assert!(!log.contains(raw), "{log}");
        assert!(log.contains("j********************m"), "{log}");
    }

    #[test]
    fn a_masked_value_stays_as_it_is() {
        let masked = "j********************m";
        let result = &results(&[artifact(
            "a.txt",
            ArtifactOutcome::Findings(vec![email(text_at(1, 1, 23), masked)]),
        )])[0];

        assert!(result["message"]["text"]
            .as_str()
            .unwrap()
            .ends_with(masked));
    }

    #[test]
    fn fingerprints_are_stable_and_do_not_depend_on_the_line_or_the_value() {
        let at_line = |line, value| {
            fingerprint(
                &results(&[artifact(
                    "a.txt",
                    ArtifactOutcome::Findings(vec![email(text_at(line, 5, 27), value)]),
                )])[0],
            )
        };

        // The same finding in two runs, and after lines were added above it.
        assert_eq!(at_line(1, "john.smith@example.com"), "c33c61669ab9682b:0");
        assert_eq!(
            at_line(1, "john.smith@example.com"),
            at_line(40, "john.smith@example.com")
        );
        // A different value that masks the same way is the same alert; the value itself is
        // not an input.
        assert_eq!(
            at_line(1, "jane.smith@example.com"),
            at_line(1, "john.smith@example.com")
        );
        assert_ne!(
            at_line(1, "john.smith@example.org"),
            at_line(1, "john.smith@example.co")
        );
    }

    #[test]
    fn findings_with_the_same_rule_file_and_masked_value_count_up() {
        let results = results(&[
            artifact(
                "a.txt",
                ArtifactOutcome::Findings(vec![
                    email(text_at(1, 1, 7), "a@b.io"),
                    email(text_at(2, 1, 7), "a@d.io"),
                ]),
            ),
            artifact(
                "b.txt",
                ArtifactOutcome::Findings(vec![email(text_at(1, 1, 7), "a@b.io")]),
            ),
        ]);

        let counted: Vec<String> = results
            .iter()
            .map(|result| fingerprint(result).split_once(':').unwrap().1.to_string())
            .collect();
        let hashes: Vec<String> = results
            .iter()
            .map(|result| fingerprint(result).split_once(':').unwrap().0.to_string())
            .collect();
        assert_eq!(counted, ["0", "1", "0"]);
        assert_eq!(hashes[0], hashes[1]);
        assert_ne!(hashes[0], hashes[2], "another file is another alert");
    }

    #[test]
    fn rules_are_sorted_and_results_point_at_them_whatever_the_file_order() {
        let ner = |label: &str| {
            SarifFinding::new(
                RedacterType::LocalNer,
                DetectionLabel::new(label),
                None,
                text_at(1, 1, 2),
                "x",
            )
        };
        let artifacts = vec![
            artifact(
                "a.txt",
                ArtifactOutcome::Findings(vec![ner("per"), ner("org")]),
            ),
            artifact(
                "b.txt",
                ArtifactOutcome::Findings(vec![email(text_at(1, 1, 2), "x")]),
            ),
        ];
        let mut reversed = artifacts.clone();
        reversed.reverse();

        let rule_ids = |artifacts: &[SarifArtifact]| -> Vec<String> {
            log_of(artifacts)["runs"][0]["tool"]["driver"]["rules"]
                .as_array()
                .unwrap()
                .iter()
                .map(|rule| rule["id"].as_str().unwrap().to_string())
                .collect()
        };
        assert_eq!(
            rule_ids(&artifacts),
            ["local-ner/org", "local-ner/per", "local-rules/email"]
        );
        assert_eq!(rule_ids(&artifacts), rule_ids(&reversed));
        let by_index: Vec<(i64, String)> = results(&artifacts)
            .iter()
            .map(|result| {
                (
                    result["ruleIndex"].as_i64().unwrap(),
                    result["ruleId"].as_str().unwrap().to_string(),
                )
            })
            .collect();
        assert_eq!(
            by_index,
            [
                (1, "local-ner/per".to_string()),
                (0, "local-ner/org".to_string()),
                (2, "local-rules/email".to_string()),
            ]
        );
    }

    #[test]
    fn a_label_a_model_chose_cannot_break_a_line_of_the_log() {
        let finding = SarifFinding::new(
            RedacterType::GeminiLlm,
            DetectionLabel::new("ssn\n\u{1b}[31mred"),
            None,
            None,
            "x",
        );

        let result = &results(&[artifact("a.txt", ArtifactOutcome::Findings(vec![finding]))])[0];

        assert_eq!(result["ruleId"], "gemini-llm/ssn\\n\\u{1b}[31mred");
    }

    #[test]
    fn locations_of_a_local_source_are_relative_to_the_working_directory() {
        let tmp = tempfile::TempDir::with_prefix("sarif_locator").unwrap();
        let dir = tmp.path().join("existing dir");
        std::fs::create_dir(&dir).unwrap();
        std::fs::write(dir.join("a b.txt"), "").unwrap();
        let working_dir = tmp.path();
        let uri = |source: &str, path: &str| {
            let location = ArtifactLocator::new(source, working_dir).locate(path);
            (location.uri, location.uri_base_id)
        };
        let source_root = Some(SOURCE_ROOT_ID);

        // A directory of the working directory, with and without the slash or the dot.
        assert_eq!(
            uri("docs/", "a/b.txt"),
            ("docs/a/b.txt".to_string(), source_root)
        );
        assert_eq!(
            uri("./docs", "b.txt"),
            ("docs/b.txt".to_string(), source_root)
        );
        assert_eq!(uri(".", "b.txt"), ("b.txt".to_string(), source_root));
        assert_eq!(uri("./", "d/e.txt"), ("d/e.txt".to_string(), source_root));
        // One file, whose path in the listing is its name.
        assert_eq!(
            uri("docs/a.txt", "a.txt"),
            ("docs/a.txt".to_string(), source_root)
        );
        assert_eq!(
            uri("file://docs/a.txt", "a.txt"),
            ("docs/a.txt".to_string(), source_root)
        );
        // A directory that exists on disk is one without the trailing slash.
        assert_eq!(
            uri(&dir.to_string_lossy(), "a b.txt"),
            ("existing%20dir/a%20b.txt".to_string(), source_root)
        );
        // Characters of a URI are encoded.
        assert_eq!(
            uri("docs/", "x #1?.txt"),
            ("docs/x%20%231%3F.txt".to_string(), source_root)
        );
        assert_eq!(
            uri("docs/", "é.txt"),
            ("docs/%C3%A9.txt".to_string(), source_root)
        );
        // Outside of the working directory the path is a full URI.
        assert_eq!(
            uri("../other/", "a.txt"),
            (
                format!(
                    "file:///{}/other/a.txt",
                    encoded_path(working_dir.parent().unwrap())
                ),
                None
            )
        );
    }

    #[test]
    fn locations_of_a_remote_source_are_full_uris() {
        let uri = |source: &str, path: &str| {
            let location = ArtifactLocator::new(source, Path::new("/work")).locate(path);
            (location.uri, location.uri_base_id)
        };

        assert_eq!(
            uri("gs://bucket/dir/", "a/b c.txt"),
            ("gs://bucket/dir/a/b%20c.txt".to_string(), None)
        );
        assert_eq!(
            uri("gs://bucket/dir/obj.txt", "obj.txt"),
            ("gs://bucket/dir/obj.txt".to_string(), None)
        );
        assert_eq!(
            uri("s3://bucket/", "a.txt"),
            ("s3://bucket/a.txt".to_string(), None)
        );
        assert_eq!(
            uri("zip://archive.zip", "inner/a.txt"),
            ("zip://archive.zip/inner/a.txt".to_string(), None)
        );
    }
}
