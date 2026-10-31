//! The datasets a benchmark run scores against: each is a directory of files for `scan` to read
//! and, per file, the truth to score its report with.

use crate::categories::Category;
use crate::scoring::{FileTruth, Rect, Target};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::{self, Display};
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DatasetKind {
    Fixtures,
    Secrets,
    Public,
    Images,
}

impl DatasetKind {
    /// Whether the truth lists everything sensitive in the files, which is what makes a finding
    /// on no target a false alarm. The annotation of the public dataset is not exhaustive.
    pub fn has_complete_truth(self) -> bool {
        matches!(self, DatasetKind::Fixtures | DatasetKind::Secrets)
    }

    /// Fewest targets a category needs in this dataset for its recall to be shown.
    pub fn min_category_support(self) -> usize {
        match self {
            DatasetKind::Public => 10,
            _ => 1,
        }
    }
}

impl FromStr for DatasetKind {
    type Err = String;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        match name {
            "fixtures" => Ok(DatasetKind::Fixtures),
            "secrets" => Ok(DatasetKind::Secrets),
            "public" => Ok(DatasetKind::Public),
            "images" => Ok(DatasetKind::Images),
            other => Err(format!(
                "unknown dataset {other:?} in BENCH_DLP_DATASETS, expected fixtures, secrets, public or images"
            )),
        }
    }
}

impl Display for DatasetKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            DatasetKind::Fixtures => "fixtures",
            DatasetKind::Secrets => "secrets",
            DatasetKind::Public => "public",
            DatasetKind::Images => "images",
        })
    }
}

/// A dataset written to disk, ready to scan.
pub struct Dataset {
    pub kind: DatasetKind,
    pub dir: PathBuf,
    /// Truth by the path `scan` reports for the file.
    pub truth: BTreeMap<String, FileTruth>,
    /// Options of `scan` this dataset needs beyond the redacters.
    pub scan_args: &'static [&'static str],
}

impl Dataset {
    pub fn sensitive_count(&self) -> usize {
        self.truth.values().map(|truth| truth.sensitive.len()).sum()
    }
}

/// Where a dataset reads its fixtures and writes its files.
pub struct DatasetPaths<'a> {
    pub fixtures: &'a Path,
    pub work: &'a Path,
}

impl DatasetPaths<'_> {
    pub fn load(&self, kind: DatasetKind) -> Result<Dataset, String> {
        let dir = self.dataset_dir(kind)?;
        let (truth, scan_args): (BTreeMap<String, FileTruth>, &'static [&'static str]) = match kind
        {
            DatasetKind::Fixtures => (self.fixtures_truth(&dir)?, &[]),
            DatasetKind::Secrets => (self.secrets_truth(&dir)?, &["--unsupported-as-text"]),
            DatasetKind::Public => (self.public_truth(&dir)?, &[]),
            DatasetKind::Images => (self.images_truth(&dir)?, &[]),
        };
        Ok(Dataset {
            kind,
            dir,
            truth,
            scan_args,
        })
    }

    fn dataset_dir(&self, kind: DatasetKind) -> Result<PathBuf, String> {
        let dir = self.work.join("datasets").join(kind.to_string());
        if dir.exists() {
            fs::remove_dir_all(&dir).map_err(|e| format!("clear {}: {e}", dir.display()))?;
        }
        fs::create_dir_all(&dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
        Ok(dir)
    }

    fn read_json<T: for<'de> Deserialize<'de>>(&self, relative: &str) -> Result<T, String> {
        let path = self.fixtures.join(relative);
        let text =
            fs::read_to_string(&path).map_err(|e| format!("read {}: {e}", path.display()))?;
        serde_json::from_str(&text).map_err(|e| format!("parse {}: {e}", path.display()))
    }

    /// Copies `test-fixtures/<relative>` into `dir` under its file name.
    fn copy_fixture(&self, relative: &str, dir: &Path) -> Result<String, String> {
        let source = self.fixtures.join(relative);
        let name = source
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| format!("{relative} has no file name"))?
            .to_string();
        fs::copy(&source, dir.join(&name))
            .map_err(|e| format!("copy {}: {e}", source.display()))?;
        Ok(name)
    }

    fn fixtures_truth(&self, dir: &Path) -> Result<BTreeMap<String, FileTruth>, String> {
        let entries: BTreeMap<String, TextTruthEntry> = self.read_json("bench-dlp/truth.json")?;
        let mut truth = BTreeMap::new();
        for (name, entry) in entries {
            let copied = self.copy_fixture(&format!("documents/{name}"), dir)?;
            truth.insert(copied, FileTruth::from(entry));
        }
        Ok(truth)
    }

    fn secrets_truth(&self, dir: &Path) -> Result<BTreeMap<String, FileTruth>, String> {
        let corpus: SecretsCorpus = self.read_json("bench-dlp/secrets/secrets.json")?;
        let mut truth = BTreeMap::new();
        for (name, (content, file_truth)) in corpus.assemble()? {
            fs::write(dir.join(&name), content).map_err(|e| format!("write {name}: {e}"))?;
            truth.insert(name, file_truth);
        }
        Ok(truth)
    }

    fn images_truth(&self, dir: &Path) -> Result<BTreeMap<String, FileTruth>, String> {
        let entries: BTreeMap<String, ImageTruthEntry> = self.read_json("bench-dlp/images.json")?;
        let mut truth = BTreeMap::new();
        for (relative, entry) in entries {
            let name = self.copy_fixture(&relative, dir)?;
            truth.insert(name, FileTruth::from(entry));
        }
        Ok(truth)
    }

    fn public_truth(&self, dir: &Path) -> Result<BTreeMap<String, FileTruth>, String> {
        let records: Vec<PublicRecord> = self.read_json("bench-dlp/public.json")?;
        let mut truth = BTreeMap::new();
        for (index, record) in records.iter().enumerate() {
            let name = format!("{index:04}.txt");
            fs::write(dir.join(&name), &record.text).map_err(|e| format!("write {name}: {e}"))?;
            truth.insert(
                name.clone(),
                FileTruth::try_from(record).map_err(|e| format!("{name}: {e}"))?,
            );
        }
        Ok(truth)
    }
}

#[derive(Deserialize)]
struct TextTruthEntry {
    pii: Vec<PiiEntry>,
    entities: Vec<String>,
    keep: Vec<String>,
}

#[derive(Deserialize)]
struct PiiEntry {
    text: String,
    category: Category,
}

impl From<TextTruthEntry> for FileTruth {
    fn from(entry: TextTruthEntry) -> Self {
        FileTruth::new(
            entry
                .pii
                .into_iter()
                .map(|pii| (pii.category, Target::Text(pii.text)))
                .collect(),
            entry.entities.into_iter().map(Target::Text).collect(),
            entry.keep.into_iter().map(Target::Text).collect(),
        )
    }
}

#[derive(Deserialize)]
struct ImageTruthEntry {
    sensitive: Vec<ImageRegion>,
    keep: Vec<Rect>,
}

#[derive(Deserialize)]
struct ImageRegion {
    category: Category,
    region: Rect,
}

impl From<ImageTruthEntry> for FileTruth {
    fn from(entry: ImageTruthEntry) -> Self {
        FileTruth::new(
            entry
                .sensitive
                .into_iter()
                .map(|item| (item.category, Target::Region(item.region)))
                .collect(),
            Vec::new(),
            entry.keep.into_iter().map(Target::Region).collect(),
        )
    }
}

/// The synthetic secrets corpus. Values are stored in parts and the files as templates over
/// `{{value id}}`, so that no committed line is a complete token a secret scanner would flag.
#[derive(Deserialize)]
struct SecretsCorpus {
    values: BTreeMap<String, Vec<String>>,
    files: BTreeMap<String, String>,
    controls: BTreeMap<String, SecretsControl>,
}

/// A file with nothing to find, and the strings in it that look like secrets.
#[derive(Deserialize)]
struct SecretsControl {
    content: String,
    keep: Vec<String>,
}

impl SecretsCorpus {
    /// The files with their values put back, and the truth of each: every value a file's
    /// template names is a credential to find in it.
    fn assemble(&self) -> Result<BTreeMap<String, (String, FileTruth)>, String> {
        let mut files = BTreeMap::new();
        for (name, template) in &self.files {
            let mut content = template.clone();
            let mut truth = FileTruth::default();
            for (id, parts) in &self.values {
                let placeholder = format!("{{{{{id}}}}}");
                if content.contains(&placeholder) {
                    let value = parts.concat();
                    content = content.replace(&placeholder, &value);
                    truth
                        .sensitive
                        .push((Category::Credential, Target::Text(value)));
                }
            }
            if let Some(start) = content.find("{{") {
                let unknown: String = content[start..].chars().take(40).collect();
                return Err(format!("{name} names a value the corpus lacks: {unknown}"));
            }
            files.insert(name.clone(), (content, truth));
        }
        for (name, control) in &self.controls {
            let truth = FileTruth {
                keep: control.keep.iter().cloned().map(Target::Text).collect(),
                ..FileTruth::default()
            };
            files.insert(name.clone(), (control.content.clone(), truth));
        }
        Ok(files)
    }
}

/// One record of the committed sample of the public dataset: its text and the annotated spans,
/// as character offsets into the text.
#[derive(Deserialize)]
struct PublicRecord {
    text: String,
    spans: Vec<PublicSpan>,
}

#[derive(Deserialize)]
struct PublicSpan {
    start: usize,
    end: usize,
    label: String,
}

impl TryFrom<&PublicRecord> for FileTruth {
    type Error = String;

    fn try_from(record: &PublicRecord) -> Result<Self, Self::Error> {
        let characters: Vec<char> = record.text.chars().collect();
        let (mut sensitive, mut neutral) = (Vec::new(), Vec::new());
        for span in &record.spans {
            let value: String = characters
                .get(span.start..span.end)
                .ok_or_else(|| format!("span {}..{} lies outside the text", span.start, span.end))?
                .iter()
                .collect();
            match Category::of_nemotron_label(&span.label) {
                Some(category) => sensitive.push((category, Target::Text(value))),
                None => neutral.push(Target::Text(value)),
            }
        }
        Ok(FileTruth::new(sensitive, neutral, Vec::new()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn corpus(files: &str) -> SecretsCorpus {
        serde_json::from_str(&format!(
            r#"{{"values": {{"token": ["ab", "cd"]}}, "files": {files},
                "controls": {{"prose.txt": {{"content": "a commit abc", "keep": ["abc"]}}}}}}"#
        ))
        .unwrap()
    }

    #[test]
    fn secrets_are_reassembled_from_their_parts_and_become_truth() {
        let files = corpus(r#"{".env": "TOKEN={{token}}\n"}"#)
            .assemble()
            .unwrap();

        let (content, truth) = &files[".env"];
        assert_eq!(content, "TOKEN=abcd\n");
        assert_eq!(
            truth.sensitive,
            vec![(Category::Credential, Target::Text("abcd".to_string()))]
        );
        assert_eq!(
            files["prose.txt"].1.keep,
            vec![Target::Text("abc".to_string())]
        );
        assert!(corpus(r#"{".env": "TOKEN={{missing}}"}"#)
            .assemble()
            .is_err());
    }

    #[test]
    fn public_spans_are_cut_by_character_and_neutral_labels_are_not_sensitive() {
        let record: PublicRecord = serde_json::from_str(
            r#"{"text": "Zoë lives in Köln", "spans": [
                {"start": 0, "end": 3, "label": "first_name"},
                {"start": 13, "end": 17, "label": "city"}]}"#,
        )
        .unwrap();

        let truth = FileTruth::try_from(&record).unwrap();

        assert_eq!(
            truth.sensitive,
            vec![(Category::Person, Target::Text("Zoë".to_string()))]
        );
        assert_eq!(truth.neutral, vec![Target::Text("Köln".to_string())]);
    }

    #[test]
    fn committed_datasets_load_with_truth_for_every_file() {
        let work = tempfile::tempdir().unwrap();
        let paths = DatasetPaths {
            fixtures: &Path::new(env!("CARGO_MANIFEST_DIR")).join("test-fixtures"),
            work: work.path(),
        };

        for kind in [
            DatasetKind::Fixtures,
            DatasetKind::Secrets,
            DatasetKind::Public,
            DatasetKind::Images,
        ] {
            let dataset = paths.load(kind).unwrap_or_else(|e| panic!("{kind}: {e}"));
            assert!(dataset.sensitive_count() > 0, "{kind}");
            assert!(
                dataset
                    .truth
                    .keys()
                    .all(|name| dataset.dir.join(name).is_file()),
                "{kind}"
            );
        }
    }
}
