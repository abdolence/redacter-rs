//! The datasets a benchmark run scores against: each is a directory of files for `scan` to read
//! and, per file, the truth to score its report with.

use crate::categories::Category;
use crate::scoring::{FileTruth, Rect, Target};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fmt::{self, Display};
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DatasetKind {
    Fixtures,
    Secrets,
    Public,
    Images,
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
    pub fn load(&self, kind: DatasetKind, sample_size: usize) -> Result<Dataset, String> {
        match kind {
            DatasetKind::Fixtures => self.fixtures_dataset(),
            DatasetKind::Secrets => self.secrets_dataset(),
            DatasetKind::Public => self.public_dataset(sample_size),
            DatasetKind::Images => self.images_dataset(),
        }
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

    fn fixtures_dataset(&self) -> Result<Dataset, String> {
        let dir = self.dataset_dir(DatasetKind::Fixtures)?;
        let entries: BTreeMap<String, TextTruthEntry> = self.read_json("bench-dlp/truth.json")?;
        let mut truth = BTreeMap::new();
        for (name, entry) in entries {
            let copied = self.copy_fixture(&format!("documents/{name}"), &dir)?;
            truth.insert(copied, FileTruth::from(entry));
        }
        Ok(Dataset {
            kind: DatasetKind::Fixtures,
            dir,
            truth,
            scan_args: &[],
        })
    }

    fn secrets_dataset(&self) -> Result<Dataset, String> {
        let dir = self.dataset_dir(DatasetKind::Secrets)?;
        let corpus: SecretsCorpus = self.read_json("bench-dlp/secrets/secrets.json")?;
        let mut truth = BTreeMap::new();
        for (name, (content, file_truth)) in corpus.assemble()? {
            fs::write(dir.join(&name), content).map_err(|e| format!("write {name}: {e}"))?;
            truth.insert(name, file_truth);
        }
        Ok(Dataset {
            kind: DatasetKind::Secrets,
            dir,
            truth,
            scan_args: &["--unsupported-as-text"],
        })
    }

    fn images_dataset(&self) -> Result<Dataset, String> {
        let dir = self.dataset_dir(DatasetKind::Images)?;
        let entries: BTreeMap<String, ImageTruthEntry> = self.read_json("bench-dlp/images.json")?;
        let mut truth = BTreeMap::new();
        for (relative, entry) in entries {
            let name = self.copy_fixture(&relative, &dir)?;
            truth.insert(name, FileTruth::from(entry));
        }
        Ok(Dataset {
            kind: DatasetKind::Images,
            dir,
            truth,
            scan_args: &[],
        })
    }

    fn public_dataset(&self, sample_size: usize) -> Result<Dataset, String> {
        let cache = self.work.join(format!("public-sample-{sample_size}.json"));
        let records = match fs::read_to_string(&cache) {
            Ok(cached) => serde_json::from_str(&cached)
                .map_err(|e| format!("parse cached sample {}: {e}", cache.display()))?,
            Err(_) => {
                let records = fetch_sample(sample_size)?;
                let json = serde_json::to_string(&records).map_err(|e| e.to_string())?;
                fs::write(&cache, json).map_err(|e| format!("write {}: {e}", cache.display()))?;
                records
            }
        };
        let dir = self.dataset_dir(DatasetKind::Public)?;
        let mut truth = BTreeMap::new();
        for (index, record) in records.iter().enumerate() {
            let name = format!("{index:04}.txt");
            fs::write(dir.join(&name), &record.text).map_err(|e| format!("write {name}: {e}"))?;
            truth.insert(name, FileTruth::from(record));
        }
        Ok(Dataset {
            kind: DatasetKind::Public,
            dir,
            truth,
            scan_args: &[],
        })
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
        FileTruth {
            sensitive: entry
                .pii
                .into_iter()
                .map(|pii| (pii.category, Target::Text(pii.text)))
                .collect(),
            neutral: entry.entities.into_iter().map(Target::Text).collect(),
            keep: entry.keep.into_iter().map(Target::Text).collect(),
        }
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
        FileTruth {
            sensitive: entry
                .sensitive
                .into_iter()
                .map(|item| (item.category, Target::Region(item.region)))
                .collect(),
            neutral: Vec::new(),
            keep: entry.keep.into_iter().map(Target::Region).collect(),
        }
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

/// One record of the public dataset: its text and the spans annotated in it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Record {
    text: String,
    spans: Vec<Span>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Span {
    text: String,
    label: String,
}

impl From<&Record> for FileTruth {
    fn from(record: &Record) -> Self {
        FileTruth {
            sensitive: record
                .spans
                .iter()
                .map(|span| {
                    (
                        Category::from(span.label.as_str()),
                        Target::Text(span.text.clone()),
                    )
                })
                .collect(),
            ..FileTruth::default()
        }
    }
}

const PUBLIC_DATASET: &str = "nvidia/Nemotron-PII";
const PUBLIC_SPLIT: &str = "test";
/// Rows per request: the datasets server caps a request at 100 and consecutive rows are the
/// same document family often enough that a sample should come from several places.
const PUBLIC_WINDOW: usize = 20;
const PUBLIC_SEED: u64 = 0x2026_1031;
const PUBLIC_FETCH_ATTEMPTS: usize = 4;

/// The `spans` column of a record is a Python literal, a list of dicts with the keys `start`,
/// `end`, `text` and `label`. Offsets are not used: a span whose text is not in the record, as
/// some of the dataset's offsets are wrong about, is dropped, and the rest are matched by
/// value like every other dataset.
fn spans_of(literal: &str, text: &str) -> Result<Vec<Span>, String> {
    let value = PythonLiteral::new(literal).parse()?;
    let items = value.as_array().ok_or("spans is not a list")?;
    let mut spans = Vec::new();
    for item in items {
        // A span of a numeric label, such as an age, holds its text as a number.
        let field = |name: &str| match item.get(name)? {
            Value::String(text) => Some(text.clone()),
            Value::Number(number) => Some(number.to_string()),
            _ => None,
        };
        let (Some(span_text), Some(label)) = (field("text"), field("label")) else {
            return Err(format!("span without text and label: {item}"));
        };
        if text.contains(&span_text) {
            spans.push(Span {
                text: span_text,
                label,
            });
        }
    }
    Ok(spans)
}

/// A reader of the subset of Python literals a dataset column holds: lists, dicts, strings in
/// either quote, numbers, `None`, `True` and `False`.
struct PythonLiteral<'a> {
    chars: std::iter::Peekable<std::str::Chars<'a>>,
}

impl<'a> PythonLiteral<'a> {
    fn new(source: &'a str) -> Self {
        Self {
            chars: source.chars().peekable(),
        }
    }

    fn parse(mut self) -> Result<Value, String> {
        let value = self.value()?;
        self.skip_whitespace();
        match self.chars.next() {
            None => Ok(value),
            Some(extra) => Err(format!("unexpected {extra:?} after the literal")),
        }
    }

    fn skip_whitespace(&mut self) {
        while self.chars.next_if(|c| c.is_whitespace()).is_some() {}
    }

    fn expect(&mut self, expected: char) -> Result<(), String> {
        self.skip_whitespace();
        match self.chars.next() {
            Some(c) if c == expected => Ok(()),
            other => Err(format!("expected {expected:?}, found {other:?}")),
        }
    }

    fn value(&mut self) -> Result<Value, String> {
        self.skip_whitespace();
        match self.chars.peek().copied() {
            Some('[') => self.sequence('[', ']', Self::value).map(Value::Array),
            Some('{') => self
                .sequence('{', '}', |this| {
                    let Value::String(key) = this.value()? else {
                        return Err("a dict key is not a string".to_string());
                    };
                    this.expect(':')?;
                    Ok((key, this.value()?))
                })
                .map(|pairs| Value::Object(pairs.into_iter().collect())),
            Some(quote @ ('\'' | '"')) => {
                self.chars.next();
                self.string(quote).map(Value::String)
            }
            Some(_) => self.word(),
            None => Err("the literal ends early".to_string()),
        }
    }

    fn sequence<T>(
        &mut self,
        open: char,
        close: char,
        mut item: impl FnMut(&mut Self) -> Result<T, String>,
    ) -> Result<Vec<T>, String> {
        self.expect(open)?;
        let mut items = Vec::new();
        loop {
            self.skip_whitespace();
            if self.chars.next_if_eq(&close).is_some() {
                return Ok(items);
            }
            items.push(item(self)?);
            self.skip_whitespace();
            if self.chars.next_if_eq(&',').is_none() {
                self.expect(close)?;
                return Ok(items);
            }
        }
    }

    fn string(&mut self, quote: char) -> Result<String, String> {
        let mut out = String::new();
        loop {
            match self.chars.next().ok_or("a string is not closed")? {
                c if c == quote => return Ok(out),
                '\\' => out.push(self.escape()?),
                c => out.push(c),
            }
        }
    }

    fn escape(&mut self) -> Result<char, String> {
        let escaped = self.chars.next().ok_or("an escape is cut short")?;
        let digits = match escaped {
            'n' => return Ok('\n'),
            't' => return Ok('\t'),
            'r' => return Ok('\r'),
            'x' => 2,
            'u' => 4,
            'U' => 8,
            other => return Ok(other),
        };
        let hex: String = self.chars.by_ref().take(digits).collect();
        u32::from_str_radix(&hex, 16)
            .ok()
            .and_then(char::from_u32)
            .ok_or_else(|| format!("bad escape \\{escaped}{hex}"))
    }

    fn word(&mut self) -> Result<Value, String> {
        let mut word = String::new();
        while let Some(c) = self
            .chars
            .next_if(|c| !matches!(c, ',' | ']' | '}' | ':') && !c.is_whitespace())
        {
            word.push(c);
        }
        match word.as_str() {
            "None" => Ok(Value::Null),
            "True" => Ok(Value::Bool(true)),
            "False" => Ok(Value::Bool(false)),
            number => serde_json::from_str(number).map_err(|_| format!("unknown word {number:?}")),
        }
    }
}

/// A SplitMix64 generator: the sample must be the same on every machine and every Rust
/// version, which a seeded generator from a crate does not promise.
struct SplitMix64(u64);

impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

/// Offsets of `count` windows of [`PUBLIC_WINDOW`] rows inside a split of `total` rows.
fn window_offsets(total: usize, count: usize, seed: u64) -> Vec<usize> {
    let mut random = SplitMix64(seed);
    let room = total.saturating_sub(PUBLIC_WINDOW).max(1) as u64;
    (0..count)
        .map(|_| (random.next() % room) as usize)
        .collect()
}

/// A seeded sample of `size` records of the public dataset, fetched through the Hugging Face
/// datasets server.
fn fetch_sample(size: usize) -> Result<Vec<Record>, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| e.to_string())?;
    runtime.block_on(async {
        let client = reqwest::Client::new();
        let first = fetch_rows(&client, 0, 1).await?;
        let total = first["num_rows_total"]
            .as_u64()
            .ok_or("the datasets server did not say how many rows there are")?
            as usize;
        let mut records = Vec::new();
        let mut seen = std::collections::HashSet::new();
        let mut round = 0u64;
        while records.len() < size {
            let missing = size - records.len();
            let windows = missing.div_ceil(PUBLIC_WINDOW);
            for offset in window_offsets(total, windows, PUBLIC_SEED + round) {
                let page = fetch_rows(&client, offset, PUBLIC_WINDOW).await?;
                for row in page["rows"].as_array().into_iter().flatten() {
                    let row = &row["row"];
                    let uid = row["uid"].as_str().unwrap_or_default().to_string();
                    let (Some(text), Some(spans)) = (row["text"].as_str(), row["spans"].as_str())
                    else {
                        continue;
                    };
                    if records.len() < size && seen.insert(uid) {
                        records.push(Record {
                            text: text.to_string(),
                            spans: spans_of(spans, text)?,
                        });
                    }
                }
            }
            round += 1;
        }
        Ok(records)
    })
}

async fn fetch_rows(
    client: &reqwest::Client,
    offset: usize,
    length: usize,
) -> Result<Value, String> {
    let url = format!(
        "https://datasets-server.huggingface.co/rows?dataset={PUBLIC_DATASET}&config=default&split={PUBLIC_SPLIT}&offset={offset}&length={length}"
    );
    let mut last_error = String::new();
    for attempt in 1..=PUBLIC_FETCH_ATTEMPTS {
        match client
            .get(&url)
            .send()
            .await
            .and_then(|r| r.error_for_status())
        {
            Ok(response) => return response.json().await.map_err(|e| e.to_string()),
            Err(e) => last_error = e.to_string(),
        }
        tokio::time::sleep(Duration::from_secs(2 * attempt as u64)).await;
    }
    Err(format!(
        "the datasets server did not answer {PUBLIC_DATASET} after {PUBLIC_FETCH_ATTEMPTS} attempts: {last_error}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn python_literal_reads_the_dataset_spans() {
        let literal = r#"[{'start': 0, 'end': 5, 'text': 'Brian', 'label': 'first_name'}, {'start': 9, 'end': 20, 'text': "O'Brien\né", 'label': 'last_name'}]"#;

        let spans = spans_of(literal, "Brian said O'Brien\n\u{e9}").unwrap();

        assert_eq!(
            spans,
            vec![
                Span {
                    text: "Brian".to_string(),
                    label: "first_name".to_string()
                },
                Span {
                    text: "O'Brien\n\u{e9}".to_string(),
                    label: "last_name".to_string()
                },
            ]
        );
    }

    #[test]
    fn numeric_span_text_is_kept_as_text() {
        let literal = "[{'start': 7, 'end': 9, 'text': 47, 'label': 'age'}]";

        let spans = spans_of(literal, "He is 47").unwrap();

        assert_eq!(spans[0].text, "47");
    }

    #[test]
    fn spans_absent_from_the_text_are_dropped() {
        let literal = "[{'start': 0, 'end': 4, 'text': 'Zoë', 'label': 'first_name'}]";

        assert_eq!(spans_of(literal, "Brian").unwrap(), vec![]);
    }

    #[test]
    fn python_literal_rejects_unclosed_input() {
        assert!(spans_of("[{'text': 'x'", "x").is_err());
    }

    #[test]
    fn window_offsets_are_stable_and_inside_the_split() {
        let offsets = window_offsets(100_000, 5, 7);

        assert_eq!(offsets, window_offsets(100_000, 5, 7));
        assert_ne!(offsets, window_offsets(100_000, 5, 8));
        assert!(offsets
            .iter()
            .all(|offset| offset + PUBLIC_WINDOW <= 100_000));
    }

    #[test]
    fn record_truth_takes_the_category_of_each_label() {
        let record = Record {
            text: "Brian, 5490 3479 1287 6543".to_string(),
            spans: vec![
                Span {
                    text: "Brian".to_string(),
                    label: "first_name".to_string(),
                },
                Span {
                    text: "5490 3479 1287 6543".to_string(),
                    label: "credit_debit_card".to_string(),
                },
            ],
        };

        let truth = FileTruth::from(&record);

        assert_eq!(
            truth.sensitive,
            vec![
                (Category::Person, Target::Text("Brian".to_string())),
                (
                    Category::Financial,
                    Target::Text("5490 3479 1287 6543".to_string())
                ),
            ]
        );
    }

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
        assert!(files["prose.txt"].1.sensitive.is_empty());
    }

    #[test]
    fn secrets_template_naming_an_unknown_value_is_rejected() {
        assert!(corpus(r#"{".env": "TOKEN={{missing}}"}"#)
            .assemble()
            .is_err());
    }

    #[test]
    fn fixture_truth_entry_becomes_sensitive_neutral_and_keep_targets() {
        let entry: TextTruthEntry = serde_json::from_str(
            r#"{"pii": [{"text": "Alice", "category": "person"}], "entities": ["London"], "keep": ["Monday"]}"#,
        )
        .unwrap();

        let truth = FileTruth::from(entry);

        assert_eq!(
            truth.sensitive,
            vec![(Category::Person, Target::Text("Alice".to_string()))]
        );
        assert_eq!(truth.neutral, vec![Target::Text("London".to_string())]);
        assert_eq!(truth.keep, vec![Target::Text("Monday".to_string())]);
    }

    #[test]
    fn dataset_names_parse_and_unknown_ones_are_rejected() {
        assert_eq!("public".parse(), Ok(DatasetKind::Public));
        assert!("gretel".parse::<DatasetKind>().is_err());
    }
}
