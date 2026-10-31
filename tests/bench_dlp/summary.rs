//! What a run found, as `results.json` and as the markdown tables of `summary.md`.

use crate::categories::Category;
use crate::datasets::DatasetKind;
use crate::scoring::Tally;
use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Debug, Serialize)]
pub struct Measurement {
    pub whole_run_ms: u128,
    /// The whole run over the files of the dataset. Every run starts the binary and loads the
    /// models of its redacters, so this includes a fixed cost that a larger dataset spreads.
    pub ms_per_file: u128,
    pub files_failed: usize,
    pub files_blocked: usize,
    pub files_unsupported: usize,
    pub tally: Tally,
}

#[derive(Debug, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Outcome {
    Measured(Measurement),
    /// `scan` produced no report. The reason is the last line of its error output, which names
    /// a setting the redacter lacks when it is not configured.
    Failed {
        reason: String,
    },
}

#[derive(Debug, Serialize)]
pub struct ProviderResult {
    pub provider: String,
    pub outcome: Outcome,
}

#[derive(Debug, Serialize)]
pub struct DatasetResult {
    pub dataset: DatasetKind,
    pub files: usize,
    /// Sensitive values and boxes the dataset holds.
    pub targets: usize,
    pub providers: Vec<ProviderResult>,
}

#[derive(Debug, Serialize)]
pub struct Results {
    pub datasets: Vec<DatasetResult>,
}

fn percent(share: Option<f64>) -> String {
    share.map_or("-".to_string(), |share| format!("{:.0}%", share * 100.0))
}

fn row(cells: &[String]) -> String {
    format!("| {} |", cells.join(" | "))
}

impl Measurement {
    fn recall_in(&self, category: Category, min_support: usize) -> String {
        match self.tally.recall.get(&category) {
            Some(recall) if recall.total >= min_support => {
                format!("{}/{}", recall.found, recall.total)
            }
            _ => "-".to_string(),
        }
    }
}

impl DatasetResult {
    fn table(&self) -> String {
        let complete = self.dataset.has_complete_truth();
        let min_support = self.dataset.min_category_support();
        let mut header: Vec<String> = ["Provider", "Whole run", "Per file", "Recall"]
            .map(String::from)
            .into();
        if complete {
            header.push("Precision".to_string());
        }
        header.extend(Category::ALL.map(|category| category.to_string()));
        if complete {
            header.push("Keep hit".to_string());
        }
        header.push("Failed/blocked/unsupported".to_string());
        let mut lines = vec![row(&header), format!("|{}", "---|".repeat(header.len()))];
        for provider in &self.providers {
            let Outcome::Measured(measured) = &provider.outcome else {
                continue;
            };
            let mut cells = vec![
                provider.provider.clone(),
                format!("{} ms", measured.whole_run_ms),
                format!("{} ms", measured.ms_per_file),
                percent(measured.tally.overall_recall()),
            ];
            if complete {
                cells.push(percent(measured.tally.precision()));
            }
            cells.extend(Category::ALL.map(|category| measured.recall_in(category, min_support)));
            if complete {
                cells.push(format!(
                    "{}/{}",
                    measured.tally.keep_hit, measured.tally.keep_total
                ));
            }
            cells.push(format!(
                "{}/{}/{}",
                measured.files_failed, measured.files_blocked, measured.files_unsupported
            ));
            lines.push(row(&cells));
        }
        lines.join("\n")
    }

    /// The providers that failed and why, with the providers that share a reason named together.
    fn failure_note(&self) -> Option<String> {
        let mut by_reason: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
        for provider in &self.providers {
            if let Outcome::Failed { reason } = &provider.outcome {
                by_reason
                    .entry(reason)
                    .or_default()
                    .push(&provider.provider);
            }
        }
        let entries: Vec<String> = by_reason
            .into_iter()
            .map(|(reason, providers)| format!("{} ({reason})", providers.join(", ")))
            .collect();
        (!entries.is_empty()).then(|| format!("Failed: {}.", entries.join("; ")))
    }
}

impl Results {
    /// One row per provider and one cell per dataset: recall, precision and time per file.
    fn overview(&self) -> String {
        let Some(first) = self.datasets.first() else {
            return String::new();
        };
        let mut header = vec!["Provider".to_string()];
        header.extend(
            self.datasets
                .iter()
                .map(|dataset| dataset.dataset.to_string()),
        );
        let mut lines = vec![row(&header), format!("|{}", "---|".repeat(header.len()))];
        for (index, provider) in first.providers.iter().enumerate() {
            let mut cells = vec![provider.provider.clone()];
            for dataset in &self.datasets {
                cells.push(match &dataset.providers[index].outcome {
                    Outcome::Measured(measured) => format!(
                        "{} / {} / {} ms",
                        percent(measured.tally.overall_recall()),
                        if dataset.dataset.has_complete_truth() {
                            percent(measured.tally.precision())
                        } else {
                            "-".to_string()
                        },
                        measured.ms_per_file
                    ),
                    Outcome::Failed { .. } => "failed".to_string(),
                });
            }
            lines.push(row(&cells));
        }
        lines.join("\n")
    }

    pub fn to_markdown(&self) -> String {
        let mut sections = vec![format!(
            "## Overview\n\nEach cell is recall / precision / time per file. Precision is only \
             reported where the truth lists everything sensitive.\n\n{}",
            self.overview()
        )];
        for dataset in &self.datasets {
            let mut section = format!(
                "## {}\n\n{} files, {} sensitive values or boxes.",
                dataset.dataset, dataset.files, dataset.targets
            );
            if dataset
                .providers
                .iter()
                .any(|p| matches!(p.outcome, Outcome::Measured(_)))
            {
                section.push_str(&format!("\n\n{}", dataset.table()));
            }
            if let Some(note) = dataset.failure_note() {
                section.push_str(&format!("\n\n{note}"));
            }
            sections.push(section);
        }
        sections.join("\n\n") + "\n"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scoring::Recall;

    fn measured(found: usize, total: usize) -> Outcome {
        let mut tally = Tally::default();
        tally
            .recall
            .insert(Category::Contact, Recall { found, total });
        tally
            .recall
            .insert(Category::Other, Recall { found: 1, total: 2 });
        tally.true_findings = found;
        tally.false_findings = 1;
        tally.keep_total = 3;
        Outcome::Measured(Measurement {
            whole_run_ms: 120,
            ms_per_file: 15,
            files_failed: 0,
            files_blocked: 1,
            files_unsupported: 2,
            tally,
        })
    }

    fn failed() -> Outcome {
        Outcome::Failed {
            reason: "exit 1: --gcp-project-id is required".to_string(),
        }
    }

    #[test]
    fn summary_has_an_overview_and_a_table_per_dataset() {
        let dataset = |kind, files, targets, outcomes: [Outcome; 2]| {
            let [local, cloud] = outcomes;
            DatasetResult {
                dataset: kind,
                files,
                targets,
                providers: vec![
                    ProviderResult {
                        provider: "local-rules".to_string(),
                        outcome: local,
                    },
                    ProviderResult {
                        provider: "gcp-dlp".to_string(),
                        outcome: cloud,
                    },
                ],
            }
        };
        let results = Results {
            datasets: vec![
                dataset(DatasetKind::Fixtures, 8, 4, [measured(3, 4), failed()]),
                dataset(DatasetKind::Public, 200, 40, [measured(9, 12), failed()]),
            ],
        };

        let expected = "\
## Overview

Each cell is recall / precision / time per file. Precision is only reported where the truth lists everything sensitive.

| Provider | fixtures | public |
|---|---|---|
| local-rules | 67% / 75% / 15 ms | 71% / - / 15 ms |
| gcp-dlp | failed | failed |

## fixtures

8 files, 4 sensitive values or boxes.

| Provider | Whole run | Per file | Recall | Precision | person | contact | address | id_number | financial | date_of_birth | credential | network | other | Keep hit | Failed/blocked/unsupported |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| local-rules | 120 ms | 15 ms | 67% | 75% | - | 3/4 | - | - | - | - | - | - | 1/2 | 0/3 | 0/1/2 |

Failed: gcp-dlp (exit 1: --gcp-project-id is required).

## public

200 files, 40 sensitive values or boxes.

| Provider | Whole run | Per file | Recall | person | contact | address | id_number | financial | date_of_birth | credential | network | other | Failed/blocked/unsupported |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| local-rules | 120 ms | 15 ms | 71% | - | 9/12 | - | - | - | - | - | - | - | 0/1/2 |

Failed: gcp-dlp (exit 1: --gcp-project-id is required).
";
        assert_eq!(results.to_markdown(), expected);
    }
}
