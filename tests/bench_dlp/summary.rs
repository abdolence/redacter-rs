//! What a run found, as `results.json` and as the markdown tables of `summary.md`.

use crate::categories::Category;
use crate::datasets::DatasetKind;
use crate::scoring::Tally;
use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Debug, Serialize)]
pub struct Measurement {
    pub whole_run_ms: u128,
    /// `None` for a dataset too large to scan file by file.
    pub per_file_median_ms: Option<f64>,
    pub files_failed: usize,
    pub files_blocked: usize,
    pub files_unsupported: usize,
    pub tally: Tally,
}

#[derive(Debug, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Outcome {
    Measured(Measurement),
    /// The spec needs a setting the run does not have, or the dataset could not be built.
    NotMeasured {
        reason: String,
    },
    /// The spec was run and `scan` produced no report.
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

fn milliseconds(value: Option<f64>) -> String {
    value.map_or("-".to_string(), |value| format!("{value:.0} ms"))
}

impl Measurement {
    fn recall_in(&self, category: Category) -> String {
        match self.tally.recall.get(&category) {
            Some(recall) if recall.total > 0 => format!("{}/{}", recall.found, recall.total),
            _ => "-".to_string(),
        }
    }
}

impl DatasetResult {
    fn table(&self) -> String {
        let mut lines = vec![format!(
            "| provider | whole run | per-file median | recall | precision | {} | keep hit | failed/blocked/unsupported |",
            Category::ALL.map(|category| category.to_string()).join(" | ")
        )];
        lines.push(format!("|{}", "---|".repeat(7 + Category::ALL.len())));
        for provider in &self.providers {
            if let Outcome::Measured(measured) = &provider.outcome {
                lines.push(format!(
                    "| {} | {} ms | {} | {} | {} | {} | {}/{} | {}/{}/{} |",
                    provider.provider,
                    measured.whole_run_ms,
                    milliseconds(measured.per_file_median_ms),
                    percent(measured.tally.overall_recall()),
                    percent(measured.tally.precision()),
                    Category::ALL
                        .map(|category| measured.recall_in(category))
                        .join(" | "),
                    measured.tally.keep_hit,
                    measured.tally.keep_total,
                    measured.files_failed,
                    measured.files_blocked,
                    measured.files_unsupported,
                ));
            }
        }
        lines.join("\n")
    }

    /// The providers without a measurement and why, one line for each kind, with the
    /// providers that share a reason named together.
    fn unmeasured_notes(&self) -> Vec<String> {
        let mut not_measured: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
        let mut failed: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
        for provider in &self.providers {
            let (group, reason) = match &provider.outcome {
                Outcome::Measured(_) => continue,
                Outcome::NotMeasured { reason } => (&mut not_measured, reason),
                Outcome::Failed { reason } => (&mut failed, reason),
            };
            group.entry(reason).or_default().push(&provider.provider);
        }
        let line = |heading: &str, group: BTreeMap<&str, Vec<&str>>| {
            let entries: Vec<String> = group
                .into_iter()
                .map(|(reason, providers)| format!("{} ({reason})", providers.join(", ")))
                .collect();
            (!entries.is_empty()).then(|| format!("{heading}: {}.", entries.join("; ")))
        };
        [line("Not measured", not_measured), line("Failed", failed)]
            .into_iter()
            .flatten()
            .collect()
    }
}

impl Results {
    /// One cell per provider and dataset, recall then precision, and the per-file latency of
    /// the first dataset that measured it.
    fn overview(&self) -> String {
        let mut header = "| provider |".to_string();
        for dataset in &self.datasets {
            header.push_str(&format!(" {} recall / precision |", dataset.dataset));
        }
        header.push_str(" per-file median |");
        let mut lines = vec![
            header,
            format!("|{}", "---|".repeat(self.datasets.len() + 2)),
        ];
        let names: Vec<&str> = self
            .datasets
            .first()
            .map(|dataset| {
                dataset
                    .providers
                    .iter()
                    .map(|p| p.provider.as_str())
                    .collect()
            })
            .unwrap_or_default();
        for name in names {
            let outcomes: Vec<&Outcome> = self
                .datasets
                .iter()
                .filter_map(|dataset| dataset.providers.iter().find(|p| p.provider == name))
                .map(|provider| &provider.outcome)
                .collect();
            let mut row = format!("| {name} |");
            for outcome in &outcomes {
                row.push_str(&match outcome {
                    Outcome::Measured(measured) => format!(
                        " {} / {} |",
                        percent(measured.tally.overall_recall()),
                        percent(measured.tally.precision())
                    ),
                    Outcome::NotMeasured { .. } => " not measured |".to_string(),
                    Outcome::Failed { .. } => " failed |".to_string(),
                });
            }
            let latency = outcomes.iter().find_map(|outcome| match outcome {
                Outcome::Measured(measured) => measured.per_file_median_ms,
                _ => None,
            });
            row.push_str(&format!(" {} |", milliseconds(latency)));
            lines.push(row);
        }
        lines.join("\n")
    }

    pub fn to_markdown(&self) -> String {
        let mut sections = vec![format!("## Overview\n\n{}", self.overview())];
        for dataset in &self.datasets {
            let mut section = format!("## {}", dataset.dataset);
            if dataset
                .providers
                .iter()
                .any(|p| matches!(p.outcome, Outcome::Measured(_)))
            {
                section.push_str(&format!(
                    "\n\n{} files, {} sensitive values or boxes.\n\n{}",
                    dataset.files,
                    dataset.targets,
                    dataset.table()
                ));
            }
            for note in dataset.unmeasured_notes() {
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

    fn measured(found: usize, total: usize, per_file: Option<f64>) -> Outcome {
        let mut tally = Tally::default();
        tally
            .recall
            .insert(Category::Contact, Recall { found, total });
        tally.true_findings = found;
        tally.false_findings = 1;
        Outcome::Measured(Measurement {
            whole_run_ms: 120,
            per_file_median_ms: per_file,
            files_failed: 0,
            files_blocked: 1,
            files_unsupported: 2,
            tally,
        })
    }

    fn results() -> Results {
        let provider = |name: &str, outcome| ProviderResult {
            provider: name.to_string(),
            outcome,
        };
        let dataset = |kind, outcomes: Vec<(&str, Outcome)>| DatasetResult {
            dataset: kind,
            files: 8,
            targets: 4,
            providers: outcomes
                .into_iter()
                .map(|(name, outcome)| provider(name, outcome))
                .collect(),
        };
        Results {
            datasets: vec![
                dataset(
                    DatasetKind::Fixtures,
                    vec![
                        ("local-rules", measured(3, 4, Some(17.0))),
                        (
                            "gcp-dlp",
                            Outcome::NotMeasured {
                                reason: "set BENCH_DLP_GCP_PROJECT".to_string(),
                            },
                        ),
                        (
                            "presidio",
                            Outcome::Failed {
                                reason: "exit 1: refused".to_string(),
                            },
                        ),
                    ],
                ),
                dataset(
                    DatasetKind::Secrets,
                    vec![
                        ("local-rules", measured(1, 4, None)),
                        (
                            "gcp-dlp",
                            Outcome::NotMeasured {
                                reason: "set BENCH_DLP_GCP_PROJECT".to_string(),
                            },
                        ),
                        (
                            "presidio",
                            Outcome::Failed {
                                reason: "exit 1: refused".to_string(),
                            },
                        ),
                    ],
                ),
            ],
        }
    }

    #[test]
    fn overview_has_a_row_per_provider_and_a_cell_per_dataset() {
        let markdown = results().to_markdown();

        assert!(markdown.contains("| provider | fixtures recall / precision | secrets recall / precision | per-file median |"));
        assert!(markdown.contains("| local-rules | 75% / 75% | 25% / 50% | 17 ms |"));
        assert!(markdown.contains("| gcp-dlp | not measured | not measured | - |"));
        assert!(markdown.contains("| presidio | failed | failed | - |"));
    }

    #[test]
    fn dataset_table_lists_measured_providers_with_their_categories() {
        let table = results().datasets[0].table();

        assert!(table.contains(
            "| local-rules | 120 ms | 17 ms | 75% | 75% | - | 3/4 | - | - | - | - | - | - | - | 0/0 | 0/1/2 |"
        ));
        assert!(!table.contains("gcp-dlp"));
    }

    #[test]
    fn providers_without_a_measurement_are_listed_with_the_reason_under_the_table() {
        let markdown = results().to_markdown();

        assert!(markdown.contains("Not measured: gcp-dlp (set BENCH_DLP_GCP_PROJECT)."));
        assert!(markdown.contains("Failed: presidio (exit 1: refused)."));
    }

    #[test]
    fn results_serialise_with_a_status_per_provider() {
        let json = serde_json::to_value(results()).unwrap();

        let providers = &json["datasets"][0]["providers"];
        assert_eq!(providers[0]["outcome"]["status"], "measured");
        assert_eq!(
            providers[0]["outcome"]["tally"]["recall"]["contact"]["found"],
            3
        );
        assert_eq!(
            providers[1]["outcome"]["reason"],
            "set BENCH_DLP_GCP_PROJECT"
        );
    }
}
