//! Writes `table.rs` from `vendor/gitleaks.toml`. Run on purpose after updating the vendored
//! file; `table_is_up_to_date` fails when the two disagree.

use super::{compile_pattern, AllowTarget};
use serde::Deserialize;
use std::fmt::Write;

const VENDORED: &str = include_str!("../vendor/gitleaks.toml");
const PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/src/redacters/local_rules/rules/gitleaks/table.rs"
);

/// The gitleaks commit `vendor/gitleaks.toml` was taken from.
const COMMIT: &str = "5f6ec56cfa6f8c67648d863582c7ae2f206d5c1c";

/// Rules left out although they compile, each with the reason shown in the rules reference.
/// A rule is covered when a hand-written rule matches every shape of it and, on the
/// Nosey Parker examples, finds as many of them.
const EXCLUDED: &[(&str, &str)] = &[
    ("gcp-api-key", "covered by the hand-written `gcp-api-key`"),
    ("github-pat", "covered by the hand-written `github-token`"),
    (
        "github-fine-grained-pat",
        "covered by the hand-written `github-token`",
    ),
    ("github-oauth", "covered by the hand-written `github-token`"),
    (
        "github-app-token",
        "covered by the hand-written `github-token`",
    ),
    (
        "github-refresh-token",
        "covered by the hand-written `github-token`",
    ),
    (
        "slack-bot-token",
        "covered by the hand-written `slack-token`",
    ),
    (
        "slack-user-token",
        "covered by the hand-written `slack-token`",
    ),
    (
        "slack-legacy-bot-token",
        "covered by the hand-written `slack-token`",
    ),
    (
        "slack-legacy-workspace-token",
        "covered by the hand-written `slack-token`",
    ),
    (
        "slack-webhook-url",
        "covered by the hand-written `slack-webhook`",
    ),
    ("openai-api-key", "covered by the hand-written `openai-key`"),
    (
        "anthropic-api-key",
        "covered by the hand-written `anthropic-key`",
    ),
    (
        "anthropic-admin-api-key",
        "covered by the hand-written `anthropic-key`",
    ),
    (
        "npm-access-token",
        "covered by the hand-written `npm-token`",
    ),
    (
        "sendgrid-api-token",
        "covered by the hand-written `sendgrid-key`",
    ),
    (
        "huggingface-access-token",
        "covered by the hand-written `huggingface-token`",
    ),
    ("gitlab-pat", "covered by the hand-written `gitlab-token`"),
    ("jwt", "covered by the hand-written `jwt`"),
    (
        "private-key",
        "covered by the hand-written `private-key-block`",
    ),
];

/// Rules whose capture groups are parts of the match and not the secret, so the whole match
/// is redacted. Without an entry, a rule with one capture group redacts it, and a rule with
/// several redacts the first, as gitleaks does.
const WHOLE_MATCH: &[&str] = &[
    "atlassian-api-token",
    "jwt-base64",
    "microsoft-teams-webhook",
];

#[derive(Deserialize)]
struct Config {
    allowlist: RawAllowlist,
    rules: Vec<RawRule>,
}

#[derive(Deserialize)]
struct RawRule {
    id: String,
    description: String,
    regex: Option<String>,
    path: Option<String>,
    entropy: Option<f64>,
    #[serde(rename = "secretGroup")]
    secret_group: Option<usize>,
    #[serde(default)]
    keywords: Vec<String>,
    #[serde(default)]
    allowlists: Vec<RawAllowlist>,
}

#[derive(Deserialize, Default)]
struct RawAllowlist {
    #[serde(rename = "regexTarget")]
    regex_target: Option<String>,
    condition: Option<String>,
    #[serde(default)]
    paths: Vec<String>,
    #[serde(default)]
    regexes: Vec<String>,
    #[serde(default)]
    stopwords: Vec<String>,
}

/// `text` as a raw string literal with enough `#` to hold any quote inside it.
fn raw(text: &str) -> String {
    let mut hashes = 1;
    while text.contains(&format!("\"{}", "#".repeat(hashes))) {
        hashes += 1;
    }
    let fence = "#".repeat(hashes);
    format!("r{fence}\"{text}\"{fence}")
}

fn strings(values: &[String]) -> String {
    let items: Vec<String> = values.iter().map(|value| format!("{value:?}")).collect();
    format!("&[{}]", items.join(", "))
}

impl RawAllowlist {
    /// The allowlist as Rust source, or `None` when nothing in it can be checked on text:
    /// it holds only paths, or requires a path (`condition = "AND"`).
    fn render(&self, rule: &str) -> Result<Option<String>, String> {
        if self.condition.as_deref() == Some("AND") && !self.paths.is_empty() {
            return Ok(None);
        }
        if self.regexes.is_empty() && self.stopwords.is_empty() {
            return Ok(None);
        }
        for pattern in &self.regexes {
            compile_pattern(rule, pattern).map_err(|err| err.to_string())?;
        }
        let target = match self.regex_target.as_deref() {
            None | Some("") | Some("secret") => AllowTarget::Secret,
            Some("match") => AllowTarget::Match,
            Some("line") => AllowTarget::Line,
            Some(other) => return Err(format!("unknown regexTarget `{other}`")),
        };
        let regexes: Vec<String> = self.regexes.iter().map(|pattern| raw(pattern)).collect();
        Ok(Some(format!(
            "Allowlist {{ target: AllowTarget::{target:?}, regexes: &[{}], stopwords: {} }}",
            regexes.join(", "),
            strings(&self.stopwords)
        )))
    }
}

impl RawRule {
    /// The rule as Rust source, or the reason it is not ported.
    fn render(&self) -> Result<String, String> {
        if let Some((_, reason)) = EXCLUDED.iter().find(|(id, _)| *id == self.id) {
            return Err((*reason).to_string());
        }
        let (Some(pattern), None) = (&self.regex, &self.path) else {
            return Err(
                "applies only to files whose name matches a path pattern, which a text scan does not see"
                    .to_string(),
            );
        };
        let compiled = compile_pattern(&self.id, pattern).map_err(|err| {
            if err.to_string().contains("size limit") {
                "needs Unicode classes whose compiled size is over the regex size limit".to_string()
            } else {
                err.to_string()
            }
        })?;
        let secret_group = if WHOLE_MATCH.contains(&self.id.as_str()) {
            0
        } else {
            self.secret_group
                .unwrap_or(usize::from(compiled.captures_len() > 1))
        };
        let mut allowlists = Vec::new();
        for allowlist in &self.allowlists {
            allowlists.extend(allowlist.render(&self.id)?);
        }
        let entropy = self
            .entropy
            .map_or_else(|| "None".to_string(), |floor| format!("Some({floor:?})"));
        let description = self.description.trim_end_matches('.');
        Ok(format!(
            "    GitleaksRule {{\n        id: {:?},\n        description: {description:?},\n        pattern: {},\n        secret_group: {secret_group},\n        entropy: {entropy},\n        keywords: {},\n        allowlists: &[{}],\n    }},\n",
            self.id,
            raw(pattern),
            strings(&self.keywords),
            allowlists.join(", ")
        ))
    }
}

fn render_table() -> String {
    let config: Config = toml::from_str(VENDORED).unwrap();
    let mut out = String::from(
        "// Generated from vendor/gitleaks.toml by `cargo test gitleaks_table_regenerate -- --ignored`.\n\
         // Do not edit by hand. The rules are gitleaks rules, Copyright (c) 2019 Zachary Rice,\n\
         // MIT licence: see vendor/gitleaks.LICENSE.\n\n\
         use super::{AllowTarget, Allowlist, GitleaksRule};\n\n",
    );
    let global = config
        .allowlist
        .render("global allowlist")
        .unwrap()
        .unwrap();
    writeln!(
        out,
        "pub(in super::super) const GLOBAL_ALLOWLIST: Allowlist = {global};\n"
    )
    .unwrap();
    let mut rules = String::new();
    let mut not_ported = Vec::new();
    for rule in &config.rules {
        match rule.render() {
            Ok(source) => rules.push_str(&source),
            Err(reason) => not_ported.push((rule.id.clone(), reason)),
        }
    }
    writeln!(
        out,
        "pub(in super::super) const RULES: &[GitleaksRule] = &[\n{rules}];\n"
    )
    .unwrap();
    writeln!(
        out,
        "/// The gitleaks commit the rules were taken from.\n#[cfg(test)]\npub(in super::super) const SOURCE_COMMIT: &str = {COMMIT:?};\n"
    )
    .unwrap();
    out.push_str("/// Rules of the vendored file that are not in `RULES`, with the reason.\n");
    out.push_str("#[cfg(test)]\npub(in super::super) const NOT_PORTED: &[(&str, &str)] = &[\n");
    for (id, reason) in not_ported {
        writeln!(out, "    ({id:?}, {reason:?}),").unwrap();
    }
    out.push_str("];\n");
    out
}

#[test]
fn table_is_up_to_date() {
    let committed = std::fs::read_to_string(PATH)
        .unwrap_or_default()
        .replace("\r\n", "\n");
    assert!(
        committed == render_table(),
        "gitleaks/table.rs is stale: run `cargo test gitleaks_table_regenerate -- --ignored` to regenerate"
    );
}

#[test]
#[ignore = "writes gitleaks/table.rs; run on purpose after updating vendor/gitleaks.toml"]
fn gitleaks_table_regenerate() {
    std::fs::write(PATH, render_table()).unwrap();
}
