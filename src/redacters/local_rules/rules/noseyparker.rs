//! The example strings of Nosey Parker's rules (`vendor/noseyparker-examples.json`) as a
//! test of the rules that match the same secrets. Each example is stored as short pieces
//! and joined here, so that no complete token sits in the repository, where GitHub push
//! protection would block it.

use super::test_support::all_rules;
use serde::Deserialize;

#[derive(Deserialize)]
struct Entry {
    id: String,
    examples: Vec<Vec<String>>,
    negative_examples: Vec<Vec<String>>,
}

/// A Nosey Parker rule and the rule of ours that finds the same secrets: it finds every
/// example of the first and no negative example.
const PAIRS: &[(&str, &str)] = &[
    ("np.age.2", "age-secret-key"),
    ("np.anthropic.1", "anthropic-key"),
    ("np.atlassian.1", "atlassian-api-token"),
    ("np.aws.2", "aws-secret-key"),
    ("np.aws.4", "aws-secret-key"),
    ("np.aws.5", "aws-access-key"),
    ("np.blynk.9", "curl-auth-user"),
    ("np.doppler.2", "doppler-api-token"),
    ("np.github.1", "github-token"),
    ("np.github.4", "github-token"),
    ("np.github.7", "github-token"),
    ("np.gitlab.1", "gitlab-rrt"),
    ("np.google.5", "gcp-api-key"),
    ("np.huggingface.1", "huggingface-token"),
    ("np.linkedin.2", "linkedin-client-id"),
    ("np.mailgun.1", "mailgun-private-api-token"),
    ("np.npm.1", "npm-token"),
    ("np.pem.1", "private-key-block"),
    ("np.rubygems.1", "rubygems-api-token"),
    ("np.sendgrid.1", "sendgrid-key"),
    ("np.shopify.4", "shopify-custom-access-token"),
    ("np.slack.3", "slack-webhook"),
    ("np.sonarqube.1", "sonar-api-token"),
    ("np.stripe.1", "stripe-key"),
    ("np.stripe.2", "stripe-access-token"),
];

fn entries() -> Vec<Entry> {
    serde_json::from_str(include_str!("vendor/noseyparker-examples.json")).unwrap()
}

fn joined(pieces: &[String]) -> String {
    pieces.concat()
}

fn labels_found_in(text: &str) -> Vec<String> {
    all_rules()
        .find(text)
        .iter()
        .map(|finding| finding.rule.to_string())
        .collect()
}

#[test]
fn every_example_is_found_by_the_rule_named_for_it() {
    let entries = entries();
    for (id, label) in PAIRS {
        let entry = entries.iter().find(|entry| entry.id == *id).unwrap();
        assert!(!entry.examples.is_empty(), "{id} has no examples");
        for pieces in &entry.examples {
            let text = joined(pieces);
            assert!(
                labels_found_in(&text).iter().any(|found| found == label),
                "{id}: {label} missed an example"
            );
        }
    }
}

#[test]
fn no_negative_example_is_found_by_the_rule_named_for_it() {
    let entries = entries();
    for (id, label) in PAIRS {
        let entry = entries.iter().find(|entry| entry.id == *id).unwrap();
        for pieces in &entry.negative_examples {
            let text = joined(pieces);
            assert!(
                !labels_found_in(&text).iter().any(|found| found == label),
                "{id}: {label} found a negative example"
            );
        }
    }
}
