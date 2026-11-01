//! The `--categories` examples of the README, run against the built binary.

use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const NOTE: &str = "mail ana@example.com\nkey AKIAIOSFODNN7EXAMPLE\n";
const EMAIL: &str = "ana@example.com";
const KEY: &str = "AKIAIOSFODNN7EXAMPLE";

/// A directory with `in/a.txt` holding an email address and an AWS access key, removed on drop.
struct Workspace(PathBuf);

impl Workspace {
    fn new() -> Self {
        let root =
            std::env::temp_dir().join(format!("redacter-categories-{}", uuid::Uuid::now_v7()));
        fs::create_dir_all(root.join("in")).unwrap();
        fs::write(root.join("in/a.txt"), NOTE).unwrap();
        Self(root)
    }

    fn path(&self, relative: &str) -> PathBuf {
        self.0.join(relative)
    }

    fn run(&self, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_redacter"))
            .current_dir(&self.0)
            .args(args)
            .output()
            .unwrap()
    }

    fn scan_report(&self, categories: &str) -> Value {
        let output = self.run(&[
            "scan",
            "in",
            "-d",
            "local-rules",
            "--categories",
            categories,
            "--report-file",
            "report.json",
        ]);
        assert!(
            output.status.code() == Some(2),
            "{categories}: {}",
            String::from_utf8_lossy(&output.stdout)
        );
        serde_json::from_str(&fs::read_to_string(self.path("report.json")).unwrap()).unwrap()
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn labels(report: &Value) -> Vec<&str> {
    report["files"][0]["findings"]
        .as_array()
        .unwrap()
        .iter()
        .map(|finding| finding["label"].as_str().unwrap())
        .collect()
}

#[test]
fn a_scan_for_secrets_reports_the_key_and_not_the_email() {
    let workspace = Workspace::new();

    let report = workspace.scan_report("secrets");

    assert_eq!(labels(&report), ["aws-access-key"]);
    assert_eq!(report["categories"], serde_json::json!(["secrets"]));
}

#[test]
fn a_scan_for_everything_but_other_reports_the_resolved_categories() {
    let workspace = Workspace::new();

    let report = workspace.scan_report("!other");

    assert_eq!(labels(&report), ["email", "aws-access-key"]);
    let categories = report["categories"].as_array().unwrap();
    assert!(!categories.contains(&Value::from("other")));
    assert!(categories.contains(&Value::from("secrets")));
}

#[test]
fn a_copy_for_contact_redacts_the_email_and_leaves_the_key() {
    let workspace = Workspace::new();

    let output = workspace.run(&[
        "cp",
        "in/",
        "out/",
        "-d",
        "local-rules",
        "--categories",
        "contact",
    ]);

    assert!(output.status.success());
    let copy = fs::read_to_string(workspace.path("out/a.txt")).unwrap();
    assert!(!copy.contains(EMAIL), "{copy}");
    assert!(copy.contains(KEY), "{copy}");
}

#[test]
fn a_bad_list_of_categories_fails_before_any_file_is_read() {
    let workspace = Workspace::new();

    for categories in ["credential", "contact,!other", "!nonsense"] {
        let output = workspace.run(&[
            "scan",
            "in",
            "-d",
            "local-rules",
            "--categories",
            categories,
            "--report-file",
            "report.json",
        ]);

        assert!(!output.status.success(), "{categories}");
        assert!(!Path::new(&workspace.path("report.json")).exists());
    }
}

#[test]
fn a_rule_group_outside_the_categories_is_refused() {
    let workspace = Workspace::new();

    let output = workspace.run(&[
        "scan",
        "in",
        "-d",
        "local-rules",
        "--local-rules",
        "email,secrets",
        "--categories",
        "secrets",
    ]);

    assert_eq!(output.status.code(), Some(1));
}
