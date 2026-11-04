//! End-to-end runs of `scan`, `cp` and `ls` over a directory that holds a `.git` directory.

use std::fs;
use std::path::PathBuf;
use std::process::{Command, Output};
use tempfile::TempDir;

const REFLOG_ENTRY: &str = "0000 1111 Runner <runner@example.com> 1700000000 +0000\tclone\n";

/// A directory with a clean `in/a.txt`, `in/.git/logs/HEAD` holding an email address, and a
/// nested clone at `in/vendor/lib/.git/logs/HEAD`.
struct Workspace(TempDir);

impl Workspace {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        for git_directory in ["in/.git", "in/vendor/lib/.git"] {
            let logs = directory.path().join(git_directory).join("logs");
            fs::create_dir_all(&logs).unwrap();
            fs::write(logs.join("HEAD"), REFLOG_ENTRY).unwrap();
        }
        fs::write(directory.path().join("in/a.txt"), "nothing to find\n").unwrap();
        fs::write(directory.path().join("in/vendor/lib/b.txt"), "clean\n").unwrap();
        Self(directory)
    }

    fn path(&self, relative: &str) -> PathBuf {
        self.0.path().join(relative)
    }

    fn run(&self, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_redacter"))
            .current_dir(self.0.path())
            .args(args)
            .output()
            .unwrap()
    }
}

#[test]
fn a_scan_leaves_git_directories_out() {
    let workspace = Workspace::new();

    let output = workspace.run(&["scan", "in", "-d", "local-rules", "--unsupported-as-text"]);

    assert_eq!(
        output.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
}

#[test]
fn a_scan_with_no_ignore_leaves_git_directories_out() {
    let workspace = Workspace::new();

    let output = workspace.run(&[
        "scan",
        "in",
        "-d",
        "local-rules",
        "--unsupported-as-text",
        "--no-ignore",
    ]);

    assert_eq!(output.status.code(), Some(0));
}

#[test]
fn a_scan_of_a_path_inside_a_git_directory_still_reads_it() {
    let workspace = Workspace::new();

    let output = workspace.run(&[
        "scan",
        "in/.git",
        "-d",
        "local-rules",
        "--unsupported-as-text",
    ]);

    assert_eq!(output.status.code(), Some(2));
}

#[test]
fn a_copy_leaves_git_directories_out() {
    let workspace = Workspace::new();

    let output = workspace.run(&[
        "cp",
        "in/",
        "out/",
        "-d",
        "local-rules",
        "--unsupported-as-text",
    ]);

    assert!(output.status.success());
    assert!(workspace.path("out/a.txt").is_file());
    assert!(workspace.path("out/vendor/lib/b.txt").is_file());
    assert!(!workspace.path("out/.git").exists());
    assert!(!workspace.path("out/vendor/lib/.git").exists());
}

#[test]
fn a_listing_leaves_git_directories_out() {
    let workspace = Workspace::new();

    let output = workspace.run(&["ls", "in"]);

    let listing = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success());
    assert!(listing.contains("a.txt"), "{listing}");
    assert!(!listing.contains(".git"), "{listing}");
}
