//! A scan or a copy whose findings cannot be kept in the temporary directory fails as a whole.
//! The directory is chosen by `TMPDIR`, which only unix reads.

#![cfg(unix)]

use std::fs;
use std::process::Command;

#[test]
fn a_scan_that_cannot_keep_its_findings_fails_naming_the_temporary_directory() {
    let source = tempfile::tempdir().unwrap();
    fs::write(source.path().join("a.txt"), "mail ana@example.com\n").unwrap();
    fs::write(source.path().join("b.txt"), "mail bo@example.com\n").unwrap();
    let missing = source.path().join("no-such-temporary-directory");

    let output = Command::new(env!("CARGO_BIN_EXE_redacter"))
        .env("TMPDIR", &missing)
        .current_dir(source.path())
        .args(["scan", ".", "-d", "local-rules"])
        .output()
        .unwrap();

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(output.status.code(), Some(1), "{stdout}");
    assert!(stdout.contains(missing.to_str().unwrap()), "{stdout}");
    assert!(
        !stdout.contains("a.txt"),
        "no file is reported failed: {stdout}"
    );
}

#[test]
fn a_copy_that_cannot_keep_its_report_fails_naming_the_temporary_directory() {
    let source = tempfile::tempdir().unwrap();
    fs::write(source.path().join("a.txt"), "mail ana@example.com\n").unwrap();
    fs::write(source.path().join("b.txt"), "mail bo@example.com\n").unwrap();
    let destination = tempfile::tempdir().unwrap();
    let missing = source.path().join("no-such-temporary-directory");

    let output = Command::new(env!("CARGO_BIN_EXE_redacter"))
        .env("TMPDIR", &missing)
        .current_dir(source.path())
        .args(["cp", "."])
        .arg(destination.path())
        .args(["-d", "local-rules", "--report-file", "report.json"])
        .output()
        .unwrap();

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(output.status.code(), Some(1), "{stdout}");
    assert!(stdout.contains(missing.to_str().unwrap()), "{stdout}");
    assert!(
        !stdout.contains("a.txt"),
        "no file is reported failed: {stdout}"
    );
}
