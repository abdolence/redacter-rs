//! The temporary file a scan keeps its findings in leaves nothing behind in the temp directory.

use std::fs;
use std::process::{Command, Output};
use tempfile::TempDir;

fn scan_with_temp_dir(temp: &TempDir, report_file: &str) -> Output {
    let source = tempfile::tempdir().unwrap();
    fs::write(
        source.path().join("a.txt"),
        "mail ana@example.com\n".repeat(50),
    )
    .unwrap();
    Command::new(env!("CARGO_BIN_EXE_redacter"))
        .env("TMPDIR", temp.path())
        .env("TMP", temp.path())
        .env("TEMP", temp.path())
        .current_dir(source.path())
        .args([
            "scan",
            ".",
            "-d",
            "local-rules",
            "--report-file",
            report_file,
        ])
        .output()
        .unwrap()
}

#[test]
fn a_scan_leaves_no_temporary_file_behind() {
    let temp = tempfile::tempdir().unwrap();
    let report = tempfile::tempdir().unwrap();

    let output = scan_with_temp_dir(&temp, report.path().join("r.json").to_str().unwrap());

    assert_eq!(output.status.code(), Some(2), "findings were found");
    assert!(report.path().join("r.json").exists());
    assert_eq!(fs::read_dir(temp.path()).unwrap().count(), 0);
}

#[test]
fn a_scan_that_fails_writing_its_report_leaves_no_temporary_file_behind() {
    let temp = tempfile::tempdir().unwrap();

    let output = scan_with_temp_dir(&temp, "/nonexistent-directory/r.json");

    assert_eq!(output.status.code(), Some(1), "the report was not written");
    assert_eq!(fs::read_dir(temp.path()).unwrap().count(), 0);
}
