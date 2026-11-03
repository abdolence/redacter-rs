//! The exit code of `cp` through the built binary.

use std::fs;
use std::io::Write;
use std::process::{Command, Output};
use tempfile::TempDir;

/// A source directory holding a clean text file, and a destination to copy it to.
struct Copy {
    directory: TempDir,
}

impl Copy {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        fs::create_dir(directory.path().join("in")).unwrap();
        fs::create_dir(directory.path().join("out")).unwrap();
        fs::write(directory.path().join("in/clean.txt"), "nothing to find\n").unwrap();
        Self { directory }
    }

    fn write_source(&self, name: &str, content: &[u8]) {
        fs::write(self.directory.path().join("in").join(name), content).unwrap();
    }

    fn run(&self, extra_args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_redacter"))
            .current_dir(self.directory.path())
            .args(["cp", "in", "out/", "-d", "local-rules"])
            .args(extra_args)
            .output()
            .unwrap()
    }
}

/// Text that turns out not to be text only after the window `--unsupported-as-text` checks up
/// front, so the file fails while it is being copied.
fn text_with_a_late_nul_byte() -> Vec<u8> {
    let mut content = Vec::new();
    while content.len() < 5 * 1024 * 1024 {
        writeln!(content, "mail ana@example.com").unwrap();
    }
    content.extend_from_slice(b"\0binary tail\n");
    content
}

#[test]
fn a_copy_exits_with_1_when_a_file_fails() {
    let copy = Copy::new();
    copy.write_source("late.dat", &text_with_a_late_nul_byte());

    let output = copy.run(&["--unsupported-as-text"]);

    assert_eq!(
        output.status.code(),
        Some(1),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
}

#[test]
fn a_copy_exits_with_0_when_a_file_is_skipped_as_unsupported() {
    let copy = Copy::new();
    copy.write_source("blob.bin", &[0, 1, 2, 3]);

    let output = copy.run(&[]);

    assert_eq!(
        output.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(!copy.directory.path().join("out/blob.bin").exists());
}
