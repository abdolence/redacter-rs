use crate::errors::AppError;
use crate::file_systems::{local_source_directory, FileSystemRef, RelativeFilePath};
use crate::AppResult;
use ignore::gitignore::{Gitignore, GitignoreBuilder};
use rvstruct::ValueStruct;
use std::path::{Path, PathBuf};

/// The name of the ignore file `scan` reads at the root of a local source.
pub const IGNORE_FILE_NAME: &str = ".redacterignore";

/// Which ignore file `scan` reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IgnoreFileChoice {
    /// `.redacterignore` at the root of a local source, when it has one.
    SourceRoot,
    /// This file, for any source.
    Explicit(PathBuf),
    Disabled,
}

impl IgnoreFileChoice {
    /// The rules for a scan of `source`, or `None` when no ignore file applies.
    pub fn load(&self, source: &str) -> AppResult<Option<IgnoreRules>> {
        match self {
            Self::Disabled => Ok(None),
            Self::Explicit(path) => IgnoreRules::from_file(path).map(Some),
            Self::SourceRoot => match local_source_directory(source) {
                Some(directory) => {
                    let path = directory.join(IGNORE_FILE_NAME);
                    if path.is_file() {
                        IgnoreRules::from_file(&path).map(Some)
                    } else {
                        Ok(None)
                    }
                }
                None => Ok(None),
            },
        }
    }
}

/// The patterns of an ignore file in gitignore syntax. They match the path of a file relative
/// to the source root, the path the reports show.
#[derive(Debug, Clone)]
pub struct IgnoreRules(Gitignore);

impl IgnoreRules {
    pub fn from_file(path: &Path) -> AppResult<Self> {
        let text =
            std::fs::read_to_string(path).map_err(|source| AppError::IgnoreFileUnreadable {
                path: path.display().to_string(),
                source,
            })?;
        Self::parse(&text, path)
    }

    fn parse(text: &str, path: &Path) -> AppResult<Self> {
        let mut builder = GitignoreBuilder::new("");
        for (index, pattern) in text.lines().enumerate() {
            builder
                .add_line(None, pattern)
                .map_err(|reason| AppError::InvalidIgnorePattern {
                    path: path.display().to_string(),
                    line: Some(index + 1),
                    reason,
                })?;
        }
        let gitignore = builder
            .build()
            .map_err(|reason| AppError::InvalidIgnorePattern {
                path: path.display().to_string(),
                line: None,
                reason,
            })?;
        Ok(Self(gitignore))
    }

    /// Removes the ignored files from `files` and returns how many it removed.
    pub fn remove_ignored(&self, files: &mut Vec<FileSystemRef>) -> usize {
        let listed = files.len();
        files.retain(|file| !self.is_ignored(&file.relative_path));
        listed - files.len()
    }

    /// Whether the file at `path` is ignored: a pattern matches it or one of its directories,
    /// and no later `!` pattern brings it back.
    pub fn is_ignored(&self, path: &RelativeFilePath) -> bool {
        self.0
            .matched_path_or_any_parents(path.value().trim_start_matches('/'), false)
            .is_ignore()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rules(patterns: &str) -> IgnoreRules {
        IgnoreRules::parse(patterns, Path::new(IGNORE_FILE_NAME)).unwrap()
    }

    fn ignored(rules: &IgnoreRules, path: &str) -> bool {
        rules.is_ignored(&RelativeFilePath(path.to_string()))
    }

    #[test]
    fn patterns_follow_gitignore_semantics_on_the_relative_path() {
        let rules = rules(
            "# mock credentials\n\
             *.mock\n\
             /root-only.txt\n\
             fixtures/\n\
             docs/**/draft.md\n\
             logs/*\n\
             !logs/keep.txt\n",
        );

        assert!(ignored(&rules, "a.mock"));
        assert!(ignored(&rules, "deep/er/a.mock"));
        assert!(ignored(&rules, "root-only.txt"));
        assert!(!ignored(&rules, "sub/root-only.txt"));
        assert!(ignored(&rules, "fixtures/key.txt"));
        assert!(ignored(&rules, "src/fixtures/nested/key.txt"));
        assert!(!ignored(&rules, "fixtures.txt"));
        assert!(ignored(&rules, "docs/a/b/draft.md"));
        assert!(ignored(&rules, "docs/draft.md"));
        assert!(!ignored(&rules, "docs/final.md"));
        assert!(ignored(&rules, "logs/today.txt"));
        assert!(!ignored(&rules, "logs/keep.txt"));
        assert!(!ignored(&rules, "src/main.rs"));
    }

    #[test]
    fn a_remote_style_path_with_a_leading_slash_is_matched_from_the_source_root() {
        let rules = rules("/root-only.txt\nfixtures/\n");

        assert!(ignored(&rules, "/root-only.txt"));
        assert!(ignored(&rules, "/fixtures/key.txt"));
    }

    #[test]
    fn a_malformed_pattern_is_a_config_error_naming_the_file_and_line() {
        let error =
            IgnoreRules::parse("*.ok\n\n{unclosed\n", Path::new("ci/.redacterignore")).unwrap_err();

        assert!(matches!(
            error,
            AppError::InvalidIgnorePattern { ref path, line: Some(3), .. } if path == "ci/.redacterignore"
        ));
    }

    #[test]
    fn the_source_root_file_is_read_only_for_a_local_directory() {
        let dir = tempfile::TempDir::with_prefix("ignore_rules_pickup").unwrap();
        std::fs::write(dir.path().join(IGNORE_FILE_NAME), "*.mock\n").unwrap();
        let source = format!("{}/", dir.path().display());

        let picked_up = IgnoreFileChoice::SourceRoot.load(&source).unwrap().unwrap();
        assert!(ignored(&picked_up, "a.mock"));
        assert!(IgnoreFileChoice::Disabled.load(&source).unwrap().is_none());
        assert!(IgnoreFileChoice::SourceRoot
            .load("gs://bucket/dir/")
            .unwrap()
            .is_none());
        let empty = tempfile::TempDir::with_prefix("ignore_rules_none").unwrap();
        assert!(IgnoreFileChoice::SourceRoot
            .load(&format!("{}/", empty.path().display()))
            .unwrap()
            .is_none());
    }

    #[test]
    fn an_explicit_file_applies_to_a_remote_source_and_a_missing_one_is_an_error() {
        let dir = tempfile::TempDir::with_prefix("ignore_rules_explicit").unwrap();
        let file = dir.path().join("ci-ignore");
        std::fs::write(&file, "fixtures/\n").unwrap();

        let rules = IgnoreFileChoice::Explicit(file)
            .load("gs://bucket/dir/")
            .unwrap()
            .unwrap();
        assert!(ignored(&rules, "fixtures/key.txt"));
        assert!(matches!(
            IgnoreFileChoice::Explicit(dir.path().join("missing")).load("gs://bucket/dir/"),
            Err(AppError::IgnoreFileUnreadable { .. })
        ));
    }
}
