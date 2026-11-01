use crate::errors::AppError;
use crate::file_systems::RelativeFilePath;
use crate::AppResult;
use ignore::gitignore::{Gitignore, GitignoreBuilder};
use rvstruct::ValueStruct;
use std::path::{Path, PathBuf};

/// The name of the ignore file `scan` reads at the root of a local source.
pub const IGNORE_FILE_NAME: &str = ".redacterignore";

/// Which ignore file `scan` reads.
#[derive(Debug, Clone)]
pub enum IgnoreFileChoice {
    /// `.redacterignore` at the root of a local source, when it has one.
    SourceRoot,
    /// This file, for any source.
    Explicit(PathBuf),
    Disabled,
}

impl IgnoreFileChoice {
    /// The rules for a scan whose source is the local directory `source_directory`, or `None`
    /// for any other source. `None` is also the result when no ignore file applies.
    pub fn load(&self, source_directory: Option<&Path>) -> AppResult<Option<IgnoreRules>> {
        match self {
            Self::Disabled => Ok(None),
            Self::Explicit(path) => IgnoreRules::from_file(path).map(Some),
            Self::SourceRoot => {
                let path = match source_directory {
                    Some(directory) => directory.join(IGNORE_FILE_NAME),
                    None => return Ok(None),
                };
                if path.is_file() {
                    IgnoreRules::from_source_root_file(&path).map(Some)
                } else {
                    Ok(None)
                }
            }
        }
    }
}

/// The patterns of an ignore file in gitignore syntax. They match the path of a file relative
/// to the source root, the path the reports show.
///
/// A `!` pattern brings back a file even when a pattern ignores one of its directories, which
/// git does not.
#[derive(Debug, Clone)]
pub struct IgnoreRules(Gitignore);

impl IgnoreRules {
    /// The rules of the ignore file at `path`.
    fn from_file(path: &Path) -> AppResult<Self> {
        Self::build(path, None)
    }

    /// The rules of the `.redacterignore` at the root of the source it applies to. The file is
    /// ignored too, since it is never a file to scan.
    fn from_source_root_file(path: &Path) -> AppResult<Self> {
        Self::build(path, Some(&format!("/{IGNORE_FILE_NAME}")))
    }

    fn build(path: &Path, implicit_pattern: Option<&str>) -> AppResult<Self> {
        let mut builder = GitignoreBuilder::new("");
        if let Some(pattern) = implicit_pattern {
            builder
                .add_line(None, pattern)
                .map_err(|source| AppError::InvalidIgnoreFile { source })?;
        }
        if let Some(source) = builder.add(path) {
            return Err(AppError::InvalidIgnoreFile { source });
        }
        let gitignore = builder
            .build()
            .map_err(|source| AppError::InvalidIgnoreFile { source })?;
        Ok(Self(gitignore))
    }

    /// Whether the file at `path` is ignored: a pattern matches it or one of its directories,
    /// and no later `!` pattern brings it back.
    pub fn ignores_file(&self, path: &RelativeFilePath) -> bool {
        self.0
            .matched_path_or_any_parents(Self::normalised(path), false)
            .is_ignore()
    }

    /// Whether every file under the directory at `path` is ignored, so a walk need not enter it.
    /// Never true while a `!` pattern exists, since one can bring back a file under it.
    pub fn ignores_directory(&self, path: &RelativeFilePath) -> bool {
        self.0.num_whitelists() == 0
            && self
                .0
                .matched_path_or_any_parents(Self::normalised(path), true)
                .is_ignore()
    }

    /// `path` with `/` as its only separator and no leading separator: remote listings and
    /// Windows paths use `\`, and a path with a root is not matched against the patterns.
    fn normalised(path: &RelativeFilePath) -> String {
        path.value()
            .replace('\\', "/")
            .trim_start_matches('/')
            .to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rules_from(text: &str) -> AppResult<IgnoreRules> {
        let dir = tempfile::TempDir::with_prefix("ignore_rules").unwrap();
        let file = dir.path().join(IGNORE_FILE_NAME);
        std::fs::write(&file, text).unwrap();
        IgnoreRules::from_file(&file)
    }

    fn rules(text: &str) -> IgnoreRules {
        rules_from(text).unwrap()
    }

    fn path(path: &str) -> RelativeFilePath {
        RelativeFilePath(path.to_string())
    }

    fn ignores(rules: &IgnoreRules, file: &str) -> bool {
        rules.ignores_file(&path(file))
    }

    #[test]
    fn patterns_follow_gitignore_semantics_on_the_relative_path() {
        let rules = rules(
            "# mock credentials\n\
             *.mock\n\
             /root-only.txt\n\
             /anchored/*\n\
             fixtures/\n\
             logs/*\n\
             !logs/keep.txt\n",
        );

        assert!(ignores(&rules, "deep/er/a.mock"));
        assert!(ignores(&rules, "root-only.txt"));
        assert!(!ignores(&rules, "sub/root-only.txt"));
        assert!(ignores(&rules, "anchored/a.txt"));
        assert!(!ignores(&rules, "sub/anchored/a.txt"));
        assert!(ignores(&rules, "src/fixtures/nested/key.txt"));
        assert!(!ignores(&rules, "fixtures.txt"));
        assert!(ignores(&rules, "logs/today.txt"));
        assert!(!ignores(&rules, "logs/keep.txt"));
    }

    #[test]
    fn a_path_is_matched_whatever_separator_and_prefix_a_listing_gives_it() {
        let rules = rules("/root-only.txt\nfixtures/\n");

        assert!(ignores(&rules, "/root-only.txt"));
        assert!(ignores(&rules, "/fixtures/key.txt"));
        assert!(ignores(&rules, "fixtures\\key.txt"));
        assert!(ignores(&rules, "\\fixtures\\key.txt"));
        assert!(!ignores(&rules, "sub\\root-only.txt"));
    }

    #[test]
    fn a_byte_order_mark_does_not_disable_the_first_pattern() {
        let rules = rules("\u{feff}mocks/\n*.mock\n");

        assert!(ignores(&rules, "mocks/key.txt"));
        assert!(ignores(&rules, "a.mock"));
    }

    #[test]
    fn a_directory_is_skipped_only_while_no_pattern_can_bring_a_file_back() {
        let plain = rules("fixtures/\n");
        let with_negation = rules("fixtures/\n!fixtures/keep.txt\n");

        assert!(plain.ignores_directory(&path("src/fixtures")));
        assert!(!plain.ignores_directory(&path("src")));
        assert!(!with_negation.ignores_directory(&path("fixtures")));
        assert!(ignores(&with_negation, "fixtures/other.txt"));
        assert!(!ignores(&with_negation, "fixtures/keep.txt"));
    }

    #[test]
    fn a_malformed_pattern_is_an_error() {
        assert!(matches!(
            rules_from("*.ok\n\n{unclosed\n"),
            Err(AppError::InvalidIgnoreFile { .. })
        ));
    }

    #[test]
    fn the_source_root_file_is_read_for_a_local_directory_and_lists_itself() {
        let dir = tempfile::TempDir::with_prefix("ignore_rules_pickup").unwrap();
        std::fs::write(dir.path().join(IGNORE_FILE_NAME), "*.mock\n").unwrap();

        let picked_up = IgnoreFileChoice::SourceRoot
            .load(Some(dir.path()))
            .unwrap()
            .unwrap();
        assert!(ignores(&picked_up, "a.mock"));
        assert!(ignores(&picked_up, IGNORE_FILE_NAME));
        assert!(!ignores(&picked_up, &format!("sub/{IGNORE_FILE_NAME}")));
        assert!(IgnoreFileChoice::Disabled
            .load(Some(dir.path()))
            .unwrap()
            .is_none());
        assert!(IgnoreFileChoice::SourceRoot.load(None).unwrap().is_none());
        let empty = tempfile::TempDir::with_prefix("ignore_rules_none").unwrap();
        assert!(IgnoreFileChoice::SourceRoot
            .load(Some(empty.path()))
            .unwrap()
            .is_none());
    }

    #[test]
    fn an_explicit_file_applies_to_any_source_and_a_missing_one_is_an_error() {
        let dir = tempfile::TempDir::with_prefix("ignore_rules_explicit").unwrap();
        let file = dir.path().join("ci-ignore");
        std::fs::write(&file, "fixtures/\n").unwrap();

        let rules = IgnoreFileChoice::Explicit(file)
            .load(None)
            .unwrap()
            .unwrap();
        assert!(ignores(&rules, "fixtures/key.txt"));
        assert!(!ignores(&rules, IGNORE_FILE_NAME));
        assert!(matches!(
            IgnoreFileChoice::Explicit(dir.path().join("missing")).load(None),
            Err(AppError::InvalidIgnoreFile { .. })
        ));
    }
}
