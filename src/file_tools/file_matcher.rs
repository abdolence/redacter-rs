use crate::file_systems::FileSystemRef;
use rvstruct::ValueStruct;

#[derive(Debug, Clone)]
pub struct FileMatcher {
    pub filename_matcher: Option<globset::GlobMatcher>,
    pub max_size_limit: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FileMatcherResult {
    Matched,
    SkippedDueToSize,
    SkippedDueToName,
}

impl FileMatcher {
    pub fn new(
        filename_matcher: Option<globset::GlobMatcher>,
        max_size_limit: Option<u64>,
    ) -> Self {
        FileMatcher {
            filename_matcher,
            max_size_limit,
        }
    }

    pub fn matches(&self, file_ref: &FileSystemRef) -> FileMatcherResult {
        if let Some(max_size_limit) = self.max_size_limit {
            if let Some(file_size) = file_ref.file_size {
                if file_size > max_size_limit {
                    return FileMatcherResult::SkippedDueToSize;
                }
            }
        }

        if let Some(filename_matcher) = &self.filename_matcher {
            if !filename_matcher.is_match(file_ref.relative_path.value().as_str()) {
                return FileMatcherResult::SkippedDueToName;
            }
        }

        FileMatcherResult::Matched
    }
}

#[allow(unused_imports)]
mod tests {
    use super::*;
    use crate::file_systems::*;
    use mime::Mime;
    use std::str::FromStr;

    #[test]
    fn test_file_matcher() {
        let file_matcher = FileMatcher::new(
            Some(globset::Glob::new("*.txt").unwrap().compile_matcher()),
            Some(100),
        );

        let file_ref = FileSystemRef {
            relative_path: RelativeFilePath("test.txt".to_string()),
            media_type: Some(Mime::from_str("text/plain").unwrap()),
            file_size: Some(50),
        };

        assert_eq!(file_matcher.matches(&file_ref), FileMatcherResult::Matched);

        let file_ref = FileSystemRef {
            relative_path: RelativeFilePath("test.txt".to_string()),
            media_type: Some(Mime::from_str("text/plain").unwrap()),
            file_size: Some(150),
        };

        assert_eq!(
            file_matcher.matches(&file_ref),
            FileMatcherResult::SkippedDueToSize
        );

        let file_ref = FileSystemRef {
            relative_path: RelativeFilePath("test.md".to_string()),
            media_type: Some(Mime::from_str("text/plain").unwrap()),
            file_size: Some(50),
        };

        assert_eq!(
            file_matcher.matches(&file_ref),
            FileMatcherResult::SkippedDueToName
        );
    }
}
