use crate::filesystems::FileSystemRef;
use rvstruct::ValueStruct;

#[derive(Debug, Clone)]
pub struct FileMimeOverride {
    mime_override: Vec<(mime::Mime, globset::GlobMatcher)>,
}

impl FileMimeOverride {
    pub fn new(mime_override: Vec<(mime::Mime, globset::Glob)>) -> Self {
        Self {
            mime_override: mime_override
                .into_iter()
                .map(|(set_mime, glob)| (set_mime, glob.compile_matcher()))
                .collect(),
        }
    }

    pub fn override_for_file_ref(&self, file_ref: FileSystemRef) -> FileSystemRef {
        match self
            .mime_override
            .iter()
            .find(|(_, matcher)| matcher.is_match(file_ref.relative_path.value().as_str()))
        {
            Some((set_mime, _)) => FileSystemRef {
                media_type: Some(set_mime.clone()),
                ..file_ref
            },
            None => file_ref,
        }
    }
}
