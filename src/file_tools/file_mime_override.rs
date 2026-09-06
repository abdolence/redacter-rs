use crate::file_systems::FileSystemRef;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn overrides_media_type_of_matching_sample_fixtures() {
        let mime_override = FileMimeOverride::new(vec![(
            mime::Mime::from_str("text/plain").expect("valid mime"),
            globset::Glob::new("*.csv").expect("valid glob"),
        )]);

        let overridden = mime_override.override_for_file_ref(FileSystemRef {
            relative_path: "customers.csv".into(),
            media_type: Some(mime::TEXT_CSV),
            file_size: Some(290),
        });
        assert_eq!(overridden.media_type, Some(mime::TEXT_PLAIN));
    }

    #[test]
    fn leaves_non_matching_sample_fixtures_unchanged() {
        let mime_override = FileMimeOverride::new(vec![(
            mime::Mime::from_str("text/plain").expect("valid mime"),
            globset::Glob::new("*.md").expect("valid glob"),
        )]);

        for (name, media_type) in [
            ("customer-note.txt", mime::TEXT_PLAIN),
            ("customer.json", mime::APPLICATION_JSON),
            ("customer-profile.html", mime::TEXT_HTML),
            ("customer-form.pdf", mime::APPLICATION_PDF),
        ] {
            let file_ref = FileSystemRef {
                relative_path: name.into(),
                media_type: Some(media_type.clone()),
                file_size: Some(1),
            };
            let overridden = mime_override.override_for_file_ref(file_ref);
            assert_eq!(
                overridden.media_type,
                Some(media_type),
                "{name} should keep its detected media type"
            );
        }
    }
}
