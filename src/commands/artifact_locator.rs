//! Where the files of a source are, as the report writers name them.

use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
use std::path::{Component, Path, PathBuf};

/// Characters that cannot appear in a URI path segment as they are.
const PATH_SEGMENT: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'<')
    .add(b'>')
    .add(b'?')
    .add(b'[')
    .add(b'\\')
    .add(b']')
    .add(b'^')
    .add(b'`')
    .add(b'{')
    .add(b'|')
    .add(b'}');

/// Where the files of a source are, for the report writers.
///
/// A file is named by its full path or its full URI, which do not depend on the working
/// directory, and each writer may also ask for the form it needs.
#[derive(Debug, Clone)]
pub struct ArtifactLocator {
    pub(super) base: LocatorBase,
}

#[derive(Debug, Clone)]
pub(super) enum LocatorBase {
    Local {
        source: PathBuf,
        is_dir: bool,
        working_dir: PathBuf,
    },
    Remote {
        /// Without a trailing slash.
        source: String,
        /// Whether the paths of the files are relative to the source. The path of the one
        /// object of a `gs://` or `s3://` source is its file name only, and the source names it.
        joins_paths: bool,
    },
}

impl ArtifactLocator {
    /// `source` is the argument the user gave. A local directory is told as the local file
    /// system tells it: a trailing `/` or a directory on disk.
    pub fn new(source: &str, working_dir: &Path) -> Self {
        let base = match source.strip_prefix("file://") {
            Some(path) => Some(path),
            None if !source.contains("://") => Some(source),
            None => None,
        };
        Self {
            base: match base {
                Some(path) => LocatorBase::Local {
                    is_dir: path.ends_with('/') || Path::new(path).is_dir(),
                    source: PathBuf::from(path),
                    working_dir: working_dir.to_path_buf(),
                },
                None => LocatorBase::Remote {
                    joins_paths: source.starts_with("zip://") || source.ends_with('/'),
                    source: source.trim_end_matches('/').to_string(),
                },
            },
        }
    }

    /// The URI of a file that names it whatever the working directory is: a `file:` URI for a
    /// local file, the URI of the source for a remote one.
    pub fn full_uri(&self, path: &str) -> String {
        match &self.base {
            LocatorBase::Local {
                source,
                is_dir,
                working_dir,
            } => file_uri(working_dir, &local_file(source, *is_dir, path)),
            LocatorBase::Remote {
                source,
                joins_paths,
            } => {
                if *joins_paths {
                    format!("{source}/{}", encoded_path(Path::new(path)))
                } else {
                    source.clone()
                }
            }
        }
    }

    /// The full path of a file that names it whatever the working directory is: the absolute
    /// path of a local file, in the notation of the system, and the URI of a remote one.
    pub fn full_path(&self, path: &str) -> String {
        match &self.base {
            LocatorBase::Local {
                source,
                is_dir,
                working_dir,
            } => lexically_normal(&working_dir.join(local_file(source, *is_dir, path)))
                .to_string_lossy()
                .into_owned(),
            LocatorBase::Remote { .. } => self.full_uri(path),
        }
    }
}

pub(super) fn local_file(source: &Path, is_dir: bool, path: &str) -> PathBuf {
    if is_dir {
        source.join(path)
    } else {
        source.to_path_buf()
    }
}

/// The `file:` URI of `path`, which may be relative to `working_dir`.
pub(super) fn file_uri(working_dir: &Path, path: &Path) -> String {
    format!(
        "file:///{}",
        encoded_path(&lexically_normal(&working_dir.join(path)))
    )
}

pub(super) fn lexically_normal(path: &Path) -> PathBuf {
    let mut normal = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir if normal.file_name().is_some() => {
                normal.pop();
            }
            other => normal.push(other),
        }
    }
    normal
}

pub(super) fn encoded_path(path: &Path) -> String {
    path.components()
        .filter_map(|component| match component {
            Component::Normal(segment) => {
                Some(utf8_percent_encode(&segment.to_string_lossy(), PATH_SEGMENT).to_string())
            }
            Component::ParentDir => Some("..".to_string()),
            Component::Prefix(prefix) => Some(prefix.as_os_str().to_string_lossy().to_string()),
            Component::RootDir | Component::CurDir => None,
        })
        .collect::<Vec<_>>()
        .join("/")
}
