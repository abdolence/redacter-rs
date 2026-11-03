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
    base: LocatorBase,
}

#[derive(Debug, Clone)]
enum LocatorBase {
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
                    working_dir: lexically_normal(working_dir),
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
            } => format!(
                "file:///{}",
                encoded_path(&local_path(source, *is_dir, working_dir, path))
            ),
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
            } => local_path(source, *is_dir, working_dir, path)
                .to_string_lossy()
                .into_owned(),
            LocatorBase::Remote { .. } => self.full_uri(path),
        }
    }

    /// The URI of a local file below the working directory, relative to it. `None` for a file
    /// outside it and for a file of a remote source, which only a full URI names.
    pub fn relative_uri(&self, path: &str) -> Option<String> {
        let LocatorBase::Local {
            source,
            is_dir,
            working_dir,
        } = &self.base
        else {
            return None;
        };
        local_path(source, *is_dir, working_dir, path)
            .strip_prefix(working_dir)
            .ok()
            .map(encoded_path)
    }
}

/// The path of a file of a local source, made absolute by `working_dir` and free of `.` and `..`
/// components without asking the file system.
fn local_path(source: &Path, is_dir: bool, working_dir: &Path, path: &str) -> PathBuf {
    let file = working_dir.join(source);
    lexically_normal(&if is_dir { file.join(path) } else { file })
}

fn lexically_normal(path: &Path) -> PathBuf {
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

fn encoded_path(path: &Path) -> String {
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
