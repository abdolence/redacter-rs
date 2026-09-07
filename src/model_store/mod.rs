//! Finds the model files a run needs and, with the user's consent, downloads and verifies the
//! ones that are missing. Serves the OCR engine and the local NER redacter.

mod consent;
mod download;
mod error;
mod manifest;

pub use consent::{ConsentSource, DownloadRequest};
// `ByteStream` and `HttpFetcher` join this list in Task 3, with the `ModelStore::new` that
// builds the production fetcher; re-exporting them before anything names them is a warning.
pub use download::{download_file, Fetcher};
pub use error::ModelStoreError;
pub use manifest::{manifest, ModelFile, ModelId, ModelManifest};

use crate::reporter::AppReporter;
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Whether a missing model may be downloaded in this run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownloadPolicy {
    Ask,
    Yes,
    No,
}

/// The directory of one installed model; every file of its manifest lives here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelFiles {
    dir: PathBuf,
}

impl ModelFiles {
    pub fn path(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }
}

/// Where manually placed copies of a model may live besides the managed root, in lookup
/// order. Injected so tests can point it at a temporary directory.
pub type ManualDirs = Box<dyn Fn(&ModelManifest) -> Vec<PathBuf> + Send + Sync>;

pub struct ModelStore<'a> {
    root: PathBuf,
    manual_dirs: ManualDirs,
    policy: DownloadPolicy,
    consent: Box<dyn ConsentSource + 'a>,
    fetcher: Box<dyn Fetcher + 'a>,
    reporter: &'a AppReporter<'a>,
    /// Models resolved in this run, by manifest `dir_name`. The lock only guards the map and
    /// is never held across an `.await`.
    resolved: Mutex<HashMap<&'static str, ModelFiles>>,
}

enum FileState {
    Present,
    Missing,
    Stale(u64),
}

impl<'a> ModelStore<'a> {
    /// Builds a store from its parts. Production code uses `ModelStore::new`; tests inject a
    /// temporary root, a fixed consent answer and canned downloads.
    pub fn with_parts(
        root: PathBuf,
        manual_dirs: ManualDirs,
        policy: DownloadPolicy,
        consent: Box<dyn ConsentSource + 'a>,
        fetcher: Box<dyn Fetcher + 'a>,
        reporter: &'a AppReporter<'a>,
    ) -> Self {
        Self {
            root,
            manual_dirs,
            policy,
            consent,
            fetcher,
            reporter,
            resolved: Mutex::new(HashMap::new()),
        }
    }

    /// Finds or installs a model and returns where its files are.
    pub async fn resolve(&self, id: ModelId) -> Result<ModelFiles, ModelStoreError> {
        self.resolve_manifest(manifest(id)).await
    }

    /// Lookup order: the managed directory (every file present with its manifest size), the
    /// manual directories (every file present, sizes not checked), then a download of the
    /// missing or stale files into the managed directory under the download policy.
    pub async fn resolve_manifest(
        &self,
        manifest: &'static ModelManifest,
    ) -> Result<ModelFiles, ModelStoreError> {
        if let Some(files) = self.cached(manifest.dir_name) {
            return Ok(files);
        }
        let managed = self.root.join(manifest.dir_name);
        let mut wanted: Vec<&'static ModelFile> = Vec::new();
        let mut stale: Option<(PathBuf, u64, u64)> = None;
        for file in manifest.files {
            let path = managed.join(file.name);
            match file_state(&path, file.size)? {
                FileState::Present => {}
                FileState::Missing => wanted.push(file),
                FileState::Stale(actual) => {
                    stale.get_or_insert((path, file.size, actual));
                    wanted.push(file);
                }
            }
        }
        let files = if wanted.is_empty() {
            ModelFiles { dir: managed }
        } else {
            match stale {
                Some((path, expected, actual)) if self.policy == DownloadPolicy::No => {
                    return Err(ModelStoreError::SizeMismatch {
                        location: path.display().to_string(),
                        expected,
                        actual,
                    });
                }
                Some(_) => self.download(manifest, &managed, wanted).await?,
                None => match self.find_manual_dir(manifest) {
                    Some(dir) => ModelFiles { dir },
                    None => self.download(manifest, &managed, wanted).await?,
                },
            }
        };
        self.remember(manifest.dir_name, files.clone());
        Ok(files)
    }

    fn find_manual_dir(&self, manifest: &ModelManifest) -> Option<PathBuf> {
        (self.manual_dirs)(manifest).into_iter().find(|dir| {
            manifest
                .files
                .iter()
                .all(|file| dir.join(file.name).is_file())
        })
    }

    async fn download(
        &self,
        manifest: &'static ModelManifest,
        dir: &Path,
        files: Vec<&'static ModelFile>,
    ) -> Result<ModelFiles, ModelStoreError> {
        let request = DownloadRequest {
            model: manifest,
            dir: dir.to_path_buf(),
            total_bytes: files.iter().map(|file| file.size).sum(),
            files,
        };
        let allowed = match self.policy {
            DownloadPolicy::Yes => true,
            DownloadPolicy::No => false,
            DownloadPolicy::Ask => self.consent.ask(&request)?,
        };
        if !allowed {
            return Err(match self.policy {
                DownloadPolicy::No => ModelStoreError::NotInstalled {
                    model: manifest.dir_name.to_string(),
                    dir: dir.display().to_string(),
                    files: request
                        .files
                        .iter()
                        .map(|file| file.name)
                        .collect::<Vec<_>>()
                        .join(", "),
                },
                _ => ModelStoreError::Declined {
                    model: manifest.dir_name.to_string(),
                },
            });
        }
        for file in &request.files {
            let bar = progress_bar(file)?;
            download_file(self.fetcher.as_ref(), file, dir, &bar).await?;
            bar.finish_and_clear();
            self.reporter
                .report(format!("Downloaded {} to {}", file.name, dir.display()))
                .map_err(|err| ModelStoreError::Report {
                    reason: err.to_string(),
                })?;
        }
        Ok(ModelFiles {
            dir: dir.to_path_buf(),
        })
    }

    fn cached(&self, key: &str) -> Option<ModelFiles> {
        let resolved = self
            .resolved
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        resolved.get(key).cloned()
    }

    fn remember(&self, key: &'static str, files: ModelFiles) {
        let mut resolved = self
            .resolved
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        resolved.insert(key, files);
    }
}

fn file_state(path: &Path, expected: u64) -> Result<FileState, ModelStoreError> {
    match std::fs::metadata(path) {
        Ok(metadata) if metadata.is_file() && metadata.len() == expected => Ok(FileState::Present),
        Ok(metadata) if metadata.is_file() => Ok(FileState::Stale(metadata.len())),
        Ok(_) => Ok(FileState::Missing),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(FileState::Missing),
        Err(source) => Err(ModelStoreError::Io {
            path: path.display().to_string(),
            source,
        }),
    }
}

fn progress_bar(file: &ModelFile) -> Result<ProgressBar, ModelStoreError> {
    let style = ProgressStyle::with_template(
        "{spinner:.green} {msg} [{wide_bar:.green/237}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})",
    )
    .map_err(|err| ModelStoreError::Progress {
        reason: err.to_string(),
    })?;
    let bar = ProgressBar::new(file.size).with_style(style.progress_chars("━>─"));
    bar.set_message(file.name);
    Ok(bar)
}

#[cfg(test)]
mod tests {
    use super::download::test_support::{part_files, FakeFetcher, TEST_FILES};
    use super::manifest::no_legacy_dirs;
    use super::*;
    use console::Term;
    use std::sync::{Arc, Mutex};
    use tempfile::TempDir;

    static TEST_MANIFEST: ModelManifest = ModelManifest {
        dir_name: "test-model",
        needed_by: "A test",
        source: "https://example.invalid/",
        license: "none",
        files: &TEST_FILES,
        legacy_dirs: no_legacy_dirs,
    };

    /// Answers every prompt the same way and records the file names it was asked about. The
    /// record is shared so a test can read it after the store has taken ownership.
    struct FakeConsent {
        answer: bool,
        asked: Arc<Mutex<Vec<Vec<String>>>>,
    }

    impl FakeConsent {
        fn new(answer: bool) -> Self {
            Self {
                answer,
                asked: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn asked(&self) -> Arc<Mutex<Vec<Vec<String>>>> {
            self.asked.clone()
        }
    }

    impl ConsentSource for FakeConsent {
        fn ask(&self, request: &DownloadRequest) -> Result<bool, ModelStoreError> {
            self.asked
                .lock()
                .unwrap()
                .push(request.files.iter().map(|f| f.name.to_string()).collect());
            Ok(self.answer)
        }
    }

    struct Fixture {
        _root: TempDir,
        managed: PathBuf,
        manual: PathBuf,
        term: Term,
    }

    impl Fixture {
        fn new() -> Self {
            let root = TempDir::new().unwrap();
            let managed = root.path().join("models").join("test-model");
            let manual = root.path().join("manual").join("test-model");
            Self {
                _root: root,
                managed,
                manual,
                term: Term::stdout(),
            }
        }

        fn store<'a>(
            &'a self,
            policy: DownloadPolicy,
            consent: FakeConsent,
            fetcher: FakeFetcher,
            reporter: &'a AppReporter<'a>,
        ) -> ModelStore<'a> {
            let manual_root = self.manual.parent().unwrap().to_path_buf();
            ModelStore::with_parts(
                self.managed.parent().unwrap().to_path_buf(),
                Box::new(move |m: &ModelManifest| vec![manual_root.join(m.dir_name)]),
                policy,
                Box::new(consent),
                Box::new(fetcher),
                reporter,
            )
        }

        fn write(&self, dir: &Path, name: &str, bytes: &[u8]) {
            std::fs::create_dir_all(dir).unwrap();
            std::fs::write(dir.join(name), bytes).unwrap();
        }
    }

    #[tokio::test]
    async fn complete_managed_dir_is_a_hit_without_fetching() {
        let fx = Fixture::new();
        fx.write(&fx.managed, "weights.bin", b"hello world");
        fx.write(&fx.managed, "config.json", b"abc");
        let reporter = AppReporter::from(&fx.term);
        let store = fx.store(
            DownloadPolicy::No,
            FakeConsent::new(false),
            FakeFetcher::good(),
            &reporter,
        );
        let files = store.resolve_manifest(&TEST_MANIFEST).await.unwrap();
        assert_eq!(files.dir(), fx.managed);
        assert_eq!(files.path("weights.bin"), fx.managed.join("weights.bin"));
    }

    #[tokio::test]
    async fn manual_dir_is_a_hit_without_a_size_check() {
        let fx = Fixture::new();
        fx.write(&fx.manual, "weights.bin", b"a different size");
        fx.write(&fx.manual, "config.json", b"x");
        let reporter = AppReporter::from(&fx.term);
        let store = fx.store(
            DownloadPolicy::No,
            FakeConsent::new(false),
            FakeFetcher::good(),
            &reporter,
        );
        let files = store.resolve_manifest(&TEST_MANIFEST).await.unwrap();
        assert_eq!(files.dir(), fx.manual);
    }

    #[tokio::test]
    async fn incomplete_manual_dir_is_not_a_hit() {
        let fx = Fixture::new();
        fx.write(&fx.manual, "weights.bin", b"hello world");
        let reporter = AppReporter::from(&fx.term);
        let store = fx.store(
            DownloadPolicy::No,
            FakeConsent::new(false),
            FakeFetcher::good(),
            &reporter,
        );
        let err = store.resolve_manifest(&TEST_MANIFEST).await.unwrap_err();
        assert!(matches!(err, ModelStoreError::NotInstalled { .. }), "{err}");
    }

    #[tokio::test]
    async fn missing_under_no_names_dir_files_and_the_flag() {
        let fx = Fixture::new();
        let reporter = AppReporter::from(&fx.term);
        let fetcher = FakeFetcher::good();
        let store = fx.store(
            DownloadPolicy::No,
            FakeConsent::new(true),
            fetcher,
            &reporter,
        );
        let err = store.resolve_manifest(&TEST_MANIFEST).await.unwrap_err();
        match &err {
            ModelStoreError::NotInstalled { model, dir, files } => {
                assert_eq!(model, "test-model");
                assert_eq!(dir, &fx.managed.display().to_string());
                assert_eq!(files, "weights.bin, config.json");
            }
            other => panic!("expected NotInstalled, got {other}"),
        }
        assert!(err.to_string().contains("--download-models yes"), "{err}");
    }

    #[tokio::test]
    async fn ask_with_declined_consent_is_declined_and_lists_only_missing_files() {
        let fx = Fixture::new();
        fx.write(&fx.managed, "weights.bin", b"hello world");
        let reporter = AppReporter::from(&fx.term);
        let consent = FakeConsent::new(false);
        let asked = consent.asked();
        let fetcher = FakeFetcher::good();
        let fetches = fetcher.counter();
        let store = fx.store(DownloadPolicy::Ask, consent, fetcher, &reporter);
        let err = store.resolve_manifest(&TEST_MANIFEST).await.unwrap_err();
        assert!(
            matches!(err, ModelStoreError::Declined { ref model } if model == "test-model"),
            "{err}"
        );
        assert_eq!(
            *asked.lock().unwrap(),
            vec![vec!["config.json".to_string()]]
        );
        assert_eq!(*fetches.lock().unwrap(), 0);
        assert!(!fx.managed.join("config.json").exists());
    }

    #[tokio::test]
    async fn yes_downloads_missing_files_and_leaves_no_part_files() {
        let fx = Fixture::new();
        let reporter = AppReporter::from(&fx.term);
        let store = fx.store(
            DownloadPolicy::Yes,
            FakeConsent::new(false),
            FakeFetcher::good(),
            &reporter,
        );
        let files = store.resolve_manifest(&TEST_MANIFEST).await.unwrap();
        assert_eq!(files.dir(), fx.managed);
        assert_eq!(
            std::fs::read(fx.managed.join("weights.bin")).unwrap(),
            b"hello world"
        );
        assert_eq!(
            std::fs::read(fx.managed.join("config.json")).unwrap(),
            b"abc"
        );
        assert!(part_files(&fx.managed).is_empty());
    }

    #[tokio::test]
    async fn ask_with_consent_downloads() {
        let fx = Fixture::new();
        let reporter = AppReporter::from(&fx.term);
        let store = fx.store(
            DownloadPolicy::Ask,
            FakeConsent::new(true),
            FakeFetcher::good(),
            &reporter,
        );
        store.resolve_manifest(&TEST_MANIFEST).await.unwrap();
        assert_eq!(
            std::fs::read(fx.managed.join("config.json")).unwrap(),
            b"abc"
        );
    }

    #[tokio::test]
    async fn stale_managed_file_is_redownloaded_under_yes() {
        let fx = Fixture::new();
        fx.write(&fx.managed, "weights.bin", b"stale");
        fx.write(&fx.managed, "config.json", b"abc");
        let reporter = AppReporter::from(&fx.term);
        let store = fx.store(
            DownloadPolicy::Yes,
            FakeConsent::new(false),
            FakeFetcher::good(),
            &reporter,
        );
        store.resolve_manifest(&TEST_MANIFEST).await.unwrap();
        assert_eq!(
            std::fs::read(fx.managed.join("weights.bin")).unwrap(),
            b"hello world"
        );
    }

    #[tokio::test]
    async fn stale_managed_file_under_no_is_a_size_mismatch() {
        let fx = Fixture::new();
        fx.write(&fx.managed, "weights.bin", b"stale");
        fx.write(&fx.managed, "config.json", b"abc");
        let reporter = AppReporter::from(&fx.term);
        let store = fx.store(
            DownloadPolicy::No,
            FakeConsent::new(false),
            FakeFetcher::good(),
            &reporter,
        );
        let err = store.resolve_manifest(&TEST_MANIFEST).await.unwrap_err();
        match err {
            ModelStoreError::SizeMismatch {
                location,
                expected,
                actual,
            } => {
                assert_eq!(
                    location,
                    fx.managed.join("weights.bin").display().to_string()
                );
                assert_eq!((expected, actual), (11, 5));
            }
            other => panic!("expected SizeMismatch, got {other}"),
        }
    }

    #[tokio::test]
    async fn failed_download_leaves_no_final_file() {
        let fx = Fixture::new();
        let reporter = AppReporter::from(&fx.term);
        let fetcher = FakeFetcher::new(&[
            ("https://example.invalid/weights.bin", b"hello worle"),
            ("https://example.invalid/config.json", b"abc"),
        ]);
        let store = fx.store(
            DownloadPolicy::Yes,
            FakeConsent::new(false),
            fetcher,
            &reporter,
        );
        let err = store.resolve_manifest(&TEST_MANIFEST).await.unwrap_err();
        assert!(
            matches!(err, ModelStoreError::ChecksumMismatch { .. }),
            "{err}"
        );
        assert!(!fx.managed.join("weights.bin").exists());
        assert!(part_files(&fx.managed).is_empty());
    }

    #[tokio::test]
    async fn second_resolve_is_served_from_the_cache() {
        let fx = Fixture::new();
        let reporter = AppReporter::from(&fx.term);
        let fetcher = FakeFetcher::good();
        let fetches = fetcher.counter();
        let store = fx.store(
            DownloadPolicy::Yes,
            FakeConsent::new(false),
            fetcher,
            &reporter,
        );
        store.resolve_manifest(&TEST_MANIFEST).await.unwrap();
        assert_eq!(*fetches.lock().unwrap(), 2);
        store.resolve_manifest(&TEST_MANIFEST).await.unwrap();
        assert_eq!(
            *fetches.lock().unwrap(),
            2,
            "the second resolve must not fetch"
        );
    }
}
