//! Where the addons live: the OCR and local redacter models and the Pdfium library. Commands
//! only look them up; `redacter addons install` is the one place that downloads, and it
//! verifies every file before it takes its final name.

// A build without any addon feature still has the `addons` command, but nothing else in it
// looks an addon up.
#![cfg_attr(
    not(any(
        feature = "ocr",
        feature = "pdf-render",
        feature = "local-ner",
        feature = "local-gliner",
        feature = "local-credential-ml"
    )),
    allow(dead_code)
)]

mod download;
mod error;
mod manifest;
#[cfg(any(feature = "pdf-render", test))]
mod pdfium;

#[cfg(test)]
pub(crate) use download::test_support;
pub use download::{download_file, Fetcher, HttpFetcher};
pub use error::AddonStoreError;
pub use manifest::{Addon, AddonFile, AddonManifest};

use indicatif::{ProgressBar, ProgressStyle};
use std::path::{Path, PathBuf};

/// The directory of one installed addon; every file of its manifest lives here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddonFiles {
    dir: PathBuf,
}

impl AddonFiles {
    pub fn dir(&self) -> &Path {
        &self.dir
    }
}

/// `--addons-dir`, or `REDACTER_ADDONS_DIR`; without either, the platform cache directory.
#[derive(Debug, Clone, Default)]
pub struct AddonStoreOptions {
    pub addons_dir: Option<PathBuf>,
}

/// What `addons install` did with one addon.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Installation {
    /// Every file was already in this directory and matched its digest.
    Found(PathBuf),
    /// The missing or damaged files were downloaded into this directory.
    Downloaded(PathBuf),
}

pub struct AddonStore {
    /// `None` on a system with no cache directory and no `--addons-dir`: lookups still find
    /// manual copies, and only an install fails for the lack of it.
    root: Option<PathBuf>,
    /// `<exe dir>/addons`, where addons may be bundled next to the binary.
    bundled_root: Option<PathBuf>,
}

/// How far a file in the store is checked against its pin.
#[derive(Clone, Copy)]
enum Verification {
    Size,
    SizeAndDigest,
}

enum FileState {
    Verified,
    Missing,
    Damaged,
}

impl AddonStore {
    pub fn new(options: &AddonStoreOptions) -> Self {
        Self {
            root: options
                .addons_dir
                .clone()
                .or_else(|| dirs::cache_dir().map(|cache| cache.join("redacter").join("addons"))),
            bundled_root: std::env::current_exe()
                .ok()
                .and_then(|exe| exe.parent().map(|dir| dir.join("addons"))),
        }
    }

    #[cfg(test)]
    pub fn with_parts(root: Option<PathBuf>, bundled_root: Option<PathBuf>) -> Self {
        Self { root, bundled_root }
    }

    /// Finds an installed addon and never downloads. Lookup order: the addons root, where
    /// every file must match its pinned size (and its SHA-256 for native code); then, for
    /// anything but native code, the manual directories, where every file only has to exist.
    /// An addon directory in the addons root that holds some of its files, or any with other
    /// contents than pinned, is `Damaged` and never passed over for a manual copy. Native
    /// code is never returned from a manual directory; the Pdfium converter searches the
    /// tool's own directories by hand itself, outside this store.
    pub fn find(&self, addon: Addon) -> Result<AddonFiles, AddonStoreError> {
        let manifest = addon
            .manifest()
            .ok_or(AddonStoreError::NotAvailableForPlatform { addon })?;
        self.find_manifest(addon, manifest)
    }

    /// Downloads the files of an addon that are missing in the addons root or differ from
    /// their pinned SHA-256 there.
    pub async fn install(
        &self,
        addon: Addon,
        fetcher: &dyn Fetcher,
    ) -> Result<Installation, AddonStoreError> {
        let manifest = addon
            .manifest()
            .ok_or(AddonStoreError::NotAvailableForPlatform { addon })?;
        self.install_manifest(manifest, fetcher).await
    }

    fn find_manifest(
        &self,
        addon: Addon,
        manifest: &AddonManifest,
    ) -> Result<AddonFiles, AddonStoreError> {
        if let Some(root) = &self.root {
            let dir = root.join(manifest.dir_name);
            if manifest.is_installed_in(addon, &dir)? {
                return Ok(AddonFiles { dir });
            }
        }
        if manifest.native_code {
            return Err(AddonStoreError::NotInstalled { addon });
        }
        let manual = self.manual_dirs(manifest).into_iter().find(|dir| {
            manifest
                .files
                .iter()
                .all(|file| dir.join(file.name).is_file())
        });
        match manual {
            Some(dir) => Ok(AddonFiles { dir }),
            None => Err(AddonStoreError::NotInstalled { addon }),
        }
    }

    /// Where a manual copy of an addon may live besides the addons root, in lookup order:
    /// next to the executable, then the addon's own legacy directories.
    fn manual_dirs(&self, manifest: &AddonManifest) -> Vec<PathBuf> {
        self.bundled_root
            .iter()
            .map(|bundled| bundled.join(manifest.dir_name))
            .chain((manifest.legacy_dirs)())
            .collect()
    }

    async fn install_manifest(
        &self,
        manifest: &AddonManifest,
        fetcher: &dyn Fetcher,
    ) -> Result<Installation, AddonStoreError> {
        let root = self.root.as_ref().ok_or(AddonStoreError::NoCacheDir)?;
        let dir = root.join(manifest.dir_name);
        let mut downloaded = false;
        for file in manifest.files {
            if let FileState::Verified = file.state_in(&dir, Verification::SizeAndDigest)? {
                continue;
            }
            let bar = progress_bar(file)?;
            let result = download_file(fetcher, file, &dir, &bar).await;
            bar.finish_and_clear();
            result?;
            downloaded = true;
        }
        Ok(if downloaded {
            Installation::Downloaded(dir)
        } else {
            Installation::Found(dir)
        })
    }
}

impl AddonManifest {
    fn verification(&self) -> Verification {
        if self.native_code {
            Verification::SizeAndDigest
        } else {
            Verification::Size
        }
    }

    /// Whether every file is in `dir` as pinned; `false` when none of them is. A file that
    /// differs, or a directory that holds only some of the files, is an error rather than a
    /// miss, so that a damaged install is never passed over for an unverified manual copy.
    fn is_installed_in(&self, addon: Addon, dir: &Path) -> Result<bool, AddonStoreError> {
        let damaged = |file: &AddonFile| AddonStoreError::Damaged {
            addon,
            path: dir.join(file.name).display().to_string(),
        };
        let mut first_missing = None;
        let mut present = 0;
        for file in self.files {
            match file.state_in(dir, self.verification())? {
                FileState::Verified => present += 1,
                FileState::Missing => {
                    first_missing.get_or_insert(file);
                }
                FileState::Damaged => return Err(damaged(file)),
            }
        }
        match first_missing {
            None => Ok(true),
            Some(_) if present == 0 => Ok(false),
            Some(file) => Err(damaged(file)),
        }
    }
}

impl AddonFile {
    /// Checks the copy of this file in `dir` against its pin.
    fn state_in(
        &self,
        dir: &Path,
        verification: Verification,
    ) -> Result<FileState, AddonStoreError> {
        let path = dir.join(self.name);
        let metadata = match std::fs::metadata(&path) {
            Ok(metadata) if metadata.is_file() => metadata,
            Ok(_) => return Ok(FileState::Missing),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Ok(FileState::Missing)
            }
            Err(source) => return Err(AddonStoreError::io(&path, source)),
        };
        if metadata.len() != self.size
            || (matches!(verification, Verification::SizeAndDigest)
                && download::sha256_of_file(&path)? != self.sha256)
        {
            return Ok(FileState::Damaged);
        }
        Ok(FileState::Verified)
    }
}

fn progress_bar(file: &AddonFile) -> Result<ProgressBar, AddonStoreError> {
    let style = ProgressStyle::with_template(
        "{spinner:.green} {msg} [{wide_bar:.green/237}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})",
    )
    .map_err(|err| AddonStoreError::Progress {
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
    use tempfile::TempDir;

    static TEST_MANIFEST: AddonManifest = AddonManifest {
        dir_name: "test-model",
        files: &TEST_FILES,
        legacy_dirs: no_legacy_dirs,
        native_code: false,
    };

    /// The tests' stand-in for whichever addon `TEST_MANIFEST` is installed as.
    const TEST_ADDON: Addon = Addon::LocalGliner;

    struct Fixture {
        _root: TempDir,
        managed: PathBuf,
        manual: PathBuf,
    }

    impl Fixture {
        fn new() -> Self {
            let root = TempDir::new().unwrap();
            let managed = root.path().join("addons").join("test-model");
            let manual = root.path().join("manual").join("test-model");
            Self {
                _root: root,
                managed,
                manual,
            }
        }

        fn store(&self) -> AddonStore {
            self.store_rooted(Some(self.managed.parent().unwrap().to_path_buf()))
        }

        fn store_rooted(&self, root: Option<PathBuf>) -> AddonStore {
            AddonStore::with_parts(root, Some(self.manual.parent().unwrap().to_path_buf()))
        }

        fn write(&self, dir: &Path, name: &str, bytes: &[u8]) {
            std::fs::create_dir_all(dir).unwrap();
            std::fs::write(dir.join(name), bytes).unwrap();
        }

        fn write_installed(&self) {
            self.write(&self.managed, "weights.bin", b"hello world");
            self.write(&self.managed, "config.json", b"abc");
        }
    }

    #[test]
    fn an_installed_addon_is_found_in_the_addons_root() {
        let fx = Fixture::new();
        fx.write_installed();
        let files = fx
            .store()
            .find_manifest(TEST_ADDON, &TEST_MANIFEST)
            .unwrap();
        assert_eq!(files.dir(), fx.managed);
    }

    #[test]
    fn manual_dir_is_a_hit_without_a_size_check() {
        let fx = Fixture::new();
        fx.write(&fx.manual, "weights.bin", b"a different size");
        fx.write(&fx.manual, "config.json", b"x");
        let files = fx
            .store()
            .find_manifest(TEST_ADDON, &TEST_MANIFEST)
            .unwrap();
        assert_eq!(files.dir(), fx.manual);
    }

    #[test]
    fn a_missing_addon_is_not_installed_and_nothing_is_written() {
        let fx = Fixture::new();
        fx.write(&fx.manual, "weights.bin", b"hello world");
        let err = fx
            .store()
            .find_manifest(TEST_ADDON, &TEST_MANIFEST)
            .unwrap_err();
        assert!(
            matches!(err, AddonStoreError::NotInstalled { addon } if addon == TEST_ADDON),
            "{err}"
        );
        assert!(!fx.managed.exists());
    }

    #[test]
    fn a_damaged_file_in_the_addons_root_is_an_error_not_a_miss() {
        let fx = Fixture::new();
        fx.write(&fx.managed, "weights.bin", b"stale");
        fx.write(&fx.managed, "config.json", b"abc");
        fx.write(&fx.manual, "weights.bin", b"hello world");
        fx.write(&fx.manual, "config.json", b"abc");
        let err = fx
            .store()
            .find_manifest(TEST_ADDON, &TEST_MANIFEST)
            .unwrap_err();
        match err {
            AddonStoreError::Damaged { addon, path } => {
                assert_eq!(addon, TEST_ADDON);
                assert_eq!(path, fx.managed.join("weights.bin").display().to_string());
            }
            other => panic!("expected Damaged, got {other}"),
        }
    }

    /// Pdfium is native code the process loads and runs, so a copy of the right size with
    /// other bytes in it must never be handed to the loader.
    #[test]
    fn a_pdfium_copy_with_other_bytes_is_not_found() {
        let Some(manifest) = Addon::Pdf.manifest() else {
            return;
        };
        let fx = Fixture::new();
        let root = fx.managed.parent().unwrap().to_path_buf();
        let library = &manifest.files[0];
        fx.write(
            &root.join(manifest.dir_name),
            library.name,
            &vec![0; library.size as usize],
        );
        let err = fx.store_rooted(Some(root)).find(Addon::Pdf).unwrap_err();
        assert!(
            matches!(
                err,
                AddonStoreError::Damaged {
                    addon: Addon::Pdf,
                    ..
                }
            ),
            "{err}"
        );
    }

    /// The store only ever returns native code from the verified copy in the addons root: a
    /// manual copy is never looked at, whatever its size.
    #[test]
    fn a_manual_copy_of_native_code_is_not_found() {
        static NATIVE_MANIFEST: AddonManifest = AddonManifest {
            dir_name: "test-model",
            files: &TEST_FILES,
            legacy_dirs: no_legacy_dirs,
            native_code: true,
        };
        let fx = Fixture::new();
        fx.write(&fx.manual, "weights.bin", b"hello world");
        fx.write(&fx.manual, "config.json", b"abc");
        let err = fx
            .store()
            .find_manifest(TEST_ADDON, &NATIVE_MANIFEST)
            .unwrap_err();
        assert!(
            matches!(err, AddonStoreError::NotInstalled { addon } if addon == TEST_ADDON),
            "{err}"
        );
    }

    #[test]
    fn a_partly_installed_addon_is_damaged_not_a_fall_through_to_a_manual_copy() {
        let fx = Fixture::new();
        fx.write(&fx.managed, "weights.bin", b"hello world");
        fx.write(&fx.manual, "weights.bin", b"hello world");
        fx.write(&fx.manual, "config.json", b"abc");
        let err = fx
            .store()
            .find_manifest(TEST_ADDON, &TEST_MANIFEST)
            .unwrap_err();
        match err {
            AddonStoreError::Damaged { addon, path } => {
                assert_eq!(addon, TEST_ADDON);
                assert_eq!(path, fx.managed.join("config.json").display().to_string());
            }
            other => panic!("expected Damaged, got {other}"),
        }
    }

    #[test]
    fn without_an_addons_root_a_manual_copy_is_still_found() {
        let fx = Fixture::new();
        fx.write(&fx.manual, "weights.bin", b"hello world");
        fx.write(&fx.manual, "config.json", b"abc");
        let files = fx
            .store_rooted(None)
            .find_manifest(TEST_ADDON, &TEST_MANIFEST)
            .unwrap();
        assert_eq!(files.dir(), fx.manual);
    }

    #[tokio::test]
    async fn install_downloads_and_verifies_the_missing_files() {
        let fx = Fixture::new();
        let installation = fx
            .store()
            .install_manifest(&TEST_MANIFEST, &FakeFetcher::good())
            .await
            .unwrap();
        assert_eq!(installation, Installation::Downloaded(fx.managed.clone()));
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
    async fn an_installed_addon_is_not_downloaded_again() {
        let fx = Fixture::new();
        fx.write_installed();
        let fetcher = FakeFetcher::good();
        let fetches = fetcher.counter();
        let installation = fx
            .store()
            .install_manifest(&TEST_MANIFEST, &fetcher)
            .await
            .unwrap();
        assert_eq!(installation, Installation::Found(fx.managed.clone()));
        assert_eq!(*fetches.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn install_replaces_a_damaged_file() {
        let fx = Fixture::new();
        fx.write(&fx.managed, "weights.bin", b"hello worle");
        fx.write(&fx.managed, "config.json", b"abc");
        let fetcher = FakeFetcher::good();
        let fetches = fetcher.counter();
        fx.store()
            .install_manifest(&TEST_MANIFEST, &fetcher)
            .await
            .unwrap();
        assert_eq!(
            std::fs::read(fx.managed.join("weights.bin")).unwrap(),
            b"hello world"
        );
        assert_eq!(
            *fetches.lock().unwrap(),
            1,
            "only the damaged file is fetched"
        );
    }

    #[tokio::test]
    async fn a_failed_download_leaves_no_final_file() {
        let fx = Fixture::new();
        let fetcher = FakeFetcher::new(&[
            ("https://example.invalid/weights.bin", b"hello worle"),
            ("https://example.invalid/config.json", b"abc"),
        ]);
        let err = fx
            .store()
            .install_manifest(&TEST_MANIFEST, &fetcher)
            .await
            .unwrap_err();
        assert!(
            matches!(err, AddonStoreError::ChecksumMismatch { .. }),
            "{err}"
        );
        assert!(!fx.managed.join("weights.bin").exists());
        assert!(part_files(&fx.managed).is_empty());
    }

    #[tokio::test]
    async fn install_without_an_addons_root_is_the_cache_dir_failure() {
        let fx = Fixture::new();
        let err = fx
            .store_rooted(None)
            .install_manifest(&TEST_MANIFEST, &FakeFetcher::good())
            .await
            .unwrap_err();
        assert!(matches!(err, AddonStoreError::NoCacheDir), "{err}");
    }

    #[test]
    fn manual_dirs_start_next_to_the_executable_then_legacy_dirs() {
        fn legacy() -> Vec<PathBuf> {
            vec![PathBuf::from("/legacy/one"), PathBuf::from("/legacy/two")]
        }
        static WITH_LEGACY: AddonManifest = AddonManifest {
            dir_name: "test-model",
            files: &TEST_FILES,
            legacy_dirs: legacy,
            native_code: false,
        };
        let store = AddonStore::with_parts(None, Some(PathBuf::from("/opt/redacter/addons")));
        assert_eq!(
            store.manual_dirs(&WITH_LEGACY),
            [
                PathBuf::from("/opt/redacter/addons/test-model"),
                PathBuf::from("/legacy/one"),
                PathBuf::from("/legacy/two"),
            ]
        );
    }

    #[test]
    fn new_uses_the_addons_dir_override_as_root() {
        let options = AddonStoreOptions {
            addons_dir: Some(PathBuf::from("/override/addons")),
        };
        assert_eq!(
            AddonStore::new(&options).root,
            Some(PathBuf::from("/override/addons")),
            "--addons-dir is the addons root"
        );
    }
}
