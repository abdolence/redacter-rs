use crate::file_converters::ocr::Ocr;
use crate::file_converters::pdf::PdfToImage;
use crate::model_store::ModelStore;
use crate::reporter::AppReporter;
use crate::AppResult;

pub mod ocr;
pub mod pdf;

#[cfg(feature = "pdf-render")]
mod pdf_image_converter;

#[cfg(feature = "ocr")]
mod ocr_ocrs;

pub struct FileConverters<'a> {
    pub pdf_image_converter: Option<Box<dyn PdfToImage + 'a>>,
    pub ocr: Option<Box<dyn Ocr + 'a>>,
}

impl<'a> FileConverters<'a> {
    pub fn new() -> Self {
        Self {
            pdf_image_converter: None,
            ocr: None,
        }
    }

    /// Binds the converters this run can use. `needs_ocr` says whether the files it is
    /// about to copy can reach the OCR engine at all: the OCR model is only looked up —
    /// and only offered for download — when they can, so a run over text files never
    /// prompts for it.
    // `mut self` is only mutated inside the pdf-render/ocr blocks below; unused when neither
    // feature is enabled. `app_reporter`, `models` and `needs_ocr` are only read inside the
    // ocr block; unused whenever the ocr feature is off, pdf-render-only builds included.
    #[cfg_attr(not(any(feature = "pdf-render", feature = "ocr")), allow(unused_mut))]
    #[cfg_attr(not(feature = "ocr"), allow(unused_variables))]
    pub async fn init(
        mut self,
        app_reporter: &'a AppReporter<'a>,
        models: &ModelStore<'_>,
        needs_ocr: bool,
    ) -> AppResult<Self> {
        #[cfg(feature = "pdf-render")]
        {
            if let Ok(pdf_image_converter) = pdf_image_converter::PdfImageConverter::new() {
                self.pdf_image_converter = Some(Box::new(pdf_image_converter));
            }
        }
        #[cfg(feature = "ocr")]
        if needs_ocr {
            use crate::model_store::ModelId;
            match models.resolve(ModelId::Ocrs).await {
                Ok(files) => {
                    if let Ok(ocr) = ocr_ocrs::Ocrs::new(&files, app_reporter) {
                        self.ocr = Some(Box::new(ocr));
                    }
                }
                // The model is not installed, was declined, or what is on disk cannot be
                // read: OCR is off for this run, which is a line of output and not a
                // reason to copy nothing. Only a download that went wrong aborts the run.
                Err(err) if err.is_pre_consent_lookup_failure() => {
                    app_reporter.report(format!("OCR is unavailable: {err}"))?;
                }
                Err(err) => return Err(err.into()),
            }
        }

        Ok(self)
    }
}

#[cfg(all(test, feature = "ocr"))]
mod tests {
    use super::*;
    use crate::model_store::test_support::{fixed_root, FakeConsent, FakeFetcher};
    use crate::model_store::{manifest, DownloadPolicy, ManualDirs, ModelId};
    use crate::redacters::test_support::pdfium_test_lock;
    use console::Term;
    use std::path::Path;
    use tempfile::TempDir;

    /// No manual directories at all: whatever the machine running the test has installed
    /// in `~/.cache/ocrs` or next to the test binary must never turn a miss into a hit.
    fn no_manual_dirs() -> ManualDirs {
        Box::new(|_| Vec::new())
    }

    /// Fills the managed OCR directory with files of the right names and the wrong size.
    fn write_stale_ocr_model(root: &Path) {
        let managed = root.join("ocrs");
        std::fs::create_dir_all(&managed).unwrap();
        for file in manifest(ModelId::Ocrs).files {
            std::fs::write(managed.join(file.name), b"junk").unwrap();
        }
    }

    /// A copy over a model directory left half-written by an earlier run must still copy:
    /// the files are looked up before anything was downloaded, so the only thing lost is
    /// OCR. `init` binds pdfium as well, hence the lock.
    #[tokio::test]
    async fn a_stale_managed_model_turns_ocr_off_instead_of_failing_the_run() {
        let lock = pdfium_test_lock();
        let _guard = lock.lock().await;

        let root = TempDir::new().unwrap();
        write_stale_ocr_model(root.path());
        let term = Term::stdout();
        let reporter = AppReporter::from(&term);
        let fetcher = FakeFetcher::new(&[]);
        let fetches = fetcher.counter();
        let store = ModelStore::with_parts(
            fixed_root(root.path().to_path_buf()),
            no_manual_dirs(),
            DownloadPolicy::No,
            Box::new(FakeConsent::new(false)),
            Box::new(fetcher),
            &reporter,
        );
        let converters = FileConverters::new()
            .init(&reporter, &store, true)
            .await
            .expect("a lookup failure must not fail the run");
        assert!(converters.ocr.is_none());
        assert_eq!(
            *fetches.lock().unwrap(),
            0,
            "nothing may be fetched before consent"
        );
    }

    /// The other side of the split: a download the user agreed to that goes wrong is not
    /// "OCR is unavailable", it is a failed install and it stops the run.
    #[tokio::test]
    async fn a_failed_download_of_the_ocr_model_aborts_the_run() {
        let lock = pdfium_test_lock();
        let _guard = lock.lock().await;

        let root = TempDir::new().unwrap();
        let term = Term::stdout();
        let reporter = AppReporter::from(&term);
        let fetcher = FakeFetcher::new(&[]);
        let fetches = fetcher.counter();
        let store = ModelStore::with_parts(
            fixed_root(root.path().to_path_buf()),
            no_manual_dirs(),
            DownloadPolicy::Yes,
            Box::new(FakeConsent::new(true)),
            Box::new(fetcher),
            &reporter,
        );
        let err = match FileConverters::new().init(&reporter, &store, true).await {
            Err(err) => err,
            Ok(_) => panic!("a failed download must abort the run"),
        };
        assert!(err.to_string().contains("404"), "{err}");
        assert_eq!(*fetches.lock().unwrap(), 1);
    }

    /// A run whose files cannot reach the OCR engine never looks the model up, so it never
    /// asks to download it.
    #[tokio::test]
    async fn a_run_that_cannot_use_ocr_never_asks_for_the_model() {
        let lock = pdfium_test_lock();
        let _guard = lock.lock().await;

        let root = TempDir::new().unwrap();
        let term = Term::stdout();
        let reporter = AppReporter::from(&term);
        let consent = FakeConsent::new(true);
        let asked = consent.asked();
        let fetcher = FakeFetcher::new(&[]);
        let fetches = fetcher.counter();
        let store = ModelStore::with_parts(
            fixed_root(root.path().to_path_buf()),
            no_manual_dirs(),
            DownloadPolicy::Ask,
            Box::new(consent),
            Box::new(fetcher),
            &reporter,
        );
        let converters = FileConverters::new()
            .init(&reporter, &store, false)
            .await
            .unwrap();
        assert!(converters.ocr.is_none());
        assert!(
            asked.lock().unwrap().is_empty(),
            "a run that cannot use OCR must not prompt for it"
        );
        assert_eq!(*fetches.lock().unwrap(), 0);
    }
}
