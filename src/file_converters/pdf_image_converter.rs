use crate::errors::AppError;
use crate::file_converters::pdf::{PdfInfo, PdfPageInfo, PdfToImage};
use crate::model_store::{ModelStore, ModelStoreError};
use crate::reporter::AppReporter;
use crate::AppResult;
use bytes::Bytes;
use pdfium_render::prelude::*;
use std::path::{Path, PathBuf};

pub struct PdfImageConverter {
    pdfium: Pdfium,
}

/// Where a Pdfium library can be loaded from.
#[derive(Debug, PartialEq, Eq)]
enum PdfiumLocation {
    /// The library file of the pinned build in the models directory, loaded by full path so
    /// that no other Pdfium on the machine can take its place.
    Pinned(PathBuf),
    /// The platform's library file name inside this directory.
    Directory(PathBuf),
    /// Whatever the system's library search path offers.
    System,
}

impl PdfiumLocation {
    /// The locations in the order they are tried: the pinned copy first, then the places a
    /// user-installed Pdfium has always been looked for.
    fn search_order(pinned: Option<PathBuf>, executable_dir: &Path) -> Vec<Self> {
        let lib_beside_bin = executable_dir
            .parent()
            .map(|parent| parent.join("lib"))
            .unwrap_or_else(|| executable_dir.to_path_buf());
        pinned
            .map(Self::Pinned)
            .into_iter()
            .chain([
                Self::Directory(PathBuf::from("./")),
                Self::Directory(PathBuf::from("./lib")),
                Self::Directory(lib_beside_bin),
                Self::Directory(executable_dir.to_path_buf()),
                Self::System,
            ])
            .collect()
    }

    fn bind(&self) -> Result<Box<dyn PdfiumLibraryBindings>, PdfiumError> {
        match self {
            Self::Pinned(library) => Pdfium::bind_to_library(library),
            Self::Directory(dir) => {
                Pdfium::bind_to_library(Pdfium::pdfium_platform_library_name_at_path(dir))
            }
            Self::System => Pdfium::bind_to_system_library(),
        }
    }
}

/// The library file of the pinned build. Looked up without a download unless `may_download`
/// says this run needs PDF rendering. `Ok(None)` on a platform that has no pinned build.
#[cfg(pdfium_download)]
async fn pinned_library(
    models: &ModelStore<'_>,
    may_download: bool,
) -> Result<Option<PathBuf>, ModelStoreError> {
    use crate::model_store::ModelId;
    let files = if may_download {
        models.resolve(ModelId::Pdfium).await?
    } else {
        models.resolve_installed(ModelId::Pdfium).await?
    };
    Ok(Some(Pdfium::pdfium_platform_library_name_at_path(
        files.dir(),
    )))
}

#[cfg(not(pdfium_download))]
async fn pinned_library(
    _models: &ModelStore<'_>,
    _may_download: bool,
) -> Result<Option<PathBuf>, ModelStoreError> {
    Ok(None)
}

impl PdfImageConverter {
    /// Binds Pdfium from the first location that has it. `may_download` is true when the run
    /// is about to render PDFs: only then may the pinned build be offered for download, and
    /// only then is a missing Pdfium worth a line of output. `Ok(None)` means PDF rendering
    /// is off for this run; an error is a download the user agreed to that went wrong.
    pub async fn new<'a>(
        models: &ModelStore<'_>,
        reporter: &'a AppReporter<'a>,
        may_download: bool,
    ) -> AppResult<Option<Self>> {
        let (pinned, not_installed) = match pinned_library(models, may_download).await {
            Ok(pinned) => (pinned, None),
            Err(err) if err.is_pre_consent_lookup_failure() => (None, Some(err)),
            Err(err) => return Err(err.into()),
        };

        let executable = std::env::current_exe()?;
        let executable_dir = executable.parent().ok_or(AppError::SystemError {
            message: "No parent directory for executable".to_string(),
        })?;

        for location in PdfiumLocation::search_order(pinned, executable_dir) {
            match location.bind() {
                Ok(bindings) => {
                    tracing::debug!(?location, "bound Pdfium");
                    return Ok(Some(Self {
                        pdfium: Pdfium::new(bindings),
                    }));
                }
                Err(err) => match &location {
                    PdfiumLocation::Pinned(library) => reporter.report(format!(
                        "The Pdfium at {} cannot be loaded: {err}",
                        library.display()
                    ))?,
                    _ => tracing::debug!(?location, %err, "no Pdfium here"),
                },
            }
        }

        if may_download {
            let reason = match not_installed {
                Some(err) => err.to_string(),
                None => "no Pdfium library was found for this platform (README, \"PDF redaction\")"
                    .to_string(),
            };
            reporter.report(format!("PDF rendering is unavailable: {reason}"))?;
        }
        Ok(None)
    }
}

impl PdfToImage for PdfImageConverter {
    fn convert_to_images(&self, pdf_bytes: Bytes) -> AppResult<PdfInfo> {
        let render_config = PdfRenderConfig::default();
        let document = self.pdfium.load_pdf_from_byte_vec(pdf_bytes.into(), None)?;
        let mut pdf_info = PdfInfo { pages: Vec::new() };
        for page in document.pages().iter() {
            let image = page.render_with_config(&render_config)?.as_image()?;
            let page_info = PdfPageInfo {
                height: page.height(),
                width: page.width(),
                page_as_images: image,
            };
            pdf_info.pages.push(page_info);
        }
        Ok(pdf_info)
    }

    fn images_to_pdf(&self, pdf_info: PdfInfo) -> AppResult<Bytes> {
        let mut document = self.pdfium.create_new_pdf()?;
        for src_page in pdf_info.pages.iter().rev() {
            let mut page =
                document
                    .pages_mut()
                    .create_page_at_start(PdfPagePaperSize::from_points(
                        src_page.width,
                        src_page.height,
                    ))?;
            let object = PdfPageImageObject::new_with_size(
                &document,
                &src_page.page_as_images,
                src_page.width,
                src_page.height,
            )?;
            page.objects_mut().add_image_object(object)?;
        }
        Ok(document.save_to_bytes()?.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model_store::{ModelStore, ModelStoreOptions};
    use console::Term;

    /// pdfium is bound to a native library that is not committed to the repo (see
    /// `lib/libpdfium.so`, gitignored). Machines without it should skip rather than fail.
    async fn converter_or_skip(test_name: &str) -> Option<PdfImageConverter> {
        let term = Term::stdout();
        let reporter = AppReporter::from(&term);
        let store = ModelStore::new(&ModelStoreOptions::default(), &reporter);
        match PdfImageConverter::new(&store, &reporter, false).await {
            Ok(Some(converter)) => Some(converter),
            Ok(None) => {
                println!("Skipping {test_name}: pdfium is not available");
                None
            }
            Err(error) => {
                println!("Skipping {test_name}: pdfium cannot be set up ({error})");
                None
            }
        }
    }

    #[tokio::test]
    async fn renders_sample_form_pdf_to_a_single_page_image() -> AppResult<()> {
        // Binding pdfium races with any other test that also binds it (directly, or
        // through `command_copy`), so serialize on the shared test lock. See
        // `pdfium_test_lock`'s doc comment for what happens without it.
        let _guard = crate::redacters::test_support::pdfium_test_lock()
            .lock()
            .await;

        let Some(converter) =
            converter_or_skip("renders_sample_form_pdf_to_a_single_page_image").await
        else {
            return Ok(());
        };

        let pdf_bytes: Bytes = std::fs::read("test-fixtures/documents/customer-form.pdf")?.into();
        let pdf_info = converter.convert_to_images(pdf_bytes)?;

        assert_eq!(pdf_info.pages.len(), 1, "the fixture is a one-page PDF");
        let page = &pdf_info.pages[0];
        assert!(
            page.page_as_images.width() > 100 && page.page_as_images.height() > 100,
            "expected a sensibly sized rendered page, got {}x{}",
            page.page_as_images.width(),
            page.page_as_images.height()
        );

        let round_tripped = converter.images_to_pdf(pdf_info)?;
        assert!(
            !round_tripped.is_empty(),
            "converting the rendered page back to a PDF should not be empty"
        );

        Ok(())
    }

    #[test]
    fn the_pinned_copy_is_tried_before_any_other_location() {
        let pinned = PathBuf::from("/models/pdfium-7881/libpdfium.so");
        let order =
            PdfiumLocation::search_order(Some(pinned.clone()), Path::new("/opt/redacter/bin"));
        assert_eq!(order.first(), Some(&PdfiumLocation::Pinned(pinned)));
    }

    #[test]
    fn without_a_pinned_copy_the_search_starts_in_the_working_directory() {
        let order = PdfiumLocation::search_order(None, Path::new("/opt/redacter/bin"));
        assert_eq!(
            order,
            [
                PdfiumLocation::Directory(PathBuf::from("./")),
                PdfiumLocation::Directory(PathBuf::from("./lib")),
                PdfiumLocation::Directory(PathBuf::from("/opt/redacter/lib")),
                PdfiumLocation::Directory(PathBuf::from("/opt/redacter/bin")),
                PdfiumLocation::System,
            ]
        );
    }

    /// The pinned build is loaded by full path, but a user who places a Pdfium by hand in
    /// the models directory (README, "Models and downloads") uses the platform's own name.
    #[cfg(pdfium_download)]
    #[test]
    fn the_pinned_build_is_stored_under_the_name_pdfium_render_loads() {
        use crate::model_store::{manifest, ModelId};
        let names: Vec<&str> = manifest(ModelId::Pdfium)
            .files
            .iter()
            .map(|file| file.name)
            .collect();
        assert_eq!(
            names,
            [Pdfium::pdfium_platform_library_name()
                .to_str()
                .expect("a library name is UTF-8")]
        );
    }
}
