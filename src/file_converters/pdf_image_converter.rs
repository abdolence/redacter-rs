use crate::addon_store::{Addon, AddonStore};
use crate::errors::AppError;
use crate::file_converters::pdf::{PdfInfo, PdfPageInfo, PdfToImage};
use crate::AppResult;
use bytes::Bytes;
use pdfium_render::prelude::*;
use std::path::{Path, PathBuf};

pub struct PdfImageConverter {
    pdfium: Pdfium,
}

impl PdfImageConverter {
    pub fn new(addons: &AddonStore) -> AppResult<Self> {
        let executable = std::env::current_exe()?;
        let current_dir = executable
            .parent()
            .ok_or(AppError::SystemError {
                message: "No parent directory for executable".to_string(),
            })?
            .to_path_buf();
        let installed = match addons.find(Addon::Pdf) {
            Ok(files) => Some(files.dir().to_path_buf()),
            Err(err) => {
                tracing::debug!(%err, "no Pdfium from setup-addons");
                None
            }
        };

        let bindings = library_dirs(installed, &current_dir)
            .iter()
            .find_map(|dir| {
                Pdfium::bind_to_library(Pdfium::pdfium_platform_library_name_at_path(dir)).ok()
            })
            .map_or_else(Pdfium::bind_to_system_library, Ok)?;

        let pdfium = Pdfium::new(bindings);
        Ok(Self { pdfium })
    }
}

/// The directories a Pdfium library is loaded from, in order: the verified copy
/// `setup-addons` installed, then where a Pdfium installed by hand has always been looked for.
/// The system library path is tried after all of them.
fn library_dirs(installed: Option<PathBuf>, executable_dir: &Path) -> Vec<PathBuf> {
    installed
        .into_iter()
        .chain([
            PathBuf::from("./"),
            PathBuf::from("./lib"),
            executable_dir
                .parent()
                .map(|parent| parent.join("lib"))
                .unwrap_or_else(|| executable_dir.to_path_buf()),
            executable_dir.to_path_buf(),
        ])
        .collect()
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
    use crate::addon_store::AddonStoreOptions;

    /// pdfium is bound to a native library that is not committed to the repo (see
    /// `lib/libpdfium.so`, gitignored). Machines without it should skip rather than fail.
    fn converter_or_skip(test_name: &str) -> Option<PdfImageConverter> {
        match PdfImageConverter::new(&AddonStore::new(&AddonStoreOptions::default())) {
            Ok(converter) => Some(converter),
            Err(error) => {
                println!("Skipping {test_name}: pdfium is not available ({error})");
                None
            }
        }
    }

    #[test]
    fn renders_sample_form_pdf_to_a_single_page_image() -> AppResult<()> {
        // Binding pdfium races with any other test that also binds it (directly, or
        // through `command_copy`), so serialize on the shared test lock. See
        // `pdfium_test_lock`'s doc comment for what happens without it.
        let _guard = crate::redacters::test_support::pdfium_test_lock().blocking_lock();

        let Some(converter) = converter_or_skip("renders_sample_form_pdf_to_a_single_page_image")
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

    /// The copy `setup-addons` installed is the build the tool is compiled against, so it
    /// comes before any Pdfium installed by hand.
    #[test]
    fn the_installed_pdfium_is_tried_before_any_installed_by_hand() {
        let installed = PathBuf::from("/addons/pdfium-7881");
        assert_eq!(
            library_dirs(Some(installed.clone()), Path::new("/opt/redacter/bin")),
            [
                installed,
                PathBuf::from("./"),
                PathBuf::from("./lib"),
                PathBuf::from("/opt/redacter/lib"),
                PathBuf::from("/opt/redacter/bin"),
            ]
        );
    }

    /// The installed copy is bound by the platform's library name, so the table has to
    /// store it under that name.
    #[test]
    fn the_installed_pdfium_is_stored_under_the_name_it_is_loaded_by() {
        let Some(manifest) = Addon::Pdf.manifest() else {
            return;
        };
        let names: Vec<&str> = manifest.files.iter().map(|file| file.name).collect();
        assert_eq!(
            names,
            [Pdfium::pdfium_platform_library_name()
                .to_str()
                .expect("a library name is UTF-8")]
        );
    }
}
