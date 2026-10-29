use crate::addon_store::{Addon, AddonStore, AddonStoreError};
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
    /// Binds Pdfium from the verified copy `addons install pdf` put in the addons root. A
    /// copy that is there but does not match its pin is an error and never falls back to
    /// another library. Without an installed copy, a Pdfium installed by hand is used: the
    /// `lib` directory beside the tool's own, the tool's own directory, then the system
    /// library directories of the platform. The current directory is never searched.
    pub fn new(addons: &AddonStore) -> AppResult<Self> {
        let bindings = match addons.find(Addon::Pdf) {
            Ok(files) => {
                Pdfium::bind_to_library(Pdfium::pdfium_platform_library_name_at_path(files.dir()))?
            }
            Err(
                AddonStoreError::NotInstalled { .. }
                | AddonStoreError::NotAvailableForPlatform { .. },
            ) => Self::bind_by_hand()?,
            Err(err) => return Err(err.into()),
        };
        Ok(Self {
            pdfium: Pdfium::new(bindings),
        })
    }

    fn bind_by_hand() -> AppResult<Box<dyn PdfiumLibraryBindings>> {
        let executable = std::env::current_exe()?;
        let executable_dir = executable.parent().ok_or(AppError::SystemError {
            message: "No parent directory for executable".to_string(),
        })?;
        let lib_dir = executable_dir.parent().map(|parent| parent.join("lib"));
        let bound = lib_dir
            .iter()
            .map(PathBuf::as_path)
            .chain([executable_dir])
            .chain(SYSTEM_LIBRARY_DIRS.iter().map(Path::new))
            .find_map(|dir| {
                Pdfium::bind_to_library(Pdfium::pdfium_platform_library_name_at_path(dir)).ok()
            })
            .map_or_else(bind_by_system_loader, Ok);
        bound
    }
}

/// System library directories tried by full path. A bare library name would also be resolved
/// against the current directory by the macOS loader, so a library planted next to the files
/// being redacted would be loaded.
#[cfg(target_os = "macos")]
const SYSTEM_LIBRARY_DIRS: [&str; 3] = ["/opt/homebrew/lib", "/usr/local/lib", "/usr/lib"];

/// Only macOS has a fixed list. The Windows loader resolves a bare name against the current
/// directory too, so there only the addons root and the tool's own directories are searched.
#[cfg(not(target_os = "macos"))]
const SYSTEM_LIBRARY_DIRS: [&str; 0] = [];

/// The Linux loader takes the library by its bare name from the system directories and
/// `LD_LIBRARY_PATH`, in which an empty entry means the current directory.
#[cfg(target_os = "linux")]
fn bind_by_system_loader() -> AppResult<Box<dyn PdfiumLibraryBindings>> {
    Ok(Pdfium::bind_to_system_library()?)
}

#[cfg(not(target_os = "linux"))]
fn bind_by_system_loader() -> AppResult<Box<dyn PdfiumLibraryBindings>> {
    Err(AppError::SystemError {
        message: "No Pdfium library found beside the executable or in the system library \
                  directories"
            .to_string(),
    })
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

    /// pdfium is bound to a native library that is not committed to the repo. Machines
    /// without one installed should skip rather than fail.
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

    /// A copy in the addons root that is not the pinned build is not a reason to look
    /// anywhere else: Pdfium stays off.
    #[test]
    fn a_damaged_installed_pdfium_is_an_error_and_never_falls_back() {
        let Some(manifest) = Addon::Pdf.manifest() else {
            return;
        };
        let addons_dir = tempfile::TempDir::new().unwrap();
        let installed = addons_dir.path().join(manifest.dir_name);
        std::fs::create_dir_all(&installed).unwrap();
        let library = &manifest.files[0];
        std::fs::write(installed.join(library.name), vec![0; library.size as usize]).unwrap();
        let addons = AddonStore::new(&AddonStoreOptions {
            addons_dir: Some(addons_dir.path().to_path_buf()),
        });
        match PdfImageConverter::new(&addons) {
            Err(AppError::AddonStoreError(AddonStoreError::Damaged {
                addon: Addon::Pdf, ..
            })) => {}
            Err(other) => panic!("expected a damaged Pdfium, got {other}"),
            Ok(_) => panic!("a damaged Pdfium must not be bound"),
        }
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
