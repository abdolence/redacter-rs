use crate::errors::AppError;
use crate::file_converters::pdf::{PdfInfo, PdfPageInfo, PdfToImage};
use crate::AppResult;
use bytes::Bytes;
use pdfium_render::prelude::*;

pub struct PdfImageConverter {
    pdfium: Pdfium,
}

impl PdfImageConverter {
    pub fn new() -> AppResult<Self> {
        let executable = std::env::current_exe()?;
        let current_dir = executable
            .parent()
            .ok_or(AppError::SystemError {
                message: "No parent directory for executable".to_string(),
            })?
            .to_path_buf();

        let bindings = Pdfium::bind_to_library(
            // Attempt to bind to a pdfium library in the current working directory...
            Pdfium::pdfium_platform_library_name_at_path("./"),
        )
        .or_else(|_| Pdfium::bind_to_library(Pdfium::pdfium_platform_library_name_at_path("./lib")))
        .or_else(|_| {
            Pdfium::bind_to_library(Pdfium::pdfium_platform_library_name_at_path(
                &current_dir
                    .parent()
                    .map(|p| p.join("lib"))
                    .unwrap_or(current_dir.clone()),
            ))
        })
        .or_else(|_| {
            Pdfium::bind_to_library(Pdfium::pdfium_platform_library_name_at_path(&current_dir))
        })
        .or_else(|_| Pdfium::bind_to_system_library())?;

        let pdfium = Pdfium::new(bindings);
        Ok(Self { pdfium })
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

    /// pdfium is bound to a native library that is not committed to the repo (see
    /// `lib/libpdfium.so`, gitignored). Machines without it should skip rather than fail.
    fn converter_or_skip(test_name: &str) -> Option<PdfImageConverter> {
        match PdfImageConverter::new() {
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
}
