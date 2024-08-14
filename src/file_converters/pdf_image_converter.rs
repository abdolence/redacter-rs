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
        let render_config = PdfRenderConfig::new()
            .set_target_width(2000)
            .set_maximum_height(2000)
            .rotate_if_landscape(PdfPageRenderRotation::Degrees90, true);
        let document = self
            .pdfium
            .load_pdf_from_byte_vec(pdf_bytes.to_vec(), None)?;
        let mut pdf_info = PdfInfo { pages: Vec::new() };
        for page in document.pages().iter() {
            let image = page.render_with_config(&render_config)?.as_image();
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
        for src_page in pdf_info.pages {
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
