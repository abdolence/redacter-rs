use crate::file_converters::pdf::PdfToImage;
use crate::AppResult;

pub mod pdf;

#[cfg(feature = "pdf-render")]
mod pdf_image_converter;

pub struct FileConverters {
    pub pdf_image_converter: Option<Box<dyn PdfToImage + 'static>>,
}

impl FileConverters {
    pub fn new() -> Self {
        Self {
            pdf_image_converter: None,
        }
    }

    #[cfg(feature = "pdf-render")]
    pub async fn init(&mut self) -> AppResult<()> {
        match crate::file_converters::pdf_image_converter::PdfImageConverter::new().ok() {
            Some(pdf_image_converter) => {
                self.pdf_image_converter = Some(Box::new(pdf_image_converter));
                Ok(())
            }
            None => Ok(()),
        }
    }

    #[cfg(not(feature = "pdf-render"))]
    pub async fn init(&mut self) -> AppResult<()> {
        Ok(())
    }
}
