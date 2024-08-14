use crate::file_converters::pdf::PdfToImage;
use crate::AppResult;

pub mod pdf;

pub struct FileConverters {
    pub pdf_image_converter: Option<Box<dyn PdfToImage + 'static>>,
}

impl FileConverters {
    pub fn new() -> Self {
        Self {
            pdf_image_converter: None,
        }
    }

    pub async fn init(&mut self) -> AppResult<()> {
        match pdf::PdfImageConverter::new().ok() {
            Some(pdf_image_converter) => {
                self.pdf_image_converter = Some(Box::new(pdf_image_converter));
                Ok(())
            }
            None => Ok(()),
        }
    }
}
