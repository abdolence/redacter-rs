use crate::file_converters::ocr::Ocr;
use crate::file_converters::pdf::PdfToImage;
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

    pub async fn init(mut self, app_reporter: &'a AppReporter<'a>) -> AppResult<Self> {
        #[cfg(feature = "pdf-render")]
        {
            if let Ok(pdf_image_converter) = pdf_image_converter::PdfImageConverter::new() {
                self.pdf_image_converter = Some(Box::new(pdf_image_converter));
            }
        }
        #[cfg(feature = "ocr")]
        {
            if let Ok(ocr) = ocr_ocrs::Ocrs::new(app_reporter) {
                self.ocr = Some(Box::new(ocr));
            }
        }

        Ok(self)
    }
}
