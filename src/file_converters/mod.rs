use crate::addon_store::AddonStore;
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

    /// Binds the converters whose addons are installed. Nothing is downloaded: a converter
    /// whose addon is missing is off for this run.
    // `mut self`, `app_reporter` and `addons` are only used inside the feature-gated blocks
    // below; unused when neither the pdf-render nor ocr feature is enabled.
    #[cfg_attr(
        not(any(feature = "pdf-render", feature = "ocr")),
        allow(unused_mut, unused_variables)
    )]
    pub async fn init(
        mut self,
        app_reporter: &'a AppReporter<'a>,
        addons: &AddonStore,
    ) -> AppResult<Self> {
        #[cfg(feature = "pdf-render")]
        {
            if let Ok(pdf_image_converter) = pdf_image_converter::PdfImageConverter::new(addons) {
                self.pdf_image_converter = Some(Box::new(pdf_image_converter));
            }
        }
        #[cfg(feature = "ocr")]
        match addons.find(crate::addon_store::Addon::Ocr) {
            Ok(files) => {
                if let Ok(ocr) = ocr_ocrs::Ocrs::new(&files, app_reporter) {
                    self.ocr = Some(Box::new(ocr));
                }
            }
            Err(err) => tracing::debug!(%err, "OCR is off"),
        }

        Ok(self)
    }
}
