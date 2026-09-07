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

    // `mut self` is only mutated inside the pdf-render/ocr blocks below; unused when neither
    // feature is enabled. `app_reporter` and `models` are only read inside the ocr block;
    // unused whenever the ocr feature is off, pdf-render-only builds included.
    #[cfg_attr(not(any(feature = "pdf-render", feature = "ocr")), allow(unused_mut))]
    #[cfg_attr(not(feature = "ocr"), allow(unused_variables))]
    pub async fn init(
        mut self,
        app_reporter: &'a AppReporter<'a>,
        models: &ModelStore<'_>,
    ) -> AppResult<Self> {
        #[cfg(feature = "pdf-render")]
        {
            if let Ok(pdf_image_converter) = pdf_image_converter::PdfImageConverter::new() {
                self.pdf_image_converter = Some(Box::new(pdf_image_converter));
            }
        }
        #[cfg(feature = "ocr")]
        {
            use crate::model_store::{ModelId, ModelStoreError};
            match models.resolve(ModelId::Ocrs).await {
                Ok(files) => {
                    if let Ok(ocr) = ocr_ocrs::Ocrs::new(&files, app_reporter) {
                        self.ocr = Some(Box::new(ocr));
                    }
                }
                Err(
                    err @ (ModelStoreError::NotInstalled { .. } | ModelStoreError::Declined { .. }),
                ) => {
                    app_reporter.report_debug(format!("OCR is unavailable: {err}"));
                }
                Err(err) => return Err(err.into()),
            }
        }

        Ok(self)
    }
}
