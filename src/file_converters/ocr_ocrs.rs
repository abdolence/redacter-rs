use crate::common_types::TextImageCoords;
use crate::file_converters::ocr::Ocr;
use crate::model_store::ModelFiles;
use crate::reporter::AppReporter;
use crate::AppResult;
use ocrs::{ImageSource, OcrEngine, OcrEngineParams, OcrInput, TextItem};

pub struct Ocrs<'a> {
    ocr_engine: OcrEngine,
    #[allow(dead_code)]
    app_reporter: &'a AppReporter<'a>,
}

impl<'a> Ocrs<'a> {
    pub fn new(models: &ModelFiles, app_reporter: &'a AppReporter<'a>) -> AppResult<Self> {
        app_reporter.report(format!(
            "Loading OCR models from {}",
            models.dir().to_string_lossy()
        ))?;
        let detection_model = rten::Model::load_file(models.path("text-detection.rten"))?;
        let recognition_model = rten::Model::load_file(models.path("text-recognition.rten"))?;
        let ocr_engine = OcrEngine::new(OcrEngineParams {
            detection_model: Some(detection_model),
            recognition_model: Some(recognition_model),
            ..Default::default()
        })?;
        Ok(Self {
            ocr_engine,
            app_reporter,
        })
    }
}

impl Ocr for Ocrs<'_> {
    fn image_to_text(&self, image: image::DynamicImage) -> AppResult<Vec<TextImageCoords>> {
        let rgb_image = image.to_rgb8();
        let image_source = ImageSource::from_bytes(rgb_image.as_raw(), rgb_image.dimensions())?;
        let input: OcrInput = self.ocr_engine.prepare_input(image_source)?;
        let word_rects = self.ocr_engine.detect_words(&input)?;
        let line_rects = self.ocr_engine.find_text_lines(&input, &word_rects);
        let mut text_image_coords = vec![];
        for text_line in self
            .ocr_engine
            .recognize_text(&input, &line_rects)?
            .into_iter()
            .flatten()
        {
            let mut current_word = "".to_string();
            let mut current_word_rect: Option<rten_imageproc::Rect> = None;

            for char in text_line.chars() {
                match current_word_rect {
                    None => {
                        current_word_rect = Some(char.rect);
                        current_word = char.char.to_string();
                    }
                    Some(ref current_rect) if char.char == ' ' => {
                        text_image_coords.push(TextImageCoords {
                            text: Some(current_word.clone()),
                            x1: current_rect.left() as f32,
                            y1: current_rect.top() as f32,
                            x2: current_rect.right() as f32,
                            y2: current_rect.bottom() as f32,
                        });
                        current_word_rect = None;
                    }
                    Some(current_rect) => {
                        current_word_rect = Some(current_rect.union(char.rect));
                        current_word.push(char.char);
                    }
                }
            }
        }
        Ok(text_image_coords)
    }
}

#[allow(unused_imports)]
mod tests {
    use super::*;
    use crate::model_store::{ModelId, ModelStore, ModelStoreOptions};
    use console::Term;

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-ocr"), ignore)]
    async fn test_recognise_png_file() -> AppResult<()> {
        let term = Term::stdout();
        let app_reporter = AppReporter::from(&term);
        let store = ModelStore::new(&ModelStoreOptions::default(), &app_reporter)?;
        let files = store.resolve(ModelId::Ocrs).await?;
        let ocrs = Ocrs::new(&files, &app_reporter)?;
        let image = image::open("test-fixtures/media/form-example.png")?;
        let text_image_coords = ocrs.image_to_text(image)?;
        assert!(text_image_coords.len() > 10);
        Ok(())
    }
}
