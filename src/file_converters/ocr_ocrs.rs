use crate::common_types::TextImageCoords;
use crate::errors::AppError;
use crate::file_converters::ocr::Ocr;
use crate::reporter::AppReporter;
use crate::AppResult;
use ocrs::{ImageSource, OcrEngine, OcrEngineParams, OcrInput, TextItem};
use std::path::PathBuf;

pub struct Ocrs<'a> {
    ocr_engine: OcrEngine,
    #[allow(dead_code)]
    app_reporter: &'a AppReporter<'a>,
}

impl<'a> Ocrs<'a> {
    pub fn new(app_reporter: &'a AppReporter<'a>) -> AppResult<Self> {
        let find_models_dir = Self::find_models_dir()?;
        app_reporter.report(format!(
            "Loading OCR models from {}",
            find_models_dir.to_string_lossy()
        ))?;
        let detection_model_path = find_models_dir.join("text-detection.rten");
        let rec_model_path = find_models_dir.join("text-recognition.rten");
        let detection_model = rten::Model::load_file(detection_model_path)?;
        let recognition_model = rten::Model::load_file(rec_model_path)?;
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

    fn find_models_dir() -> AppResult<std::path::PathBuf> {
        let executable = std::env::current_exe()?;
        let current_dir = executable.parent().map(|p| p.to_path_buf());

        vec![
            current_dir.clone().map(|p| p.join("models").join("ocrs")),
            current_dir
                .clone()
                .and_then(|p| p.parent().map(|p| p.join("share").join("ocrs"))),
            dirs::home_dir().map(|p| p.join(".cache").join("ocrs")),
        ]
        .into_iter()
        .collect::<Vec<Option<PathBuf>>>()
        .iter()
        .flatten()
        .find(|p| p.exists())
        .cloned()
        .ok_or_else(|| AppError::SystemError {
            message: "Could not find models directory".to_string(),
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
    use console::Term;

    #[test]
    #[cfg_attr(not(feature = "ci-ocr"), ignore)]
    fn test_recognise_png_file() -> AppResult<()> {
        let term = Term::stdout();
        let app_reporter = AppReporter::from(&term);
        let ocrs = Ocrs::new(&app_reporter)?;
        let image = image::open("test-fixtures/media/form-example.png")?;
        let text_image_coords = ocrs.image_to_text(image)?;
        assert!(text_image_coords.len() > 10);
        Ok(())
    }
}
