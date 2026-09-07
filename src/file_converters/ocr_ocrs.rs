use crate::common_types::TextImageCoords;
use crate::file_converters::ocr::Ocr;
use crate::model_store::ModelFiles;
use crate::reporter::AppReporter;
use crate::AppResult;
use ocrs::{ImageSource, OcrEngine, OcrEngineParams, OcrInput, TextChar, TextItem};

/// Splits one recognised line's characters into words, using any run of one or more `' '`
/// characters as a separator. The `line` field of every returned word is left at `0`; the
/// caller sets it to the recognised line's index.
///
/// This never drops the word that runs to the end of the character slice: unlike splitting
/// on a delimiter, there is no trailing separator after the last word to trigger a flush, so
/// the loop flushes explicitly once it runs out of characters.
fn words_from_line(chars: &[TextChar]) -> Vec<TextImageCoords> {
    let mut words = vec![];
    let mut current_word = String::new();
    let mut current_word_rect: Option<rten_imageproc::Rect> = None;

    for char in chars {
        match current_word_rect {
            None if char.char == ' ' => {
                // A leading or repeated separator: nothing pending to flush.
            }
            None => {
                current_word_rect = Some(char.rect);
                current_word = char.char.to_string();
            }
            Some(current_rect) if char.char == ' ' => {
                words.push(TextImageCoords {
                    text: Some(std::mem::take(&mut current_word)),
                    x1: current_rect.left() as f32,
                    y1: current_rect.top() as f32,
                    x2: current_rect.right() as f32,
                    y2: current_rect.bottom() as f32,
                    line: 0,
                });
                current_word_rect = None;
            }
            Some(current_rect) => {
                current_word_rect = Some(current_rect.union(char.rect));
                current_word.push(char.char);
            }
        }
    }

    // The word that runs to the end of the line has no trailing separator to flush it, so
    // it must be pushed explicitly here. This is the fix for the last word of every OCR
    // line being silently dropped.
    if let Some(current_rect) = current_word_rect {
        words.push(TextImageCoords {
            text: Some(current_word),
            x1: current_rect.left() as f32,
            y1: current_rect.top() as f32,
            x2: current_rect.right() as f32,
            y2: current_rect.bottom() as f32,
            line: 0,
        });
    }

    words
}

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
        for (line, text_line) in self
            .ocr_engine
            .recognize_text(&input, &line_rects)?
            .into_iter()
            .flatten()
            .enumerate()
        {
            for mut word in words_from_line(text_line.chars()) {
                word.line = line;
                text_image_coords.push(word);
            }
        }
        Ok(text_image_coords)
    }
}

#[cfg(test)]
#[allow(unused_imports)]
mod tests {
    use super::*;
    use crate::model_store::{ModelId, ModelStore, ModelStoreOptions};
    use console::Term;
    use rten_imageproc::Rect;

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-ocr"), ignore)]
    async fn test_recognise_png_file() -> AppResult<()> {
        let term = Term::stdout();
        let app_reporter = AppReporter::from(&term);
        let store = ModelStore::new(&ModelStoreOptions::default(), &app_reporter);
        let files = store.resolve(ModelId::Ocrs).await?;
        let ocrs = Ocrs::new(&files, &app_reporter)?;
        let image = image::open("test-fixtures/media/form-example.png")?;
        let text_image_coords = ocrs.image_to_text(image)?;
        assert!(text_image_coords.len() > 10);
        Ok(())
    }

    /// Places one character at `x..x+1`, all sharing the same 0..10 vertical extent, so a
    /// word's rect is easy to predict: the union of its characters' `x` ranges.
    fn char_at(c: char, x: i32) -> TextChar {
        TextChar {
            char: c,
            rect: Rect::from_tlbr(0, x, 10, x + 1),
        }
    }

    fn line(spec: &str) -> Vec<TextChar> {
        spec.chars()
            .enumerate()
            .map(|(i, c)| char_at(c, i as i32))
            .collect()
    }

    fn word_texts(words: &[TextImageCoords]) -> Vec<&str> {
        words.iter().map(|w| w.text.as_deref().unwrap()).collect()
    }

    #[test]
    fn three_words_separated_by_single_spaces_all_come_back() {
        let words = words_from_line(&line("a b c"));
        assert_eq!(word_texts(&words), vec!["a", "b", "c"]);
        // "c" is the last character of the line: this is the case the flush after the loop
        // exists for. Before that fix the loop drops it, because there is no trailing
        // separator to trigger a push.
        let c = &words[2];
        assert_eq!((c.x1, c.x2), (4.0, 5.0));
    }

    #[test]
    fn a_trailing_space_does_not_produce_an_empty_word() {
        let words = words_from_line(&line("ab "));
        assert_eq!(word_texts(&words), vec!["ab"]);
        assert_eq!((words[0].x1, words[0].x2), (0.0, 2.0));
    }

    #[test]
    fn a_single_word_line_yields_one_word() {
        let words = words_from_line(&line("hi"));
        assert_eq!(word_texts(&words), vec!["hi"]);
        assert_eq!((words[0].x1, words[0].x2), (0.0, 2.0));
    }

    #[test]
    fn double_spaces_do_not_produce_an_empty_word() {
        let words = words_from_line(&line("a  b"));
        assert_eq!(word_texts(&words), vec!["a", "b"]);
    }
}
