use crate::common_types::TextImageCoords;
use crate::AppResult;

pub trait Ocr {
    fn image_to_text(&self, image: image::DynamicImage) -> AppResult<Vec<TextImageCoords>>;
}
