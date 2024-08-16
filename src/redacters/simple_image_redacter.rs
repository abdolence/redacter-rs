use crate::errors::AppError;
use crate::AppResult;
use bytes::Bytes;
use image::ImageFormat;
use mime::Mime;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PiiImageCoords {
    pub x1: f32,
    pub y1: f32,
    pub x2: f32,
    pub y2: f32,
    pub text: Option<String>,
}

pub fn redact_image_at_coords(
    mime: Mime,
    data: Bytes,
    pii_coords: Vec<PiiImageCoords>,
    approximation_factor: f32,
) -> AppResult<Bytes> {
    let image_format = ImageFormat::from_mime_type(&mime).ok_or_else(|| AppError::SystemError {
        message: format!("Unsupported image mime type: {}", mime),
    })?;
    let image = image::load_from_memory_with_format(&data, image_format)?;
    let mut image = image.to_rgba8();
    for PiiImageCoords { x1, y1, x2, y2, .. } in pii_coords {
        for x in
            ((x1 - x1 * approximation_factor) as u32)..((x2 + x2 * approximation_factor) as u32)
        {
            for y in
                ((y1 - y1 * approximation_factor) as u32)..((y2 + y2 * approximation_factor) as u32)
            {
                let safe_x = x.min(image.width() - 1).max(0);
                let safe_y = y.min(image.height() - 1).max(0);
                image.put_pixel(safe_x, safe_y, image::Rgba([0, 0, 0, 255]));
            }
        }
    }
    let mut output = std::io::Cursor::new(Vec::new());
    image.write_to(&mut output, image_format)?;
    Ok(output.into_inner().into())
}
