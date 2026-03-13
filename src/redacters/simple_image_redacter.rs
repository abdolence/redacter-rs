use crate::common_types::TextImageCoords;
use crate::errors::AppError;
use crate::AppResult;
use bytes::Bytes;
use image::{ImageFormat, RgbImage};
use mime::Mime;

pub fn redact_image_at_coords(
    mime: Mime,
    data: Bytes,
    pii_coords: Vec<TextImageCoords>,
    approximation_factor: f32,
) -> AppResult<Bytes> {
    let image_format = ImageFormat::from_mime_type(&mime).ok_or_else(|| AppError::SystemError {
        message: format!("Unsupported image mime type: {mime}"),
    })?;
    let image = image::load_from_memory_with_format(&data, image_format)?;
    let mut image = image.to_rgb8();
    redact_rgba_image_at_coords(&mut image, &pii_coords, approximation_factor);
    let mut output = std::io::Cursor::new(Vec::new());
    image.write_to(&mut output, image_format)?;
    Ok(output.into_inner().into())
}

pub fn redact_rgba_image_at_coords(
    image: &mut RgbImage,
    pii_coords: &Vec<TextImageCoords>,
    approximation_factor: f32,
) {
    for TextImageCoords { x1, y1, x2, y2, .. } in pii_coords {
        for x in
            ((x1 - x1 * approximation_factor) as u32)..((x2 + x2 * approximation_factor) as u32)
        {
            for y in
                ((y1 - y1 * approximation_factor) as u32)..((y2 + y2 * approximation_factor) as u32)
            {
                let safe_x = x.min(image.width() - 1);
                let safe_y = y.min(image.height() - 1);
                image.put_pixel(safe_x, safe_y, image::Rgb([0, 0, 0]));
            }
        }
    }
}
