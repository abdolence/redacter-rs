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
    let width = image.width();
    let height = image.height();
    for TextImageCoords { x1, y1, x2, y2, .. } in pii_coords {
        // Pad each box by `approximation_factor` of its own size, not of its distance
        // from the origin, so a box far from the origin isn't padded far more than one
        // near it. Clamp everything to non-negative coordinates and to the image bounds
        // up front, rather than clamping each out-of-range pixel to the last row/column
        // inside the loop, which would repaint that edge repeatedly.
        let pad_x = (x2 - x1).max(0.0) * approximation_factor;
        let pad_y = (y2 - y1).max(0.0) * approximation_factor;

        let start_x = (x1 - pad_x).max(0.0) as u32;
        let end_x = ((x2 + pad_x).max(0.0) as u32).min(width);
        let start_y = (y1 - pad_y).max(0.0) as u32;
        let end_y = ((y2 + pad_y).max(0.0) as u32).min(height);

        for x in start_x..end_x {
            for y in start_y..end_y {
                image.put_pixel(x, y, image::Rgb([0, 0, 0]));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn white_image(width: u32, height: u32) -> RgbImage {
        RgbImage::from_pixel(width, height, image::Rgb([255, 255, 255]))
    }

    fn is_black(image: &RgbImage, x: u32, y: u32) -> bool {
        *image.get_pixel(x, y) == image::Rgb([0, 0, 0])
    }

    #[test]
    fn pads_box_by_its_own_size_not_by_distance_from_origin() {
        let mut image = white_image(100, 100);
        let coords = vec![TextImageCoords {
            x1: 40.0,
            y1: 40.0,
            x2: 60.0,
            y2: 50.0,
            text: None,
        }];
        redact_rgba_image_at_coords(&mut image, &coords, 0.5);

        // Box is 20x10, padded by 50% of its own size: x in 30..70, y in 35..55.
        for x in 30..70 {
            for y in 35..55 {
                assert!(is_black(&image, x, y), "expected ({x},{y}) to be black");
            }
        }

        // Just outside the padded box on every side stays untouched.
        assert!(!is_black(&image, 29, 45), "left of box should stay white");
        assert!(!is_black(&image, 70, 45), "right of box should stay white");
        assert!(!is_black(&image, 50, 34), "above box should stay white");
        assert!(!is_black(&image, 50, 55), "below box should stay white");
    }

    #[test]
    fn box_touching_origin_does_not_panic_and_stays_in_bounds() {
        let mut image = white_image(50, 50);
        let coords = vec![TextImageCoords {
            x1: 0.0,
            y1: 0.0,
            x2: 10.0,
            y2: 10.0,
            text: None,
        }];
        redact_rgba_image_at_coords(&mut image, &coords, 0.5);

        // Padding would push the start below zero; it must clamp to 0 rather than panic.
        assert!(is_black(&image, 0, 0));
        assert!(is_black(&image, 14, 14));
    }

    #[test]
    fn box_beyond_image_edge_is_clamped_and_does_not_paint_unrelated_pixels() {
        let mut image = white_image(100, 100);
        let coords = vec![TextImageCoords {
            x1: 150.0,
            y1: 150.0,
            x2: 200.0,
            y2: 200.0,
            text: None,
        }];
        redact_rgba_image_at_coords(&mut image, &coords, 0.1);

        // The box lies entirely outside the image even after padding, so nothing is
        // painted, and in particular the last row/column must not be repainted as a
        // side effect of clamping out-of-range coordinates.
        assert!(!is_black(&image, 99, 99));
        assert!(!is_black(&image, 0, 0));
    }
}
