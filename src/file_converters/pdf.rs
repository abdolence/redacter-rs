use crate::AppResult;
use gcloud_sdk::prost::bytes;

#[derive(Debug, Clone)]
pub struct PdfInfo {
    pub pages: Vec<PdfPageInfo>,
}

#[derive(Debug, Clone)]
pub struct PdfPageInfo {
    // Only read by the pdf-render feature's converter and its callers in
    // stream_redacter.rs; unread when that feature is disabled.
    #[cfg_attr(not(feature = "pdf-render"), allow(dead_code))]
    pub height: PdfPoints,
    #[cfg_attr(not(feature = "pdf-render"), allow(dead_code))]
    pub width: PdfPoints,
    pub page_as_images: image::DynamicImage,
}

#[cfg(feature = "pdf-render")]
type PdfPoints = pdfium_render::prelude::PdfPoints;

#[cfg(not(feature = "pdf-render"))]
type PdfPoints = f32;

pub trait PdfToImage {
    fn convert_to_images(&self, pdf_bytes: bytes::Bytes) -> AppResult<PdfInfo>;

    fn images_to_pdf(&self, pdf_info: PdfInfo) -> AppResult<bytes::Bytes>;
}
