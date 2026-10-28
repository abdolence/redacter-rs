//! Detection over one file: every selected redacter reads the same original content, and
//! every detection is tagged with the redacter that made it. Nothing is chained or merged,
//! so two redacters that find the same value both report it.

use crate::args::RedacterType;
use crate::errors::AppError;
use crate::file_converters::FileConverters;
use crate::file_systems::FileSystemRef;
use crate::redacters::{
    Detection, DetectionLocation, OcrText, RedactSupport, Redacter, RedacterBaseOptions,
    RedacterDataItem, RedacterDataItemContent, Redacters, RedactionBlocked, StreamRedacter,
};
use crate::AppResult;
use futures::{Stream, TryStreamExt};
use image::ImageFormat;
use mime::Mime;

/// How one redacter reads a file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DetectRoute {
    /// The content as the file holds it; for a PDF, each page rendered to an image.
    Native,
    /// A CSV table read as plain text, for a redacter that reads text but not tables.
    TableAsText,
    /// The text OCR reads from the image, or from each rendered PDF page.
    Ocr,
}

struct PlannedDetector<'a> {
    redacter: &'a Redacters<'a>,
    route: DetectRoute,
}

/// Which redacters read a file, and how.
pub struct DetectPlan<'a> {
    detectors: Vec<PlannedDetector<'a>>,
    /// Why no redacter can read the file, when the reason is a converter this build lacks.
    pub blocked: Option<RedactionBlocked>,
}

impl<'a> DetectPlan<'a> {
    pub fn is_empty(&self) -> bool {
        self.detectors.is_empty()
    }

    fn routed(&self, route: DetectRoute) -> impl Iterator<Item = &'a Redacters<'a>> + '_ {
        self.detectors
            .iter()
            .filter(move |detector| detector.route == route)
            .map(|detector| detector.redacter)
    }

    async fn detect_in_text(
        &self,
        route: DetectRoute,
        item: &RedacterDataItem,
    ) -> AppResult<Vec<(RedacterType, Detection)>> {
        let mut detections = Vec::new();
        for redacter in self.routed(route) {
            detections.extend(redacter.tagged_detect(item).await?);
        }
        Ok(detections)
    }

    async fn detect_in_image(
        &self,
        item: &RedacterDataItem,
        page: Option<usize>,
    ) -> AppResult<Vec<(RedacterType, Detection)>> {
        let mut detections = Vec::new();
        for redacter in self.routed(DetectRoute::Native) {
            for (redacter_type, mut detection) in redacter.tagged_detect(item).await? {
                if let DetectionLocation::Image {
                    page: ref mut detected_page,
                    ..
                } = detection.location
                {
                    *detected_page = page;
                }
                detections.push((redacter_type, detection));
            }
        }
        Ok(detections)
    }
}

impl Redacters<'_> {
    async fn detects(&self, file_ref: &FileSystemRef, media_type: Mime) -> AppResult<bool> {
        let support = self
            .detect_support(&FileSystemRef {
                media_type: Some(media_type),
                ..file_ref.clone()
            })
            .await?;
        Ok(support == RedactSupport::Supported)
    }

    async fn tagged_detect(
        &self,
        item: &RedacterDataItem,
    ) -> AppResult<Vec<(RedacterType, Detection)>> {
        Ok(self
            .detect(item)
            .await?
            .into_iter()
            .map(|detection| (self.redacter_type(), detection))
            .collect())
    }
}

pub struct DetectStreamResult {
    pub detections: Vec<(RedacterType, Detection)>,
    /// Set when no redacter ran because OCR cannot read the image format.
    pub blocked: Option<RedactionBlocked>,
}

pub struct StreamDetector<'a> {
    redacter_base_options: &'a RedacterBaseOptions,
    file_converters: &'a FileConverters<'a>,
}

impl<'a> StreamDetector<'a> {
    pub fn new(
        redacter_base_options: &'a RedacterBaseOptions,
        file_converters: &'a FileConverters<'a>,
    ) -> Self {
        Self {
            redacter_base_options,
            file_converters,
        }
    }

    /// Picks, for every redacter on its own, how it reads this file. A PDF is always read as
    /// rendered pages, natively when the redacter reads images and with OCR otherwise.
    pub async fn create_detect_plan<'r>(
        &self,
        redacters: &'r [Redacters<'r>],
        file_ref: &FileSystemRef,
    ) -> AppResult<DetectPlan<'r>> {
        let mut plan = DetectPlan {
            detectors: Vec::new(),
            blocked: None,
        };
        let Some(media_type) = &file_ref.media_type else {
            return Ok(plan);
        };
        for redacter in redacters {
            if let Some(route) = self.route(redacter, file_ref, media_type).await? {
                plan.detectors.push(PlannedDetector { redacter, route });
            }
        }
        if plan.detectors.is_empty() {
            plan.blocked = self.blocked(media_type);
        }
        Ok(plan)
    }

    async fn route(
        &self,
        redacter: &Redacters<'_>,
        file_ref: &FileSystemRef,
        media_type: &Mime,
    ) -> AppResult<Option<DetectRoute>> {
        let ocr = self.file_converters.ocr.is_some();
        if Redacters::is_mime_pdf(media_type) {
            if self.file_converters.pdf_image_converter.is_none() {
                return Ok(None);
            }
            return Ok(if redacter.detects(file_ref, mime::IMAGE_PNG).await? {
                Some(DetectRoute::Native)
            } else if ocr && redacter.detects(file_ref, mime::TEXT_PLAIN).await? {
                Some(DetectRoute::Ocr)
            } else {
                None
            });
        }
        Ok(if redacter.detects(file_ref, media_type.clone()).await? {
            Some(DetectRoute::Native)
        } else if Redacters::is_mime_table(media_type)
            && redacter.detects(file_ref, mime::TEXT_PLAIN).await?
        {
            Some(DetectRoute::TableAsText)
        } else if Redacters::is_mime_image(media_type)
            && ocr
            && redacter.detects(file_ref, mime::TEXT_PLAIN).await?
        {
            Some(DetectRoute::Ocr)
        } else {
            None
        })
    }

    fn blocked(&self, media_type: &Mime) -> Option<RedactionBlocked> {
        let is_pdf = Redacters::is_mime_pdf(media_type);
        if is_pdf && self.file_converters.pdf_image_converter.is_none() {
            Some(RedactionBlocked::PdfRendererUnavailable)
        } else if (is_pdf || Redacters::is_mime_image(media_type))
            && self.file_converters.ocr.is_none()
        {
            Some(RedactionBlocked::OcrUnavailable)
        } else {
            None
        }
    }

    pub async fn detect_stream<
        S: Stream<Item = AppResult<bytes::Bytes>> + Send + Unpin + Sync + 'static,
    >(
        &self,
        input: S,
        plan: &DetectPlan<'_>,
        file_ref: &FileSystemRef,
    ) -> AppResult<DetectStreamResult> {
        let chunks: Vec<bytes::Bytes> = input.try_collect().await?;
        let data = bytes::Bytes::from(chunks.concat());
        let media_type = file_ref
            .media_type
            .clone()
            .ok_or_else(|| AppError::SystemError {
                message: "Media type is not provided to detect".to_string(),
            })?;
        if Redacters::is_mime_pdf(&media_type) {
            self.detect_in_pdf(data, plan, file_ref).await
        } else if Redacters::is_mime_image(&media_type) {
            self.detect_in_image(data, media_type, plan, file_ref).await
        } else if Redacters::is_mime_table(&media_type) {
            self.detect_in_table(data, plan, file_ref).await
        } else {
            let item = self.text_item(data, file_ref).await?;
            Ok(DetectStreamResult {
                detections: plan.detect_in_text(DetectRoute::Native, &item).await?,
                blocked: None,
            })
        }
    }

    fn readers(&self) -> StreamRedacter<'a> {
        StreamRedacter::new(self.redacter_base_options, self.file_converters)
    }

    async fn text_item(
        &self,
        data: bytes::Bytes,
        file_ref: &FileSystemRef,
    ) -> AppResult<RedacterDataItem> {
        self.readers()
            .stream_to_text_redact_item(one_chunk(data), file_ref)
            .await
    }

    async fn detect_in_table(
        &self,
        data: bytes::Bytes,
        plan: &DetectPlan<'_>,
        file_ref: &FileSystemRef,
    ) -> AppResult<DetectStreamResult> {
        let mut detections = Vec::new();
        if plan.routed(DetectRoute::Native).next().is_some() {
            let table = self
                .readers()
                .stream_to_table_redact_item(
                    self.redacter_base_options,
                    one_chunk(data.clone()),
                    file_ref,
                )
                .await?;
            for redacter in plan.routed(DetectRoute::Native) {
                detections.extend(redacter.tagged_detect(&table).await?);
            }
        }
        if plan.routed(DetectRoute::TableAsText).next().is_some() {
            let item = self.text_item(data, file_ref).await?;
            detections.extend(plan.detect_in_text(DetectRoute::TableAsText, &item).await?);
        }
        Ok(DetectStreamResult {
            detections,
            blocked: None,
        })
    }

    async fn detect_in_image(
        &self,
        data: bytes::Bytes,
        mime_type: Mime,
        plan: &DetectPlan<'_>,
        file_ref: &FileSystemRef,
    ) -> AppResult<DetectStreamResult> {
        let mut detections = Vec::new();
        let ran_natively = plan.routed(DetectRoute::Native).next().is_some();
        let mut blocked = None;
        if plan.routed(DetectRoute::Ocr).next().is_some() {
            match ImageFormat::from_mime_type(&mime_type) {
                Some(format) => {
                    let image = image::load_from_memory_with_format(&data, format)?;
                    detections.extend(self.detect_with_ocr(&image, None, plan, file_ref).await?);
                }
                None if ran_natively => {}
                None => blocked = Some(RedactionBlocked::OcrImageFormatNotSupported),
            }
        }
        let item = RedacterDataItem {
            content: RedacterDataItemContent::Image { mime_type, data },
            file_ref: file_ref.clone(),
        };
        detections.extend(plan.detect_in_image(&item, None).await?);
        Ok(DetectStreamResult {
            detections,
            blocked,
        })
    }

    async fn detect_in_pdf(
        &self,
        data: bytes::Bytes,
        plan: &DetectPlan<'_>,
        file_ref: &FileSystemRef,
    ) -> AppResult<DetectStreamResult> {
        let converter = self
            .file_converters
            .pdf_image_converter
            .as_ref()
            .ok_or_else(|| AppError::SystemError {
                message: "No PDF renderer is available to detect in a PDF".to_string(),
            })?;
        let mut detections = Vec::new();
        for (page, page_info) in converter
            .convert_to_images(data)?
            .pages
            .into_iter()
            .enumerate()
        {
            if plan.routed(DetectRoute::Ocr).next().is_some() {
                detections.extend(
                    self.detect_with_ocr(&page_info.page_as_images, Some(page + 1), plan, file_ref)
                        .await?,
                );
            }
            if plan.routed(DetectRoute::Native).next().is_some() {
                let mut png = std::io::Cursor::new(Vec::new());
                page_info
                    .page_as_images
                    .write_to(&mut png, ImageFormat::Png)?;
                let item = RedacterDataItem {
                    content: RedacterDataItemContent::Image {
                        mime_type: mime::IMAGE_PNG,
                        data: png.into_inner().into(),
                    },
                    file_ref: file_ref.clone(),
                };
                detections.extend(plan.detect_in_image(&item, Some(page + 1)).await?);
            }
        }
        Ok(DetectStreamResult {
            detections,
            blocked: None,
        })
    }

    /// Runs the OCR-routed redacters on the text OCR reads from `image`, and boxes every
    /// detection by the words it covers. A detection that covers no word, only the spaces
    /// between them, has no box and is dropped.
    async fn detect_with_ocr(
        &self,
        image: &image::DynamicImage,
        page: Option<usize>,
        plan: &DetectPlan<'_>,
        file_ref: &FileSystemRef,
    ) -> AppResult<Vec<(RedacterType, Detection)>> {
        let ocr = self
            .file_converters
            .ocr
            .as_ref()
            .ok_or_else(|| AppError::SystemError {
                message: "No OCR engine is available to detect in an image".to_string(),
            })?;
        let ocr_text = OcrText::from(ocr.image_to_text(image.clone())?.as_slice());
        let item = RedacterDataItem {
            content: RedacterDataItemContent::Value(ocr_text.text.clone()),
            file_ref: file_ref.clone(),
        };
        let mut detections = Vec::new();
        for redacter in plan.routed(DetectRoute::Ocr) {
            for (redacter_type, detection) in redacter.tagged_detect(&item).await? {
                let DetectionLocation::Text { start, end, .. } = detection.location else {
                    continue;
                };
                match ocr_text.image_location(start, end, page) {
                    Some(location) => detections.push((
                        redacter_type,
                        Detection {
                            location,
                            ..detection
                        },
                    )),
                    None => tracing::debug!(
                        redacter = %redacter.redacter_type(),
                        start,
                        end,
                        "dropping a detection that covers no OCR word"
                    ),
                }
            }
        }
        Ok(detections)
    }
}

impl OcrText {
    /// The box around every word that overlaps the half-open byte range `start..end` of the
    /// text, or `None` when the range covers no word.
    fn image_location(
        &self,
        start: usize,
        end: usize,
        page: Option<usize>,
    ) -> Option<DetectionLocation> {
        self.words
            .iter()
            .filter(|word| word.span.start < end && start < word.span.end)
            .map(|word| &word.coords)
            .fold(None, |covered, coords| {
                Some(match covered {
                    None => (coords.x1, coords.y1, coords.x2, coords.y2),
                    Some((x1, y1, x2, y2)) => (
                        coords.x1.min(x1),
                        coords.y1.min(y1),
                        coords.x2.max(x2),
                        coords.y2.max(y2),
                    ),
                })
            })
            .map(|(x1, y1, x2, y2)| DetectionLocation::Image {
                page,
                x1,
                y1,
                x2,
                y2,
            })
    }
}

fn one_chunk(
    data: bytes::Bytes,
) -> impl Stream<Item = AppResult<bytes::Bytes>> + Send + Sync + Unpin + 'static {
    futures::stream::iter(vec![Ok(data)])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common_types::TextImageCoords;
    use crate::file_converters::ocr::Ocr;
    #[cfg(feature = "pdf-render")]
    use crate::file_converters::pdf::{PdfInfo, PdfPageInfo, PdfToImage};
    use crate::redacters::{
        parse_inline_rule, DetectionLabel, LocalRulesRedacter, LocalRulesRedacterOptions, RuleGroup,
    };
    use crate::reporter::AppReporter;
    use std::collections::BTreeSet;

    struct FakeOcr {
        words: Vec<TextImageCoords>,
    }

    impl Ocr for FakeOcr {
        fn image_to_text(&self, _image: image::DynamicImage) -> AppResult<Vec<TextImageCoords>> {
            Ok(self.words.clone())
        }
    }

    /// Renders as many blank pages as it is told; the OCR fake decides what they say.
    #[cfg(feature = "pdf-render")]
    struct FakePdf {
        pages: usize,
    }

    #[cfg(feature = "pdf-render")]
    impl PdfToImage for FakePdf {
        fn convert_to_images(&self, _pdf_bytes: bytes::Bytes) -> AppResult<PdfInfo> {
            Ok(PdfInfo {
                pages: (0..self.pages)
                    .map(|_| PdfPageInfo {
                        height: pdfium_render::prelude::PdfPoints::ZERO,
                        width: pdfium_render::prelude::PdfPoints::ZERO,
                        page_as_images: image::DynamicImage::new_rgb8(4, 4),
                    })
                    .collect(),
            })
        }

        fn images_to_pdf(&self, _pdf_info: PdfInfo) -> AppResult<bytes::Bytes> {
            unreachable!("detection never writes a PDF")
        }
    }

    fn word(text: &str, line: usize, x1: f32, y1: f32, x2: f32, y2: f32) -> TextImageCoords {
        TextImageCoords {
            x1,
            y1,
            x2,
            y2,
            text: Some(text.to_string()),
            line,
        }
    }

    /// The email and phone rows of the form fixture, as OCR reads them: each label on a line
    /// of its own beside its value.
    fn form_words() -> Vec<TextImageCoords> {
        vec![
            word("Email:", 0, 50.0, 193.0, 125.0, 218.0),
            word("john.smith@example.com", 1, 310.0, 193.0, 622.0, 218.0),
            word("Phone:", 2, 50.0, 263.0, 131.0, 288.0),
            word("+1", 3, 310.0, 263.0, 338.0, 288.0),
            word("(555)", 3, 345.0, 262.0, 415.0, 290.0),
            word("123-4567", 3, 425.0, 263.0, 540.0, 288.0),
        ]
    }

    fn base_options() -> RedacterBaseOptions {
        RedacterBaseOptions {
            allow_unsupported_copies: false,
            csv_headers_disable: false,
            csv_delimiter: None,
            sampling_size: None,
            limit_dlp_requests: None,
        }
    }

    fn reporter() -> &'static AppReporter<'static> {
        // Leaked so the reporter, and the terminal it borrows, outlive the redacters built
        // from it in a short-lived test process.
        let term: &'static console::Term = Box::leak(Box::new(console::Term::stdout()));
        Box::leak(Box::new(AppReporter::from(term)))
    }

    async fn local_rules(groups: BTreeSet<RuleGroup>, inline_rules: &[&str]) -> Redacters<'static> {
        let user_rules = inline_rules
            .iter()
            .map(|rule| parse_inline_rule(rule).unwrap())
            .collect();
        Redacters::LocalRules(
            LocalRulesRedacter::new(LocalRulesRedacterOptions { groups, user_rules }, reporter())
                .await
                .unwrap(),
        )
    }

    async fn email_and_phone_rules() -> Redacters<'static> {
        local_rules(
            [RuleGroup::Email, RuleGroup::Phone].into_iter().collect(),
            &[],
        )
        .await
    }

    fn file_ref(name: &str, media_type: Mime) -> FileSystemRef {
        FileSystemRef {
            relative_path: name.into(),
            media_type: Some(media_type),
            file_size: None,
        }
    }

    fn white_png() -> bytes::Bytes {
        let image = image::RgbImage::from_pixel(8, 8, image::Rgb([255, 255, 255]));
        let mut out = std::io::Cursor::new(Vec::new());
        image.write_to(&mut out, ImageFormat::Png).unwrap();
        out.into_inner().into()
    }

    async fn scan(
        converters: &FileConverters<'_>,
        redacters: &[Redacters<'static>],
        file_ref: &FileSystemRef,
        data: bytes::Bytes,
    ) -> AppResult<DetectStreamResult> {
        let options = base_options();
        let detector = StreamDetector::new(&options, converters);
        let plan = detector.create_detect_plan(redacters, file_ref).await?;
        assert!(!plan.is_empty(), "nothing can read {file_ref:?}");
        detector
            .detect_stream(one_chunk(data), &plan, file_ref)
            .await
    }

    fn located(result: &DetectStreamResult) -> Vec<(DetectionLocation, &str)> {
        result
            .detections
            .iter()
            .map(|(_, detection)| (detection.location, detection.value.as_str()))
            .collect()
    }

    #[tokio::test]
    async fn an_ocr_detection_is_boxed_by_the_words_it_covers() {
        let mut converters = FileConverters::new();
        converters.ocr = Some(Box::new(FakeOcr {
            words: form_words(),
        }));
        let redacters = vec![email_and_phone_rules().await];

        let result = scan(
            &converters,
            &redacters,
            &file_ref("form.png", mime::IMAGE_PNG),
            white_png(),
        )
        .await
        .unwrap();

        assert_eq!(
            located(&result),
            vec![
                (
                    DetectionLocation::Image {
                        page: None,
                        x1: 310.0,
                        y1: 193.0,
                        x2: 622.0,
                        y2: 218.0
                    },
                    "john.smith@example.com"
                ),
                (
                    DetectionLocation::Image {
                        page: None,
                        x1: 310.0,
                        y1: 262.0,
                        x2: 540.0,
                        y2: 290.0
                    },
                    "+1 (555) 123-4567"
                ),
            ]
        );
        assert!(result
            .detections
            .iter()
            .all(|(redacter, _)| *redacter == RedacterType::LocalRules));
    }

    #[tokio::test]
    async fn an_image_format_ocr_cannot_decode_is_blocked_and_reports_nothing() {
        let mut converters = FileConverters::new();
        converters.ocr = Some(Box::new(FakeOcr {
            words: form_words(),
        }));
        let redacters = vec![email_and_phone_rules().await];

        for media_type in [mime::IMAGE_SVG, "image/heic".parse().unwrap()] {
            let result = scan(
                &converters,
                &redacters,
                &file_ref("picture", media_type.clone()),
                white_png(),
            )
            .await
            .unwrap();
            assert_eq!(
                result.blocked,
                Some(RedactionBlocked::OcrImageFormatNotSupported),
                "{media_type}"
            );
            assert!(result.detections.is_empty(), "{media_type}");
        }
    }

    #[cfg(feature = "pdf-render")]
    #[tokio::test]
    async fn every_page_of_a_pdf_is_read_and_named() {
        let mut converters = FileConverters::new();
        converters.ocr = Some(Box::new(FakeOcr {
            words: form_words(),
        }));
        converters.pdf_image_converter = Some(Box::new(FakePdf { pages: 2 }));
        let redacters = vec![local_rules([RuleGroup::Email].into_iter().collect(), &[]).await];

        let result = scan(
            &converters,
            &redacters,
            &file_ref("form.pdf", mime::APPLICATION_PDF),
            bytes::Bytes::from_static(b"%PDF"),
        )
        .await
        .unwrap();

        let pages: Vec<Option<usize>> = result
            .detections
            .iter()
            .map(|(_, detection)| match detection.location {
                DetectionLocation::Image { page, .. } => page,
                other => panic!("a PDF page yields image locations, got {other:?}"),
            })
            .collect();
        assert_eq!(pages, vec![Some(1), Some(2)]);
    }

    /// Redaction chains redacters, so the second one sees the first one's `[REDACTED]`;
    /// detection must not, or a value two redacters both recognise is reported once.
    #[tokio::test]
    async fn every_redacter_reads_the_original_content() {
        let converters = FileConverters::new();
        let redacters = vec![
            local_rules([RuleGroup::Email].into_iter().collect(), &[]).await,
            local_rules(BTreeSet::new(), &["contact=a@b\\.io"]).await,
        ];

        let result = scan(
            &converters,
            &redacters,
            &file_ref("note.txt", mime::TEXT_PLAIN),
            bytes::Bytes::from_static(b"mail a@b.io"),
        )
        .await
        .unwrap();

        let labels: Vec<&DetectionLabel> = result
            .detections
            .iter()
            .map(|(_, detection)| &detection.label)
            .collect();
        assert_eq!(
            labels,
            vec![
                &DetectionLabel::new("email"),
                &DetectionLabel::new("contact")
            ]
        );
    }

    #[tokio::test]
    async fn a_csv_file_is_read_as_a_table_of_cells() {
        let converters = FileConverters::new();
        let redacters = vec![local_rules([RuleGroup::Email].into_iter().collect(), &[]).await];

        let result = scan(
            &converters,
            &redacters,
            &file_ref("customers.csv", mime::TEXT_CSV),
            bytes::Bytes::from_static(b"id,contact\n1,none\n2,a@b.io\n"),
        )
        .await
        .unwrap();

        assert_eq!(
            located(&result),
            vec![(
                DetectionLocation::TableCell {
                    row: 2,
                    column: 2,
                    start: 0,
                    end: 6
                },
                "a@b.io"
            )]
        );
    }

    /// Ink extents of two values of the form, measured from the PNG fixture and from page 1
    /// of the PDF fixture as pdfium renders it (one pixel per point): `(x1, y1, x2, y2)`. The
    /// phone number is three OCR words, so its box is a union.
    const FORM_FIXTURE_INK: [(&str, Option<usize>, &str, [f32; 4]); 4] = [
        (
            "test-fixtures/media/form-example.png",
            None,
            "email",
            [310.0, 195.0, 620.0, 218.0],
        ),
        (
            "test-fixtures/media/form-example.png",
            None,
            "phone-international",
            [313.0, 265.0, 538.0, 286.0],
        ),
        (
            "test-fixtures/documents/customer-form.pdf",
            Some(1),
            "email",
            [97.0, 148.0, 234.0, 159.0],
        ),
        (
            "test-fixtures/documents/customer-form.pdf",
            Some(1),
            "phone-international",
            [102.0, 173.0, 200.0, 184.0],
        ),
    ];

    /// Pdfium binds once per process, so the image and the PDF share one set of converters.
    #[tokio::test]
    #[cfg_attr(not(feature = "ci-ocr"), ignore)]
    async fn detections_in_the_form_fixtures_are_boxed_where_the_values_are_printed() {
        use crate::model_store::{ModelStore, ModelStoreOptions};
        use crate::redacters::test_support::pdfium_test_lock;
        let _guard = pdfium_test_lock().lock().await;
        let store = ModelStore::new(&ModelStoreOptions::default(), reporter());
        let converters = FileConverters::new()
            .init(reporter(), &store, true)
            .await
            .unwrap();
        assert!(
            converters.ocr.is_some() && converters.pdf_image_converter.is_some(),
            "the ci-ocr feature needs the OCR model and pdfium"
        );
        let redacters = vec![email_and_phone_rules().await];

        for (path, page, label, [ink_x1, ink_y1, ink_x2, ink_y2]) in FORM_FIXTURE_INK {
            let media_type = mime_guess::from_path(path).first().unwrap();
            let data = std::fs::read(path).unwrap();
            let result = scan(
                &converters,
                &redacters,
                &file_ref(path, media_type),
                data.into(),
            )
            .await
            .unwrap();
            let (_, found) = result
                .detections
                .iter()
                .find(|(_, detection)| detection.label == DetectionLabel::new(label))
                .unwrap_or_else(|| panic!("{path}: no {label} found"));
            let DetectionLocation::Image {
                page: found_page,
                x1,
                y1,
                x2,
                y2,
            } = found.location
            else {
                panic!("{path}: an image location is expected, got {found:?}");
            };
            assert_eq!(found_page, page, "{path}");
            assert!(
                x1 <= ink_x1 + 2.0
                    && y1 <= ink_y1 + 2.0
                    && x2 >= ink_x2 - 2.0
                    && y2 >= ink_y2 - 2.0,
                "{path}: the {label} box {:?} does not cover its ink",
                (x1, y1, x2, y2)
            );
            assert!(
                x2 - x1 <= ink_x2 - ink_x1 + 10.0 && y2 - y1 <= ink_y2 - ink_y1 + 10.0,
                "{path}: the {label} box {:?} reaches past its value",
                (x1, y1, x2, y2)
            );
        }
    }

    #[tokio::test]
    async fn an_image_or_pdf_without_its_converters_is_blocked_and_unknown_types_are_not() {
        let converters = FileConverters::new();
        let options = base_options();
        let detector = StreamDetector::new(&options, &converters);
        let redacters = vec![email_and_phone_rules().await];

        for (file, blocked) in [
            (
                file_ref("form.pdf", mime::APPLICATION_PDF),
                Some(RedactionBlocked::PdfRendererUnavailable),
            ),
            (
                file_ref("form.png", mime::IMAGE_PNG),
                Some(RedactionBlocked::OcrUnavailable),
            ),
            (
                file_ref("archive.bin", mime::APPLICATION_OCTET_STREAM),
                None,
            ),
        ] {
            let plan = detector
                .create_detect_plan(&redacters, &file)
                .await
                .unwrap();
            assert!(plan.is_empty(), "{file:?}");
            assert_eq!(plan.blocked, blocked, "{file:?}");
        }
    }
}
