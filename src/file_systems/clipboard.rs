use crate::errors::AppError;
use crate::file_systems::{AbsoluteFilePath, FileSystemConnection, FileSystemRef, ListFilesResult};
use crate::file_tools::FileMatcher;
use crate::redacters::Redacters;
use crate::reporter::AppReporter;
use crate::AppResult;
use arboard::Clipboard;
use bytes::Bytes;
use futures::{Stream, TryStreamExt};
use image::{ImageBuffer, ImageFormat};
use rvstruct::ValueStruct;

pub struct ClipboardFileSystem<'a> {
    clipboard: Clipboard,
    reporter: &'a AppReporter<'a>,
}

impl<'a> ClipboardFileSystem<'a> {
    pub async fn new(root_path: &str, reporter: &'a AppReporter<'a>) -> AppResult<Self> {
        if root_path != "clipboard://" {
            return Err(AppError::SystemError {
                message: "Clipboard should be specified as clipboard://".into(),
            });
        }
        Ok(Self {
            clipboard: Clipboard::new()?,
            reporter,
        })
    }
}

impl<'a> FileSystemConnection<'a> for ClipboardFileSystem<'a> {
    async fn download(
        &mut self,
        _file_ref: Option<&FileSystemRef>,
    ) -> AppResult<(
        FileSystemRef,
        Box<dyn Stream<Item = AppResult<Bytes>> + Send + Sync + Unpin + 'static>,
    )> {
        let filename = format!(
            "{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs()
        );
        match self.clipboard.get().image() {
            Ok(image_data) => {
                let maybe_image: Option<image::RgbaImage> = image::ImageBuffer::from_raw(
                    image_data.width as u32,
                    image_data.height as u32,
                    image_data.bytes.into_owned(),
                );
                if let Some(image) = maybe_image {
                    let mut writer = std::io::Cursor::new(Vec::new());
                    image.write_to(&mut writer, ImageFormat::Png)?;
                    let png_image_bytes = writer.into_inner();
                    Ok((
                        FileSystemRef {
                            relative_path: format!("{}.png", filename).into(),
                            media_type: Some(mime::IMAGE_PNG),
                            file_size: Some(png_image_bytes.len()),
                        },
                        Box::new(futures::stream::iter(vec![Ok(bytes::Bytes::from(
                            png_image_bytes,
                        ))])),
                    ))
                } else {
                    Err(AppError::SystemError {
                        message: "Clipboard can't get any supported image format from clipboard://"
                            .into(),
                    })
                }
            }
            Err(_) => {
                let text = self.clipboard.get().text()?;
                Ok((
                    FileSystemRef {
                        relative_path: format!("{}.txt", filename).into(),
                        media_type: Some(mime::TEXT_PLAIN),
                        file_size: Some(text.len()),
                    },
                    Box::new(futures::stream::iter(vec![Ok(bytes::Bytes::from(text))])),
                ))
            }
        }
    }

    async fn upload<S: Stream<Item = AppResult<Bytes>> + Send + Unpin + Sync + 'static>(
        &mut self,
        input: S,
        file_ref: Option<&FileSystemRef>,
    ) -> AppResult<()> {
        match file_ref {
            Some(file_ref) => {
                if let Some(mime) = file_ref.media_type.clone() {
                    let all_chunks: Vec<bytes::Bytes> = input.try_collect().await?;
                    let all_bytes = all_chunks.concat();
                    if Redacters::is_mime_image(&mime) {
                        if let Some(image_format) = image::ImageFormat::from_mime_type(&mime) {
                            let image =
                                image::load_from_memory_with_format(&all_bytes, image_format)?;

                            let image_width = image.width() as usize;
                            let image_height = image.height() as usize;
                            let image_buf: image::RgbaImage = ImageBuffer::from(image);
                            let raw = image_buf.into_raw();
                            self.clipboard.set_image(arboard::ImageData {
                                width: image_width,
                                height: image_height,
                                bytes: raw.into(),
                            })?;

                            Ok(())
                        } else {
                            Err(AppError::SystemError {
                                message: "ClipboardFileSystem doesn't support this image format"
                                    .into(),
                            })
                        }
                    } else {
                        self.clipboard
                            .set_text(String::from_utf8_lossy(&all_bytes))?;
                        Ok(())
                    }
                } else {
                    Err(AppError::SystemError {
                        message: "ClipboardFileSystem requires MIME from source".into(),
                    })
                }
            }
            None => Err(AppError::SystemError {
                message: "FileSystemRef is required for ClipboardFileSystem".into(),
            }),
        }
    }

    async fn list_files(
        &mut self,
        _file_matcher: Option<&FileMatcher>,
        _max_files_limit: Option<usize>,
    ) -> AppResult<ListFilesResult> {
        self.reporter
            .report("Listing in clipboard is not supported")?;
        Ok(ListFilesResult::EMPTY)
    }

    async fn close(self) -> AppResult<()> {
        Ok(())
    }

    async fn has_multiple_files(&self) -> AppResult<bool> {
        Ok(false)
    }

    async fn accepts_multiple_files(&self) -> AppResult<bool> {
        Ok(false)
    }

    fn resolve(&self, file_ref: Option<&FileSystemRef>) -> AbsoluteFilePath {
        AbsoluteFilePath {
            file_path: format!(
                "clipboard://{}",
                file_ref
                    .map(|fr| fr.relative_path.value().to_string())
                    .unwrap_or("".to_string())
            ),
        }
    }
}

#[allow(unused_imports)]
mod tests {
    use super::*;
    use crate::file_systems::DetectFileSystem;
    use console::Term;
    use image::RgbaImage;

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-clibpoard"), ignore)]
    async fn upload_download_text_test() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let term = Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);

        let mut fs = DetectFileSystem::open("clipboard://", &reporter).await?;

        let test_content = "Test content";

        fs.upload(
            futures::stream::iter(vec![Ok(bytes::Bytes::from(test_content))]),
            Some(&FileSystemRef {
                relative_path: "temp_file.txt".into(),
                media_type: Some(mime::TEXT_PLAIN),
                file_size: Some(13),
            }),
        )
        .await?;

        let (file_ref, stream) = fs.download(None).await?;

        let downloaded_bytes: Vec<bytes::Bytes> = stream.try_collect().await?;
        let flattened_bytes = downloaded_bytes.concat();
        let downloaded_content = std::str::from_utf8(&flattened_bytes)?;
        assert_eq!(downloaded_content, test_content);
        assert_eq!(file_ref.media_type, Some(mime::TEXT_PLAIN));
        assert_eq!(file_ref.file_size, Some(test_content.len()));

        fs.close().await?;

        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-clibpoard"), ignore)]
    async fn upload_download_image_test() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let term = Term::stdout();
        let reporter: AppReporter = AppReporter::from(&term);

        let mut fs = DetectFileSystem::open("clipboard://", &reporter).await?;

        let test_content: image::RgbaImage = RgbaImage::new(100, 100);
        let mut writer = std::io::Cursor::new(Vec::new());
        test_content.write_to(&mut writer, ImageFormat::Png)?;
        let png_image_bytes = writer.into_inner();
        let png_images_bytes_len = png_image_bytes.len();

        fs.upload(
            futures::stream::iter(vec![Ok(bytes::Bytes::from(png_image_bytes))]),
            Some(&FileSystemRef {
                relative_path: "temp_file.png".into(),
                media_type: Some(mime::IMAGE_PNG),
                file_size: Some(png_images_bytes_len),
            }),
        )
        .await?;

        let (file_ref, stream) = fs.download(None).await?;

        let downloaded_bytes: Vec<bytes::Bytes> = stream.try_collect().await?;
        let flattened_bytes = downloaded_bytes.concat();
        let downloaded_content =
            image::load_from_memory_with_format(&flattened_bytes, ImageFormat::Png)?;
        assert_eq!(downloaded_content.width(), 100);
        assert_eq!(downloaded_content.height(), 100);
        assert_eq!(file_ref.media_type, Some(mime::IMAGE_PNG));
        assert_eq!(file_ref.file_size, Some(png_images_bytes_len));

        fs.close().await?;

        Ok(())
    }
}
