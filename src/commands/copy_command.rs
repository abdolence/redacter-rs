use crate::errors::AppError;
use crate::file_converters::FileConverters;
use crate::file_systems::{DetectFileSystem, FileSystemConnection, FileSystemRef};
use crate::file_tools::{FileMatcher, FileMatcherResult, FileMimeOverride};
use crate::redacters::{
    RedacterBaseOptions, RedacterOptions, RedacterThrottler, Redacters, StreamRedacter,
};
use crate::reporter::AppReporter;
use crate::AppResult;
use console::{pad_str, Alignment, Style, Term};
use futures::Stream;
use gcloud_sdk::prost::bytes;
use indicatif::*;
use serde::Serialize;
use std::error::Error;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Serialize)]
pub struct CopyCommandResult {
    pub files_copied: usize,
    pub files_redacted: usize,
    pub files_skipped: usize,
}

#[derive(Debug, Clone)]
pub struct CopyCommandOptions {
    pub file_matcher: FileMatcher,
    pub file_mime_override: FileMimeOverride,
    pub max_files_limit: Option<usize>,
}

impl CopyCommandOptions {
    pub fn new(
        filename_filter: Option<globset::Glob>,
        max_size_limit: Option<usize>,
        max_files_limit: Option<usize>,
        mime_override: Vec<(mime::Mime, globset::Glob)>,
    ) -> Self {
        let filename_matcher = filename_filter
            .as_ref()
            .map(|filter| filter.compile_matcher());
        CopyCommandOptions {
            file_matcher: FileMatcher::new(filename_matcher, max_size_limit),
            file_mime_override: FileMimeOverride::new(mime_override),
            max_files_limit,
        }
    }
}

pub async fn command_copy(
    term: &Term,
    source: &str,
    destination: &str,
    options: CopyCommandOptions,
    redacter_options: Option<RedacterOptions>,
) -> AppResult<CopyCommandResult> {
    let term_reporter = AppReporter::from(term);
    let file_converters = FileConverters::new().init(&term_reporter).await?;

    report_copy_info(
        term,
        source,
        destination,
        &redacter_options,
        &file_converters,
    )
    .await?;

    let bar = ProgressBar::new(1);
    bar.set_style(
        ProgressStyle::with_template(
            "{spinner:.green} [{elapsed_precise}] [{wide_bar:.green/237}] {pos:>3}/{len:3}",
        )?
        .progress_chars("━>─"),
    );
    bar.enable_steady_tick(Duration::from_millis(100));
    let app_reporter = AppReporter::from(&bar);

    let mut source_fs = DetectFileSystem::open(source, &app_reporter).await?;
    let mut destination_fs = DetectFileSystem::open(destination, &app_reporter).await?;
    let mut redacter_throttler = redacter_options
        .as_ref()
        .and_then(|o| o.base_options.limit_dlp_requests.clone())
        .map(|limit| limit.to_throttling_counter());

    let maybe_redacters = match redacter_options {
        Some(options) => {
            let mut redacters = Vec::with_capacity(options.provider_options.len());
            for provider_options in options.provider_options {
                let redacter = Redacters::new_redacter(provider_options, &app_reporter).await?;
                redacters.push(redacter);
            }
            Some((options.base_options, redacters))
        }
        None => None,
    };

    let copy_result: AppResult<CopyCommandResult> = if source_fs.has_multiple_files().await? {
        if !destination_fs.accepts_multiple_files().await? {
            return Err(AppError::DestinationDoesNotSupportMultipleFiles {
                destination: destination.to_string(),
            });
        }
        bar.println("Copying directory and listing source files...");
        let source_files_result = source_fs
            .list_files(Some(&options.file_matcher), options.max_files_limit)
            .await?;
        let source_files: Vec<FileSystemRef> = source_files_result.files;
        let files_found = source_files.len();
        let files_total_size: usize = source_files
            .iter()
            .map(|file| file.file_size.unwrap_or(0))
            .sum();
        let bold_style = Style::new().bold();
        bar.println(
            format!(
                "Found {} files. Total size: {}",
                bold_style.apply_to(files_found),
                bold_style.apply_to(HumanBytes(files_total_size as u64))
            )
            .as_str(),
        );

        bar.set_length(files_found as u64);

        let mut total_files_copied = 0;
        let mut total_files_redacted = 0;
        let mut total_files_skipped = source_files_result.skipped;
        for source_file in source_files {
            match transfer_and_redact_file(
                term,
                &bar,
                Some(&source_file),
                &mut source_fs,
                &mut destination_fs,
                &options,
                &maybe_redacters,
                &file_converters,
                &mut redacter_throttler,
            )
            .await?
            {
                TransferFileResult::Copied => total_files_copied += 1,
                TransferFileResult::RedactedAndCopied => {
                    total_files_redacted += 1;
                    total_files_copied += 1;
                }
                TransferFileResult::Skipped => total_files_skipped += 1,
            }
        }
        Ok(CopyCommandResult {
            files_copied: total_files_copied,
            files_redacted: total_files_redacted,
            files_skipped: total_files_skipped,
        })
    } else {
        Ok(
            match transfer_and_redact_file(
                term,
                &bar,
                None,
                &mut source_fs,
                &mut destination_fs,
                &options,
                &maybe_redacters,
                &file_converters,
                &mut redacter_throttler,
            )
            .await?
            {
                TransferFileResult::Copied => CopyCommandResult {
                    files_copied: 1,
                    files_redacted: 0,
                    files_skipped: 0,
                },
                TransferFileResult::RedactedAndCopied => CopyCommandResult {
                    files_copied: 1,
                    files_redacted: 1,
                    files_skipped: 0,
                },
                TransferFileResult::Skipped => CopyCommandResult {
                    files_copied: 0,
                    files_redacted: 0,
                    files_skipped: 1,
                },
            },
        )
    };

    destination_fs.close().await?;
    source_fs.close().await?;
    copy_result
}

async fn report_copy_info(
    term: &Term,
    source: &str,
    destination: &str,
    redacter_options: &Option<RedacterOptions>,
    file_converters: &FileConverters<'_>,
) -> AppResult<()> {
    let bold_style = Style::new().bold();
    let redacted_output = if let Some(ref options) = redacter_options.as_ref() {
        bold_style
            .clone()
            .green()
            .apply_to(format!("✓ Yes ({})", options))
    } else {
        bold_style.clone().red().apply_to("✗ No".to_string())
    };
    let sampling_output = if let Some(ref sampling_size) = redacter_options
        .as_ref()
        .and_then(|o| o.base_options.sampling_size)
    {
        Style::new().apply_to(format!("{sampling_size} bytes."))
    } else {
        Style::new().dim().apply_to("-".to_string())
    };

    let converter_style = Style::new();
    let pdf_support_output = if file_converters.pdf_image_converter.is_some() {
        converter_style
            .clone()
            .green()
            .apply_to("✓ Yes".to_string())
    } else {
        converter_style.clone().dim().apply_to("✗ No".to_string())
    };

    let ocr_support_output = if file_converters.ocr.is_some() {
        converter_style
            .clone()
            .green()
            .apply_to("✓ Yes".to_string())
    } else {
        converter_style.clone().dim().apply_to("✗ No".to_string())
    };

    term.write_line(
        format!(
            "Copying from {} to {}.\nRedacting: {}. | Sampling: {} | PDF to image support: {} | OCR support: {}\n",
            bold_style.clone().white().apply_to(source),
            bold_style.clone().yellow().apply_to(destination),
            redacted_output,
            sampling_output,
            pdf_support_output,
            ocr_support_output,
        )
        .as_str(),
    )?;
    Ok(())
}

enum TransferFileResult {
    Copied,
    RedactedAndCopied,
    Skipped,
}

const PROCESSING_MEDIA_TYPE_WIDTH: usize = 28;
const PROCESSING_MIN_PATH_WIDTH: usize = 8;
const PROCESSING_FALLBACK_WIDTH: usize = 80;

/// Truncates `s` to at most `width` display columns, keeping a prefix and a
/// (slightly longer) suffix joined by a single `…`, so the most identifying
/// part of a path (its file name) tends to survive. Widths are measured with
/// `console::measure_text_width` so multi-byte and wide characters are
/// accounted for rather than counted as one byte each.
fn truncate_middle(s: &str, width: usize) -> String {
    if width == 0 {
        return String::new();
    }
    if console::measure_text_width(s) <= width {
        return s.to_string();
    }
    if width == 1 {
        return "…".to_string();
    }
    let budget = width - 1; // reserve one column for the ellipsis
    let head_budget = budget / 2;
    let tail_budget = budget - head_budget;

    let chars: Vec<char> = s.chars().collect();

    let mut head = String::new();
    let mut used = 0;
    for &c in &chars {
        let w = console::measure_text_width(&c.to_string());
        if used + w > head_budget {
            break;
        }
        head.push(c);
        used += w;
    }

    let mut tail = String::new();
    used = 0;
    for &c in chars.iter().rev() {
        let w = console::measure_text_width(&c.to_string());
        if used + w > tail_budget {
            break;
        }
        tail.insert(0, c);
        used += w;
    }

    format!("{head}…{tail}")
}

/// Fits `s` into exactly `width` display columns: pads short values on the
/// right, and middle-truncates values that overflow.
fn fit_column(s: &str, width: usize) -> String {
    if console::measure_text_width(s) <= width {
        pad_str(s, width, Alignment::Left, None).to_string()
    } else {
        truncate_middle(s, width)
    }
}

/// Builds the "Processing ... to ... Size: ..." progress line so it fits
/// within `width` display columns instead of wrapping on its own padding.
/// The two path columns share whatever room is left after the literal text,
/// the fixed media-type column and the (unpadded) size column, split evenly
/// with a sane minimum so narrow terminals degrade to truncated names rather
/// than zero-width columns.
fn format_processing_line(
    width: usize,
    source_path: &str,
    destination_path: &str,
    media_type: &str,
    size: &str,
    bold_style: &Style,
) -> String {
    let width = if width == 0 {
        PROCESSING_FALLBACK_WIDTH
    } else {
        width
    };

    let literal_width = console::measure_text_width("Processing ")
        + console::measure_text_width(" to ")
        + console::measure_text_width(" ")
        + console::measure_text_width(" Size: ");
    let size_width = console::measure_text_width(size);

    let available_for_paths = width
        .saturating_sub(literal_width)
        .saturating_sub(PROCESSING_MEDIA_TYPE_WIDTH)
        .saturating_sub(size_width);
    let path_width = (available_for_paths / 2).max(PROCESSING_MIN_PATH_WIDTH);

    let source_display = fit_column(source_path, path_width);
    let destination_display = fit_column(destination_path, path_width);
    let media_type_display = fit_column(media_type, PROCESSING_MEDIA_TYPE_WIDTH);

    format!(
        "Processing {} to {} {} Size: {}",
        bold_style.apply_to(source_display),
        bold_style.apply_to(destination_display),
        media_type_display,
        bold_style.apply_to(size)
    )
}

#[allow(clippy::too_many_arguments)]
async fn transfer_and_redact_file<
    'a,
    SFS: FileSystemConnection<'a>,
    DFS: FileSystemConnection<'a>,
>(
    term: &Term,
    bar: &ProgressBar,
    source_file_ref: Option<&FileSystemRef>,
    source_fs: &mut SFS,
    destination_fs: &mut DFS,
    options: &CopyCommandOptions,
    redacter: &Option<(RedacterBaseOptions, Vec<Redacters<'a>>)>,
    file_converters: &FileConverters<'a>,
    redacter_throttler: &mut Option<RedacterThrottler>,
) -> AppResult<TransferFileResult> {
    let bold_style = Style::new().bold().white();
    let (base_file_ref, source_reader) = source_fs.download(source_file_ref).await?;

    let base_resolved_file_ref = source_fs.resolve(Some(&base_file_ref));
    match options.file_matcher.matches(&base_file_ref) {
        FileMatcherResult::SkippedDueToSize | FileMatcherResult::SkippedDueToName => {
            bar.inc(1);
            return Ok(TransferFileResult::Skipped);
        }
        FileMatcherResult::Matched => {}
    }

    let file_ref = source_file_ref.unwrap_or(&base_file_ref);

    let dest_file_ref = FileSystemRef {
        relative_path: file_ref.relative_path.clone(),
        media_type: file_ref.media_type.clone(),
        file_size: file_ref.file_size,
    };
    bar.println(
        format_processing_line(
            term.width() as usize,
            &base_resolved_file_ref.file_path,
            destination_fs
                .resolve(Some(&dest_file_ref))
                .file_path
                .as_str(),
            file_ref
                .media_type
                .as_ref()
                .map(|media_type| media_type.to_string())
                .unwrap_or_else(|| "unknown".to_string())
                .as_str(),
            HumanBytes(file_ref.file_size.map(|sz| sz as u64).unwrap_or(0_u64))
                .to_string()
                .as_str(),
            &bold_style,
        )
        .as_str(),
    );
    let transfer_result = if let Some(ref redacter_with_options) = redacter {
        redact_upload_file::<SFS, DFS, _>(
            bar,
            destination_fs,
            bold_style,
            source_reader,
            file_ref,
            options,
            redacter_with_options,
            file_converters,
            redacter_throttler,
        )
        .await?
    } else {
        destination_fs
            .upload(source_reader, Some(&dest_file_ref))
            .await?;
        TransferFileResult::Copied
    };
    bar.inc(1);
    Ok(transfer_result)
}

#[allow(clippy::too_many_arguments)]
async fn redact_upload_file<
    'a,
    SFS: FileSystemConnection<'a>,
    DFS: FileSystemConnection<'a>,
    S: Stream<Item = AppResult<bytes::Bytes>> + Send + Unpin + Sync + 'static,
>(
    bar: &ProgressBar,
    destination_fs: &mut DFS,
    bold_style: Style,
    source_reader: S,
    dest_file_ref: &FileSystemRef,
    options: &CopyCommandOptions,
    redacter_with_options: &(RedacterBaseOptions, Vec<Redacters<'a>>),
    file_converters: &FileConverters<'a>,
    redacter_throttler: &mut Option<RedacterThrottler>,
) -> AppResult<TransferFileResult> {
    let (redacter_base_options, redacters) = redacter_with_options;
    let stream_redacter = StreamRedacter::new(redacter_base_options, file_converters, bar);

    let dest_file_ref_overridden = options
        .file_mime_override
        .override_for_file_ref(dest_file_ref.clone());

    let redact_plan = stream_redacter
        .create_redact_plan(redacters, &dest_file_ref_overridden)
        .await?;

    if !redact_plan.supported_redacters.is_empty() {
        if let Some(ref mut throttler) = redacter_throttler {
            *throttler = throttler.update(Instant::now());
            let delay = throttler.delay();
            if delay.as_millis() > 0 {
                bar.println(
                    format!(
                        "⧗ Delaying redaction for {} seconds",
                        bold_style
                            .clone()
                            .yellow()
                            .apply_to(throttler.delay().as_secs().to_string())
                    )
                    .as_str(),
                );
                tokio::time::sleep(*delay).await;
            }
        }
        match stream_redacter
            .redact_stream(source_reader, redact_plan, &dest_file_ref_overridden)
            .await
        {
            Ok(redacted_result)
                if redacted_result.number_of_redactions > 0
                    || redacter_base_options.allow_unsupported_copies =>
            {
                destination_fs
                    .upload(redacted_result.stream, Some(dest_file_ref))
                    .await?;
                if redacted_result.number_of_redactions > 0 {
                    Ok(TransferFileResult::RedactedAndCopied)
                } else {
                    Ok(TransferFileResult::Copied)
                }
            }
            Ok(_) => {
                bar.println(
                    format!(
                        "↲ Skipping redaction because {} redactions were applied",
                        bold_style.yellow().apply_to("no suitable".to_string())
                    )
                    .as_str(),
                );
                Ok(TransferFileResult::Skipped)
            }
            Err(ref error) => {
                bar.println(
                    format!(
                        "↲ {}. Skipping due to: {}\n{:?}\n",
                        bold_style.clone().red().apply_to("Error redacting"),
                        bold_style.apply_to(error),
                        error.source()
                    )
                    .as_str(),
                );
                Ok(TransferFileResult::Skipped)
            }
        }
    } else if redacter_base_options.allow_unsupported_copies {
        bar.println(
            format!(
                "↳ Copying {} because it is explicitly allowed by arguments",
                bold_style
                    .clone()
                    .yellow()
                    .apply_to("unredacted".to_string())
            )
            .as_str(),
        );
        destination_fs
            .upload(source_reader, Some(dest_file_ref))
            .await?;
        Ok(TransferFileResult::Copied)
    } else {
        bar.println(
            format!(
                "↲ Skipping redaction because {} media type is not supported",
                bold_style.apply_to(
                    dest_file_ref
                        .media_type
                        .as_ref()
                        .map(|mt| mt.to_string())
                        .unwrap_or("".to_string())
                )
            )
            .as_str(),
        );
        Ok(TransferFileResult::Skipped)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common_types::{GcpProjectId, GcpRegion};
    use crate::redacters::test_support::{
        initialize_crypto, pdfium_test_lock, TEST_DOCUMENTS_DIR, TEST_DOCUMENT_NAMES,
        TEST_DOCUMENT_SAMPLE_EMAIL, TEST_DOCUMENT_SAMPLE_PHONE,
    };
    use crate::redacters::{
        GcpDlpRedacterOptions, GcpVertexAiRedacterOptions, LlmImageMode, RedacterProviderOptions,
    };
    use tempfile::TempDir;

    const SAMPLE_DOCUMENTS_DIR: &str = TEST_DOCUMENTS_DIR;
    const SAMPLE_FILE_NAMES: [&str; 5] = TEST_DOCUMENT_NAMES;
    const SAMPLE_EMAIL: &str = TEST_DOCUMENT_SAMPLE_EMAIL;
    const SAMPLE_PHONE: &str = TEST_DOCUMENT_SAMPLE_PHONE;

    fn plain_style() -> Style {
        Style::new()
    }

    #[test]
    fn processing_line_fits_within_width_with_long_destination() {
        let destination = "redacted/redacted.zip:documents/customer-profile.html";
        assert_eq!(console::measure_text_width(destination), 53);

        let line = format_processing_line(
            150,
            "documents/customer-profile.html",
            destination,
            "text/html",
            "12.34 KiB",
            &plain_style(),
        );

        assert!(
            console::measure_text_width(&line) <= 150,
            "line was {} columns wide: {:?}",
            console::measure_text_width(&line),
            line
        );
        assert!(line.ends_with("12.34 KiB"), "line was: {line:?}");
        assert_eq!(
            line,
            line.trim_end(),
            "line has trailing whitespace: {line:?}"
        );
    }

    #[test]
    fn processing_line_middle_truncates_long_paths_at_narrow_width() {
        let source = "documents/very-long-nested-directory-path/a.txt";
        let destination = "redacted/very-long-nested-directory-path/a.txt";

        let line = format_processing_line(
            80,
            source,
            destination,
            "text/plain",
            "1.00 KiB",
            &plain_style(),
        );

        assert!(
            console::measure_text_width(&line) <= 80,
            "line was {} columns wide: {:?}",
            console::measure_text_width(&line),
            line
        );
        assert!(line.contains('…'), "line was: {line:?}");
        // The tail (file name) must survive truncation.
        assert!(line.contains("a.txt"), "line was: {line:?}");
    }

    #[test]
    fn fit_column_pads_short_values_without_truncating() {
        let result = fit_column("a.txt", 20);
        assert!(!result.contains('…'), "result was: {result:?}");
        assert_eq!(console::measure_text_width(&result), 20);
        assert!(result.starts_with("a.txt"));
    }

    #[test]
    fn processing_line_falls_back_to_80_columns_when_width_is_zero() {
        // Short enough that whether the path column is computed from a
        // width of 0 or of 80 changes how much padding trails it, so a
        // missing width == 0 fallback shows up as a mismatch here.
        let line_zero = format_processing_line(
            0,
            "src.txt",
            "dst.txt",
            "text/plain",
            "1.00 KiB",
            &plain_style(),
        );
        let line_eighty = format_processing_line(
            80,
            "src.txt",
            "dst.txt",
            "text/plain",
            "1.00 KiB",
            &plain_style(),
        );
        assert_eq!(line_zero, line_eighty);
    }

    #[test]
    fn truncate_middle_measures_multibyte_characters_by_display_width() {
        let source = "documents/café-résumé-naïve-longer-file-name.txt";
        // Every character in this path is 1 display column wide (no CJK),
        // so display width differs from byte length but not from char count.
        assert!(source.len() > source.chars().count());

        let truncated = truncate_middle(source, 20);
        assert_eq!(console::measure_text_width(&truncated), 20);
        assert!(truncated.contains('…'));

        let wide = "文档/非常长的中文文件名称示例说明.txt";
        let truncated_wide = truncate_middle(wide, 20);
        assert!(console::measure_text_width(&truncated_wide) <= 20);
        assert!(truncated_wide.contains('…'));
    }

    #[test]
    fn processing_line_truncates_media_type_longer_than_28_columns() {
        let media_type = "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
        assert!(console::measure_text_width(media_type) > 28);

        let line = format_processing_line(
            150,
            "documents/source.txt",
            "redacted/destination.txt",
            media_type,
            "1.00 KiB",
            &plain_style(),
        );

        assert!(line.contains('…'), "line was: {line:?}");
        assert!(!line.contains(media_type), "line was: {line:?}");
    }

    fn base_redacter_options(provider_options: RedacterProviderOptions) -> RedacterOptions {
        RedacterOptions {
            provider_options: vec![provider_options],
            base_options: RedacterBaseOptions {
                allow_unsupported_copies: false,
                csv_headers_disable: false,
                csv_delimiter: None,
                sampling_size: None,
                limit_dlp_requests: None,
            },
        }
    }

    #[tokio::test]
    async fn copies_sample_documents_without_redaction_byte_identical_test(
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // `command_copy` binds pdfium unconditionally (to detect PDF support), so this
        // races with any other test doing the same. See `pdfium_test_lock`'s doc comment.
        let lock = pdfium_test_lock();
        let _guard = lock.lock().await;

        let term = Term::stdout();
        let temp_dir = TempDir::with_prefix("copy_command_tests_no_redaction")?;

        let result = command_copy(
            &term,
            SAMPLE_DOCUMENTS_DIR,
            &temp_dir.path().to_string_lossy(),
            CopyCommandOptions::new(None, None, None, vec![]),
            None,
        )
        .await?;

        assert_eq!(result.files_copied, SAMPLE_FILE_NAMES.len());
        assert_eq!(result.files_redacted, 0);
        assert_eq!(result.files_skipped, 0);

        for name in SAMPLE_FILE_NAMES {
            let original = std::fs::read(std::path::Path::new(SAMPLE_DOCUMENTS_DIR).join(name))?;
            let copied = std::fs::read(temp_dir.path().join(name))?;
            assert_eq!(original, copied, "{name} should be copied byte-identical");
        }

        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-gcp"), ignore)]
    async fn command_copy_gcp_dlp_redacts_sample_documents_test(
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        initialize_crypto();
        let lock = pdfium_test_lock();
        let _guard = lock.lock().await;

        let term = Term::stdout();
        let test_gcp_project_id =
            std::env::var("TEST_GCP_PROJECT").expect("TEST_GCP_PROJECT required");
        let temp_dir = TempDir::with_prefix("copy_command_tests_gcp_dlp")?;

        let redacter_options =
            base_redacter_options(RedacterProviderOptions::GcpDlp(GcpDlpRedacterOptions {
                project_id: GcpProjectId::new(test_gcp_project_id),
                user_defined_built_in_info_types: vec![],
                user_defined_stored_info_types: vec![],
            }));

        command_copy(
            &term,
            SAMPLE_DOCUMENTS_DIR,
            &temp_dir.path().to_string_lossy(),
            CopyCommandOptions::new(None, None, None, vec![]),
            Some(redacter_options),
        )
        .await?;

        for name in [
            "customer-note.txt",
            "customers.csv",
            "customer.json",
            "customer-profile.html",
        ] {
            let content = tokio::fs::read_to_string(temp_dir.path().join(name)).await?;
            assert!(
                !content.contains(SAMPLE_EMAIL),
                "{name} should have its email redacted"
            );
            assert!(
                !content.contains(SAMPLE_PHONE),
                "{name} should have its phone number redacted"
            );
        }

        let pdf_path = temp_dir.path().join("customer-form.pdf");
        if pdf_path.exists() {
            let pdf_bytes = tokio::fs::read(&pdf_path).await?;
            assert!(
                !pdf_bytes.is_empty(),
                "the redacted PDF should not be empty"
            );
        } else {
            println!(
                "Skipping PDF assertion in command_copy_gcp_dlp_redacts_sample_documents_test: \
                 pdfium is not available, so the PDF was skipped rather than redacted"
            );
        }

        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-gcp-vertex-ai"), ignore)]
    async fn command_copy_gcp_vertex_ai_redacts_note_and_form_test(
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        initialize_crypto();
        let lock = pdfium_test_lock();
        let _guard = lock.lock().await;

        let term = Term::stdout();
        let test_gcp_project_id =
            std::env::var("TEST_GCP_PROJECT").expect("TEST_GCP_PROJECT required");
        let test_gcp_region =
            std::env::var("TEST_GCP_REGION").unwrap_or_else(|_| "global".to_string());
        let temp_dir = TempDir::with_prefix("copy_command_tests_gcp_vertex_ai")?;

        let vertex_ai_options = || {
            base_redacter_options(RedacterProviderOptions::GcpVertexAi(
                GcpVertexAiRedacterOptions {
                    project_id: GcpProjectId::new(test_gcp_project_id.clone()),
                    gcp_region: GcpRegion::new(test_gcp_region.clone()),
                    image_mode: LlmImageMode::Auto,
                    text_model: None,
                    image_model: None,
                    block_none_harmful: false,
                },
            ))
        };

        // The PDF copy goes first: it is the only one of the two that needs pdfium, and
        // pdfium has been observed to bind successfully only once per process (a second
        // `PdfImageConverter::new()` call in the same process fails even after the first
        // instance was dropped). Running it first gives it the best chance of a live bind
        // when this test is the first thing in the process to touch pdfium.
        let pdf_destination = temp_dir.path().join("customer-form.pdf");
        command_copy(
            &term,
            "test-fixtures/documents/customer-form.pdf",
            &pdf_destination.to_string_lossy(),
            CopyCommandOptions::new(None, None, None, vec![]),
            Some(vertex_ai_options()),
        )
        .await?;

        let note_destination = temp_dir.path().join("customer-note.txt");
        command_copy(
            &term,
            "test-fixtures/documents/customer-note.txt",
            &note_destination.to_string_lossy(),
            CopyCommandOptions::new(None, None, None, vec![]),
            Some(vertex_ai_options()),
        )
        .await?;
        let note_content = tokio::fs::read_to_string(&note_destination).await?;
        assert!(
            !note_content.contains(SAMPLE_EMAIL),
            "the redacted note should have its email redacted"
        );
        assert!(
            !note_content.contains(SAMPLE_PHONE),
            "the redacted note should have its phone number redacted"
        );

        if pdf_destination.exists() {
            let pdf_bytes = tokio::fs::read(&pdf_destination).await?;
            assert!(
                !pdf_bytes.is_empty(),
                "the redacted PDF should not be empty"
            );
            let output_dir = std::env::var("TEST_REDACTED_OUTPUT_DIR")
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|_| std::env::temp_dir());
            let saved_path = output_dir.join("vertex-ai-redacted-form.pdf");
            std::fs::copy(&pdf_destination, &saved_path)?;
            term.write_line(&format!("Redacted PDF written to {}", saved_path.display()))?;
        } else {
            println!(
                "Skipping PDF assertion in command_copy_gcp_vertex_ai_redacts_note_and_form_test: \
                 pdfium is not available, so the PDF was skipped rather than redacted"
            );
        }

        Ok(())
    }
}
