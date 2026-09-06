use crate::commands::copy_output::{
    format_capabilities_line, format_file_row, format_legend, format_summary_line,
    CopyCapabilities, CopyFileOutcome, CopyOutputStyles, CopySummary, NotRedactedReason,
    SkipReason,
};
use crate::errors::AppError;
use crate::file_converters::FileConverters;
use crate::file_systems::{DetectFileSystem, FileSystemConnection, FileSystemRef};
use crate::file_tools::{FileMatcher, FileMatcherResult, FileMimeOverride};
use crate::redacters::{
    RedacterBaseOptions, RedacterOptions, RedacterThrottler, Redacters, StreamRedacter,
};
use crate::reporter::AppReporter;
use crate::AppResult;
use console::{Style, Term};
use futures::Stream;
use gcloud_sdk::prost::bytes;
use indicatif::*;
use rvstruct::ValueStruct;
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

/// What one file's transfer produced: the counters it feeds, the outcome its row
/// shows, and its size for the closing summary.
struct TransferReport {
    result: TransferFileResult,
    outcome: CopyFileOutcome,
    file_size: Option<usize>,
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
    let styles = CopyOutputStyles::new();

    report_copy_info(
        term,
        source,
        destination,
        &redacter_options,
        &file_converters,
        &styles,
    )?;

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

    let mut summary = CopySummary::default();
    let copy_result: AppResult<CopyCommandResult> = if source_fs.has_multiple_files().await? {
        if !destination_fs.accepts_multiple_files().await? {
            return Err(AppError::DestinationDoesNotSupportMultipleFiles {
                destination: destination.to_string(),
            });
        }
        tracing::debug!(source, "copying a directory and listing the source files");
        let source_files_result = source_fs
            .list_files(Some(&options.file_matcher), options.max_files_limit)
            .await?;
        let source_files: Vec<FileSystemRef> = source_files_result.files;
        let files_found = source_files.len();
        let files_total_size: usize = source_files
            .iter()
            .map(|file| file.file_size.unwrap_or(0))
            .sum();
        bar.println(
            format!(
                "Found {} files. Total size: {}",
                styles.highlighted.apply_to(files_found),
                styles
                    .highlighted
                    .apply_to(HumanBytes(files_total_size as u64))
            )
            .as_str(),
        );
        if files_found > 0 {
            bar.println(format_legend(&styles));
        }

        bar.set_length(files_found as u64);

        let mut total_files_copied = 0;
        let mut total_files_redacted = 0;
        let mut total_files_skipped = source_files_result.skipped;
        summary.skipped = source_files_result.skipped;
        summary.total_size = files_total_size as u64;
        for source_file in source_files {
            let report = transfer_and_redact_file(
                term,
                &bar,
                Some(&source_file),
                &mut source_fs,
                &mut destination_fs,
                &options,
                &maybe_redacters,
                &file_converters,
                &mut redacter_throttler,
                &styles,
            )
            .await?;
            match report.result {
                TransferFileResult::Copied => total_files_copied += 1,
                TransferFileResult::RedactedAndCopied => {
                    total_files_redacted += 1;
                    total_files_copied += 1;
                }
                TransferFileResult::Skipped => total_files_skipped += 1,
            }
            summary.count(&report.outcome);
        }
        Ok(CopyCommandResult {
            files_copied: total_files_copied,
            files_redacted: total_files_redacted,
            files_skipped: total_files_skipped,
        })
    } else {
        bar.println(format_legend(&styles));
        let report = transfer_and_redact_file(
            term,
            &bar,
            None,
            &mut source_fs,
            &mut destination_fs,
            &options,
            &maybe_redacters,
            &file_converters,
            &mut redacter_throttler,
            &styles,
        )
        .await?;
        summary.count(&report.outcome);
        summary.total_size = report.file_size.unwrap_or(0) as u64;
        Ok(match report.result {
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
        })
    };

    destination_fs.close().await?;
    source_fs.close().await?;
    bar.finish_and_clear();
    term.write_line("")?;
    term.write_line(format_summary_line(&summary, &styles).as_str())?;
    copy_result
}

impl CopySummary {
    /// Adds one file's outcome to the counts shown in the summary.
    fn count(&mut self, outcome: &CopyFileOutcome) {
        match outcome {
            CopyFileOutcome::Redacted { .. } => self.redacted += 1,
            CopyFileOutcome::Copied | CopyFileOutcome::CopiedUnredacted { .. } => {
                self.copied_as_is += 1
            }
            CopyFileOutcome::Skipped { .. } => self.skipped += 1,
            CopyFileOutcome::Error { .. } => self.errors += 1,
        }
    }
}

fn report_copy_info(
    term: &Term,
    source: &str,
    destination: &str,
    redacter_options: &Option<RedacterOptions>,
    file_converters: &FileConverters<'_>,
    styles: &CopyOutputStyles,
) -> AppResult<()> {
    let capabilities = CopyCapabilities {
        redacters: redacter_options
            .as_ref()
            .map(|options| {
                options
                    .provider_options
                    .iter()
                    .map(|provider| provider.redacter_type())
                    .collect()
            })
            .unwrap_or_default(),
        pdf_rendering: file_converters.pdf_image_converter.is_some(),
        ocr: file_converters.ocr.is_some(),
        sampling_size: redacter_options
            .as_ref()
            .and_then(|o| o.base_options.sampling_size),
    };

    term.write_line(
        format!(
            "Copying from {} to {}.",
            styles.highlighted.apply_to(source),
            Style::new().bold().yellow().apply_to(destination),
        )
        .as_str(),
    )?;
    term.write_line(format_capabilities_line(&capabilities, styles).as_str())?;
    term.write_line("")?;
    Ok(())
}

enum TransferFileResult {
    Copied,
    RedactedAndCopied,
    Skipped,
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
    styles: &CopyOutputStyles,
) -> AppResult<TransferReport> {
    let (base_file_ref, source_reader) = source_fs.download(source_file_ref).await?;

    let skip_reason = match options.file_matcher.matches(&base_file_ref) {
        FileMatcherResult::SkippedDueToSize => Some(SkipReason::OverSizeLimit),
        FileMatcherResult::SkippedDueToName => Some(SkipReason::NameFilter),
        FileMatcherResult::Matched => None,
    };
    if let Some(reason) = skip_reason {
        let outcome = CopyFileOutcome::Skipped { reason };
        print_file_row(term, bar, &base_file_ref, &outcome, styles);
        bar.inc(1);
        return Ok(TransferReport {
            result: TransferFileResult::Skipped,
            outcome,
            file_size: base_file_ref.file_size,
        });
    }

    let file_ref = source_file_ref.unwrap_or(&base_file_ref);

    let dest_file_ref = FileSystemRef {
        relative_path: file_ref.relative_path.clone(),
        media_type: file_ref.media_type.clone(),
        file_size: file_ref.file_size,
    };
    tracing::debug!(
        source = %source_fs.resolve(Some(&base_file_ref)).file_path,
        destination = %destination_fs.resolve(Some(&dest_file_ref)).file_path,
        "processing a file"
    );
    let (transfer_result, outcome) = if let Some(ref redacter_with_options) = redacter {
        redact_upload_file::<SFS, DFS, _>(
            bar,
            destination_fs,
            styles,
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
        (TransferFileResult::Copied, CopyFileOutcome::Copied)
    };
    print_file_row(term, bar, file_ref, &outcome, styles);
    bar.inc(1);
    Ok(TransferReport {
        result: transfer_result,
        outcome,
        file_size: file_ref.file_size,
    })
}

/// Prints the row for a file once its processing has finished.
fn print_file_row(
    term: &Term,
    bar: &ProgressBar,
    file_ref: &FileSystemRef,
    outcome: &CopyFileOutcome,
    styles: &CopyOutputStyles,
) {
    bar.println(format_file_row(
        term.width() as usize,
        file_ref.relative_path.value(),
        file_ref
            .media_type
            .as_ref()
            .map(|media_type| media_type.to_string())
            .unwrap_or_default()
            .as_str(),
        HumanBytes(file_ref.file_size.map(|sz| sz as u64).unwrap_or(0_u64))
            .to_string()
            .as_str(),
        outcome,
        styles,
    ));
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
    styles: &CopyOutputStyles,
    source_reader: S,
    dest_file_ref: &FileSystemRef,
    options: &CopyCommandOptions,
    redacter_with_options: &(RedacterBaseOptions, Vec<Redacters<'a>>),
    file_converters: &FileConverters<'a>,
    redacter_throttler: &mut Option<RedacterThrottler>,
) -> AppResult<(TransferFileResult, CopyFileOutcome)> {
    let (redacter_base_options, redacters) = redacter_with_options;
    let stream_redacter = StreamRedacter::new(redacter_base_options, file_converters);

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
                    styles
                        .dimmed
                        .apply_to(format!(
                            "⧗ Delaying redaction for {} seconds",
                            throttler.delay().as_secs()
                        ))
                        .to_string(),
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
                let summary = redacted_result.summary;
                let outcome = if !summary.applied.is_empty() {
                    CopyFileOutcome::Redacted {
                        redacters: summary.applied,
                        conversion: summary.conversion,
                    }
                } else {
                    CopyFileOutcome::CopiedUnredacted {
                        reason: not_redacted_reason(summary.blocked),
                    }
                };
                if redacted_result.number_of_redactions > 0 {
                    Ok((TransferFileResult::RedactedAndCopied, outcome))
                } else {
                    Ok((TransferFileResult::Copied, outcome))
                }
            }
            Ok(redacted_result) => Ok((
                TransferFileResult::Skipped,
                CopyFileOutcome::Skipped {
                    reason: SkipReason::NotRedacted(not_redacted_reason(
                        redacted_result.summary.blocked,
                    )),
                },
            )),
            Err(ref error) => {
                tracing::debug!(error = %error, source = ?error.source(), "redaction failed");
                Ok((
                    TransferFileResult::Skipped,
                    CopyFileOutcome::Error {
                        message: error.to_string(),
                    },
                ))
            }
        }
    } else if redacter_base_options.allow_unsupported_copies {
        destination_fs
            .upload(source_reader, Some(dest_file_ref))
            .await?;
        Ok((
            TransferFileResult::Copied,
            CopyFileOutcome::CopiedUnredacted {
                reason: NotRedactedReason::TypeNotSupported,
            },
        ))
    } else {
        Ok((
            TransferFileResult::Skipped,
            CopyFileOutcome::Skipped {
                reason: SkipReason::NotRedacted(NotRedactedReason::TypeNotSupported),
            },
        ))
    }
}

/// Why redaction did not happen, defaulting to "there was nothing left to redact"
/// when the pipeline did not record a reason.
fn not_redacted_reason(blocked: Option<crate::redacters::RedactionBlocked>) -> NotRedactedReason {
    use crate::redacters::RedactionBlocked;
    match blocked {
        Some(RedactionBlocked::PdfRendererUnavailable) => NotRedactedReason::PdfRendererUnavailable,
        Some(RedactionBlocked::OcrUnavailable) => NotRedactedReason::OcrUnavailable,
        Some(RedactionBlocked::OcrImageFormatNotSupported) => {
            NotRedactedReason::OcrImageFormatNotSupported
        }
        None => NotRedactedReason::AlreadyRedacted,
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
