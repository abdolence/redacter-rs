use crate::commands::copy_output::{
    format_capabilities_line, format_file_row, format_legend, format_summary_line,
    CopyCapabilities, CopyFileOutcome, CopyOutputStyles, CopySummary, NotRedactedReason,
    SkipReason,
};
use crate::errors::AppError;
use crate::file_converters::FileConverters;
use crate::file_systems::{DetectFileSystem, FileSystemConnection, FileSystemRef};
use crate::file_tools::{FileMatcher, FileMatcherResult, FileMimeOverride};
use crate::model_store::{DownloadModels, ModelStore, ModelStoreOptions};
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
    pub model_store: ModelStoreOptions,
}

impl CopyCommandOptions {
    pub fn new(
        filename_filter: Option<globset::Glob>,
        max_size_limit: Option<usize>,
        max_files_limit: Option<usize>,
        mime_override: Vec<(mime::Mime, globset::Glob)>,
        model_store: ModelStoreOptions,
    ) -> Self {
        let filename_matcher = filename_filter
            .as_ref()
            .map(|filter| filter.compile_matcher());
        CopyCommandOptions {
            file_matcher: FileMatcher::new(filename_matcher, max_size_limit),
            file_mime_override: FileMimeOverride::new(mime_override),
            max_files_limit,
            model_store,
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
    let models = ModelStore::new(
        &store_options_for(&options.model_store, redacter_options.is_some()),
        &term_reporter,
    );
    let styles = CopyOutputStyles::new();

    // The bar exists from here on because the file systems report through it, but it is
    // not drawn until every model this run needs has been resolved: a consent prompt or a
    // download bar must never land under a live progress bar.
    let bar = ProgressBar::new(1);
    bar.set_style(
        ProgressStyle::with_template(
            "{spinner:.green} [{elapsed_precise}] [{wide_bar:.green/237}] {pos:>3}/{len:3}",
        )?
        .progress_chars("━>─"),
    );
    bar.set_draw_target(ProgressDrawTarget::hidden());
    let app_reporter = AppReporter::from(&bar);

    let mut source_fs = DetectFileSystem::open(source, &app_reporter).await?;
    let mut destination_fs = DetectFileSystem::open(destination, &app_reporter).await?;

    // The source is listed before the models are resolved: the listing is what says whether
    // this run can use OCR at all, and a run that cannot must never be asked to download it.
    let source_listing = if source_fs.has_multiple_files().await? {
        if !destination_fs.accepts_multiple_files().await? {
            return Err(AppError::DestinationDoesNotSupportMultipleFiles {
                destination: destination.to_string(),
            });
        }
        tracing::debug!(source, "copying a directory and listing the source files");
        Some(
            source_fs
                .list_files(Some(&options.file_matcher), options.max_files_limit)
                .await?,
        )
    } else {
        None
    };
    let needs_ocr = match &source_listing {
        Some(listing) => run_needs_ocr(
            redacter_options.is_some(),
            &listing.files,
            &options.file_mime_override,
        ),
        None => single_source_needs_ocr(
            redacter_options.is_some(),
            source_fs.resolve(None).file_path.as_str(),
            &options.file_mime_override,
        ),
    };
    let file_converters = FileConverters::new()
        .init(&term_reporter, &models, needs_ocr)
        .await?;

    report_copy_info(
        term,
        source,
        destination,
        &redacter_options,
        &file_converters,
        &styles,
    )?;

    // Every model the redacters need is resolved while the bar is still hidden, for the
    // same reason.
    if let Some(ref redacter_options) = redacter_options {
        for provider in &redacter_options.provider_options {
            for id in provider.required_models() {
                models.resolve(id).await?;
            }
        }
    }

    bar.set_draw_target(ProgressDrawTarget::stderr());
    bar.enable_steady_tick(Duration::from_millis(100));

    let mut redacter_throttler = redacter_options
        .as_ref()
        .and_then(|o| o.base_options.limit_dlp_requests.clone())
        .map(|limit| limit.to_throttling_counter());

    let maybe_redacters = match redacter_options {
        Some(options) => {
            let mut redacters = Vec::with_capacity(options.provider_options.len());
            for provider_options in options.provider_options {
                let redacter =
                    Redacters::new_redacter(provider_options, &app_reporter, &models).await?;
                redacters.push(redacter);
            }
            Some((options.base_options, redacters))
        }
        None => None,
    };

    let mut summary = CopySummary::default();
    let copy_result: AppResult<CopyCommandResult> = if let Some(listing) = source_listing {
        let source_files: Vec<FileSystemRef> = listing.files;
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
        let mut total_files_skipped = listing.skipped;
        summary.skipped = listing.skipped;
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

/// The model options this run may act on. A plain copy redacts nothing and so needs no
/// model: whatever `--download-models` says, it is forced to `no` there, so a copy can
/// never be interrupted by a consent prompt or a download.
fn store_options_for(options: &ModelStoreOptions, has_redacters: bool) -> ModelStoreOptions {
    ModelStoreOptions {
        download: if has_redacters {
            options.download
        } else {
            DownloadModels::No
        },
        models_dir: options.models_dir.clone(),
    }
}

/// OCR only ever runs on images and, through the PDF renderer, on PDF pages. A listed file
/// of any other type — a file the listing could not type included, since the redact plan
/// reads the very same media type — can never reach it.
fn media_type_needs_ocr(media_type: Option<&mime::Mime>) -> bool {
    media_type.is_some_and(|media_type| {
        Redacters::is_mime_image(media_type) || Redacters::is_mime_pdf(media_type)
    })
}

/// Whether this run can reach the OCR engine at all, over the files it has just listed. Only
/// a redacting run can: a copy of a directory of text files must not resolve the OCR model,
/// let alone offer to download it.
fn run_needs_ocr(
    has_redacters: bool,
    files: &[FileSystemRef],
    mime_override: &FileMimeOverride,
) -> bool {
    has_redacters
        && files.iter().any(|file| {
            media_type_needs_ocr(
                mime_override
                    .override_for_file_ref(file.clone())
                    .media_type
                    .as_ref(),
            )
        })
}

/// The same question for a single-file source, which is never listed: what it holds is
/// guessed from its path, the way the file systems guess it when they hand the file over.
/// A path that says nothing is taken as one that may need OCR — `clipboard://` carries an
/// image or text and only says which once it is read, and guessing "text" there would turn
/// OCR off for a clipboard image without a word.
fn single_source_needs_ocr(
    has_redacters: bool,
    path: &str,
    mime_override: &FileMimeOverride,
) -> bool {
    let file_ref = mime_override.override_for_file_ref(single_source_file_ref(path));
    has_redacters
        && (file_ref.media_type.is_none() || media_type_needs_ocr(file_ref.media_type.as_ref()))
}

/// The one file a single-file source stands for, named and typed from its path.
fn single_source_file_ref(path: &str) -> FileSystemRef {
    FileSystemRef {
        relative_path: path.rsplit('/').next().unwrap_or(path).into(),
        media_type: mime_guess::from_path(path).first(),
        file_size: None,
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

/// Why redaction did not happen, defaulting to "no redacter applied" when the
/// pipeline did not record a reason.
fn not_redacted_reason(blocked: Option<crate::redacters::RedactionBlocked>) -> NotRedactedReason {
    use crate::redacters::RedactionBlocked;
    match blocked {
        Some(RedactionBlocked::PdfRendererUnavailable) => NotRedactedReason::PdfRendererUnavailable,
        Some(RedactionBlocked::OcrUnavailable) => NotRedactedReason::OcrUnavailable,
        Some(RedactionBlocked::OcrImageFormatNotSupported) => {
            NotRedactedReason::OcrImageFormatNotSupported
        }
        None => NotRedactedReason::NoRedacterApplied,
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
        GcpDlpRedacterOptions, GcpVertexAiRedacterOptions, LlmImageMode, LocalRulesRedacterOptions,
        RedacterProviderOptions, RuleGroup,
    };
    use tempfile::TempDir;

    const SAMPLE_DOCUMENTS_DIR: &str = TEST_DOCUMENTS_DIR;
    const SAMPLE_FILE_NAMES: [&str; 6] = TEST_DOCUMENT_NAMES;
    const SAMPLE_EMAIL: &str = TEST_DOCUMENT_SAMPLE_EMAIL;
    const SAMPLE_PHONE: &str = TEST_DOCUMENT_SAMPLE_PHONE;

    fn listed(name: &str, media_type: Option<mime::Mime>) -> FileSystemRef {
        FileSystemRef {
            relative_path: name.into(),
            media_type,
            file_size: Some(10),
        }
    }

    fn no_mime_override() -> FileMimeOverride {
        FileMimeOverride::new(vec![])
    }

    /// The listing is what decides whether the OCR model is looked up at all, so the rule
    /// it is read by is asserted on its own: text in, no OCR.
    #[test]
    fn a_listing_without_an_image_or_a_pdf_never_needs_ocr() {
        let text_only = [
            listed("customer-note.txt", Some(mime::TEXT_PLAIN)),
            listed("customers.csv", Some(mime::TEXT_CSV)),
            listed("customer.json", Some(mime::APPLICATION_JSON)),
            listed("notes", None),
        ];
        assert!(!run_needs_ocr(true, &text_only, &no_mime_override()));
        assert!(!run_needs_ocr(true, &[], &no_mime_override()));
    }

    #[test]
    fn one_image_or_pdf_in_the_listing_is_enough_to_need_ocr() {
        for media_type in [mime::IMAGE_PNG, mime::IMAGE_JPEG, mime::APPLICATION_PDF] {
            let files = [
                listed("customer-note.txt", Some(mime::TEXT_PLAIN)),
                listed("scan", Some(media_type.clone())),
            ];
            assert!(
                run_needs_ocr(true, &files, &no_mime_override()),
                "{media_type}"
            );
        }
    }

    /// A copy is not a redaction: nothing it does can reach the OCR engine.
    #[test]
    fn a_copy_without_a_redacter_never_needs_ocr() {
        let files = [listed("form.png", Some(mime::IMAGE_PNG))];
        assert!(!run_needs_ocr(false, &files, &no_mime_override()));
    }

    /// `--mime-override` is what the redacters read, so it is what this rule reads too.
    #[test]
    fn the_mime_override_decides_whether_a_file_needs_ocr() {
        let to_png = FileMimeOverride::new(vec![(
            mime::IMAGE_PNG,
            globset::Glob::new("*.dat").expect("valid glob"),
        )]);
        let files = [listed("scan.dat", Some(mime::TEXT_PLAIN))];
        assert!(!run_needs_ocr(true, &files, &no_mime_override()));
        assert!(run_needs_ocr(true, &files, &to_png));
    }

    /// A single-file source is never listed, so its one file is typed from its path.
    #[test]
    fn a_single_file_source_is_typed_from_its_path() {
        let png = single_source_file_ref("/tmp/scans/form-example.png");
        assert_eq!(png.relative_path.value(), "form-example.png");
        assert_eq!(png.media_type, Some(mime::IMAGE_PNG));

        let text = single_source_file_ref("customer-note.txt");
        assert_eq!(text.relative_path.value(), "customer-note.txt");
        assert_eq!(text.media_type, Some(mime::TEXT_PLAIN));

        assert!(single_source_needs_ocr(
            true,
            "/tmp/scans/form-example.png",
            &no_mime_override()
        ));
        assert!(single_source_needs_ocr(
            true,
            "gs://bucket/scan.pdf",
            &no_mime_override()
        ));
        assert!(!single_source_needs_ocr(
            true,
            "customer-note.txt",
            &no_mime_override()
        ));
        assert!(!single_source_needs_ocr(
            false,
            "/tmp/scans/form-example.png",
            &no_mime_override()
        ));
    }

    /// A source whose path says nothing about what it holds may still hold an image:
    /// `clipboard://` decides that when it is read, long after this question is asked.
    #[test]
    fn a_single_file_source_of_an_unknown_type_may_need_ocr() {
        assert_eq!(single_source_file_ref("clipboard://").media_type, None);
        assert!(single_source_needs_ocr(
            true,
            "clipboard://",
            &no_mime_override()
        ));
        assert!(single_source_needs_ocr(
            true,
            "/tmp/scan-without-an-extension",
            &no_mime_override()
        ));
        assert!(!single_source_needs_ocr(
            false,
            "clipboard://",
            &no_mime_override()
        ));
    }

    /// A plain copy needs no model, so it may never prompt for one however the flag was
    /// set. Everything else about the options is passed through untouched.
    #[test]
    fn a_plain_copy_is_forced_offline_whatever_the_flag_says() {
        let asked = ModelStoreOptions {
            download: DownloadModels::Ask,
            models_dir: Some(std::path::PathBuf::from("/models")),
        };
        let for_copy = store_options_for(&asked, false);
        assert_eq!(for_copy.download, DownloadModels::No);
        assert_eq!(for_copy.models_dir, asked.models_dir);

        for download in [DownloadModels::Ask, DownloadModels::Yes, DownloadModels::No] {
            let options = ModelStoreOptions {
                download,
                models_dir: None,
            };
            assert_eq!(store_options_for(&options, true).download, download);
        }
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
            CopyCommandOptions::new(None, None, None, vec![], ModelStoreOptions::default()),
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

    /// A models directory left half-written by an interrupted download is not a reason to
    /// copy nothing: the files are looked at before anything is downloaded, so the run goes
    /// on without OCR. Reproduces `cp --models-dir <stale> --download-models no in out`.
    #[cfg(feature = "ocr")]
    #[tokio::test]
    async fn copies_over_a_stale_models_directory(
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use crate::model_store::{manifest, ModelId};
        let lock = pdfium_test_lock();
        let _guard = lock.lock().await;

        let term = Term::stdout();
        let temp_dir = TempDir::with_prefix("copy_command_tests_stale_models")?;
        let models_dir = temp_dir.path().join("models");
        let ocrs_dir = models_dir.join("ocrs");
        std::fs::create_dir_all(&ocrs_dir)?;
        for file in manifest(ModelId::Ocrs).files {
            std::fs::write(ocrs_dir.join(file.name), b"junk")?;
        }
        let destination = temp_dir.path().join("out");
        std::fs::create_dir_all(&destination)?;

        let offline_over_stale_models = || {
            CopyCommandOptions::new(
                None,
                None,
                None,
                vec![],
                ModelStoreOptions {
                    download: DownloadModels::No,
                    models_dir: Some(models_dir.clone()),
                },
            )
        };

        let result = command_copy(
            &term,
            SAMPLE_DOCUMENTS_DIR,
            &destination.to_string_lossy(),
            offline_over_stale_models(),
            None,
        )
        .await?;
        assert_eq!(result.files_copied, SAMPLE_FILE_NAMES.len());

        // The same directory with a redacter and an image, which is the run that does look
        // the OCR model up: it must report OCR unavailable and skip the file, not abort.
        let image_destination = temp_dir.path().join("images");
        std::fs::create_dir_all(&image_destination)?;
        let result = command_copy(
            &term,
            "test-fixtures/media/",
            &image_destination.to_string_lossy(),
            offline_over_stale_models(),
            Some(base_redacter_options(RedacterProviderOptions::LocalRules(
                LocalRulesRedacterOptions {
                    groups: RuleGroup::all(),
                    user_rules: vec![],
                },
            ))),
        )
        .await?;
        assert_eq!(result.files_copied, 0);
        assert_eq!(result.files_skipped, 1, "the image is skipped without OCR");

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
            CopyCommandOptions::new(None, None, None, vec![], ModelStoreOptions::default()),
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
            CopyCommandOptions::new(None, None, None, vec![], ModelStoreOptions::default()),
            Some(vertex_ai_options()),
        )
        .await?;

        let note_destination = temp_dir.path().join("customer-note.txt");
        command_copy(
            &term,
            "test-fixtures/documents/customer-note.txt",
            &note_destination.to_string_lossy(),
            CopyCommandOptions::new(None, None, None, vec![], ModelStoreOptions::default()),
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
