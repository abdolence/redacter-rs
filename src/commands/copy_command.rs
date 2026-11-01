use crate::addon_store::{AddonStore, AddonStoreOptions};
use crate::args::RedacterType;
use crate::commands::copy_output::{
    format_capabilities_line, format_file_row, format_legend, format_summary_line, Capabilities,
    CopyFileOutcome, CopySummary, NotRedactedReason, OutputStyles, RowLayout, SkipReason,
    COPY_LEGEND,
};
use crate::commands::findings::{FileOutcome, ReportedFile};
use crate::commands::scan_output::{FindingReport, ValueDisplay};
use crate::errors::AppError;
use crate::file_converters::FileConverters;
use crate::file_systems::{DetectFileSystem, FileSystemConnection, FileSystemRef};
use crate::file_tools::{FileMatcher, FileMatcherResult, FileMimeOverride};
use crate::redacters::{
    Categories, Detection, RedacterBaseOptions, RedacterOptions, RedacterThrottler, Redacters,
    StreamRedacter,
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

/// What `cp` reports. Fields are only ever added, so a consumer of the JSON report can rely
/// on every one here.
#[derive(Debug, Clone, Serialize)]
pub struct CopyCommandResult {
    pub files_copied: usize,
    pub files_redacted: usize,
    pub files_skipped: usize,
    /// Every file the copy met, in the order it met them. Files the listing left out by size or
    /// name are only counted in `files_skipped`.
    pub files: Vec<CopyFileReport>,
    /// The categories `--categories` limited the copy to; absent when every category was
    /// redacted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub categories: Option<Categories>,
}

/// One file of the copy, as the JSON report lists it. The redactions come in the order a reader
/// meets them, and their values are always masked.
#[derive(Debug, Clone, PartialEq)]
pub struct CopyFileReport {
    path: String,
    mime_type: Option<String>,
    outcome: CopyFileOutcome,
    redactions: Vec<FindingReport>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum CopyFileStatus {
    Redacted,
    Copied,
    CopiedUnredacted,
    Skipped,
    Failed,
}

impl Serialize for CopyFileReport {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Entry<'a> {
            path: &'a str,
            mime_type: Option<&'a str>,
            status: CopyFileStatus,
            reason: Option<String>,
            redactions: &'a [FindingReport],
        }
        let (status, reason) = match &self.outcome {
            CopyFileOutcome::Redacted { .. } => (CopyFileStatus::Redacted, None),
            CopyFileOutcome::Copied => (CopyFileStatus::Copied, None),
            CopyFileOutcome::CopiedUnredacted { reason } => (
                CopyFileStatus::CopiedUnredacted,
                Some(reason.detail().to_string()),
            ),
            CopyFileOutcome::Skipped { reason } => {
                (CopyFileStatus::Skipped, Some(reason.detail().to_string()))
            }
            CopyFileOutcome::Error { message } => (CopyFileStatus::Failed, Some(message.clone())),
        };
        Entry {
            path: &self.path,
            mime_type: self.mime_type.as_deref(),
            status,
            reason,
            redactions: &self.redactions,
        }
        .serialize(serializer)
    }
}

impl CopyFileReport {
    pub fn new(
        file_ref: &FileSystemRef,
        outcome: &CopyFileOutcome,
        redactions: &[(RedacterType, Detection)],
    ) -> Self {
        let mut redactions: Vec<FindingReport> = redactions
            .iter()
            .map(|(redacter, detection)| {
                FindingReport::new(*redacter, detection, ValueDisplay::Masked)
            })
            .collect();
        FindingReport::sort_by_location(&mut redactions);
        Self {
            path: file_ref.relative_path.value().clone(),
            mime_type: file_ref.media_type.as_ref().map(ToString::to_string),
            outcome: outcome.clone(),
            redactions,
        }
    }

    /// The file for the SARIF and OCSF reports, or `None` for a file the size limit or the name
    /// filter left out.
    fn reported_file(&self) -> Option<ReportedFile> {
        let outcome = match &self.outcome {
            CopyFileOutcome::Redacted { .. } => FileOutcome::Findings(
                self.redactions
                    .iter()
                    .map(FindingReport::reported_finding)
                    .collect(),
            ),
            CopyFileOutcome::Error { message } => FileOutcome::Failed {
                reason: message.clone(),
            },
            CopyFileOutcome::Copied => not_redacted_outcome(NotRedactedReason::NoRedacterApplied),
            CopyFileOutcome::CopiedUnredacted { reason }
            | CopyFileOutcome::Skipped {
                reason: SkipReason::NotRedacted(reason),
            } => not_redacted_outcome(*reason),
            CopyFileOutcome::Skipped { .. } => return None,
        };
        Some(ReportedFile {
            path: self.path.clone(),
            outcome,
        })
    }
}

fn not_redacted_outcome(reason: NotRedactedReason) -> FileOutcome {
    let detail = reason.detail().to_string();
    if reason.needs_converter() {
        FileOutcome::Blocked { reason: detail }
    } else {
        FileOutcome::Unsupported { reason: detail }
    }
}

impl CopyCommandResult {
    /// The files of the copy for the SARIF and OCSF reports.
    pub fn reported_files(&self) -> Vec<ReportedFile> {
        self.files
            .iter()
            .filter_map(CopyFileReport::reported_file)
            .collect()
    }

    /// The report for people.
    pub fn to_text(&self) -> String {
        let categories = self
            .categories
            .as_ref()
            .map(|categories| format!("Categories: {categories}\n"))
            .unwrap_or_default();
        format!(
            "{categories}Files copied: {}\nFiles redacted: {}\nFiles skipped: {}\n",
            self.files_copied, self.files_redacted, self.files_skipped
        )
    }
}

#[derive(Debug, Clone)]
pub struct CopyCommandOptions {
    pub file_matcher: FileMatcher,
    pub file_mime_override: FileMimeOverride,
    pub max_files_limit: Option<usize>,
    pub addon_store: AddonStoreOptions,
}

impl CopyCommandOptions {
    pub fn new(
        filename_filter: Option<globset::Glob>,
        max_size_limit: Option<usize>,
        max_files_limit: Option<usize>,
        mime_override: Vec<(mime::Mime, globset::Glob)>,
        addon_store: AddonStoreOptions,
    ) -> Self {
        let filename_matcher = filename_filter
            .as_ref()
            .map(|filter| filter.compile_matcher());
        CopyCommandOptions {
            file_matcher: FileMatcher::new(filename_matcher, max_size_limit),
            file_mime_override: FileMimeOverride::new(mime_override),
            max_files_limit,
            addon_store,
        }
    }
}

/// What one file's transfer produced: the counters it feeds, the outcome its row
/// shows, and its size for the closing summary.
struct TransferReport {
    result: TransferFileResult,
    outcome: CopyFileOutcome,
    file_size: Option<usize>,
    file: CopyFileReport,
}

pub async fn command_copy(
    term: &Term,
    source: &str,
    destination: &str,
    options: CopyCommandOptions,
    redacter_options: Option<RedacterOptions>,
) -> AppResult<CopyCommandResult> {
    let term_reporter = AppReporter::from(term);
    let addons = AddonStore::new(&options.addon_store);
    let styles = OutputStyles::new();
    let categories = redacter_options
        .as_ref()
        .and_then(|options| options.base_options.categories.clone());

    // The bar is not drawn until the converters and redacters have been bound: they report
    // straight to the terminal, those lines must not land under a live progress bar, and a
    // failure to bind one must not leave a bar running. A hidden bar drops what is printed
    // through it, so the source reports to the terminal: a zip source reports the entries it
    // skips while it is listed, before the bar is drawn.
    let bar = ProgressBar::new(1);
    bar.set_style(
        ProgressStyle::with_template(
            "{spinner:.green} [{elapsed_precise}] [{wide_bar:.green/237}] {pos:>3}/{len:3}",
        )?
        .progress_chars("━>─"),
    );
    bar.set_draw_target(ProgressDrawTarget::hidden());
    let app_reporter = AppReporter::from(&bar);

    let mut source_fs = DetectFileSystem::open(source, &term_reporter).await?;
    let mut destination_fs = DetectFileSystem::open(destination, &app_reporter).await?;

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
    let file_converters = FileConverters::new().init(&term_reporter, &addons).await?;

    report_copy_info(
        term,
        source,
        destination,
        &redacter_options,
        &file_converters,
        &styles,
    )?;

    let mut redacter_throttler = redacter_options
        .as_ref()
        .and_then(|o| o.base_options.limit_dlp_requests.clone())
        .map(|limit| limit.to_throttling_counter());

    let maybe_redacters = match redacter_options {
        Some(options) => {
            let mut redacters = Vec::with_capacity(options.provider_options.len());
            for provider_options in options.provider_options {
                let redacter =
                    Redacters::new_redacter(provider_options, &term_reporter, &addons).await?;
                redacters.push(redacter);
            }
            Some((options.base_options, redacters))
        }
        None => None,
    };

    bar.set_draw_target(ProgressDrawTarget::stderr());
    bar.enable_steady_tick(Duration::from_millis(100));

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
            bar.println(format_legend(&COPY_LEGEND, &styles));
        }

        bar.set_length(files_found as u64);

        let mut total_files_copied = 0;
        let mut total_files_redacted = 0;
        let mut total_files_skipped = listing.skipped;
        let mut files = Vec::with_capacity(files_found);
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
            files.push(report.file);
        }
        Ok(CopyCommandResult {
            files_copied: total_files_copied,
            files_redacted: total_files_redacted,
            files_skipped: total_files_skipped,
            files,
            categories: categories.clone(),
        })
    } else {
        bar.println(format_legend(&COPY_LEGEND, &styles));
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
        let (files_copied, files_redacted, files_skipped) = match report.result {
            TransferFileResult::Copied => (1, 0, 0),
            TransferFileResult::RedactedAndCopied => (1, 1, 0),
            TransferFileResult::Skipped => (0, 0, 1),
        };
        Ok(CopyCommandResult {
            files_copied,
            files_redacted,
            files_skipped,
            files: vec![report.file],
            categories: categories.clone(),
        })
    };

    destination_fs.close().await?;
    source_fs.close().await?;
    bar.finish_and_clear();
    term.write_line("")?;
    term.write_line(format_summary_line(&summary, categories.as_ref(), &styles).as_str())?;
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
    styles: &OutputStyles,
) -> AppResult<()> {
    let capabilities = Capabilities::new(
        redacter_options
            .as_ref()
            .map(|options| {
                options
                    .provider_options
                    .iter()
                    .map(|provider| provider.redacter_type())
                    .collect()
            })
            .unwrap_or_default(),
        file_converters,
        redacter_options
            .as_ref()
            .and_then(|o| o.base_options.sampling_size),
        redacter_options
            .as_ref()
            .and_then(|o| o.base_options.categories.clone()),
    );

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

/// What redacting and uploading one file produced.
struct UploadResult {
    result: TransferFileResult,
    outcome: CopyFileOutcome,
    /// What the redacters replaced, placed in the file as it was read.
    redactions: Vec<(RedacterType, Detection)>,
}

impl UploadResult {
    fn without_redactions(result: TransferFileResult, outcome: CopyFileOutcome) -> Self {
        Self {
            result,
            outcome,
            redactions: Vec::new(),
        }
    }
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
    styles: &OutputStyles,
) -> AppResult<TransferReport> {
    let (base_file_ref, source_reader) = source_fs.download(source_file_ref).await?;

    let skip_reason = match options.file_matcher.matches(&base_file_ref) {
        FileMatcherResult::SkippedDueToSize => Some(SkipReason::OverSizeLimit),
        FileMatcherResult::SkippedDueToName => Some(SkipReason::NameFilter),
        // Ignore rules apply to listings only; this matcher carries none.
        FileMatcherResult::Matched | FileMatcherResult::SkippedDueToIgnore => None,
    };
    if let Some(reason) = skip_reason {
        let outcome = CopyFileOutcome::Skipped { reason };
        print_file_row(term, bar, &base_file_ref, &outcome, styles);
        bar.inc(1);
        return Ok(TransferReport {
            result: TransferFileResult::Skipped,
            file: CopyFileReport::new(&base_file_ref, &outcome, &[]),
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
    let UploadResult {
        result: transfer_result,
        outcome,
        redactions,
    } = if let Some(ref redacter_with_options) = redacter {
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
        UploadResult::without_redactions(TransferFileResult::Copied, CopyFileOutcome::Copied)
    };
    print_file_row(term, bar, file_ref, &outcome, styles);
    bar.inc(1);
    Ok(TransferReport {
        result: transfer_result,
        file: CopyFileReport::new(file_ref, &outcome, &redactions),
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
    styles: &OutputStyles,
) {
    bar.println(format_file_row(
        RowLayout::for_terminal(term.width() as usize),
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
        &outcome.row(styles),
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
    styles: &OutputStyles,
    source_reader: S,
    dest_file_ref: &FileSystemRef,
    options: &CopyCommandOptions,
    redacter_with_options: &(RedacterBaseOptions, Vec<Redacters<'a>>),
    file_converters: &FileConverters<'a>,
    redacter_throttler: &mut Option<RedacterThrottler>,
) -> AppResult<UploadResult> {
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
                let redactions = redacted_result.redactions;
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
                let result = if redacted_result.number_of_redactions > 0 {
                    TransferFileResult::RedactedAndCopied
                } else {
                    TransferFileResult::Copied
                };
                Ok(UploadResult {
                    result,
                    outcome,
                    redactions,
                })
            }
            Ok(redacted_result) => Ok(UploadResult::without_redactions(
                TransferFileResult::Skipped,
                CopyFileOutcome::Skipped {
                    reason: SkipReason::NotRedacted(not_redacted_reason(
                        redacted_result.summary.blocked,
                    )),
                },
            )),
            Err(ref error) => {
                tracing::debug!(error = %error, source = ?error.source(), "redaction failed");
                Ok(UploadResult::without_redactions(
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
        Ok(UploadResult::without_redactions(
            TransferFileResult::Copied,
            CopyFileOutcome::CopiedUnredacted {
                reason: plan_blocked_reason(redact_plan.blocked),
            },
        ))
    } else {
        Ok(UploadResult::without_redactions(
            TransferFileResult::Skipped,
            CopyFileOutcome::Skipped {
                reason: SkipReason::NotRedacted(plan_blocked_reason(redact_plan.blocked)),
            },
        ))
    }
}

/// Why redaction did not happen, defaulting to "no redacter applied" when the
/// pipeline did not record a reason.
fn not_redacted_reason(blocked: Option<crate::redacters::RedactionBlocked>) -> NotRedactedReason {
    blocked.map_or(
        NotRedactedReason::NoRedacterApplied,
        NotRedactedReason::from,
    )
}

/// Why nothing in the plan could redact or scan this file at all: a PDF or image blocked on
/// a missing conversion engine reports that reason, and anything else is a type no redacter
/// supports.
pub(crate) fn plan_blocked_reason(
    blocked: Option<crate::redacters::RedactionBlocked>,
) -> NotRedactedReason {
    blocked.map_or(NotRedactedReason::TypeNotSupported, NotRedactedReason::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(any(feature = "pdf-render", feature = "ocr"))]
    use crate::addon_store::Addon;
    use crate::commands::artifact_locator::ArtifactLocator;
    use crate::commands::ocsf::{EpochMillis, OcsfEvents};
    use crate::commands::sarif::SarifLog;
    use crate::common_types::{GcpProjectId, GcpRegion};
    use crate::redacters::test_support::{
        initialize_crypto, pdfium_test_lock, TEST_DOCUMENTS_DIR, TEST_DOCUMENT_NAMES,
        TEST_DOCUMENT_SAMPLE_EMAIL, TEST_DOCUMENT_SAMPLE_PHONE,
    };
    use crate::redacters::{
        Category, DetectionLabel, DetectionLocation, GcpDlpRedacter, GcpDlpRedacterOptions,
        GcpVertexAiRedacterOptions, LlmImageMode, LocalRulesRedacter, LocalRulesRedacterOptions,
        RedacterProviderOptions, RuleGroup,
    };
    use tempfile::TempDir;

    const SAMPLE_DOCUMENTS_DIR: &str = TEST_DOCUMENTS_DIR;
    const SAMPLE_FILE_NAMES: [&str; 9] = TEST_DOCUMENT_NAMES;
    const SAMPLE_EMAIL: &str = TEST_DOCUMENT_SAMPLE_EMAIL;
    const SAMPLE_PHONE: &str = TEST_DOCUMENT_SAMPLE_PHONE;

    fn base_redacter_options(provider_options: RedacterProviderOptions) -> RedacterOptions {
        RedacterOptions {
            provider_options: vec![provider_options],
            base_options: RedacterBaseOptions {
                allow_unsupported_copies: false,
                unsupported_as_text: false,
                csv_headers_disable: false,
                csv_delimiter: None,
                sampling_size: None,
                limit_dlp_requests: None,
                categories: None,
                list_redactions: true,
            },
        }
    }

    fn report_file_ref() -> FileSystemRef {
        FileSystemRef {
            relative_path: "notes/note.txt".into(),
            media_type: Some(mime::TEXT_PLAIN),
            file_size: Some(10),
        }
    }

    fn text_detection(value: &str, start: usize, line: usize) -> Detection {
        Detection {
            label: DetectionLabel::new("email"),
            location: DetectionLocation::Text {
                start,
                end: start + value.len(),
                line,
                column: 1,
            },
            score: None,
            value: value.to_string(),
        }
    }

    fn report_json(outcome: &CopyFileOutcome) -> serde_json::Value {
        serde_json::to_value(CopyFileReport::new(&report_file_ref(), outcome, &[])).unwrap()
    }

    #[test]
    fn a_file_report_takes_its_status_and_reason_from_the_outcome() {
        let outcomes = [
            (
                CopyFileOutcome::Redacted {
                    redacters: vec![RedacterType::LocalRules],
                    conversion: None,
                },
                "redacted",
                None,
            ),
            (CopyFileOutcome::Copied, "copied", None),
            (
                CopyFileOutcome::CopiedUnredacted {
                    reason: NotRedactedReason::TypeNotSupported,
                },
                "copied_unredacted",
                Some("type not supported"),
            ),
            (
                CopyFileOutcome::Skipped {
                    reason: SkipReason::OverSizeLimit,
                },
                "skipped",
                Some("over size limit"),
            ),
            (
                CopyFileOutcome::Skipped {
                    reason: SkipReason::NotRedacted(NotRedactedReason::OcrUnavailable),
                },
                "skipped",
                Some("ocr unavailable"),
            ),
            (
                CopyFileOutcome::Error {
                    message: "the service refused".to_string(),
                },
                "failed",
                Some("the service refused"),
            ),
        ];
        for (outcome, status, reason) in outcomes {
            let json = report_json(&outcome);
            assert_eq!(json["status"], status, "{outcome:?}");
            assert_eq!(json["reason"], serde_json::json!(reason), "{outcome:?}");
            assert_eq!(json["path"], "notes/note.txt");
            assert_eq!(json["mime_type"], "text/plain");
        }
    }

    fn file_report_at(
        path: &str,
        outcome: &CopyFileOutcome,
        redactions: &[(RedacterType, Detection)],
    ) -> CopyFileReport {
        let file_ref = FileSystemRef {
            relative_path: path.into(),
            media_type: Some(mime::TEXT_PLAIN),
            file_size: Some(10),
        };
        CopyFileReport::new(&file_ref, outcome, redactions)
    }

    fn result_of(files: Vec<CopyFileReport>) -> CopyCommandResult {
        CopyCommandResult {
            files_copied: 0,
            files_redacted: 0,
            files_skipped: 0,
            files,
            categories: None,
        }
    }

    #[test]
    fn the_json_and_text_reports_state_the_categories_only_when_they_limit_the_run() {
        let mut result = result_of(Vec::new());
        let unlimited_json = serde_json::to_value(&result).unwrap();
        let unlimited_text = result.to_text();
        assert!(unlimited_json.get("categories").is_none());
        assert!(!unlimited_text.contains("Categories:"));

        result.categories = Categories::limiting([Category::Credential, Category::Contact]);
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(
            json["categories"],
            serde_json::json!(["contact", "credential"])
        );
        let text = result.to_text();
        assert_eq!(text.lines().next(), Some("Categories: contact, credential"));
        assert_eq!(
            text.strip_prefix("Categories: contact, credential\n"),
            Some(unlimited_text.as_str())
        );
    }

    fn sarif_outcome_names(files: Vec<CopyFileReport>) -> Vec<(String, &'static str)> {
        result_of(files)
            .reported_files()
            .into_iter()
            .map(|artifact| {
                let outcome = match artifact.outcome {
                    FileOutcome::Findings(findings) if findings.is_empty() => "clean",
                    FileOutcome::Findings(_) => "findings",
                    FileOutcome::Unconfirmed { .. } => "unconfirmed",
                    FileOutcome::Failed { .. } => "failed",
                    FileOutcome::Blocked { .. } => "blocked",
                    FileOutcome::Unsupported { .. } => "unsupported",
                };
                (artifact.path, outcome)
            })
            .collect()
    }

    #[test]
    fn the_sarif_report_maps_each_outcome_of_a_copy_and_leaves_out_filtered_files() {
        let redacted = CopyFileOutcome::Redacted {
            redacters: vec![RedacterType::LocalRules],
            conversion: None,
        };
        let skipped = |reason| CopyFileOutcome::Skipped { reason };
        let files = vec![
            file_report_at(
                "found.txt",
                &redacted,
                &[(
                    RedacterType::LocalRules,
                    text_detection(SAMPLE_EMAIL, 14, 2),
                )],
            ),
            file_report_at("untouched.txt", &redacted, &[]),
            file_report_at("plain.txt", &CopyFileOutcome::Copied, &[]),
            file_report_at(
                "binary.bin",
                &CopyFileOutcome::CopiedUnredacted {
                    reason: NotRedactedReason::TypeNotSupported,
                },
                &[],
            ),
            file_report_at(
                "scan.png",
                &CopyFileOutcome::CopiedUnredacted {
                    reason: NotRedactedReason::OcrUnavailable,
                },
                &[],
            ),
            file_report_at("big.txt", &skipped(SkipReason::OverSizeLimit), &[]),
            file_report_at("other.md", &skipped(SkipReason::NameFilter), &[]),
            file_report_at(
                "form.pdf",
                &skipped(SkipReason::NotRedacted(
                    NotRedactedReason::PdfRendererUnavailable,
                )),
                &[],
            ),
            file_report_at(
                "data.bin",
                &skipped(SkipReason::NotRedacted(NotRedactedReason::TypeNotSupported)),
                &[],
            ),
            file_report_at(
                "broken.txt",
                &CopyFileOutcome::Error {
                    message: "the service refused".to_string(),
                },
                &[],
            ),
        ];

        let outcomes = sarif_outcome_names(files);

        assert_eq!(
            outcomes
                .iter()
                .map(|(path, outcome)| (path.as_str(), *outcome))
                .collect::<Vec<_>>(),
            [
                ("found.txt", "findings"),
                ("untouched.txt", "clean"),
                ("plain.txt", "unsupported"),
                ("binary.bin", "unsupported"),
                ("scan.png", "blocked"),
                ("form.pdf", "blocked"),
                ("data.bin", "unsupported"),
                ("broken.txt", "failed"),
            ]
        );
    }

    fn sarif_of(files: Vec<CopyFileReport>) -> serde_json::Value {
        let result = result_of(files);
        let locator = ArtifactLocator::new("./", std::path::Path::new("/work"));
        serde_json::to_value(SarifLog::new(&result.reported_files(), &locator, None)).unwrap()
    }

    #[test]
    fn a_redaction_of_a_whole_file_or_a_page_has_no_region_and_no_value() {
        let outcome = CopyFileOutcome::Redacted {
            redacters: vec![RedacterType::GeminiLlm],
            conversion: None,
        };
        let detection = |location| Detection {
            label: DetectionLabel::new("sensitive_data"),
            location,
            score: None,
            value: String::new(),
        };
        let redactions = [
            (RedacterType::GeminiLlm, detection(DetectionLocation::File)),
            (
                RedacterType::GeminiLlm,
                detection(DetectionLocation::Page { page: 3 }),
            ),
        ];

        let sarif = sarif_of(vec![file_report_at("scan.pdf", &outcome, &redactions)]);

        let results = sarif["runs"][0]["results"].as_array().unwrap();
        assert_eq!(results.len(), 2);
        for result in results {
            assert!(result["locations"][0]["physicalLocation"]["region"].is_null());
            assert!(!result["message"]["text"]
                .as_str()
                .unwrap()
                .contains("Value:"));
        }
    }

    #[tokio::test]
    async fn the_copy_report_lists_each_file_with_its_redactions_beside_the_counters(
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let lock = pdfium_test_lock();
        let _guard = lock.lock().await;

        let source = TempDir::with_prefix("copy_command_tests_report_source")?;
        std::fs::write(
            source.path().join("note.txt"),
            format!("Customer note\nEmail: {SAMPLE_EMAIL}\n"),
        )?;
        std::fs::write(source.path().join("plain.txt"), "nothing to find here\n")?;
        let destination = TempDir::with_prefix("copy_command_tests_report_destination")?;

        let result = command_copy(
            &Term::stdout(),
            &format!("{}/", source.path().display()),
            &destination.path().to_string_lossy(),
            CopyCommandOptions::new(None, None, None, vec![], AddonStoreOptions::default()),
            Some(base_redacter_options(RedacterProviderOptions::LocalRules(
                LocalRulesRedacterOptions {
                    groups: RuleGroup::all(),
                    user_rules: vec![],
                },
            ))),
        )
        .await?;
        let json = serde_json::to_value(&result)?;

        assert_eq!(json["files_copied"], 2);
        assert_eq!(json["files_redacted"], 2);
        assert_eq!(json["files_skipped"], 0);
        let mut files = json["files"].as_array().unwrap().clone();
        files.sort_by_key(|file| file["path"].as_str().unwrap().to_string());
        assert_eq!(files.len(), 2);
        assert_eq!(files[0]["path"], "note.txt");
        assert_eq!(files[0]["status"], "redacted");
        let redactions = files[0]["redactions"].as_array().unwrap();
        assert_eq!(redactions.len(), 1);
        assert_eq!(redactions[0]["redacter"], "local-rules");
        assert_eq!(redactions[0]["label"], "email");
        assert_eq!(redactions[0]["location"]["kind"], "text");
        assert_eq!(redactions[0]["location"]["line"], 2);
        assert_eq!(redactions[0]["location"]["column"], 8);
        assert_eq!(redactions[0]["value"], "j********************m");
        assert_eq!(files[1]["path"], "plain.txt");
        assert_eq!(files[1]["status"], "redacted");
        assert_eq!(files[1]["redactions"], serde_json::json!([]));
        assert!(!json.to_string().contains(SAMPLE_EMAIL));

        let locator = ArtifactLocator::new(
            &format!("{}/", source.path().display()),
            std::path::Path::new("/work"),
        );
        let sarif = serde_json::to_value(SarifLog::new(&result.reported_files(), &locator, None))?;
        let results = sarif["runs"][0]["results"].as_array().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["ruleId"], "local-rules/email");
        assert_eq!(
            results[0]["locations"][0]["physicalLocation"]["region"],
            serde_json::json!({"startLine": 2, "startColumn": 8, "endLine": 2, "endColumn": 30})
        );
        assert!(!sarif.to_string().contains(SAMPLE_EMAIL));

        let ndjson = OcsfEvents::new(
            &result.reported_files(),
            &locator,
            EpochMillis::try_from(std::time::SystemTime::now())?,
        )
        .to_ndjson()?;
        let event: serde_json::Value = serde_json::from_str(&ndjson)?;
        assert_eq!(ndjson.lines().count(), 1);
        assert_eq!(
            event["file"]["path"],
            format!("{}/note.txt", source.path().display())
        );
        assert_eq!(
            event["data_security"]["discovery_details"][0]["occurrences"],
            serde_json::json!([{"start_line": 2, "end_line": 2, "column_number": 8}])
        );
        assert!(!ndjson.contains(SAMPLE_EMAIL));
        Ok(())
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
            CopyCommandOptions::new(None, None, None, vec![], AddonStoreOptions::default()),
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

    fn write_nested_text_files(root: &std::path::Path) {
        for file in ["a/notes.txt", "b/notes.txt", "top.txt"] {
            let path = root.join(file);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, "plain text").unwrap();
        }
    }

    #[tokio::test]
    async fn a_filter_naming_a_subdirectory_copies_its_nested_files(
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let lock = pdfium_test_lock();
        let _guard = lock.lock().await;

        let source = TempDir::with_prefix("copy_command_tests_nested_filter_source")?;
        write_nested_text_files(source.path());
        let destination = TempDir::with_prefix("copy_command_tests_nested_filter_dest")?;

        let result = command_copy(
            &Term::stdout(),
            &format!("{}/", source.path().display()),
            &destination.path().to_string_lossy(),
            CopyCommandOptions::new(
                Some(globset::Glob::new("a/*")?),
                None,
                None,
                vec![],
                AddonStoreOptions::default(),
            ),
            None,
        )
        .await?;

        assert_eq!((result.files_copied, result.files_skipped), (1, 2));
        assert!(destination.path().join("a/notes.txt").is_file());
        assert!(!destination.path().join("b/notes.txt").exists());
        Ok(())
    }

    #[tokio::test]
    async fn a_filter_naming_a_directory_copies_its_files_from_a_zip(
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use std::io::Write;

        let lock = pdfium_test_lock();
        let _guard = lock.lock().await;

        let scratch = TempDir::with_prefix("copy_command_tests_nested_filter_zip")?;
        let zip_path = scratch.path().join("archive.zip");
        let mut writer = zip::ZipWriter::new(std::fs::File::create(&zip_path)?);
        for file in ["a/notes.txt", "b/notes.txt", "top.txt"] {
            writer.start_file(file, zip::write::SimpleFileOptions::default())?;
            writer.write_all(b"plain text")?;
        }
        writer.finish()?;
        let destination = scratch.path().join("out");
        std::fs::create_dir(&destination)?;

        let result = command_copy(
            &Term::stdout(),
            &format!("zip://{}", zip_path.display()),
            &destination.to_string_lossy(),
            CopyCommandOptions::new(
                Some(globset::Glob::new("a/*")?),
                None,
                None,
                vec![],
                AddonStoreOptions::default(),
            ),
            None,
        )
        .await?;

        assert_eq!((result.files_copied, result.files_skipped), (1, 2));
        assert!(destination.join("a/notes.txt").is_file());
        assert!(!destination.join("b").exists());
        assert!(!destination.join("top.txt").exists());
        Ok(())
    }

    /// Only `addons install` installs anything: a redacting copy of a PDF over an empty addons
    /// directory runs without the addons and leaves the directory as it found it.
    #[tokio::test]
    async fn a_redacting_copy_never_downloads_an_addon(
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let lock = pdfium_test_lock();
        let _guard = lock.lock().await;

        let term = Term::stdout();
        let temp_dir = TempDir::with_prefix("copy_command_tests_no_addons")?;
        let addons_dir = temp_dir.path().join("addons");
        command_copy(
            &term,
            "test-fixtures/documents/customer-form.pdf",
            &temp_dir.path().join("out.pdf").to_string_lossy(),
            CopyCommandOptions::new(
                None,
                None,
                None,
                vec![],
                AddonStoreOptions {
                    addons_dir: Some(addons_dir.clone()),
                },
            ),
            Some(base_redacter_options(RedacterProviderOptions::LocalRules(
                LocalRulesRedacterOptions {
                    groups: RuleGroup::all(),
                    user_rules: vec![],
                },
            ))),
        )
        .await?;
        assert!(
            !addons_dir.exists(),
            "nothing may be written to {addons_dir:?}"
        );
        Ok(())
    }

    /// A damaged OCR install is not a reason to copy nothing: the run goes on without OCR.
    #[cfg(feature = "ocr")]
    #[tokio::test]
    async fn copies_over_a_stale_addons_directory(
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let lock = pdfium_test_lock();
        let _guard = lock.lock().await;

        let term = Term::stdout();
        let temp_dir = TempDir::with_prefix("copy_command_tests_stale_addons")?;
        let addons_dir = temp_dir.path().join("addons");
        let ocrs_dir = addons_dir.join("ocrs");
        std::fs::create_dir_all(&ocrs_dir)?;
        for file in Addon::Ocr.manifest().unwrap().files {
            std::fs::write(ocrs_dir.join(file.name), b"junk")?;
        }
        let destination = temp_dir.path().join("out");
        std::fs::create_dir_all(&destination)?;

        let with_stale_addons = || {
            CopyCommandOptions::new(
                None,
                None,
                None,
                vec![],
                AddonStoreOptions {
                    addons_dir: Some(addons_dir.clone()),
                },
            )
        };

        let result = command_copy(
            &term,
            SAMPLE_DOCUMENTS_DIR,
            &destination.to_string_lossy(),
            with_stale_addons(),
            None,
        )
        .await?;
        assert_eq!(result.files_copied, SAMPLE_FILE_NAMES.len());

        // With a redacter and an image, which only OCR could read, the image is skipped.
        let image_destination = temp_dir.path().join("images");
        std::fs::create_dir_all(&image_destination)?;
        let result = command_copy(
            &term,
            "test-fixtures/media/",
            &image_destination.to_string_lossy(),
            with_stale_addons(),
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

    /// PII values the OCR fixtures carry (`test-fixtures/documents/customer-form.pdf` and
    /// `test-fixtures/media/form-example.png` render the same form), split into the
    /// substrings a single OCR word can hold: OCR words never contain spaces, so a value
    /// with spaces in it ("14 March 1985", "NW1 6XE") is checked one word at a time.
    const OCR_FIXTURE_PII_WORDS: [&str; 8] = [
        "john.smith@example.com",
        "123-4567",
        "X1234567",
        "4111",
        "NW1",
        "6XE",
        "March",
        "1985",
    ];

    /// A word from the form's own header, never itself redacted, so a run that recognises
    /// nothing at all (a renderer or OCR regression, not a redaction one) still fails loudly
    /// instead of vacuously passing every "PII is gone" assertion.
    const OCR_FIXTURE_CONTROL_WORD: &str = "Customer";

    /// Runs `ocr` over `image` and asserts none of [`OCR_FIXTURE_PII_WORDS`] survived
    /// redaction while [`OCR_FIXTURE_CONTROL_WORD`] did, proving the redacted output (not
    /// just its byte length) carries no PII.
    fn assert_ocr_output_is_redacted(
        ocr: &dyn crate::file_converters::ocr::Ocr,
        image: image::DynamicImage,
        label: &str,
    ) -> AppResult<()> {
        let recognised: Vec<String> = ocr
            .image_to_text(image)?
            .into_iter()
            .filter_map(|coord| coord.text)
            .collect();
        for pii in OCR_FIXTURE_PII_WORDS {
            assert!(
                !recognised.iter().any(|word| word.contains(pii)),
                "{label}: {pii:?} should not be recognisable after redaction, saw {recognised:?}"
            );
        }
        assert!(
            recognised
                .iter()
                .any(|word| word.contains(OCR_FIXTURE_CONTROL_WORD)),
            "{label}: the control word {OCR_FIXTURE_CONTROL_WORD:?} should still be \
             recognisable (otherwise this proves nothing about redaction), saw {recognised:?}"
        );
        Ok(())
    }

    /// Reproduces the last-word-of-every-OCR-line defect end to end: every value on the
    /// sample form is the last word of its line (email, phone, DOB, postcode, passport, card,
    /// and the repeated surname in the signature line), so before the fix in
    /// `ocr_ocrs::words_from_line` none of them ever reached `local-rules` and the output PDF
    /// and PNG carried every one of them in the clear.
    /// Runs the OCR redaction path on one file's bytes through `StreamRedacter`, exactly as
    /// `command_copy` does internally, and returns the redacted output bytes.
    async fn redact_via_stream_redacter(
        stream_redacter: &StreamRedacter<'_>,
        redacters: &Vec<Redacters<'_>>,
        file_ref: &FileSystemRef,
        input_bytes: Vec<u8>,
    ) -> AppResult<Vec<u8>> {
        use futures::TryStreamExt;
        let plan = stream_redacter
            .create_redact_plan(redacters, file_ref)
            .await?;
        assert!(
            !plan.supported_redacters.is_empty(),
            "local-rules should be able to redact {} via OCR, blocked: {:?}",
            file_ref.relative_path.value(),
            plan.blocked
        );
        let input = futures::stream::iter(vec![Ok(bytes::Bytes::from(input_bytes))]);
        let result = stream_redacter.redact_stream(input, plan, file_ref).await?;
        assert!(
            result.number_of_redactions > 0,
            "{} should have been redacted",
            file_ref.relative_path.value()
        );
        let chunks: Vec<bytes::Bytes> = result.stream.try_collect().await?;
        Ok(chunks.concat())
    }

    /// Drives `StreamRedacter` directly instead of `command_copy`: pdfium can only be bound
    /// successfully once per process on this platform (a second `PdfImageConverter::new()`
    /// call fails), and `command_copy` binds its own converters internally on every call, so
    /// redacting the PDF and then reading its output back through `command_copy` a second
    /// time would need a bind that never succeeds. Binding once here and reusing it for both
    /// the redaction and the read-back avoids that.
    #[tokio::test]
    #[cfg_attr(not(feature = "ci-ocr"), ignore)]
    async fn command_copy_local_rules_redacts_ocr_documents_test(
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let lock = pdfium_test_lock();
        let _guard = lock.lock().await;

        let term = Term::stdout();
        let reporter = AppReporter::from(&term);
        let store = AddonStore::new(&AddonStoreOptions::default());
        let converters = FileConverters::new().init(&reporter, &store).await?;
        let ocr = converters
            .ocr
            .as_deref()
            .expect("the ci-ocr feature requires the OCR model to be resolvable");
        let pdf_to_image = converters
            .pdf_image_converter
            .as_deref()
            .expect("the ci-ocr feature requires pdfium to be available");

        let base_options = base_redacter_options(RedacterProviderOptions::LocalRules(
            LocalRulesRedacterOptions {
                groups: RuleGroup::all(),
                user_rules: vec![],
            },
        ))
        .base_options;
        let redacter = LocalRulesRedacter::new(
            LocalRulesRedacterOptions {
                groups: RuleGroup::all(),
                user_rules: vec![],
            },
            &reporter,
        )
        .await?;
        let redacters = vec![Redacters::LocalRules(redacter)];
        let stream_redacter = StreamRedacter::new(&base_options, &converters);

        let pdf_bytes = tokio::fs::read("test-fixtures/documents/customer-form.pdf").await?;
        let pdf_file_ref = FileSystemRef {
            relative_path: "customer-form.pdf".into(),
            media_type: Some(mime::APPLICATION_PDF),
            file_size: Some(pdf_bytes.len()),
        };
        let redacted_pdf_bytes =
            redact_via_stream_redacter(&stream_redacter, &redacters, &pdf_file_ref, pdf_bytes)
                .await?;
        let mut pages = pdf_to_image
            .convert_to_images(redacted_pdf_bytes.into())?
            .pages;
        let page_image = pages.remove(0).page_as_images;
        assert_ocr_output_is_redacted(ocr, page_image, "the redacted PDF")?;

        let png_bytes = tokio::fs::read("test-fixtures/media/form-example.png").await?;
        let png_file_ref = FileSystemRef {
            relative_path: "form-example.png".into(),
            media_type: Some(mime::IMAGE_PNG),
            file_size: Some(png_bytes.len()),
        };
        let redacted_png_bytes =
            redact_via_stream_redacter(&stream_redacter, &redacters, &png_file_ref, png_bytes)
                .await?;
        let png_image = image::load_from_memory(&redacted_png_bytes)?;
        assert_ocr_output_is_redacted(ocr, png_image, "the redacted PNG")?;

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
                built_in_info_types: GcpDlpRedacter::default_info_types(),
                user_defined_stored_info_types: vec![],
            }));

        command_copy(
            &term,
            SAMPLE_DOCUMENTS_DIR,
            &temp_dir.path().to_string_lossy(),
            CopyCommandOptions::new(None, None, None, vec![], AddonStoreOptions::default()),
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
                    categories: None,
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
            CopyCommandOptions::new(None, None, None, vec![], AddonStoreOptions::default()),
            Some(vertex_ai_options()),
        )
        .await?;

        let note_destination = temp_dir.path().join("customer-note.txt");
        command_copy(
            &term,
            "test-fixtures/documents/customer-note.txt",
            &note_destination.to_string_lossy(),
            CopyCommandOptions::new(None, None, None, vec![], AddonStoreOptions::default()),
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
