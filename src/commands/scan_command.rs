//! The `scan` command: runs the selected redacters in detect-only mode over every file of a
//! source and reports what they found and where. The source is only read.

use crate::args::RedacterType;
use crate::commands::copy_command::{plan_blocked_reason, run_needs_ocr, single_source_needs_ocr};
use crate::commands::copy_output::{NotRedactedReason, SkipReason};
use crate::commands::scan_output::{
    format_file_lines, format_scan_header, format_scan_summary, FindingReport, ScanCommandResult,
    ScanFileReport, ScanOutputStyles, ValueDisplay,
};
use crate::file_converters::FileConverters;
use crate::file_systems::{DetectFileSystem, FileSystemConnection, FileSystemRef};
use crate::file_tools::{FileMatcher, FileMatcherResult, FileMimeOverride};
use crate::model_store::{ModelStore, ModelStoreOptions};
use crate::redacters::{
    DetectStreamResult, RedacterOptions, RedacterThrottler, Redacters, Score, StreamDetector,
};
use crate::reporter::AppReporter;
use crate::AppResult;
use console::Term;
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use std::error::Error;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct ScanCommandOptions {
    pub file_matcher: FileMatcher,
    pub file_mime_override: FileMimeOverride,
    pub max_files_limit: Option<usize>,
    pub model_store: ModelStoreOptions,
    pub min_score: Option<Score>,
    pub values: ValueDisplay,
}

impl ScanCommandOptions {
    pub fn new(
        filename_filter: Option<globset::Glob>,
        max_size_limit: Option<usize>,
        max_files_limit: Option<usize>,
        mime_override: Vec<(mime::Mime, globset::Glob)>,
        model_store: ModelStoreOptions,
        min_score: Option<Score>,
        values: ValueDisplay,
    ) -> Self {
        let filename_matcher = filename_filter
            .as_ref()
            .map(|filter| filter.compile_matcher());
        Self {
            file_matcher: FileMatcher::new(filename_matcher, max_size_limit),
            file_mime_override: FileMimeOverride::new(mime_override),
            max_files_limit,
            model_store,
            min_score,
            values,
        }
    }
}

/// What every file of one scan shares.
struct ScanContext<'s> {
    term: &'s Term,
    options: &'s ScanCommandOptions,
    redacters: &'s [Redacters<'s>],
    detector: &'s StreamDetector<'s>,
    styles: &'s ScanOutputStyles,
    bar: &'s ProgressBar,
}

pub async fn command_scan(
    term: &Term,
    source: &str,
    options: ScanCommandOptions,
    redacter_options: RedacterOptions,
) -> AppResult<ScanCommandResult> {
    let term_reporter = AppReporter::from(term);
    let models = ModelStore::new(&options.model_store, &term_reporter);
    let styles = ScanOutputStyles::new();

    // Hidden until every model is resolved, so a consent prompt or a download bar never
    // lands under a live progress bar.
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
    let source_listing = if source_fs.has_multiple_files().await? {
        Some(
            source_fs
                .list_files(Some(&options.file_matcher), options.max_files_limit)
                .await?,
        )
    } else {
        None
    };
    let needs_ocr = match &source_listing {
        Some(listing) => run_needs_ocr(true, &listing.files, &options.file_mime_override),
        None => single_source_needs_ocr(
            true,
            source_fs.resolve(None).file_path.as_str(),
            &options.file_mime_override,
        ),
    };
    let file_converters = FileConverters::new()
        .init(&term_reporter, &models, needs_ocr)
        .await?;

    let redacter_types: Vec<RedacterType> = redacter_options
        .provider_options
        .iter()
        .map(|provider| provider.redacter_type())
        .collect();
    term.write_line(&format_scan_header(
        source,
        &redacter_types,
        file_converters.pdf_image_converter.is_some(),
        file_converters.ocr.is_some(),
        &styles,
    ))?;
    term.write_line("")?;
    for provider in &redacter_options.provider_options {
        for id in provider.required_models() {
            models.resolve(id).await?;
        }
    }

    bar.set_draw_target(ProgressDrawTarget::stderr());
    bar.enable_steady_tick(Duration::from_millis(100));

    let mut throttler = redacter_options
        .base_options
        .limit_dlp_requests
        .as_ref()
        .map(|limit| limit.to_throttling_counter());
    let mut redacters = Vec::with_capacity(redacter_options.provider_options.len());
    for provider_options in redacter_options.provider_options {
        redacters.push(Redacters::new_redacter(provider_options, &app_reporter, &models).await?);
    }
    let detector = StreamDetector::new(&redacter_options.base_options, &file_converters);
    let context = ScanContext {
        term,
        options: &options,
        redacters: &redacters,
        detector: &detector,
        styles: &styles,
        bar: &bar,
    };

    let mut result = ScanCommandResult::default();
    match source_listing {
        Some(listing) => {
            result.files_skipped += listing.skipped;
            bar.set_length(listing.files.len() as u64);
            for file in &listing.files {
                let report =
                    scan_file(&context, Some(file), &mut source_fs, &mut throttler).await?;
                result.push(report);
            }
        }
        None => {
            let report = scan_file(&context, None, &mut source_fs, &mut throttler).await?;
            result.push(report);
        }
    }

    source_fs.close().await?;
    bar.finish_and_clear();
    term.write_line("")?;
    term.write_line(&format_scan_summary(&result, &styles))?;
    Ok(result)
}

/// Scans one file and prints its lines. A failure of a redacter on the file is part of the
/// report, not an error of the scan: the other files are still scanned.
async fn scan_file<'a, SFS: FileSystemConnection<'a>>(
    context: &ScanContext<'_>,
    source_file_ref: Option<&FileSystemRef>,
    source_fs: &mut SFS,
    throttler: &mut Option<RedacterThrottler>,
) -> AppResult<ScanFileReport> {
    let (base_file_ref, reader) = source_fs.download(source_file_ref).await?;
    let file_ref = context
        .options
        .file_mime_override
        .override_for_file_ref(source_file_ref.unwrap_or(&base_file_ref).clone());

    let skip_reason = match context.options.file_matcher.matches(&base_file_ref) {
        FileMatcherResult::SkippedDueToSize => Some(SkipReason::OverSizeLimit),
        FileMatcherResult::SkippedDueToName => Some(SkipReason::NameFilter),
        FileMatcherResult::Matched => None,
    };
    let report = match skip_reason {
        Some(reason) => ScanFileReport::not_scanned(&file_ref, reason),
        None => {
            let plan = context
                .detector
                .create_detect_plan(context.redacters, &file_ref)
                .await?;
            if plan.is_empty() {
                ScanFileReport::not_scanned(
                    &file_ref,
                    SkipReason::NotRedacted(plan_blocked_reason(plan.blocked)),
                )
            } else {
                if let Some(throttler) = throttler {
                    *throttler = throttler.update(Instant::now());
                    let delay = *throttler.delay();
                    if !delay.is_zero() {
                        context.bar.println(
                            context
                                .styles
                                .dimmed
                                .apply_to(format!(
                                    "⧗ Delaying detection for {} seconds",
                                    delay.as_secs()
                                ))
                                .to_string(),
                        );
                        tokio::time::sleep(delay).await;
                    }
                }
                match context
                    .detector
                    .detect_stream(reader, &plan, &file_ref)
                    .await
                {
                    Ok(DetectStreamResult {
                        blocked: Some(blocked),
                        ..
                    }) => ScanFileReport::not_scanned(
                        &file_ref,
                        SkipReason::NotRedacted(NotRedactedReason::from(blocked)),
                    ),
                    Ok(scanned) => ScanFileReport::scanned(
                        &file_ref,
                        scanned
                            .detections
                            .iter()
                            .filter(|scanned| {
                                context.options.min_score.is_none_or(|min_score| {
                                    scanned.detection().meets_min_score(min_score)
                                })
                            })
                            .map(|scanned| FindingReport::new(scanned, context.options.values))
                            .collect(),
                    ),
                    Err(error) => {
                        tracing::debug!(error = %error, source = ?error.source(), "detection failed");
                        ScanFileReport::failed(&file_ref, error.to_string())
                    }
                }
            }
        }
    };
    // Written to the terminal rather than through the bar, which drops what it is given to
    // print when stdout is not a terminal, as in CI.
    context.bar.suspend(|| -> AppResult<()> {
        for line in format_file_lines(&report, context.styles) {
            context.term.write_line(&line)?;
        }
        Ok(())
    })?;
    context.bar.inc(1);
    Ok(report)
}
