//! The `scan` command: runs the selected redacters in detect-only mode over every file of a
//! source and reports what they found and where. The source is only read.

use crate::addon_store::{AddonStore, AddonStoreOptions};
use crate::commands::copy_command::plan_blocked_reason;
use crate::commands::copy_output::{
    format_capabilities_line, format_legend, Capabilities, NotRedactedReason, OutputStyles,
    RowLayout, SkipReason,
};
use crate::commands::scan_output::{
    format_file_lines, format_scan_summary, FindingReport, ScanCommandResult, ScanFileReport,
    ValueDisplay, SCAN_LEGEND,
};
use crate::errors::AppError;
use crate::file_converters::FileConverters;
use crate::file_systems::{DetectFileSystem, FileSystemConnection, FileSystemRef};
use crate::file_tools::{FileMatcher, FileMatcherResult, FileMimeOverride, IgnoreFileChoice};
use crate::redacters::{
    DetectStreamResult, Detection, RedacterOptions, RedacterThrottler, Redacters, Score,
    StreamRedacter,
};
use crate::reporter::AppReporter;
use crate::AppResult;
use console::Term;
use futures::Stream;
use indicatif::{HumanBytes, ProgressBar, ProgressDrawTarget, ProgressStyle};
use std::error::Error;
use std::time::{Duration, Instant};
use tokio_util::bytes;

#[derive(Debug, Clone)]
pub struct ScanCommandOptions {
    pub file_matcher: FileMatcher,
    pub file_mime_override: FileMimeOverride,
    pub max_files_limit: Option<usize>,
    pub addon_store: AddonStoreOptions,
    pub min_score: Option<Score>,
    pub values: ValueDisplay,
    pub ignore: IgnoreFileChoice,
}

impl ScanCommandOptions {
    pub fn new(
        filename_filter: Option<globset::Glob>,
        max_size_limit: Option<usize>,
        max_files_limit: Option<usize>,
        mime_override: Vec<(mime::Mime, globset::Glob)>,
        addon_store: AddonStoreOptions,
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
            addon_store,
            min_score,
            values,
            ignore: IgnoreFileChoice::SourceRoot,
        }
    }

    pub fn with_ignore(self, ignore: IgnoreFileChoice) -> Self {
        Self { ignore, ..self }
    }
}

/// What every file of one scan shares.
struct ScanContext<'s> {
    term: &'s Term,
    options: &'s ScanCommandOptions,
    redacters: &'s [Redacters<'s>],
    stream_redacter: &'s StreamRedacter<'s>,
    styles: &'s OutputStyles,
    bar: &'s ProgressBar,
}

pub async fn command_scan(
    term: &Term,
    source: &str,
    options: ScanCommandOptions,
    redacter_options: RedacterOptions,
) -> AppResult<ScanCommandResult> {
    let term_reporter = AppReporter::from(term);
    let addons = AddonStore::new(&options.addon_store);
    let styles = OutputStyles::new();

    // The bar exists from here on because the file systems report through it, but it is not
    // drawn until the converters and redacters have been bound: they report straight to the
    // terminal, those lines must not land under a live progress bar, and a failure to bind one
    // must not leave a bar running.
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
        let ignore_rules = options.ignore.load(source_fs.local_directory())?;
        let listing_matcher = options.file_matcher.clone().with_ignore(ignore_rules);
        Some(
            source_fs
                .list_files(Some(&listing_matcher), options.max_files_limit)
                .await?,
        )
    } else {
        None
    };
    let file_converters = FileConverters::new().init(&term_reporter, &addons).await?;

    term.write_line(&format!(
        "Scanning {}.",
        styles.highlighted.apply_to(source)
    ))?;
    term.write_line(&format_capabilities_line(
        &Capabilities::new(
            redacter_options
                .provider_options
                .iter()
                .map(|provider| provider.redacter_type())
                .collect(),
            &file_converters,
            redacter_options.base_options.sampling_size,
        ),
        &styles,
    ))?;
    term.write_line("")?;

    let mut throttler = redacter_options
        .base_options
        .limit_dlp_requests
        .as_ref()
        .map(|limit| limit.to_throttling_counter());
    let mut redacters = Vec::with_capacity(redacter_options.provider_options.len());
    for provider_options in redacter_options.provider_options {
        redacters.push(Redacters::new_redacter(provider_options, &term_reporter, &addons).await?);
    }

    bar.set_draw_target(ProgressDrawTarget::stderr());
    bar.enable_steady_tick(Duration::from_millis(100));
    let stream_redacter = StreamRedacter::new(&redacter_options.base_options, &file_converters);
    let context = ScanContext {
        term,
        options: &options,
        redacters: &redacters,
        stream_redacter: &stream_redacter,
        styles: &styles,
        bar: &bar,
    };

    let mut result = ScanCommandResult::default();
    match source_listing {
        Some(listing) => {
            let total_size: usize = listing.files.iter().filter_map(|file| file.file_size).sum();
            let mut header = vec![format!(
                "Found {} files. Total size: {}",
                styles.highlighted.apply_to(listing.files.len()),
                styles.highlighted.apply_to(HumanBytes(total_size as u64))
            )];
            if listing.ignored > 0 {
                header.push(format!(
                    "{} files ignored.",
                    styles.highlighted.apply_to(listing.ignored)
                ));
            }
            if !listing.files.is_empty() {
                header.push(format_legend(&SCAN_LEGEND, &styles));
            }
            write_lines(term, &bar, &header)?;
            result.files_skipped += listing.skipped;
            result.files_ignored += listing.ignored;
            bar.set_length(listing.files.len() as u64);
            for file in &listing.files {
                let report =
                    scan_file(&context, Some(file), &mut source_fs, &mut throttler).await?;
                result.push(report);
            }
        }
        None => {
            write_lines(term, &bar, &[format_legend(&SCAN_LEGEND, &styles)])?;
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

/// Scans one file and prints its lines. A failure on the file, in reading it or in a
/// redacter, is part of the report, not an error of the scan: the other files are still
/// scanned.
async fn scan_file<'a, SFS: FileSystemConnection<'a>>(
    context: &ScanContext<'_>,
    source_file_ref: Option<&FileSystemRef>,
    source_fs: &mut SFS,
    throttler: &mut Option<RedacterThrottler>,
) -> AppResult<ScanFileReport> {
    let report = match source_fs.download(source_file_ref).await {
        Ok((base_file_ref, reader)) => {
            let file_ref = context
                .options
                .file_mime_override
                .override_for_file_ref(source_file_ref.unwrap_or(&base_file_ref).clone());
            let skip_reason = match context.options.file_matcher.matches(&base_file_ref) {
                FileMatcherResult::SkippedDueToSize => Some(SkipReason::OverSizeLimit),
                FileMatcherResult::SkippedDueToName => Some(SkipReason::NameFilter),
                // Ignore rules apply to listings only; this matcher carries none.
                FileMatcherResult::Matched | FileMatcherResult::SkippedDueToIgnore => None,
            };
            match skip_reason {
                Some(reason) => ScanFileReport::not_scanned(&file_ref, reason),
                None => detect_in_file(context, reader, &file_ref, throttler).await?,
            }
        }
        Err(error) => {
            let file_ref = source_file_ref.cloned().unwrap_or_else(|| FileSystemRef {
                relative_path: source_fs.resolve(None).file_path.into(),
                media_type: None,
                file_size: None,
            });
            failed_report(&file_ref, &error)
        }
    };
    write_lines(
        context.term,
        context.bar,
        &format_file_lines(
            &report,
            RowLayout::for_terminal(usize::from(context.term.size().1)),
            context.styles,
        ),
    )?;
    context.bar.inc(1);
    Ok(report)
}

/// Writes `lines` to the terminal rather than through the bar, which drops what it is given to
/// print when stdout is not a terminal, as in CI.
fn write_lines(term: &Term, bar: &ProgressBar, lines: &[String]) -> AppResult<()> {
    bar.suspend(|| lines.iter().try_for_each(|line| term.write_line(line)))?;
    Ok(())
}

fn failed_report(file_ref: &FileSystemRef, error: &AppError) -> ScanFileReport {
    tracing::debug!(error = %error, source = ?error.source(), "scan of a file failed");
    ScanFileReport::failed(file_ref, error.to_string())
}

/// What the redacters found in the file's content, or why they could not read it. The error is
/// the terminal's, never the file's.
async fn detect_in_file(
    context: &ScanContext<'_>,
    reader: impl Stream<Item = AppResult<bytes::Bytes>> + Send + Unpin + Sync + 'static,
    file_ref: &FileSystemRef,
    throttler: &mut Option<RedacterThrottler>,
) -> AppResult<ScanFileReport> {
    let plan = match context
        .stream_redacter
        .create_detect_plan(context.redacters, file_ref)
        .await
    {
        Ok(plan) => plan,
        Err(error) => return Ok(failed_report(file_ref, &error)),
    };
    if plan.supported_redacters.is_empty() {
        return Ok(ScanFileReport::not_scanned(
            file_ref,
            SkipReason::NotRedacted(plan_blocked_reason(plan.blocked)),
        ));
    }
    if let Some(throttler) = throttler {
        *throttler = throttler.update(Instant::now());
        let delay = *throttler.delay();
        if !delay.is_zero() {
            write_lines(
                context.term,
                context.bar,
                &[context
                    .styles
                    .dimmed
                    .apply_to(format!(
                        "⧗ Delaying detection for {} seconds",
                        delay.as_secs()
                    ))
                    .to_string()],
            )?;
            tokio::time::sleep(delay).await;
        }
    }
    Ok(
        match context
            .stream_redacter
            .detect_stream(reader, &plan, file_ref)
            .await
        {
            Ok(DetectStreamResult {
                blocked: Some(blocked),
                ..
            }) => ScanFileReport::not_scanned(
                file_ref,
                SkipReason::NotRedacted(NotRedactedReason::from(blocked)),
            ),
            Ok(scanned) => {
                let meets_min_score = |detection: &Detection| {
                    context.options.min_score.is_none_or(|min_score| {
                        detection.score.is_none_or(|score| score >= min_score)
                    })
                };
                ScanFileReport::scanned(
                    file_ref,
                    scanned
                        .detections
                        .iter()
                        .filter(|(_, detection)| meets_min_score(detection))
                        .map(|(redacter, detection)| {
                            FindingReport::new(*redacter, detection, context.options.values)
                        })
                        .collect(),
                    scanned.unlocated,
                )
                .with_findings_ignored(
                    scanned
                        .ignored
                        .iter()
                        .filter(|(_, detection)| meets_min_score(detection))
                        .count(),
                )
            }
            Err(error) => failed_report(file_ref, &error),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::args::FailOn;
    use crate::redacters::{
        LocalRulesRedacterOptions, RedacterBaseOptions, RedacterProviderOptions, RuleGroup,
    };

    fn email_rules() -> RedacterOptions {
        RedacterOptions {
            provider_options: vec![RedacterProviderOptions::LocalRules(
                LocalRulesRedacterOptions {
                    groups: [RuleGroup::Email].into_iter().collect(),
                    user_rules: Vec::new(),
                },
            )],
            base_options: RedacterBaseOptions {
                allow_unsupported_copies: false,
                unsupported_as_text: false,
                csv_headers_disable: false,
                csv_delimiter: None,
                sampling_size: None,
                limit_dlp_requests: None,
            },
        }
    }

    /// A terminal that writes to `/dev/null`, so the rows of a scan do not land in the output
    /// of the test run.
    fn silent_term() -> Term {
        Term::read_write_pair(
            std::fs::File::open("/dev/null").unwrap(),
            std::fs::OpenOptions::new()
                .write(true)
                .open("/dev/null")
                .unwrap(),
        )
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_file_that_cannot_be_read_is_reported_failed_and_the_scan_goes_on() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::TempDir::with_prefix("scan_unreadable_file").unwrap();
        std::fs::write(dir.path().join("a.txt"), "mail a@b.io").unwrap();
        let unreadable = dir.path().join("b.txt");
        std::fs::write(&unreadable, "mail c@d.io").unwrap();
        std::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(0o000)).unwrap();
        if std::fs::File::open(&unreadable).is_ok() {
            // Permissions do not stop this user, such as root, so no file can fail to open.
            return;
        }

        let result = command_scan(
            &silent_term(),
            &format!("{}/", dir.path().display()),
            ScanCommandOptions::new(
                None,
                None,
                None,
                Vec::new(),
                AddonStoreOptions::default(),
                Some(Score::from_static(0.9)),
                ValueDisplay::Masked,
            ),
            email_rules(),
        )
        .await
        .expect("one unreadable file does not abort the scan");

        // A local-rules finding has no score, so `--min-score` keeps it.
        assert_eq!(
            (result.files_scanned, result.files_failed, result.findings),
            (1, 1, 1)
        );
        assert_eq!(result.exit_code(FailOn::Findings), 1);
    }

    async fn scan_directory(
        directory: &std::path::Path,
        ignore: IgnoreFileChoice,
        max_files_limit: Option<usize>,
    ) -> ScanCommandResult {
        command_scan(
            &silent_term(),
            &format!("{}/", directory.display()),
            ScanCommandOptions::new(
                None,
                None,
                max_files_limit,
                Vec::new(),
                AddonStoreOptions::default(),
                None,
                ValueDisplay::Masked,
            )
            .with_ignore(ignore),
            email_rules(),
        )
        .await
        .unwrap()
    }

    /// A source with an email in every file, one in a directory and one under a name a pattern
    /// can hit anywhere, beside the ignore file the patterns come from.
    fn source_with_mock_files() -> tempfile::TempDir {
        let dir = tempfile::TempDir::with_prefix("scan_ignore").unwrap();
        std::fs::create_dir(dir.path().join("mocks")).unwrap();
        for file in ["real.txt", "mocks/key.txt", "token.mock.txt"] {
            std::fs::write(dir.path().join(file), "mail a@b.io").unwrap();
        }
        std::fs::write(dir.path().join("clean.txt"), "nothing here").unwrap();
        std::fs::write(dir.path().join(".redacterignore"), "mocks/\n*.mock.txt\n").unwrap();
        dir
    }

    #[tokio::test]
    async fn the_ignore_file_at_the_source_root_keeps_its_files_out_of_the_scan() {
        let dir = source_with_mock_files();

        let result = scan_directory(dir.path(), IgnoreFileChoice::SourceRoot, None).await;

        assert_eq!(
            (
                result.files_ignored,
                result.files_scanned,
                result.files_unsupported,
                result.findings
            ),
            (3, 2, 0, 1),
            "the ignored files are not read, and the ignore file excludes itself"
        );
        assert_eq!(result.exit_code(FailOn::Findings), 2);
    }

    #[tokio::test]
    async fn no_ignore_scans_every_file_of_the_source() {
        let dir = source_with_mock_files();

        let result = scan_directory(dir.path(), IgnoreFileChoice::Disabled, None).await;

        assert_eq!((result.files_ignored, result.findings), (0, 3));
    }

    #[tokio::test]
    async fn an_explicit_ignore_file_replaces_the_one_at_the_source_root() {
        let dir = source_with_mock_files();
        let elsewhere = tempfile::TempDir::with_prefix("scan_ignore_explicit").unwrap();
        let explicit = elsewhere.path().join("ci-ignore");
        std::fs::write(&explicit, "real.txt\n").unwrap();

        let result = scan_directory(dir.path(), IgnoreFileChoice::Explicit(explicit), None).await;

        assert_eq!((result.files_ignored, result.findings), (1, 2));
    }

    #[tokio::test]
    async fn a_marked_line_is_left_out_of_the_findings_and_counted() {
        let dir = tempfile::TempDir::with_prefix("scan_allow_marker").unwrap();
        std::fs::write(
            dir.path().join("config.txt"),
            "mail a@b.io\nmock c@d.io # redacter:allow\nmail e@f.io\n",
        )
        .unwrap();

        let result = scan_directory(dir.path(), IgnoreFileChoice::Disabled, None).await;

        assert_eq!(
            (
                result.findings,
                result.findings_ignored,
                result.files_with_findings
            ),
            (2, 1, 1)
        );
        let written: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&result).unwrap()).unwrap();
        assert_eq!(written["findings_ignored"], 1);
    }

    #[tokio::test]
    async fn a_file_named_as_the_source_is_scanned_even_when_the_ignore_file_lists_it() {
        let dir = source_with_mock_files();
        let explicit = dir.path().join("token.mock.txt");

        let result = command_scan(
            &silent_term(),
            &explicit.display().to_string(),
            ScanCommandOptions::new(
                None,
                None,
                None,
                Vec::new(),
                AddonStoreOptions::default(),
                None,
                ValueDisplay::Masked,
            )
            .with_ignore(IgnoreFileChoice::Explicit(
                dir.path().join(".redacterignore"),
            )),
            email_rules(),
        )
        .await
        .unwrap();

        assert_eq!((result.files_ignored, result.findings), (0, 1));
    }

    #[tokio::test]
    async fn ignored_files_do_not_use_up_the_file_limit() {
        let dir = tempfile::TempDir::with_prefix("scan_ignore_limit").unwrap();
        std::fs::create_dir(dir.path().join("mocks")).unwrap();
        for index in 0..20 {
            std::fs::write(dir.path().join(format!("mocks/{index}.txt")), "mail a@b.io").unwrap();
        }
        std::fs::write(dir.path().join("real.txt"), "mail c@d.io").unwrap();
        std::fs::write(dir.path().join("config.mock"), "mail e@f.io").unwrap();
        std::fs::write(dir.path().join(".redacterignore"), "mocks/\n*.mock\n").unwrap();

        let result = scan_directory(dir.path(), IgnoreFileChoice::SourceRoot, Some(1)).await;

        assert_eq!((result.files_scanned, result.findings), (1, 1));
        assert_eq!(result.exit_code(FailOn::Findings), 2);
    }
}
