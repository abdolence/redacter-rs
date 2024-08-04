use crate::errors::AppError;
use crate::filesystems::{
    AbsoluteFilePath, DetectFileSystem, FileMatcher, FileMatcherResult, FileSystemConnection,
    FileSystemRef,
};
use crate::redacters::{Redacter, RedacterOptions, Redacters};
use crate::reporter::AppReporter;
use crate::AppResult;
use console::{Style, Term};
use futures::Stream;
use gcloud_sdk::prost::bytes;
use indicatif::*;
use std::error::Error;
use std::fmt::Write;
use std::time::Duration;

pub struct CopyCommandResult {
    pub files_copied: usize,
    pub files_skipped: usize,
}

#[derive(Debug, Clone)]
pub struct CopyCommandOptions {
    pub file_matcher: FileMatcher,
}

impl CopyCommandOptions {
    pub fn new(filename_filter: Option<globset::Glob>, max_size_limit: Option<u64>) -> Self {
        let filename_matcher = filename_filter
            .as_ref()
            .map(|filter| filter.compile_matcher());
        CopyCommandOptions {
            file_matcher: FileMatcher::new(filename_matcher, max_size_limit),
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
    let bold_style = Style::new().bold();
    let redacted_output = if let Some(ref options) = redacter_options {
        bold_style
            .clone()
            .green()
            .apply_to(format!("✓ Yes ({})", options))
    } else {
        bold_style.clone().red().apply_to("✗ No".to_string())
    };
    term.write_line(
        format!(
            "Copying from {} to {}.\nRedacting: {}.",
            bold_style.clone().white().apply_to(source),
            bold_style.clone().yellow().apply_to(destination),
            redacted_output
        )
        .as_str(),
    )?;
    let bar = ProgressBar::new(1);
    bar.set_style(ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({eta})")
        .unwrap()
        .with_key("eta", |state: &ProgressState, w: &mut dyn Write| write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap())
        .progress_chars("◉>◯"));
    bar.enable_steady_tick(Duration::from_millis(100));
    let app_reporter = AppReporter::from(&bar);

    let mut source_fs = DetectFileSystem::open(source, &app_reporter).await?;
    let mut destination_fs = DetectFileSystem::open(destination, &app_reporter).await?;

    let maybe_redacter = match redacter_options {
        Some(ref options) => Some(Redacters::new_redacter(options, &app_reporter).await?),
        None => None,
    };

    let copy_result: AppResult<CopyCommandResult> = if source_fs.has_multiple_files().await? {
        if !destination_fs.accepts_multiple_files().await? {
            return Err(AppError::DestinationDoesNotSupportMultipleFiles {
                destination: destination.to_string(),
            });
        }
        bar.println("Copying directory and listing source files...");
        let source_files_result = source_fs.list_files(Some(&options.file_matcher)).await?;
        let source_files = source_files_result.files;
        let files_found = source_files.len();
        let files_total_size: u64 = source_files
            .iter()
            .map(|file| file.file_size.unwrap_or(0))
            .sum();
        bar.println(
            format!(
                "Found {} files. Total size: {}",
                bold_style.apply_to(files_found),
                bold_style.apply_to(HumanBytes(files_total_size))
            )
            .as_str(),
        );

        bar.set_length(files_total_size);
        let mut total_files_copied = 0;
        let mut total_files_skipped = source_files_result.skipped;
        for source_file in source_files {
            match transfer_and_redact_file(
                Some(&source_file),
                &bar,
                &mut source_fs,
                &mut destination_fs,
                &options,
                &maybe_redacter,
            )
            .await?
            {
                TransferFileResult::Copied => total_files_copied += 1,
                TransferFileResult::Skipped => total_files_skipped += 1,
            }
        }
        Ok(CopyCommandResult {
            files_copied: total_files_copied,
            files_skipped: total_files_skipped,
        })
    } else {
        Ok(
            match transfer_and_redact_file(
                None,
                &bar,
                &mut source_fs,
                &mut destination_fs,
                &options,
                &maybe_redacter,
            )
            .await?
            {
                TransferFileResult::Copied => CopyCommandResult {
                    files_copied: 1,
                    files_skipped: 0,
                },
                TransferFileResult::Skipped => CopyCommandResult {
                    files_copied: 0,
                    files_skipped: 1,
                },
            },
        )
    };

    destination_fs.close().await?;
    source_fs.close().await?;
    copy_result
}

enum TransferFileResult {
    Copied,
    Skipped,
}

async fn transfer_and_redact_file<
    'a,
    SFS: FileSystemConnection<'a>,
    DFS: FileSystemConnection<'a>,
>(
    source_file_ref: Option<&FileSystemRef>,
    bar: &ProgressBar,
    source_fs: &mut SFS,
    destination_fs: &mut DFS,
    options: &CopyCommandOptions,
    redacter: &Option<impl Redacter>,
) -> AppResult<TransferFileResult> {
    let bold_style = Style::new().bold().white();
    let (base_file_ref, source_reader) = source_fs.download(source_file_ref).await?;
    let base_resolved_file_ref = source_fs.resolve(Some(&base_file_ref));
    match options.file_matcher.matches(&base_file_ref) {
        FileMatcherResult::SkippedDueToSize | FileMatcherResult::SkippedDueToName => {
            bar.inc(base_file_ref.file_size.unwrap_or(0));
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
        format!(
            "Copying {} ({}) to {}. Size: {}",
            bold_style.apply_to(&base_resolved_file_ref.file_path),
            file_ref
                .media_type
                .as_ref()
                .map(|media_type| media_type.to_string())
                .unwrap_or_else(|| "unknown".to_string()),
            bold_style.apply_to(destination_fs.resolve(Some(&dest_file_ref)).file_path),
            bold_style.apply_to(HumanBytes(file_ref.file_size.unwrap_or(0)))
        )
        .as_str(),
    );
    let transfer_result = if let Some(ref redacter) = redacter {
        redact_upload_file::<SFS, DFS>(
            bar,
            destination_fs,
            bold_style,
            source_reader,
            &base_resolved_file_ref,
            file_ref,
            redacter,
        )
        .await?
    } else {
        destination_fs
            .upload(source_reader, Some(&dest_file_ref))
            .await?;
        TransferFileResult::Copied
    };
    bar.inc(file_ref.file_size.unwrap_or(0));
    Ok(transfer_result)
}

async fn redact_upload_file<'a, SFS: FileSystemConnection<'a>, DFS: FileSystemConnection<'a>>(
    bar: &ProgressBar,
    destination_fs: &mut DFS,
    bold_style: Style,
    source_reader: Box<dyn Stream<Item = AppResult<bytes::Bytes>> + Send + Sync + Unpin + 'static>,
    base_resolved_file_ref: &AbsoluteFilePath,
    dest_file_ref: &FileSystemRef,
    redacter: &impl Redacter,
) -> AppResult<TransferFileResult> {
    if redacter.is_redact_supported(dest_file_ref).await? {
        match redacter.redact_stream(source_reader, dest_file_ref).await {
            Ok(redacted_reader) => {
                destination_fs
                    .upload(redacted_reader, Some(dest_file_ref))
                    .await?;
                Ok(TransferFileResult::Copied)
            }
            Err(ref error) => {
                bar.println(
                    format!(
                        "{}. Skipping {} due to: {}\n{:?}\n",
                        bold_style.clone().red().apply_to("Error redacting"),
                        bold_style.apply_to(&base_resolved_file_ref.file_path),
                        bold_style.apply_to(error),
                        error.source()
                    )
                    .as_str(),
                );
                Ok(TransferFileResult::Skipped)
            }
        }
    } else if redacter.options().allow_unsupported_copies {
        bar.println(
            format!(
                "Still copying {} {} because it is allowed by arguments",
                bold_style.apply_to(&base_resolved_file_ref.file_path),
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
                "Skipping redaction of {} because {} media type is not supported",
                bold_style.apply_to(&base_resolved_file_ref.file_path),
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
