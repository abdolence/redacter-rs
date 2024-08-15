use crate::errors::AppError;
use crate::file_converters::FileConverters;
use crate::file_systems::{DetectFileSystem, FileSystemConnection, FileSystemRef};
use crate::file_tools::{FileMatcher, FileMatcherResult, FileMimeOverride};
use crate::redacters::{
    RedactSupportedOptions, Redacter, RedacterBaseOptions, RedacterOptions, Redacters,
};
use crate::reporter::AppReporter;
use crate::AppResult;
use console::{pad_str, Alignment, Style, Term};
use futures::Stream;
use gcloud_sdk::prost::bytes;
use indicatif::*;
use std::error::Error;
use std::time::Duration;

pub struct CopyCommandResult {
    pub files_copied: usize,
    pub files_skipped: usize,
}

#[derive(Debug, Clone)]
pub struct CopyCommandOptions {
    pub file_matcher: FileMatcher,
    pub file_mime_override: FileMimeOverride,
}

impl CopyCommandOptions {
    pub fn new(
        filename_filter: Option<globset::Glob>,
        max_size_limit: Option<u64>,
        mime_override: Vec<(mime::Mime, globset::Glob)>,
    ) -> Self {
        let filename_matcher = filename_filter
            .as_ref()
            .map(|filter| filter.compile_matcher());
        CopyCommandOptions {
            file_matcher: FileMatcher::new(filename_matcher, max_size_limit),
            file_mime_override: FileMimeOverride::new(mime_override),
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
    let redacted_output = if let Some(ref options) = redacter_options.as_ref() {
        bold_style
            .clone()
            .green()
            .apply_to(format!("✓ Yes ({})", &options))
    } else {
        bold_style.clone().red().apply_to("✗ No".to_string())
    };
    let sampling_output = if let Some(ref sampling_size) = redacter_options
        .as_ref()
        .and_then(|o| o.base_options.sampling_size)
    {
        Style::new().apply_to(format!("{} bytes.", sampling_size))
    } else {
        Style::new().dim().apply_to("-".to_string())
    };

    let mut file_converters = FileConverters::new();
    file_converters.init().await?;

    let converter_style = Style::new();
    let pdf_support_output = if file_converters.pdf_image_converter.is_some() {
        converter_style
            .clone()
            .green()
            .apply_to("✓ Yes".to_string())
    } else {
        converter_style.clone().dim().apply_to("✗ No".to_string())
    };

    term.write_line(
        format!(
            "Copying from {} to {}.\nRedacting: {}.\nSampling: {}\nPDF to image support: {}\n",
            bold_style.clone().white().apply_to(source),
            bold_style.clone().yellow().apply_to(destination),
            redacted_output,
            sampling_output,
            pdf_support_output,
        )
        .as_str(),
    )?;

    let bar = ProgressBar::new(1);
    bar.set_style(
        ProgressStyle::with_template(
            "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos:>3}/{len:3}",
        )?
        .progress_chars("◉>◯"),
    );
    bar.enable_steady_tick(Duration::from_millis(100));
    let app_reporter = AppReporter::from(&bar);

    let mut source_fs = DetectFileSystem::open(source, &app_reporter).await?;
    let mut destination_fs = DetectFileSystem::open(destination, &app_reporter).await?;

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
        let source_files_result = source_fs.list_files(Some(&options.file_matcher)).await?;
        let source_files: Vec<FileSystemRef> = source_files_result.files;
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

        bar.set_length(files_found as u64);

        let mut total_files_copied = 0;
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
                term,
                &bar,
                None,
                &mut source_fs,
                &mut destination_fs,
                &options,
                &maybe_redacters,
                &file_converters,
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
    redacter: &Option<(RedacterBaseOptions, Vec<impl Redacter>)>,
    file_converters: &FileConverters,
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
    let max_filename_width = (term.width() as f64 * 0.25) as usize;
    bar.println(
        format!(
            "Processing {} to {} {} Size: {}",
            bold_style.apply_to(pad_str(
                &base_resolved_file_ref.file_path,
                max_filename_width,
                Alignment::Left,
                None
            )),
            bold_style.apply_to(pad_str(
                destination_fs
                    .resolve(Some(&dest_file_ref))
                    .file_path
                    .as_str(),
                max_filename_width,
                Alignment::Left,
                None
            )),
            pad_str(
                file_ref
                    .media_type
                    .as_ref()
                    .map(|media_type| media_type.to_string())
                    .unwrap_or_else(|| "unknown".to_string())
                    .as_str(),
                28,
                Alignment::Left,
                None
            ),
            bold_style.apply_to(pad_str(
                HumanBytes(file_ref.file_size.unwrap_or(0))
                    .to_string()
                    .as_str(),
                16,
                Alignment::Left,
                None
            ))
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
    redacter_with_options: &(RedacterBaseOptions, Vec<impl Redacter>),
    file_converters: &FileConverters,
) -> AppResult<TransferFileResult> {
    let (redacter_base_options, redacters) = redacter_with_options;
    let mut support_redacters = Vec::new();
    let dest_file_ref_overridden = options
        .file_mime_override
        .override_for_file_ref(dest_file_ref.clone());
    for redacter in redacters {
        let redacter_supported_options = redacter
            .redact_supported_options(&dest_file_ref_overridden)
            .await?;
        if redacter_supported_options != RedactSupportedOptions::Unsupported {
            support_redacters.push(redacter);
        }
    }
    if !support_redacters.is_empty() {
        match crate::redacters::redact_stream(
            &support_redacters,
            redacter_base_options,
            source_reader,
            &dest_file_ref_overridden,
            file_converters,
            bar,
        )
        .await
        {
            Ok(redacted_reader) => {
                destination_fs
                    .upload(redacted_reader, Some(dest_file_ref))
                    .await?;
                Ok(TransferFileResult::Copied)
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
