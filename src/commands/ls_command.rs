use crate::filesystems::FileSystemConnection;
use crate::filesystems::{DetectFileSystem, FileMatcher};
use crate::AppResult;
use console::{pad_str, Alignment, Style, Term};
use indicatif::{HumanBytes, TermLike};
use rvstruct::ValueStruct;

#[derive(Debug, Clone)]
pub struct LsCommandOptions {
    pub file_matcher: FileMatcher,
}

impl LsCommandOptions {
    pub fn new(filename_filter: Option<globset::Glob>, max_size_limit: Option<u64>) -> Self {
        let filename_matcher = filename_filter
            .as_ref()
            .map(|filter| filter.compile_matcher());
        LsCommandOptions {
            file_matcher: FileMatcher::new(filename_matcher, max_size_limit),
        }
    }
}

pub async fn command_ls(term: &Term, source: &str, options: LsCommandOptions) -> AppResult<()> {
    let bold_style = Style::new().bold();
    let highlighted = bold_style.clone().white();
    let dimmed_style = Style::new().dim();
    term.write_line(format!("Listing files in {}.", bold_style.apply_to(source)).as_str())?;
    let app_reporter = crate::reporter::AppReporter::from(term);
    let mut source_fs = DetectFileSystem::open(source, &app_reporter).await?;
    let list_files_result = source_fs.list_files(Some(&options.file_matcher)).await?;
    let total_size: u64 = list_files_result
        .files
        .iter()
        .map(|f| f.file_size.unwrap_or(0))
        .sum();

    if !list_files_result.files.is_empty() {
        let max_filename_width = std::cmp::min(
            list_files_result
                .files
                .iter()
                .map(|f| f.relative_path.value().len())
                .max()
                .unwrap_or(25)
                + 5,
            (term.width() * 2 / 3) as usize,
        );
        term.write_line(
            format!(
                "\n  {} {} {}",
                dimmed_style.apply_to(pad_str(
                    "Filename",
                    max_filename_width,
                    Alignment::Left,
                    None
                )),
                dimmed_style.apply_to(pad_str("Media Type", 40, Alignment::Left, None)),
                dimmed_style.apply_to(pad_str("Size", 16, Alignment::Left, None))
            )
            .as_str(),
        )?;

        for file in &list_files_result.files {
            term.write_line(
                format!(
                    "- {} {} {}",
                    highlighted.apply_to(pad_str(
                        file.relative_path.value(),
                        max_filename_width,
                        Alignment::Left,
                        Some("...")
                    )),
                    pad_str(
                        file.media_type
                            .as_ref()
                            .map(|mime| mime.to_string())
                            .unwrap_or("".to_string())
                            .as_str(),
                        40,
                        Alignment::Left,
                        None
                    ),
                    highlighted.apply_to(pad_str(
                        format!("{}", HumanBytes(file.file_size.unwrap_or(0))).as_str(),
                        16,
                        Alignment::Left,
                        None
                    ))
                )
                .as_str(),
            )?;
        }
        term.write_line("")?;
    }
    term.write_line(
        format!(
            "{} files found. Total size: {}",
            highlighted.apply_to(list_files_result.files.len()),
            highlighted.apply_to(HumanBytes(total_size))
        )
        .as_str(),
    )?;
    term.write_line(
        format!(
            "{} files skipped/filtered out.",
            dimmed_style.apply_to(list_files_result.skipped.to_string())
        )
        .as_str(),
    )?;
    source_fs.close().await?;
    Ok(())
}
