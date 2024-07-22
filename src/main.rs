use clap::Parser;
use console::{Style, Term};

use std::error::Error;

mod args;
use crate::commands::*;
use crate::errors::AppError;
use args::*;

mod reporter;

mod filesystems;

mod errors;

mod commands;

mod redacters;

pub type AppResult<T> = Result<T, AppError>;

mod common_types;

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|e| format!("{}: {}", name, e))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let term = Term::stdout();
    let bold_style = Style::new().bold();

    term.write_line(
        format!(
            "{} v{} (https://github.com/abdolence/redacter-rs)",
            bold_style.clone().green().apply_to("Redacter"),
            bold_style.apply_to(env!("CARGO_PKG_VERSION"))
        )
        .as_str(),
    )?;

    let cli = CliArgs::parse();
    if let Err(err) = handle_args(cli, &term).await {
        term.write_line(
            format!(
                "{}: {}\nDetails: {:?}",
                bold_style.clone().red().apply_to("Error"),
                err,
                err.source()
            )
            .as_str(),
        )?;
    }

    Ok(())
}

async fn handle_args(cli: CliArgs, term: &Term) -> AppResult<()> {
    let bold_style = Style::new().bold();

    match cli.command {
        CliCommand::Cp {
            source,
            destination,
            max_size_limit,
            filename_filter,
            redacter_args,
        } => {
            let options = CopyCommandOptions::new(filename_filter, max_size_limit);
            let copy_result = command_copy(
                term,
                &source,
                &destination,
                options,
                redacter_args.map(|args| args.try_into()).transpose()?,
            )
            .await?;
            term.write_line(
                format!(
                    "{} -> {}\n{} files processed.\n{} files skipped.",
                    source,
                    destination,
                    bold_style
                        .clone()
                        .green()
                        .apply_to(copy_result.files_copied),
                    Style::new().yellow().apply_to(copy_result.files_skipped),
                )
                .as_str(),
            )?;
        }
    }

    Ok(())
}
