use std::error::Error;

use args::*;
use clap::Parser;
use console::{Style, Term};

use crate::commands::*;
use crate::errors::AppError;

mod args;
mod reporter;

mod file_systems;
mod file_tools;

mod errors;

mod commands;

mod redacters;

pub type AppResult<T> = Result<T, AppError>;

mod common_types;

mod file_converters;

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
    match handle_args(cli, &term).await {
        Err(err) => {
            term.write_line(
                format!(
                    "{}: {}\nDetails: {:?}",
                    bold_style.clone().red().apply_to("Error"),
                    &err,
                    &err.source()
                )
                .as_str(),
            )?;
            std::process::exit(1);
        }
        Ok(_) => Ok(()),
    }
}

async fn handle_args(cli: CliArgs, term: &Term) -> AppResult<()> {
    match cli.command {
        CliCommand::Cp {
            source,
            destination,
            max_size_limit,
            max_files_limit,
            filename_filter,
            redacter_args,
            mime_override,
        } => {
            let options = CopyCommandOptions::new(
                filename_filter,
                max_size_limit,
                max_files_limit,
                mime_override,
            );
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
                    "\n{} -> {}: {} files processed. {} files skipped.",
                    source,
                    destination,
                    Style::new()
                        .bold()
                        .green()
                        .apply_to(copy_result.files_copied),
                    Style::new().yellow().apply_to(copy_result.files_skipped),
                )
                .as_str(),
            )?;
        }
        CliCommand::Ls {
            source,
            max_size_limit,
            filename_filter,
        } => {
            let options = LsCommandOptions::new(filename_filter, max_size_limit);
            command_ls(term, &source, options).await?;
        }
    }

    Ok(())
}
