#![allow(clippy::needless_lifetimes)]
#![allow(clippy::result_large_err)]
#![allow(clippy::large_enum_variant)]

use std::error::Error;

use crate::addon_store::AddonStoreOptions;
use crate::commands::*;
use crate::errors::AppError;
use args::*;
use console::{Style, Term};

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

mod addon_store;

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|e| format!("{name}: {e}"))
}

/// Sends the internal progress detail to `tracing`, off unless `RUST_LOG` asks for it
/// (`RUST_LOG=debug` shows what the command is doing behind its table).
fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("off"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();
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
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    let (cli, deprecated) = match CliArgs::try_parse_from_with_deprecations(std::env::args_os()) {
        Ok(parsed) => parsed,
        Err(err) => {
            err.print()?;
            // clap exits with 2 on a usage error, which `scan` reserves for findings.
            std::process::exit(if err.use_stderr() { 1 } else { 0 });
        }
    };
    let stderr = Term::stderr();
    for DeprecatedSpelling { used, replacement } in deprecated {
        stderr.write_line(&format!(
            "{}: `{used}` is deprecated, use `{replacement}`",
            Style::new().bold().yellow().apply_to("Warning")
        ))?;
    }
    match handle_args(cli, &term).await {
        Err(err) => {
            term.write_line(
                format!(
                    "{}: {}\nDetails: {:?}",
                    bold_style.clone().red().apply_to("Error"),
                    err,
                    err.source()
                )
                .as_str(),
            )?;
            std::process::exit(1);
        }
        Ok(0) => Ok(()),
        Ok(exit_code) => std::process::exit(exit_code),
    }
}

/// Runs the command and returns the process exit code.
async fn handle_args(cli: CliArgs, term: &Term) -> AppResult<i32> {
    let addon_store = AddonStoreOptions {
        addons_dir: resolve_addons_dir(cli.addons_dir, std::env::var_os("REDACTER_ADDONS_DIR")),
    };
    match cli.command {
        CliCommand::Cp {
            source,
            destination,
            max_size_limit,
            max_files_limit,
            filename_filter,
            redacter_args,
            mime_override,
            report,
        } => {
            let options = CopyCommandOptions::new(
                filename_filter,
                max_size_limit,
                max_files_limit,
                mime_override,
                addon_store,
            );
            let copy_result = command_copy(
                term,
                &source,
                &destination,
                options,
                redacter_args.map(|args| args.try_into()).transpose()?,
            )
            .await?;
            report
                .write(term, &copy_result, &copy_result.to_text())
                .await?;
            Ok(0)
        }
        CliCommand::Scan {
            source,
            max_size_limit,
            max_files_limit,
            filename_filter,
            redacter_args,
            mime_override,
            report,
            show_values,
            min_score,
            fail_on,
        } => {
            let options = ScanCommandOptions::new(
                filename_filter,
                max_size_limit,
                max_files_limit,
                mime_override,
                addon_store,
                min_score,
                if show_values {
                    ValueDisplay::Raw
                } else {
                    ValueDisplay::Masked
                },
            );
            let scan_result =
                command_scan(term, &source, options, redacter_args.try_into()?).await?;
            report
                .write(term, &scan_result, &scan_result.to_text())
                .await?;
            Ok(scan_result.exit_code(fail_on))
        }
        CliCommand::Ls {
            source,
            max_size_limit,
            filename_filter,
        } => {
            let options = LsCommandOptions::new(filename_filter, max_size_limit);
            command_ls(term, &source, options).await?;
            Ok(0)
        }
        CliCommand::Addons { command } => match command {
            AddonsCommand::Install { addons } => {
                command_addons_install(term, &addons, &addon_store).await?;
                Ok(0)
            }
            AddonsCommand::Ls => {
                command_addons_ls(term, &addon_store)?;
                Ok(0)
            }
        },
    }
}
