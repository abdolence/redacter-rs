#![allow(clippy::needless_lifetimes)]
#![allow(clippy::result_large_err)]
#![allow(clippy::large_enum_variant)]

use std::error::Error;

use crate::commands::*;
use crate::errors::AppError;
use crate::model_store::ModelStoreOptions;
use args::*;
use clap::Parser;
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

mod model_store;

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

    let cli = CliArgs::parse();
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
        Ok(_) => Ok(()),
    }
}

async fn handle_args(cli: CliArgs, term: &Term) -> AppResult<()> {
    let model_store = ModelStoreOptions {
        download: cli.download_models,
        models_dir: resolve_models_dir(cli.models_dir, std::env::var_os("REDACTER_MODELS_DIR")),
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
            save_json_results,
        } => {
            let options = CopyCommandOptions::new(
                filename_filter,
                max_size_limit,
                max_files_limit,
                mime_override,
                model_store,
            );
            let copy_result = command_copy(
                term,
                &source,
                &destination,
                options,
                redacter_args.map(|args| args.try_into()).transpose()?,
            )
            .await?;
            if let Some(json_path) = save_json_results {
                let json_result = serde_json::to_string_pretty(&copy_result)?;
                let mut file = tokio::fs::File::create(&json_path).await?;
                tokio::io::AsyncWriteExt::write_all(&mut file, json_result.as_bytes()).await?;
                term.write_line(
                    format!(
                        "Results saved to JSON file: {}",
                        Style::new().bold().apply_to(json_path.display())
                    )
                    .as_str(),
                )?;
            }
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
