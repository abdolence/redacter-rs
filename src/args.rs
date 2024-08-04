use crate::common_types::GcpProjectId;
use crate::errors::AppError;
use crate::redacters::{GcpDlpRedacterOptions, RedacterOptions, RedacterProviderOptions};
use clap::*;
use std::fmt::Display;

#[derive(Parser, Debug)]
#[command(author, about)]
pub struct CliArgs {
    #[command(subcommand)]
    pub command: CliCommand,
}

#[derive(Subcommand, Debug)]
pub enum CliCommand {
    #[command(about = "Copy and redact files from source to destination")]
    Cp {
        #[arg(
            help = "Source directory or file such as /tmp, /tmp/file.txt or gs://bucket/file.txt and others supported providers"
        )]
        source: String,
        #[arg(
            help = "Destination directory or file such as /tmp, /tmp/file.txt or gs://bucket/file.txt and others supported providers"
        )]
        destination: String,
        #[arg(short = 'm', long, help = "Maximum size of files to copy in bytes")]
        max_size_limit: Option<u64>,
        #[arg(
            short = 'f',
            long,
            help = "Filter by name using glob patterns such as *.txt"
        )]
        filename_filter: Option<globset::Glob>,

        #[command(flatten)]
        redacter_args: Option<RedacterArgs>,
    },
}

#[derive(ValueEnum, Debug, Clone)]
pub enum RedacterType {
    GcpDlp,
    AwsComprehendDlp,
}

impl std::str::FromStr for RedacterType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "gcp-dlp" => Ok(RedacterType::GcpDlp),
            "aws-comprehend-dlp" => Ok(RedacterType::AwsComprehendDlp),
            _ => Err(format!("Unknown redacter type: {}", s)),
        }
    }
}

impl Display for RedacterType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RedacterType::GcpDlp => write!(f, "gcp-dlp"),
            RedacterType::AwsComprehendDlp => write!(f, "aws-comprehend-dlp"),
        }
    }
}

#[derive(Args, Debug, Clone)]
#[group(required = false)]
pub struct RedacterArgs {
    #[arg(short = 'd', long, value_enum, help = "Redacter type")]
    redact: Option<RedacterType>,

    #[arg(
        long,
        help = "GCP project id that will be used to redact and bill API calls"
    )]
    pub gcp_project_id: Option<GcpProjectId>,

    #[arg(
        long,
        help = "Allow unsupported types to be copied without redaction",
        default_value = "false"
    )]
    pub allow_unsupported_copies: bool,

    #[arg(
        long,
        help = "Disable CSV headers (if they are not present)",
        default_value = "false"
    )]
    pub csv_headers_disable: bool,

    #[arg(long, help = "CSV delimiter (default is ','")]
    pub csv_delimiter: Option<char>,

    #[arg(long, help = "AWS region for AWS Comprehend DLP redacter")]
    pub aws_region: Option<String>,
}

impl TryInto<RedacterOptions> for RedacterArgs {
    type Error = AppError;

    fn try_into(self) -> Result<RedacterOptions, Self::Error> {
        let provider_options = match self.redact {
            Some(RedacterType::GcpDlp) => match self.gcp_project_id {
                Some(project_id) => Ok(RedacterProviderOptions::GcpDlp(GcpDlpRedacterOptions {
                    project_id,
                })),
                None => Err(AppError::RedacterConfigError {
                    message: "GCP project id is required for GCP DLP redacter".to_string(),
                }),
            },
            Some(RedacterType::AwsComprehendDlp) => Ok(RedacterProviderOptions::AwsComprehendDlp(
                crate::redacters::AwsComprehendDlpRedacterOptions {
                    region: self.aws_region.map(aws_config::Region::new),
                },
            )),
            None => Err(AppError::RedacterConfigError {
                message: "Redacter type is required".to_string(),
            }),
        }?;
        Ok(RedacterOptions {
            provider_options,
            allow_unsupported_copies: self.allow_unsupported_copies,
            csv_headers_disable: self.csv_headers_disable,
            csv_delimiter: self.csv_delimiter.map(|c| c as u8),
        })
    }
}
