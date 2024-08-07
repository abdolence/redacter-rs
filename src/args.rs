use crate::common_types::GcpProjectId;
use crate::errors::AppError;
use crate::redacters::{
    GcpDlpRedacterOptions, GeminiLlmModelName, OpenAiLlmApiKey, OpenAiModelName, RedacterOptions,
    RedacterProviderOptions,
};
use clap::*;
use std::fmt::Display;
use url::Url;

#[derive(Parser, Debug)]
#[command(author, about)]
pub struct CliArgs {
    #[command(subcommand)]
    pub command: CliCommand,
}

#[derive(Subcommand, Debug)]
#[allow(clippy::large_enum_variant)]
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
    #[command(about = "List files in the source")]
    Ls {
        #[arg(
            help = "Source directory or file such as /tmp, /tmp/file.txt or gs://bucket/file.txt and others supported providers"
        )]
        source: String,
        #[arg(short = 'm', long, help = "Maximum size of files to copy in bytes")]
        max_size_limit: Option<u64>,
        #[arg(
            short = 'f',
            long,
            help = "Filter by name using glob patterns such as *.txt"
        )]
        filename_filter: Option<globset::Glob>,
    },
}

#[derive(ValueEnum, Debug, Clone)]
pub enum RedacterType {
    GcpDlp,
    AwsComprehend,
    MsPresidio,
    GeminiLlm,
    OpenAiLlm,
}

impl std::str::FromStr for RedacterType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "gcp-dlp" => Ok(RedacterType::GcpDlp),
            "aws-comprehend" => Ok(RedacterType::AwsComprehend),
            "ms-presidio" => Ok(RedacterType::MsPresidio),
            "gemini-llm" => Ok(RedacterType::GeminiLlm),
            _ => Err(format!("Unknown redacter type: {}", s)),
        }
    }
}

impl Display for RedacterType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RedacterType::GcpDlp => write!(f, "gcp-dlp"),
            RedacterType::AwsComprehend => write!(f, "aws-comprehend"),
            RedacterType::MsPresidio => write!(f, "ms-presidio"),
            RedacterType::GeminiLlm => write!(f, "gemini-llm"),
            RedacterType::OpenAiLlm => write!(f, "openai-llm"),
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

    #[arg(long, help = "URL for text analyze endpoint for MsPresidio redacter")]
    pub ms_presidio_text_analyze_url: Option<Url>,

    #[arg(long, help = "URL for image redact endpoint for MsPresidio redacter")]
    pub ms_presidio_image_redact_url: Option<Url>,

    #[arg(
        long,
        help = "Gemini model name for Gemini LLM redacter. Default is 'models/gemini-1.5-flash'"
    )]
    pub gemini_model: Option<GeminiLlmModelName>,

    #[arg(
        long,
        help = "Sampling size in bytes before redacting files. Disabled by default"
    )]
    pub sampling_size: Option<usize>,

    #[arg(long, help = "API key for OpenAI LLM redacter")]
    pub open_ai_api_key: Option<OpenAiLlmApiKey>,

    #[arg(
        long,
        help = "Open AI model name for OpenAI LLM redacter. Default is 'gpt-4o-mini'"
    )]
    pub open_ai_model: Option<OpenAiModelName>,
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
            Some(RedacterType::AwsComprehend) => Ok(RedacterProviderOptions::AwsComprehend(
                crate::redacters::AwsComprehendRedacterOptions {
                    region: self.aws_region.map(aws_config::Region::new),
                },
            )),
            Some(RedacterType::MsPresidio) => {
                if self.ms_presidio_text_analyze_url.is_none()
                    && self.ms_presidio_image_redact_url.is_none()
                {
                    return Err(AppError::RedacterConfigError {
                        message:
                            "MsPresidio requires text analyze/image URL specified (at least one)"
                                .to_string(),
                    });
                }
                Ok(RedacterProviderOptions::MsPresidio(
                    crate::redacters::MsPresidioRedacterOptions {
                        text_analyze_url: self.ms_presidio_text_analyze_url,
                        image_redact_url: self.ms_presidio_image_redact_url,
                    },
                ))
            }
            Some(RedacterType::GeminiLlm) => Ok(RedacterProviderOptions::GeminiLlm(
                crate::redacters::GeminiLlmRedacterOptions {
                    project_id: self.gcp_project_id.ok_or_else(|| {
                        AppError::RedacterConfigError {
                            message: "GCP project id is required for Gemini LLM redacter"
                                .to_string(),
                        }
                    })?,
                    gemini_model: self.gemini_model,
                },
            )),
            Some(RedacterType::OpenAiLlm) => Ok(RedacterProviderOptions::OpenAiLlm(
                crate::redacters::OpenAiLlmRedacterOptions {
                    api_key: self
                        .open_ai_api_key
                        .ok_or_else(|| AppError::RedacterConfigError {
                            message: "OpenAI API key is required for OpenAI LLM redacter"
                                .to_string(),
                        })?,
                    model: self.open_ai_model,
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
            sampling_size: self.sampling_size,
        })
    }
}
