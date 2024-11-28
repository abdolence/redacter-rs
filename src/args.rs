use crate::common_types::{DlpRequestLimit, GcpProjectId, GcpRegion};
use crate::errors::AppError;
use crate::redacters::{
    GcpDlpRedacterOptions, GcpVertexAiModelName, GeminiLlmModelName, OpenAiLlmApiKey,
    OpenAiModelName, RedacterBaseOptions, RedacterOptions, RedacterProviderOptions,
};
use clap::*;
use std::fmt::Display;
use std::path::PathBuf;
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
        max_size_limit: Option<usize>,

        #[arg(
            short = 'n',
            long,
            help = "Maximum number of files to copy. Sort order is not guaranteed and depends on the provider"
        )]
        max_files_limit: Option<usize>,

        #[arg(
            short = 'f',
            long,
            help = "Filter by name using glob patterns such as *.txt"
        )]
        filename_filter: Option<globset::Glob>,

        #[command(flatten)]
        redacter_args: Option<RedacterArgs>,

        #[arg(long, help = "Override media type detection using glob patterns such as 'text/plain=*.md'", value_parser = CliCommand::parse_key_val::<mime::Mime, globset::Glob>)]
        mime_override: Vec<(mime::Mime, globset::Glob)>,

        #[arg(
            long,
            help = "Save redacted results in JSON format to the specified file"
        )]
        save_json_results: Option<PathBuf>,
    },
    #[command(about = "List files in the source")]
    Ls {
        #[arg(
            help = "Source directory or file such as /tmp, /tmp/file.txt or gs://bucket/file.txt and others supported providers"
        )]
        source: String,
        #[arg(short = 'm', long, help = "Maximum size of files to copy in bytes")]
        max_size_limit: Option<usize>,
        #[arg(
            short = 'f',
            long,
            help = "Filter by name using glob patterns such as *.txt"
        )]
        filename_filter: Option<globset::Glob>,
    },
}

impl CliCommand {
    fn parse_key_val<T, U>(
        s: &str,
    ) -> Result<(T, U), Box<dyn std::error::Error + Send + Sync + 'static>>
    where
        T: std::str::FromStr,
        T::Err: std::error::Error + Send + Sync + 'static,
        U: std::str::FromStr,
        U::Err: std::error::Error + Send + Sync + 'static,
    {
        let pos = s
            .find('=')
            .ok_or_else(|| format!("invalid KEY=value: no `=` found in `{s}`"))?;
        Ok((s[..pos].parse()?, s[pos + 1..].parse()?))
    }
}

#[derive(ValueEnum, Debug, Clone)]
pub enum RedacterType {
    GcpDlp,
    AwsComprehend,
    MsPresidio,
    GeminiLlm,
    OpenAiLlm,
    GcpVertexAi,
    AwsBedrock,
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
            RedacterType::GcpVertexAi => write!(f, "gcp-vertex-ai"),
            RedacterType::AwsBedrock => write!(f, "aws-bedrock"),
        }
    }
}

#[derive(Args, Debug, Clone)]
#[group(required = false)]
pub struct RedacterArgs {
    #[arg(short = 'd', long, value_enum, help = "List of redacters to use")]
    redact: Option<Vec<RedacterType>>,

    #[arg(
        long,
        help = "Allow unsupported types to be copied without redaction",
        default_value = "false"
    )]
    pub allow_unsupported_copies: bool,

    #[arg(
        long,
        help = "GCP project id that will be used to redact and bill API calls"
    )]
    pub gcp_project_id: Option<GcpProjectId>,

    #[arg(long, help = "Additional GCP DLP built in info types for redaction")]
    pub gcp_dlp_built_in_info_type: Option<Vec<String>>,

    #[arg(
        long,
        help = "Additional GCP DLP user defined stored info types for redaction"
    )]
    pub gcp_dlp_stored_info_type: Option<Vec<String>>,

    #[arg(
        long,
        help = "GCP region that will be used to redact and bill API calls for Vertex AI"
    )]
    pub gcp_region: Option<GcpRegion>,

    #[arg(
        long,
        help = "Vertex AI model supports image editing natively. Default is false."
    )]
    pub gcp_vertex_ai_native_image_support: bool,

    #[arg(
        long,
        help = "Model name for text redaction in Vertex AI. Default is 'publishers/google/models/gemini-1.5-flash-001'"
    )]
    pub gcp_vertex_ai_text_model: Option<GcpVertexAiModelName>,

    #[arg(
        long,
        help = "Model name for image redaction in Vertex AI. Default is 'publishers/google/models/gemini-1.5-pro-001'"
    )]
    pub gcp_vertex_ai_image_model: Option<GcpVertexAiModelName>,

    #[arg(
        long,
        help = "Block none harmful content threshold for Vertex AI redacter. Default is BlockOnlyHigh since BlockNone is required a special billing settings.",
        default_value = "false"
    )]
    pub gcp_vertex_ai_block_none_harmful: bool,

    #[arg(
        long,
        help = "Disable CSV headers (if they are not present)",
        default_value = "false"
    )]
    pub csv_headers_disable: bool,

    #[arg(long, help = "CSV delimiter (default is ',')")]
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

    #[arg(
        long,
        help = "Limit the number of DLP requests. Some DLPs has strict quotas and to avoid errors, limit the number of requests delaying them. Default is disabled"
    )]
    pub limit_dlp_requests: Option<DlpRequestLimit>,
}

impl TryInto<RedacterOptions> for RedacterArgs {
    type Error = AppError;

    fn try_into(self) -> Result<RedacterOptions, Self::Error> {
        let mut provider_options =
            Vec::with_capacity(self.redact.as_ref().map(Vec::len).unwrap_or(0));
        for options in self.redact.unwrap_or_default() {
            let redacter_options = match options {
                RedacterType::GcpDlp => match self.gcp_project_id {
                    Some(ref project_id) => {
                        Ok(RedacterProviderOptions::GcpDlp(GcpDlpRedacterOptions {
                            project_id: project_id.clone(),
                            user_defined_built_in_info_types: self
                                .gcp_dlp_built_in_info_type
                                .clone()
                                .unwrap_or_default(),
                            user_defined_stored_info_types: self
                                .gcp_dlp_stored_info_type
                                .clone()
                                .unwrap_or_default(),
                        }))
                    }
                    None => Err(AppError::RedacterConfigError {
                        message: "GCP project id is required for GCP DLP redacter".to_string(),
                    }),
                },
                RedacterType::AwsComprehend => Ok(RedacterProviderOptions::AwsComprehend(
                    crate::redacters::AwsComprehendRedacterOptions {
                        region: self.aws_region.clone().map(aws_config::Region::new),
                    },
                )),
                RedacterType::MsPresidio => {
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
                            text_analyze_url: self.ms_presidio_text_analyze_url.clone(),
                            image_redact_url: self.ms_presidio_image_redact_url.clone(),
                        },
                    ))
                }
                RedacterType::GeminiLlm => Ok(RedacterProviderOptions::GeminiLlm(
                    crate::redacters::GeminiLlmRedacterOptions {
                        project_id: self.gcp_project_id.clone().ok_or_else(|| {
                            AppError::RedacterConfigError {
                                message: "GCP project id is required for Gemini LLM redacter"
                                    .to_string(),
                            }
                        })?,
                        gemini_model: self.gemini_model.clone(),
                    },
                )),
                RedacterType::OpenAiLlm => Ok(RedacterProviderOptions::OpenAiLlm(
                    crate::redacters::OpenAiLlmRedacterOptions {
                        api_key: self.open_ai_api_key.clone().ok_or_else(|| {
                            AppError::RedacterConfigError {
                                message: "OpenAI API key is required for OpenAI LLM redacter"
                                    .to_string(),
                            }
                        })?,
                        model: self.open_ai_model.clone(),
                    },
                )),
                RedacterType::GcpVertexAi => Ok(RedacterProviderOptions::GcpVertexAi(
                    crate::redacters::GcpVertexAiRedacterOptions {
                        project_id: self.gcp_project_id.clone().ok_or_else(|| {
                            AppError::RedacterConfigError {
                                message: "GCP project id is required for GCP Vertex AI redacter"
                                    .to_string(),
                            }
                        })?,
                        gcp_region: self.gcp_region.clone().ok_or_else(|| {
                            AppError::RedacterConfigError {
                                message: "GCP region is required for GCP Vertex AI redacter"
                                    .to_string(),
                            }
                        })?,
                        native_image_support: self.gcp_vertex_ai_native_image_support,
                        text_model: self.gcp_vertex_ai_text_model.clone(),
                        image_model: self.gcp_vertex_ai_image_model.clone(),
                        block_none_harmful: self.gcp_vertex_ai_block_none_harmful,
                    },
                )),
                RedacterType::AwsBedrock => Ok(RedacterProviderOptions::AwsBedrock(
                    crate::redacters::AwsBedrockRedacterOptions {
                        region: self.aws_region.clone().map(aws_config::Region::new),
                        text_model: None,
                        image_model: None,
                    },
                )),
            }?;
            provider_options.push(redacter_options);
        }

        let base_options = RedacterBaseOptions {
            allow_unsupported_copies: self.allow_unsupported_copies,
            csv_headers_disable: self.csv_headers_disable,
            csv_delimiter: self.csv_delimiter.map(|c| c as u8),
            sampling_size: self.sampling_size,
            limit_dlp_requests: self.limit_dlp_requests,
        };
        Ok(RedacterOptions {
            provider_options,
            base_options,
        })
    }
}
