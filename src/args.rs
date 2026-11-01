use crate::addon_store::Addon;
use crate::common_types::{DlpRequestLimit, GcpProjectId, GcpRegion};
use crate::errors::AppError;
use crate::file_tools::IgnoreFileChoice;
#[cfg(feature = "local-credential-ml")]
use crate::redacters::LocalCredentialMlRedacterOptions;
#[cfg(feature = "local-gliner")]
use crate::redacters::LocalGlinerRedacterOptions;
use crate::redacters::{
    AwsBedrockGuardrailId, AwsBedrockGuardrailVersion, AwsBedrockModelName, AwsComprehendRedacter,
    Categories, CategoriesError, CategoriesParser, Category, GcpDlpRedacter, GcpDlpRedacterOptions,
    GcpVertexAiModelName, GeminiLlmModelName, LlmImageMode, LocalRulesRedacterOptions,
    OpenAiLlmApiKey, OpenAiModelName, RedacterBaseOptions, RedacterOptions,
    RedacterProviderOptions, RuleGroup, Score, UserRule,
};
#[cfg(feature = "local-ner")]
use crate::redacters::{Entity, LocalNerRedacterOptions};
use clap::*;
use std::collections::BTreeSet;
use std::fmt::Display;
use std::path::PathBuf;
use url::Url;

/// Vertex AI location used when `--gcp-region` is not given.
const DEFAULT_GCP_VERTEX_AI_REGION: &str = "global";

/// Guardrail version used when `--aws-bedrock-guardrail-version` is not given.
const DEFAULT_AWS_BEDROCK_GUARDRAIL_VERSION: &str = "DRAFT";

#[derive(Parser, Debug)]
#[command(author, about)]
pub struct CliArgs {
    #[arg(
        long,
        global = true,
        help = "Directory holding the addons that `addons install` installs and manually placed copies, one subdirectory per addon. Overrides the REDACTER_ADDONS_DIR environment variable. Default is the user cache directory, for example ~/.cache/redacter/addons"
    )]
    pub addons_dir: Option<PathBuf>,

    #[command(subcommand)]
    pub command: CliCommand,
}

/// Merges `--addons-dir` with the `REDACTER_ADDONS_DIR` environment value, the flag winning.
/// Kept as a plain function rather than a `clap` `env` attribute so a parse of `CliArgs`
/// never reads the real process environment: production passes
/// `std::env::var_os("REDACTER_ADDONS_DIR")`, tests pass an explicit value.
pub fn resolve_addons_dir(
    cli_value: Option<PathBuf>,
    env_value: Option<std::ffi::OsString>,
) -> Option<PathBuf> {
    cli_value.or_else(|| env_value.map(PathBuf::from))
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

        #[command(flatten)]
        report: ReportArgs,
    },
    #[command(
        about = "Scan files for sensitive data and report what was found and where, without changing them. Exits with 0 when nothing is found, 2 when something is, 1 on errors",
        mut_arg("redact", |arg| arg.required(true))
    )]
    Scan {
        #[arg(
            help = "Source directory or file such as /tmp, /tmp/file.txt or gs://bucket/file.txt and others supported providers"
        )]
        source: String,

        #[arg(short = 'm', long, help = "Maximum size of files to scan in bytes")]
        max_size_limit: Option<usize>,

        #[arg(
            short = 'n',
            long,
            help = "Maximum number of files to scan. Sort order is not guaranteed and depends on the provider"
        )]
        max_files_limit: Option<usize>,

        #[arg(
            short = 'f',
            long,
            help = "Filter by name using glob patterns such as *.txt"
        )]
        filename_filter: Option<globset::Glob>,

        #[command(flatten)]
        redacter_args: RedacterArgs,

        #[arg(long, help = "Override media type detection using glob patterns such as 'text/plain=*.md'", value_parser = CliCommand::parse_key_val::<mime::Mime, globset::Glob>)]
        mime_override: Vec<(mime::Mime, globset::Glob)>,

        #[command(flatten)]
        report: ReportArgs,

        #[arg(
            long,
            help = "Show the matched values in the output and the report. They are masked by default. Does not apply to the SARIF report, which always masks them"
        )]
        show_values: bool,

        #[arg(
            long,
            help = "Drop findings whose score is below this value, between 0 and 1. Findings without a score are kept"
        )]
        min_score: Option<Score>,

        #[arg(
            long,
            value_enum,
            default_value_t = FailOn::Findings,
            help = "Which results make the command exit with 2: 'findings', 'unconfirmed' (findings, or a file without findings whose named values could not be located, marked ?) or 'never'. Errors, blocked and failed files exit with 1 whichever it is"
        )]
        fail_on: FailOn,

        #[command(flatten)]
        ignore: IgnoreArgs,
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
    #[command(
        about = "Install and list the addons: OCR models, Pdfium and the local redacter models"
    )]
    Addons {
        #[command(subcommand)]
        command: AddonsCommand,
    },
}

#[derive(Subcommand, Debug)]
pub enum AddonsCommand {
    #[command(about = "Download and verify addons; the only command that downloads anything")]
    Install {
        #[arg(
            value_enum,
            value_delimiter = ',',
            required = true,
            help = "Addons to install, comma separated, such as ocr,pdf"
        )]
        addons: Vec<Addon>,
    },
    #[command(about = "List the addons of this build and whether each is installed")]
    Ls,
}

/// Where `cp` and `scan` write their report. The terminal shows the usual output either way.
#[derive(Args, Debug, Clone)]
pub struct ReportArgs {
    #[arg(long, help = "Write the report to this file")]
    pub report_file: Option<PathBuf>,

    #[arg(
        long,
        value_enum,
        default_value = "json",
        requires = "report_file",
        help = "Format of the report file: 'json' for machines, 'text' for people, 'sarif' for GitHub code scanning, 'ocsf' for SIEMs and security data lakes (SARIF and OCSF reports always hold masked values). Requires --report-file. Default is 'json'"
    )]
    pub report_format: ReportFormat,
}

impl ReportArgs {
    pub fn lists_redactions(&self) -> bool {
        self.report_file.is_some() && self.report_format != ReportFormat::Text
    }
}

#[derive(Args, Debug, Clone)]
pub struct IgnoreArgs {
    #[arg(
        long,
        conflicts_with = "no_ignore",
        help = "Skip the files this ignore file lists, in gitignore syntax, matched against the path relative to the source. Works for any source. Without it, a local directory source uses its own .redacterignore"
    )]
    pub ignore_file: Option<PathBuf>,

    #[arg(
        long,
        help = "Do not read the .redacterignore at the root of a local source. Lines marked with redacter:allow are still ignored"
    )]
    pub no_ignore: bool,
}

impl From<IgnoreArgs> for IgnoreFileChoice {
    fn from(args: IgnoreArgs) -> Self {
        match args.ignore_file {
            Some(path) => Self::Explicit(path),
            None if args.no_ignore => Self::Disabled,
            None => Self::SourceRoot,
        }
    }
}

/// Which results make `scan` exit with 2. Errors exit with 1 whichever it is.
#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailOn {
    /// Exit with 0 on findings and unconfirmed files
    Never,
    /// Exit with 2 on findings
    Findings,
    /// Exit with 2 on findings and on unconfirmed files
    Unconfirmed,
}

#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReportFormat {
    Json,
    Text,
    /// SARIF 2.1.0, for GitHub code scanning
    Sarif,
    /// OCSF 1.9.0 Data Security Finding events, one JSON object per line, for SIEMs and
    /// security data lakes
    Ocsf,
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

/// A deprecated command line spelling that still works, and the one that replaces it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeprecatedSpelling {
    pub used: &'static str,
    pub replacement: &'static str,
}

const DEPRECATED_PRESIDIO_REDACTER: DeprecatedSpelling = DeprecatedSpelling {
    used: "ms-presidio",
    replacement: "presidio",
};

const DEPRECATED_PRESIDIO_FLAGS: [DeprecatedSpelling; 2] = [
    DeprecatedSpelling {
        used: "--ms-presidio-text-analyze-url",
        replacement: "--presidio-text-analyze-url",
    },
    DeprecatedSpelling {
        used: "--ms-presidio-image-redact-url",
        replacement: "--presidio-image-redact-url",
    },
];

impl CliArgs {
    /// Parses like [`Parser::try_parse_from`] and also returns the deprecated Presidio
    /// spellings the arguments used, each once.
    ///
    /// clap resolves an alias to the canonical name, so the redacter name is read from the
    /// values as typed. For the flags clap keeps no spelling, so the arguments before `--` are
    /// scanned for the whole token.
    pub fn try_parse_from_with_deprecations<I, T>(
        args: I,
    ) -> Result<(Self, Vec<DeprecatedSpelling>), clap::Error>
    where
        I: IntoIterator<Item = T>,
        T: Into<std::ffi::OsString>,
    {
        let args: Vec<std::ffi::OsString> = args.into_iter().map(Into::into).collect();
        let matches = Self::command().try_get_matches_from(&args)?;
        let cli = Self::from_arg_matches(&matches)?;

        let mut deprecated = Vec::new();
        let deprecated_redacter_typed = matches
            .subcommand()
            .and_then(|(_, subcommand)| subcommand.try_get_raw("redact").ok().flatten())
            .is_some_and(|mut names| names.any(|name| name == DEPRECATED_PRESIDIO_REDACTER.used));
        if deprecated_redacter_typed {
            deprecated.push(DEPRECATED_PRESIDIO_REDACTER);
        }
        let flags: Vec<&str> = args
            .iter()
            .take_while(|arg| *arg != "--")
            .filter_map(|arg| arg.to_str())
            .map(|arg| arg.split_once('=').map_or(arg, |(flag, _)| flag))
            .collect();
        deprecated.extend(
            DEPRECATED_PRESIDIO_FLAGS
                .into_iter()
                .filter(|flag| flags.contains(&flag.used)),
        );
        Ok((cli, deprecated))
    }
}

#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum RedacterType {
    GcpDlp,
    AwsComprehend,
    #[value(alias = "ms-presidio")]
    Presidio,
    GeminiLlm,
    OpenAiLlm,
    GcpVertexAi,
    AwsBedrock,
    AwsBedrockGuardrails,
    LocalRules,
    LocalNer,
    LocalGliner,
    LocalCredentialMl,
}

impl Display for RedacterType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RedacterType::GcpDlp => write!(f, "gcp-dlp"),
            RedacterType::AwsComprehend => write!(f, "aws-comprehend"),
            RedacterType::Presidio => write!(f, "presidio"),
            RedacterType::GeminiLlm => write!(f, "gemini-llm"),
            RedacterType::OpenAiLlm => write!(f, "open-ai-llm"),
            RedacterType::GcpVertexAi => write!(f, "gcp-vertex-ai"),
            RedacterType::AwsBedrock => write!(f, "aws-bedrock"),
            RedacterType::AwsBedrockGuardrails => write!(f, "aws-bedrock-guardrails"),
            RedacterType::LocalRules => write!(f, "local-rules"),
            RedacterType::LocalNer => write!(f, "local-ner"),
            RedacterType::LocalGliner => write!(f, "local-gliner"),
            RedacterType::LocalCredentialMl => write!(f, "local-credential-ml"),
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
        help = "Read files of a type no redacter supports, such as .env files, keys and source code, as text when they are valid UTF-8 without NUL bytes"
    )]
    pub unsupported_as_text: bool,

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
        value_parser = CategoriesParser,
        requires = "redact",
        help = "Data categories to look for, comma separated, or only exclusions with a leading ! to look for every category but those, such as --categories '!other' (use single quotes: an interactive bash or zsh expands a ! as history, in double quotes too). scan reports only findings in these categories. cp redacts only data in these categories and copies everything else as it is. Presidio and the LLM redacters then read images and PDF pages with OCR. Default is every category"
    )]
    pub categories: Option<BTreeSet<Category>>,

    #[arg(
        long,
        value_enum,
        help = "How LLM redacters redact images: 'native' lets the model edit the image, 'coords' asks the model for coordinates and blacks them out locally, 'auto' edits natively then verifies the edit with the coordinate pass, falling back to coordinates entirely when the model cannot edit images. Default is 'auto'"
    )]
    pub llm_image_mode: Option<LlmImageMode>,

    #[arg(
        long,
        help = "GCP location for Vertex AI. Default is 'global'; 'us' and 'eu' multi-regions and regional locations such as 'us-central1' are accepted"
    )]
    pub gcp_region: Option<GcpRegion>,

    #[arg(
        long,
        help = "Model name for text redaction in Vertex AI, also used to locate sensitive data in images. Default is 'publishers/google/models/gemini-3.8-flash'"
    )]
    pub gcp_vertex_ai_text_model: Option<GcpVertexAiModelName>,

    #[arg(
        long,
        help = "Model name for native image editing in Vertex AI. Default is 'publishers/google/models/gemini-3.1-flash-image'"
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

    #[arg(
        long,
        help = "AWS region for the AWS Comprehend and AWS Bedrock redacters"
    )]
    pub aws_region: Option<String>,

    #[arg(
        long,
        help = "Bedrock model id for text redaction, also used to locate sensitive data in images and, since Bedrock has no active image editing model, to redact images. Default is 'amazon.nova-2-lite-v1:0' with the inference profile prefix of the selected region ('us.', 'eu.' or 'jp.'), falling back to the 'global.' profile elsewhere"
    )]
    pub aws_bedrock_text_model: Option<AwsBedrockModelName>,

    #[arg(long, help = "Guardrail id for the AWS Bedrock Guardrails redacter")]
    pub aws_bedrock_guardrail_id: Option<AwsBedrockGuardrailId>,

    #[arg(
        long,
        help = "Guardrail version for the AWS Bedrock Guardrails redacter. Default is 'DRAFT'"
    )]
    pub aws_bedrock_guardrail_version: Option<AwsBedrockGuardrailVersion>,

    #[arg(
        long,
        alias = "ms-presidio-text-analyze-url",
        help = "URL for text analyze endpoint for Presidio redacter"
    )]
    pub presidio_text_analyze_url: Option<Url>,

    #[arg(
        long,
        alias = "ms-presidio-image-redact-url",
        help = "URL for image redact endpoint for Presidio redacter"
    )]
    pub presidio_image_redact_url: Option<Url>,

    #[arg(
        long,
        help = "Gemini model name for text redaction, also used to locate sensitive data in images. Default is 'models/gemini-3.8-flash'"
    )]
    pub gemini_model: Option<GeminiLlmModelName>,

    #[arg(
        long,
        help = "Gemini model name for native image editing. Default is 'models/gemini-3.1-flash-image'"
    )]
    pub gemini_image_model: Option<GeminiLlmModelName>,

    #[arg(
        long,
        help = "Sampling size in bytes before redacting files. Disabled by default"
    )]
    pub sampling_size: Option<usize>,

    #[arg(long, help = "API key for OpenAI LLM redacter")]
    pub open_ai_api_key: Option<OpenAiLlmApiKey>,

    #[arg(
        long,
        help = "Open AI chat model name for text redaction, also used to locate sensitive data in images. Default is 'gpt-5.6-luna'"
    )]
    pub open_ai_model: Option<OpenAiModelName>,

    #[arg(
        long,
        help = "Open AI model name for native image editing. Default is 'gpt-image-2'"
    )]
    pub open_ai_image_model: Option<OpenAiModelName>,

    #[arg(
        long,
        help = "Limit the number of DLP requests. Some DLPs has strict quotas and to avoid errors, limit the number of requests delaying them. Default is disabled"
    )]
    pub limit_dlp_requests: Option<DlpRequestLimit>,

    #[arg(
        long,
        value_enum,
        value_delimiter = ',',
        help = "Rule groups enabled for the local-rules redacter, comma separated. Default is every group"
    )]
    pub local_rules: Option<Vec<RuleGroup>>,

    #[arg(
        long,
        value_enum,
        value_delimiter = ',',
        help = "Rule groups disabled for the local-rules redacter, comma separated. Applied after --local-rules"
    )]
    pub local_rules_disable: Option<Vec<RuleGroup>>,

    #[arg(
        long,
        help = "User-defined regex rule for the local-rules redacter in the form name=regex. Can be repeated"
    )]
    pub local_rule: Option<Vec<String>>,

    #[arg(
        long,
        help = "JSON file with user-defined regex and dictionary rules for the local-rules redacter. Can be repeated"
    )]
    pub local_rules_file: Option<Vec<std::path::PathBuf>>,

    #[cfg(feature = "local-ner")]
    #[arg(
        long,
        value_enum,
        value_delimiter = ',',
        help = "Entity types the local-ner redacter removes, comma separated: per (people), org (organisations), loc (locations). Default is all three"
    )]
    pub local_ner_entities: Option<Vec<Entity>>,

    #[cfg(feature = "local-ner")]
    #[arg(
        long,
        default_value = "0.5",
        help = "Minimum model confidence for a word to be redacted by local-ner, between 0 and 1. Lower values redact more. Default is 0.5"
    )]
    pub local_ner_min_score: Score,

    #[cfg(feature = "local-gliner")]
    #[arg(
        long,
        value_delimiter = ',',
        help = "Entity labels the local-gliner redacter looks for, comma separated, free text understood by the model (for example \"person,email,medical condition\"). Default is a PII list: person, address, email, phone number, date of birth, passport number, national id number, credit card number, iban, ip address, username, medical condition, postal code, license plate number"
    )]
    pub local_gliner_labels: Option<Vec<String>>,

    #[cfg(feature = "local-gliner")]
    #[arg(
        long,
        default_value = "0.5",
        help = "Minimum model confidence for a span to be redacted by local-gliner, between 0 and 1. Lower values redact more. Default is 0.5"
    )]
    pub local_gliner_min_score: Score,

    #[cfg(feature = "local-credential-ml")]
    #[arg(
        long,
        default_value = "0.74",
        help = "Minimum model confidence for a line to be redacted by local-credential-ml, between 0 and 1. Lower values redact more. Default is 0.74, the model card's threshold"
    )]
    pub local_credential_ml_min_score: Score,
}

impl RedacterArgs {
    /// Options that steer how Presidio and the LLM redacters redact images do nothing under
    /// `--categories`, which has them read images with OCR instead.
    fn check_options_with_categories(&self) -> Result<(), CategoriesError> {
        let typed = [
            ("--llm-image-mode", self.llm_image_mode.is_some()),
            (
                "--presidio-image-redact-url",
                self.presidio_image_redact_url.is_some(),
            ),
            ("--gemini-image-model", self.gemini_image_model.is_some()),
            ("--open-ai-image-model", self.open_ai_image_model.is_some()),
            (
                "--gcp-vertex-ai-image-model",
                self.gcp_vertex_ai_image_model.is_some(),
            ),
        ];
        match typed.into_iter().find(|(_, is_typed)| *is_typed) {
            Some((option, _)) => Err(CategoriesError::NoEffect { option }),
            None => Ok(()),
        }
    }

    fn gcp_dlp_options(
        &self,
        project_id: &GcpProjectId,
        categories: Option<&Categories>,
    ) -> Result<GcpDlpRedacterOptions, AppError> {
        let extra_built_in = self
            .gcp_dlp_built_in_info_type
            .as_deref()
            .unwrap_or_default();
        let stored = self.gcp_dlp_stored_info_type.as_deref().unwrap_or_default();
        let mut built_in_info_types: Vec<String> = match categories {
            Some(categories) => {
                categories.check("--gcp-dlp-built-in-info-type", extra_built_in, |name| {
                    GcpDlpRedacter::info_type_category(name)
                })?;
                categories.check("--gcp-dlp-stored-info-type", stored, |_| Category::Other)?;
                categories
                    .filter(GcpDlpRedacter::INFO_TYPES, |(_, category)| *category)
                    .into_iter()
                    .map(|(name, _)| name.to_string())
                    .collect()
            }
            None => GcpDlpRedacter::default_info_types(),
        };
        built_in_info_types.extend(extra_built_in.iter().cloned());
        if let Some(categories) = categories {
            if built_in_info_types.is_empty() && stored.is_empty() {
                return Err(CategoriesError::NothingToFind {
                    redacter: RedacterType::GcpDlp,
                    categories: categories.clone(),
                }
                .into());
            }
        }
        Ok(GcpDlpRedacterOptions {
            project_id: project_id.clone(),
            built_in_info_types,
            user_defined_stored_info_types: stored.to_vec(),
        })
    }

    fn local_rules_options(
        &self,
        categories: Option<&Categories>,
    ) -> Result<LocalRulesRedacterOptions, AppError> {
        let mut inline_rules: Vec<UserRule> = Vec::new();
        for inline in self.local_rule.iter().flatten() {
            inline_rules.push(crate::redacters::parse_inline_rule(inline)?);
        }
        let mut file_rules: Vec<UserRule> = Vec::new();
        for path in self.local_rules_file.iter().flatten() {
            file_rules.extend(crate::redacters::load_rules_file(path)?);
        }
        let mut groups: BTreeSet<RuleGroup> = match (&self.local_rules, categories) {
            (Some(enabled), Some(categories)) => {
                categories.check("--local-rules", enabled, |group| group.category())?;
                enabled.iter().copied().collect()
            }
            (Some(enabled), None) => enabled.iter().copied().collect(),
            (None, Some(categories)) => categories
                .filter(RuleGroup::all(), |group| group.category())
                .into_iter()
                .collect(),
            (None, None) => RuleGroup::all(),
        };
        if let Some(categories) = categories {
            let inline_names: Vec<_> = inline_rules.iter().map(|rule| &rule.name).collect();
            let file_names: Vec<_> = file_rules.iter().map(|rule| &rule.name).collect();
            categories.check("--local-rule", &inline_names, |_| Category::Other)?;
            categories.check("--local-rules-file", &file_names, |_| Category::Other)?;
            let finds_nothing = groups.iter().all(|group| *group == RuleGroup::Custom)
                && inline_rules.is_empty()
                && file_rules.is_empty();
            if finds_nothing {
                return Err(CategoriesError::NothingToFind {
                    redacter: RedacterType::LocalRules,
                    categories: categories.clone(),
                }
                .into());
            }
        }
        for group in self.local_rules_disable.iter().flatten() {
            groups.remove(group);
        }
        let user_rules = inline_rules.into_iter().chain(file_rules).collect();
        Ok(LocalRulesRedacterOptions { groups, user_rules })
    }

    #[cfg(feature = "local-ner")]
    fn local_ner_entities(
        &self,
        categories: Option<&Categories>,
    ) -> Result<BTreeSet<Entity>, AppError> {
        let entities: BTreeSet<Entity> = match (&self.local_ner_entities, categories) {
            (Some(named), Some(categories)) => {
                let names: Vec<&str> = named.iter().map(|entity| entity.as_str()).collect();
                categories.check("--local-ner-entities", &names, |name| {
                    Entity::label_category(name)
                })?;
                named.iter().copied().collect()
            }
            (Some(named), None) => named.iter().copied().collect(),
            (None, Some(categories)) => categories
                .filter(Entity::all(), |entity| entity.category())
                .into_iter()
                .collect(),
            (None, None) => Entity::all(),
        };
        match categories {
            Some(categories) if entities.is_empty() => Err(CategoriesError::NothingToFind {
                redacter: RedacterType::LocalNer,
                categories: categories.clone(),
            }
            .into()),
            _ => Ok(entities),
        }
    }

    #[cfg(feature = "local-gliner")]
    fn local_gliner_labels(
        &self,
        categories: Option<&Categories>,
    ) -> Result<Vec<String>, AppError> {
        use crate::redacters::local_gliner::labels::{
            default_labels, label_category, DEFAULT_LABELS,
        };
        let labels: Vec<String> = match (&self.local_gliner_labels, categories) {
            (Some(named), Some(categories)) => {
                categories.check("--local-gliner-labels", named, |label| {
                    label_category(label)
                })?;
                named.clone()
            }
            (Some(named), None) => named.clone(),
            (None, Some(categories)) => categories
                .filter(DEFAULT_LABELS, |(_, category)| *category)
                .into_iter()
                .map(|(label, _)| label.to_string())
                .collect(),
            (None, None) => default_labels(),
        };
        match categories {
            Some(categories) if labels.is_empty() => Err(CategoriesError::NothingToFind {
                redacter: RedacterType::LocalGliner,
                categories: categories.clone(),
            }
            .into()),
            _ => Ok(labels),
        }
    }
}

impl TryInto<RedacterOptions> for RedacterArgs {
    type Error = AppError;

    fn try_into(self) -> Result<RedacterOptions, Self::Error> {
        let categories = self
            .categories
            .as_ref()
            .and_then(|chosen| Categories::limiting(chosen.iter().copied()));
        if categories.is_some() {
            self.check_options_with_categories()?;
        }
        let categories = categories.as_ref();
        let image_mode = self.llm_image_mode.unwrap_or_default();
        let mut provider_options =
            Vec::with_capacity(self.redact.as_ref().map(Vec::len).unwrap_or(0));
        for options in self.redact.iter().flatten().copied() {
            let redacter_options = match options {
                RedacterType::GcpDlp => match self.gcp_project_id {
                    Some(ref project_id) => self
                        .gcp_dlp_options(project_id, categories)
                        .map(RedacterProviderOptions::GcpDlp),
                    None => Err(AppError::RedacterConfigError {
                        message: "GCP project id is required for GCP DLP redacter".to_string(),
                    }),
                },
                RedacterType::AwsComprehend => {
                    if let Some(categories) = categories {
                        if !AwsComprehendRedacter::finds_any_of(categories) {
                            return Err(CategoriesError::NothingToFind {
                                redacter: options,
                                categories: categories.clone(),
                            }
                            .into());
                        }
                    }
                    Ok(RedacterProviderOptions::AwsComprehend(
                        crate::redacters::AwsComprehendRedacterOptions {
                            region: self.aws_region.clone().map(aws_config::Region::new),
                        },
                    ))
                }
                RedacterType::Presidio => {
                    if self.presidio_text_analyze_url.is_none()
                        && self.presidio_image_redact_url.is_none()
                    {
                        return Err(AppError::RedacterConfigError {
                            message:
                            "Presidio requires text analyze/image URL specified (at least one)"
                                .to_string(),
                        });
                    }
                    Ok(RedacterProviderOptions::Presidio(
                        crate::redacters::PresidioRedacterOptions {
                            text_analyze_url: self.presidio_text_analyze_url.clone(),
                            image_redact_url: self.presidio_image_redact_url.clone(),
                            categories: categories.cloned(),
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
                        gemini_image_model: self.gemini_image_model.clone(),
                        image_mode,
                        categories: categories.cloned(),
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
                        image_model: self.open_ai_image_model.clone(),
                        image_mode,
                        categories: categories.cloned(),
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
                        gcp_region: self.gcp_region.clone().unwrap_or_else(|| {
                            GcpRegion::new(DEFAULT_GCP_VERTEX_AI_REGION.to_string())
                        }),
                        image_mode,
                        text_model: self.gcp_vertex_ai_text_model.clone(),
                        image_model: self.gcp_vertex_ai_image_model.clone(),
                        block_none_harmful: self.gcp_vertex_ai_block_none_harmful,
                        categories: categories.cloned(),
                    },
                )),
                RedacterType::AwsBedrock => Ok(RedacterProviderOptions::AwsBedrock(
                    crate::redacters::AwsBedrockRedacterOptions {
                        region: self.aws_region.clone().map(aws_config::Region::new),
                        text_model: self.aws_bedrock_text_model.clone(),
                        image_mode,
                        categories: categories.cloned(),
                    },
                )),
                RedacterType::AwsBedrockGuardrails => {
                    Ok(RedacterProviderOptions::AwsBedrockGuardrails(
                        crate::redacters::AwsBedrockGuardrailsRedacterOptions {
                            region: self.aws_region.clone().map(aws_config::Region::new),
                            guardrail_id: self.aws_bedrock_guardrail_id.clone().ok_or_else(
                                || AppError::RedacterConfigError {
                                    message: "Guardrail id is required for the AWS Bedrock \
                                              Guardrails redacter"
                                        .to_string(),
                                },
                            )?,
                            guardrail_version: self
                                .aws_bedrock_guardrail_version
                                .clone()
                                .unwrap_or_else(|| {
                                    AwsBedrockGuardrailVersion::new(
                                        DEFAULT_AWS_BEDROCK_GUARDRAIL_VERSION.to_string(),
                                    )
                                }),
                        },
                    ))
                }
                RedacterType::LocalRules => self
                    .local_rules_options(categories)
                    .map(RedacterProviderOptions::LocalRules),
                #[cfg(feature = "local-ner")]
                RedacterType::LocalNer => {
                    let entities = self.local_ner_entities(categories)?;
                    LocalNerRedacterOptions::validated(entities, self.local_ner_min_score)
                        .map(RedacterProviderOptions::LocalNer)
                        .map_err(|err| AppError::RedacterConfigError {
                            message: err.to_string(),
                        })
                }
                #[cfg(not(feature = "local-ner"))]
                RedacterType::LocalNer => Err(AppError::RedacterConfigError {
                    message: "local-ner is not available in this build (compiled without the local-ner feature)"
                        .to_string(),
                }),
                #[cfg(feature = "local-gliner")]
                RedacterType::LocalGliner => {
                    let labels = self.local_gliner_labels(categories)?;
                    LocalGlinerRedacterOptions::validated(labels, self.local_gliner_min_score)
                        .map(RedacterProviderOptions::LocalGliner)
                        .map_err(|err| AppError::RedacterConfigError {
                            message: err.to_string(),
                        })
                }
                #[cfg(not(feature = "local-gliner"))]
                RedacterType::LocalGliner => Err(AppError::RedacterConfigError {
                    message: "local-gliner is not available in this build (compiled without the local-gliner feature)"
                        .to_string(),
                }),
                #[cfg(feature = "local-credential-ml")]
                RedacterType::LocalCredentialMl => {
                    if let Some(categories) = categories.filter(|c| !c.contains(Category::Secrets)) {
                        return Err(CategoriesError::NothingToFind {
                            redacter: options,
                            categories: categories.clone(),
                        }
                        .into());
                    }
                    Ok(RedacterProviderOptions::LocalCredentialMl(
                        LocalCredentialMlRedacterOptions {
                            min_score: self.local_credential_ml_min_score,
                        },
                    ))
                }
                #[cfg(not(feature = "local-credential-ml"))]
                RedacterType::LocalCredentialMl => Err(AppError::RedacterConfigError {
                    message: "local-credential-ml is not available in this build (compiled without the local-credential-ml feature)"
                        .to_string(),
                }),
            }?;
            provider_options.push(redacter_options);
        }

        let base_options = RedacterBaseOptions {
            allow_unsupported_copies: self.allow_unsupported_copies,
            unsupported_as_text: self.unsupported_as_text,
            csv_headers_disable: self.csv_headers_disable,
            csv_delimiter: self.csv_delimiter.map(|c| c as u8),
            sampling_size: self.sampling_size,
            limit_dlp_requests: self.limit_dlp_requests,
            categories: categories.cloned(),
            list_redactions: false,
        };
        Ok(RedacterOptions {
            provider_options,
            base_options,
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn addons_install_takes_a_comma_separated_list() {
        let cli = CliArgs::try_parse_from(["redacter", "addons", "install", "ocr,pdf"]).unwrap();
        match cli.command {
            CliCommand::Addons {
                command: AddonsCommand::Install { addons },
            } => assert_eq!(addons, [Addon::Ocr, Addon::Pdf]),
            other => panic!("expected addons install, got {other:?}"),
        }
    }

    #[test]
    fn scan_takes_what_makes_it_fail_and_fails_on_findings_by_default() {
        let fail_on = |flags: &[&str]| {
            let cli = CliArgs::try_parse_from(
                ["redacter", "scan", "/tmp", "--redact", "local-rules"]
                    .into_iter()
                    .chain(flags.iter().copied()),
            )
            .unwrap();
            match cli.command {
                CliCommand::Scan { fail_on, .. } => fail_on,
                other => panic!("expected scan, got {other:?}"),
            }
        };
        assert_eq!(fail_on(&[]), FailOn::Findings);
        assert_eq!(fail_on(&["--fail-on", "never"]), FailOn::Never);
        assert_eq!(fail_on(&["--fail-on", "unconfirmed"]), FailOn::Unconfirmed);
    }

    #[test]
    fn addons_ls_takes_no_arguments() {
        let cli = CliArgs::try_parse_from(["redacter", "addons", "ls"]).unwrap();
        assert!(matches!(
            cli.command,
            CliCommand::Addons {
                command: AddonsCommand::Ls
            }
        ));
    }

    #[test]
    fn addons_dir_is_accepted_before_and_after_the_subcommand() {
        let before =
            CliArgs::try_parse_from(["redacter", "--addons-dir", "/m", "addons", "install", "ocr"])
                .unwrap();
        let after =
            CliArgs::try_parse_from(["redacter", "addons", "install", "ocr", "--addons-dir", "/m"])
                .unwrap();
        assert_eq!(before.addons_dir, Some(PathBuf::from("/m")));
        assert_eq!(after.addons_dir, Some(PathBuf::from("/m")));
    }

    #[test]
    fn addons_dir_flag_wins_over_the_environment_value() {
        assert_eq!(
            resolve_addons_dir(
                Some(PathBuf::from("/flag")),
                Some(std::ffi::OsString::from("/env"))
            ),
            Some(PathBuf::from("/flag"))
        );
    }

    #[test]
    fn addons_dir_falls_back_to_the_environment_value() {
        assert_eq!(
            resolve_addons_dir(None, Some(std::ffi::OsString::from("/env"))),
            Some(PathBuf::from("/env"))
        );
    }

    #[test]
    fn addons_dir_is_none_without_either() {
        assert_eq!(resolve_addons_dir(None, None), None);
    }

    mod scan {
        use super::*;
        use clap::error::ErrorKind;

        fn parse(args: &[&str]) -> Result<CliArgs, clap::Error> {
            CliArgs::try_parse_from([&["redacter", "scan", "test-fixtures"], args].concat())
        }

        fn report_format_of(args: &[&str]) -> Result<ReportFormat, clap::Error> {
            let cli = CliArgs::try_parse_from(args)?;
            match cli.command {
                CliCommand::Scan { report, .. } | CliCommand::Cp { report, .. } => {
                    Ok(report.report_format)
                }
                other => unreachable!("{other:?}"),
            }
        }

        #[test]
        fn sarif_and_ocsf_are_report_formats_of_scan_and_of_cp() {
            for (command, extra) in [("scan", &["-d", "local-rules"][..]), ("cp", &["out/"][..])] {
                for (name, format) in [("sarif", ReportFormat::Sarif), ("ocsf", ReportFormat::Ocsf)]
                {
                    let mut args = vec!["redacter", command, "."];
                    args.extend_from_slice(extra);
                    args.extend(["--report-file", "r.out", "--report-format", name]);
                    assert_eq!(report_format_of(&args).unwrap(), format, "{command} {name}");
                }
            }
        }

        #[test]
        fn scan_needs_at_least_one_redacter() {
            let err = parse(&[]).expect_err("a scan without -d must fail");
            assert_eq!(err.kind(), ErrorKind::MissingRequiredArgument);
        }
    }

    #[test]
    fn redacter_type_names_match_the_cli_values() {
        for redacter_type in RedacterType::value_variants() {
            let cli_value = redacter_type
                .to_possible_value()
                .expect("every redacter type is selectable on the command line");
            assert_eq!(redacter_type.to_string(), cli_value.get_name());
        }
    }

    use crate::redacters::{RedacterProviderOptions, RuleGroup, UserMatcher};
    use clap::Parser;

    #[derive(Parser)]
    struct TestCli {
        #[command(flatten)]
        redacter: RedacterArgs,
    }

    fn presidio_urls(args: &[&str]) -> (Option<Url>, Option<Url>) {
        let cli = TestCli::try_parse_from([&["redacter"], args].concat())
            .unwrap_or_else(|err| panic!("{err}"));
        let options: RedacterOptions = cli.redacter.try_into().expect("valid Presidio options");
        match options.provider_options.into_iter().next() {
            Some(RedacterProviderOptions::Presidio(options)) => {
                (options.text_analyze_url, options.image_redact_url)
            }
            other => panic!("expected Presidio options, got {other:?}"),
        }
    }

    #[test]
    fn deprecated_presidio_spellings_select_the_same_options() {
        let canonical = presidio_urls(&[
            "-d",
            "presidio",
            "--presidio-text-analyze-url",
            "http://localhost:5002/analyze",
            "--presidio-image-redact-url",
            "http://localhost:5003/redact",
        ]);
        let deprecated = presidio_urls(&[
            "-d",
            "ms-presidio",
            "--ms-presidio-text-analyze-url",
            "http://localhost:5002/analyze",
            "--ms-presidio-image-redact-url",
            "http://localhost:5003/redact",
        ]);
        assert!(canonical.0.is_some() && canonical.1.is_some());
        assert_eq!(canonical, deprecated);
    }

    fn deprecated_spellings(args: &[&str]) -> Vec<DeprecatedSpelling> {
        CliArgs::try_parse_from_with_deprecations([&["redacter"], args].concat())
            .unwrap_or_else(|err| panic!("{err}"))
            .1
    }

    #[test]
    fn deprecated_redacter_name_is_reported_however_it_is_typed() {
        for redact in [
            &["-d", "ms-presidio"][..],
            &["-dms-presidio"],
            &["-d=ms-presidio"],
            &["--redact", "ms-presidio"],
            &["--redact=ms-presidio"],
            &["-d", "ms-presidio", "-d", "ms-presidio"],
        ] {
            let args = [&["scan", "/tmp"], redact].concat();
            assert_eq!(
                deprecated_spellings(&args),
                [DEPRECATED_PRESIDIO_REDACTER],
                "{redact:?}"
            );
        }
    }

    #[test]
    fn deprecated_presidio_flags_are_reported_with_and_without_equals() {
        let spellings = deprecated_spellings(&[
            "scan",
            "/tmp",
            "-d",
            "presidio",
            "--ms-presidio-text-analyze-url=http://localhost:5002/analyze",
            "--ms-presidio-image-redact-url",
            "http://localhost:5003/redact",
        ]);
        assert_eq!(spellings, DEPRECATED_PRESIDIO_FLAGS);
    }

    #[test]
    fn current_presidio_spellings_and_values_after_the_separator_are_not_deprecated() {
        assert!(deprecated_spellings(&[
            "scan",
            "/tmp",
            "-d",
            "presidio",
            "--presidio-text-analyze-url",
            "http://localhost:5002/analyze",
            "--presidio-image-redact-url=http://localhost:5003/redact",
        ])
        .is_empty());
        assert!(deprecated_spellings(&[
            "cp",
            "-d",
            "presidio",
            "--",
            "-dms-presidio",
            "--ms-presidio-text-analyze-url",
        ])
        .is_empty());
        assert!(deprecated_spellings(&["ls", "/tmp"]).is_empty());
    }

    fn local_rules_options(
        args: &[&str],
    ) -> Result<crate::redacters::LocalRulesRedacterOptions, AppError> {
        let cli = TestCli::try_parse_from([&["redacter", "-d", "local-rules"], args].concat())
            .unwrap_or_else(|err| panic!("{err}"));
        let options: RedacterOptions = cli.redacter.try_into()?;
        match options.provider_options.into_iter().next() {
            Some(RedacterProviderOptions::LocalRules(options)) => Ok(options),
            other => panic!("expected local rules options, got {other:?}"),
        }
    }

    #[test]
    fn local_rules_default_to_every_group() {
        let options = local_rules_options(&[]).unwrap();
        assert_eq!(options.groups, RuleGroup::all());
        assert!(options.user_rules.is_empty());
    }

    #[test]
    fn local_rules_enable_list_restricts_the_groups() {
        let options = local_rules_options(&["--local-rules", "email,phone"]).unwrap();
        assert_eq!(
            options.groups.into_iter().collect::<Vec<_>>(),
            vec![RuleGroup::Email, RuleGroup::Phone]
        );
    }

    #[test]
    fn local_rules_disable_wins_over_enable() {
        let options = local_rules_options(&[
            "--local-rules",
            "email,phone",
            "--local-rules-disable",
            "phone",
        ])
        .unwrap();
        assert_eq!(
            options.groups.into_iter().collect::<Vec<_>>(),
            vec![RuleGroup::Email]
        );
    }

    #[test]
    fn local_rules_unknown_group_is_rejected_by_clap() {
        let result =
            TestCli::try_parse_from(["redacter", "-d", "local-rules", "--local-rules", "emails"]);
        let err = result.err().expect("unknown group must fail");
        assert!(err.to_string().contains("emails"), "{err}");
    }

    #[test]
    fn local_rules_inline_rule_is_parsed() {
        let options = local_rules_options(&["--local-rule", "employee-id=EMP-[0-9]{6}"]).unwrap();
        assert_eq!(options.user_rules.len(), 1);
        assert_eq!(options.user_rules[0].name.as_str(), "employee-id");
        assert_eq!(
            options.user_rules[0].matcher,
            UserMatcher::Regex("EMP-[0-9]{6}".to_string())
        );
    }

    #[test]
    fn local_rules_bad_inline_rule_is_a_config_error() {
        let err = local_rules_options(&["--local-rule", "no-equals"]).unwrap_err();
        assert!(matches!(err, AppError::RedacterConfigError { .. }), "{err}");
    }

    #[test]
    fn local_rules_file_rules_are_loaded() {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            file.path(),
            r#"{"rules":[{"name":"codenames","dictionary":["Bluebird"]}]}"#,
        )
        .unwrap();
        let path = file.path().to_string_lossy().to_string();
        let options = local_rules_options(&["--local-rules-file", &path]).unwrap();
        assert_eq!(options.user_rules.len(), 1);
        assert_eq!(options.user_rules[0].name.as_str(), "codenames");
    }

    #[cfg(feature = "local-ner")]
    mod local_ner {
        use super::*;
        use crate::redacters::{Entity, LocalNerRedacterOptions};

        fn local_ner_options(args: &[&str]) -> Result<LocalNerRedacterOptions, AppError> {
            let cli = TestCli::try_parse_from([&["redacter", "-d", "local-ner"], args].concat())
                .unwrap_or_else(|err| panic!("{err}"));
            let options: RedacterOptions = cli.redacter.try_into()?;
            match options.provider_options.into_iter().next() {
                Some(RedacterProviderOptions::LocalNer(options)) => Ok(options),
                other => panic!("expected local ner options, got {other:?}"),
            }
        }

        #[test]
        fn defaults_to_every_entity_at_half_score() {
            let options = local_ner_options(&[]).unwrap();
            assert_eq!(options.entities, Entity::all());
            assert_eq!(options.min_score, Score::from_static(0.5));
        }

        #[test]
        fn entities_and_score_are_parsed() {
            let options = local_ner_options(&[
                "--local-ner-entities",
                "per,loc",
                "--local-ner-min-score",
                "0.3",
            ])
            .unwrap();
            assert_eq!(
                options.entities.into_iter().collect::<Vec<_>>(),
                vec![Entity::Per, Entity::Loc]
            );
            assert_eq!(options.min_score, Score::from_static(0.3));
        }

        #[test]
        fn unknown_entity_is_rejected_by_clap() {
            let err = TestCli::try_parse_from([
                "redacter",
                "-d",
                "local-ner",
                "--local-ner-entities",
                "person",
            ])
            .err()
            .expect("unknown entity must fail");
            assert!(err.to_string().contains("person"), "{err}");
        }
    }

    #[cfg(feature = "local-gliner")]
    mod local_gliner {
        use super::*;
        use crate::redacters::local_gliner::labels::default_labels;
        use crate::redacters::LocalGlinerRedacterOptions;

        fn local_gliner_options(args: &[&str]) -> Result<LocalGlinerRedacterOptions, AppError> {
            let cli = TestCli::try_parse_from([&["redacter", "-d", "local-gliner"], args].concat())
                .unwrap_or_else(|err| panic!("{err}"));
            let options: RedacterOptions = cli.redacter.try_into()?;
            match options.provider_options.into_iter().next() {
                Some(RedacterProviderOptions::LocalGliner(options)) => Ok(options),
                other => panic!("expected local gliner options, got {other:?}"),
            }
        }

        #[test]
        fn defaults_to_the_pii_label_list_at_half_score() {
            let options = local_gliner_options(&[]).unwrap();
            assert_eq!(options.labels, default_labels());
            assert_eq!(options.min_score, Score::from_static(0.5));
        }

        #[test]
        fn labels_and_score_are_parsed() {
            let options = local_gliner_options(&[
                "--local-gliner-labels",
                "person,medical condition",
                "--local-gliner-min-score",
                "0.3",
            ])
            .unwrap();
            assert_eq!(
                options.labels,
                vec!["person".to_string(), "medical condition".to_string()]
            );
            assert_eq!(options.min_score, Score::from_static(0.3));
        }

        #[test]
        fn an_empty_label_is_a_config_error() {
            let err = local_gliner_options(&["--local-gliner-labels", "person,  "]).unwrap_err();
            assert!(matches!(err, AppError::RedacterConfigError { .. }), "{err}");
        }

        #[test]
        fn a_trailing_comma_is_a_config_error() {
            let err =
                local_gliner_options(&["--local-gliner-labels", "person,email,"]).unwrap_err();
            assert!(matches!(err, AppError::RedacterConfigError { .. }), "{err}");
        }
    }

    mod categories {
        use super::*;
        use crate::redacters::{Categories, CategoriesError, Category};

        fn options_of(args: &[&str]) -> Result<RedacterOptions, AppError> {
            let cli = TestCli::try_parse_from([&["redacter"], args].concat())
                .unwrap_or_else(|err| panic!("{err}"));
            cli.redacter.try_into()
        }

        fn error_of(args: &[&str]) -> CategoriesError {
            match options_of(args).unwrap_err() {
                AppError::Categories(error) => error,
                other => panic!("expected a categories error, got {other:?}"),
            }
        }

        fn local_rules_of(args: &[&str]) -> LocalRulesRedacterOptions {
            let options = options_of(&[&["-d", "local-rules"], args].concat()).unwrap();
            match options.provider_options.into_iter().next() {
                Some(RedacterProviderOptions::LocalRules(options)) => options,
                other => panic!("expected local rules options, got {other:?}"),
            }
        }

        fn dlp_of(args: &[&str]) -> Result<GcpDlpRedacterOptions, AppError> {
            let options =
                options_of(&[&["-d", "gcp-dlp", "--gcp-project-id", "project"], args].concat())?;
            match options.provider_options.into_iter().next() {
                Some(RedacterProviderOptions::GcpDlp(options)) => Ok(options),
                other => panic!("expected GCP DLP options, got {other:?}"),
            }
        }

        #[test]
        fn categories_are_comma_separated() {
            let options =
                options_of(&["-d", "local-rules", "--categories", "secrets,contact"]).unwrap();
            assert_eq!(
                options.base_options.categories,
                Categories::limiting([Category::Secrets, Category::Contact])
            );
        }

        fn all_but(excluded: &[Category]) -> Option<Categories> {
            use clap::ValueEnum;
            Categories::limiting(
                Category::value_variants()
                    .iter()
                    .copied()
                    .filter(|category| !excluded.contains(category)),
            )
        }

        fn categories_of(value: &str) -> Option<Categories> {
            options_of(&["-d", "local-rules", "--categories", value])
                .unwrap_or_else(|err| panic!("{value}: {err}"))
                .base_options
                .categories
        }

        fn refused(value: &str) -> bool {
            TestCli::try_parse_from(["redacter", "-d", "local-rules", "--categories", value])
                .is_err()
        }

        #[test]
        fn credential_is_no_longer_a_category() {
            assert!(refused("credential"));
            assert!(refused("contact,credential"));
        }

        #[test]
        fn a_name_listed_twice_is_listed_once() {
            assert_eq!(
                categories_of("contact,contact"),
                Categories::limiting([Category::Contact])
            );
        }

        #[test]
        fn a_list_of_exclusions_means_every_category_but_those() {
            assert_eq!(categories_of("!other"), all_but(&[Category::Other]));
            assert_eq!(
                categories_of("!other,!network,!other"),
                all_but(&[Category::Other, Category::Network])
            );
        }

        #[test]
        fn an_exclusion_leaves_in_every_category_the_enum_has() {
            use clap::ValueEnum;
            let kept = categories_of("!other").expect("an exclusion limits the run");
            for category in Category::value_variants() {
                assert_eq!(kept.contains(*category), *category != Category::Other);
            }
        }

        #[test]
        fn names_and_exclusions_cannot_be_mixed() {
            assert!(refused("contact,!other"));
            assert!(refused("!other,contact"));
        }

        #[test]
        fn an_unknown_name_or_an_empty_entry_is_refused() {
            for value in [
                "nonsense",
                "!nonsense",
                "contact,",
                ",contact",
                "!",
                "",
                "all",
            ] {
                assert!(refused(value), "{value:?}");
            }
        }

        #[test]
        fn excluding_every_category_is_refused() {
            use clap::ValueEnum;
            let everything = Category::value_variants()
                .iter()
                .map(|category| format!("!{category}"))
                .collect::<Vec<_>>();
            assert!(refused(&everything.join(",")));
            assert!(!refused(&everything[1..].join(",")));
        }

        #[test]
        fn categories_need_a_redacter() {
            for args in [
                &["redacter", "cp", "/a", "/b", "--categories", "secrets"][..],
                &["redacter", "scan", "/a", "--categories", "secrets"],
            ] {
                let err = CliArgs::try_parse_from(args).unwrap_err();
                assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
            }
        }

        #[test]
        fn local_rules_groups_narrow_to_the_chosen_categories() {
            assert_eq!(
                local_rules_of(&["--categories", "secrets"]).groups,
                BTreeSet::from([RuleGroup::Url, RuleGroup::Secrets])
            );
            assert_eq!(
                local_rules_of(&["--categories", "contact,other"]).groups,
                BTreeSet::from([RuleGroup::Email, RuleGroup::Phone, RuleGroup::Custom])
            );
        }

        #[test]
        fn a_named_group_outside_the_categories_is_refused() {
            let error = error_of(&[
                "-d",
                "local-rules",
                "--local-rules",
                "email,secrets",
                "--categories",
                "secrets",
            ]);
            assert!(matches!(
                error,
                CategoriesError::ValueOutside {
                    option: "--local-rules",
                    ref value,
                    category: Category::Contact,
                } if value == "email"
            ));
        }

        #[test]
        fn a_user_rule_is_other() {
            let rule = ["--local-rule", "employee=EMP[0-9]+"];
            let error =
                error_of(&[&["-d", "local-rules", "--categories", "secrets"], &rule[..]].concat());
            assert!(matches!(
                error,
                CategoriesError::ValueOutside {
                    option: "--local-rule",
                    ref value,
                    category: Category::Other,
                } if value == "employee"
            ));
            let options = local_rules_of(&[&["--categories", "secrets,other"], &rule[..]].concat());
            assert_eq!(options.user_rules.len(), 1);
        }

        #[test]
        fn local_rules_with_only_user_rules_left_out_finds_nothing() {
            let error = error_of(&["-d", "local-rules", "--categories", "other"]);
            assert!(matches!(
                error,
                CategoriesError::NothingToFind {
                    redacter: RedacterType::LocalRules,
                    ..
                }
            ));
        }

        #[test]
        fn comprehend_without_an_entity_in_the_categories_finds_nothing() {
            let error = error_of(&["-d", "aws-comprehend", "--categories", "date-of-birth"]);
            assert!(matches!(
                error,
                CategoriesError::NothingToFind {
                    redacter: RedacterType::AwsComprehend,
                    ..
                }
            ));
        }

        #[test]
        fn dlp_info_types_narrow_to_the_chosen_categories() {
            let narrowed = dlp_of(&["--categories", "network"]).unwrap();
            assert_eq!(narrowed.built_in_info_types, ["IP_ADDRESS"]);

            let addresses = dlp_of(&["--categories", "address"]).unwrap();
            assert_eq!(addresses.built_in_info_types, ["LOCATION"]);

            let every = dlp_of(&[]).unwrap();
            assert_eq!(
                every.built_in_info_types,
                GcpDlpRedacter::default_info_types()
            );
        }

        #[test]
        fn dlp_info_types_the_user_added_outside_the_categories_are_refused() {
            let built_in = dlp_of(&[
                "--categories",
                "secrets",
                "--gcp-dlp-built-in-info-type",
                "EMAIL_ADDRESS",
            ])
            .unwrap_err();
            assert!(matches!(
                built_in,
                AppError::Categories(CategoriesError::ValueOutside {
                    option: "--gcp-dlp-built-in-info-type",
                    category: Category::Contact,
                    ..
                })
            ));
            let stored = dlp_of(&[
                "--categories",
                "secrets",
                "--gcp-dlp-stored-info-type",
                "employee",
            ])
            .unwrap_err();
            assert!(matches!(
                stored,
                AppError::Categories(CategoriesError::ValueOutside {
                    option: "--gcp-dlp-stored-info-type",
                    category: Category::Other,
                    ..
                })
            ));
        }

        #[test]
        fn an_image_option_has_no_effect_under_categories() {
            for (option, value) in [
                ("--llm-image-mode", "native"),
                ("--gemini-image-model", "models/image"),
                (
                    "--presidio-image-redact-url",
                    "http://localhost:5003/redact",
                ),
            ] {
                let error = error_of(&[
                    "-d",
                    "presidio",
                    "--presidio-text-analyze-url",
                    "http://localhost:5002/analyze",
                    option,
                    value,
                    "--categories",
                    "secrets",
                ]);
                assert!(
                    matches!(error, CategoriesError::NoEffect { option: refused } if refused == option),
                    "{option}"
                );
            }
        }

        #[test]
        fn the_llm_image_mode_is_auto_unless_typed() {
            let options = options_of(&["-d", "gemini-llm", "--gcp-project-id", "project"]).unwrap();
            match options.provider_options.into_iter().next() {
                Some(RedacterProviderOptions::GeminiLlm(options)) => {
                    assert_eq!(options.image_mode, LlmImageMode::Auto)
                }
                other => panic!("expected Gemini options, got {other:?}"),
            }
        }

        #[cfg(feature = "local-ner")]
        #[test]
        fn local_ner_entities_narrow_and_a_named_one_outside_is_refused() {
            let options = options_of(&["-d", "local-ner", "--categories", "person"]).unwrap();
            match options.provider_options.into_iter().next() {
                Some(RedacterProviderOptions::LocalNer(options)) => {
                    assert_eq!(
                        options.entities,
                        BTreeSet::from([crate::redacters::Entity::Per])
                    )
                }
                other => panic!("expected local ner options, got {other:?}"),
            }
            let error = error_of(&[
                "-d",
                "local-ner",
                "--local-ner-entities",
                "org",
                "--categories",
                "person",
            ]);
            assert!(matches!(
                error,
                CategoriesError::ValueOutside {
                    option: "--local-ner-entities",
                    category: Category::Organization,
                    ..
                }
            ));
            let places = options_of(&["-d", "local-ner", "--categories", "location"]).unwrap();
            match places.provider_options.into_iter().next() {
                Some(RedacterProviderOptions::LocalNer(options)) => {
                    assert_eq!(
                        options.entities,
                        BTreeSet::from([crate::redacters::Entity::Loc])
                    )
                }
                other => panic!("expected local ner options, got {other:?}"),
            }
            let nothing = error_of(&["-d", "local-ner", "--categories", "secrets"]);
            assert!(matches!(
                nothing,
                CategoriesError::NothingToFind {
                    redacter: RedacterType::LocalNer,
                    ..
                }
            ));
        }

        #[cfg(feature = "local-gliner")]
        #[test]
        fn local_gliner_labels_narrow_and_none_in_the_categories_finds_nothing() {
            let options = options_of(&["-d", "local-gliner", "--categories", "network"]).unwrap();
            match options.provider_options.into_iter().next() {
                Some(RedacterProviderOptions::LocalGliner(options)) => {
                    assert_eq!(options.labels, ["ip address"])
                }
                other => panic!("expected local gliner options, got {other:?}"),
            }
            let nothing = error_of(&["-d", "local-gliner", "--categories", "secrets"]);
            assert!(matches!(
                nothing,
                CategoriesError::NothingToFind {
                    redacter: RedacterType::LocalGliner,
                    ..
                }
            ));
        }

        #[cfg(feature = "local-gliner")]
        #[test]
        fn a_gliner_label_naming_an_organisation_can_be_asked_for_by_its_category() {
            let options = options_of(&[
                "-d",
                "local-gliner",
                "--local-gliner-labels",
                "organization",
                "--categories",
                "organization",
            ])
            .unwrap();
            match options.provider_options.into_iter().next() {
                Some(RedacterProviderOptions::LocalGliner(options)) => {
                    assert_eq!(options.labels, ["organization"])
                }
                other => panic!("expected local gliner options, got {other:?}"),
            }
        }

        #[cfg(feature = "local-credential-ml")]
        #[test]
        fn local_credential_ml_needs_the_secrets_category() {
            let error = error_of(&["-d", "local-credential-ml", "--categories", "contact"]);
            assert!(matches!(
                error,
                CategoriesError::NothingToFind {
                    redacter: RedacterType::LocalCredentialMl,
                    ..
                }
            ));
            assert!(options_of(&["-d", "local-credential-ml", "--categories", "secrets"]).is_ok());
        }
    }

    #[cfg(feature = "local-credential-ml")]
    mod local_credential_ml {
        use super::*;
        use crate::redacters::LocalCredentialMlRedacterOptions;

        fn local_credential_ml_options(args: &[&str]) -> LocalCredentialMlRedacterOptions {
            let cli = TestCli::try_parse_from(
                [&["redacter", "-d", "local-credential-ml"], args].concat(),
            )
            .unwrap_or_else(|err| panic!("{err}"));
            let options: RedacterOptions = cli.redacter.try_into().unwrap();
            match options.provider_options.into_iter().next() {
                Some(RedacterProviderOptions::LocalCredentialMl(options)) => options,
                other => panic!("expected local credential ml options, got {other:?}"),
            }
        }

        #[test]
        fn defaults_to_the_model_cards_threshold() {
            assert_eq!(
                local_credential_ml_options(&[]).min_score,
                Score::from_static(0.74)
            );
        }
    }
}
