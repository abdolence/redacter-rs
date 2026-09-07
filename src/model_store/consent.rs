use super::error::ModelStoreError;
use super::manifest::{ModelFile, ModelManifest};
use console::Term;
use std::io::IsTerminal;
use std::path::PathBuf;

/// What a download would fetch, shown to the user before anything is transferred.
#[derive(Debug)]
pub struct DownloadRequest {
    pub model: &'static ModelManifest,
    pub dir: PathBuf,
    /// Only the files that are missing or stale.
    pub files: Vec<&'static ModelFile>,
    pub total_bytes: u64,
}

/// Answers "may this be downloaded?": the terminal in production, a fixed answer in tests.
pub trait ConsentSource: Send + Sync {
    fn ask(&self, request: &DownloadRequest) -> Result<bool, ModelStoreError>;
}

/// True when a prompt can be shown and answered: stderr and stdin are both terminals.
pub fn is_interactive() -> bool {
    Term::stderr().is_term() && std::io::stdin().is_terminal()
}

/// Renders a byte count for the consent prompt: MiB with one decimal at or above 1 MiB,
/// KiB with one decimal below that, so a non-empty file is never shown as "0.0 MiB".
fn format_size(bytes: u64) -> String {
    const MIB: f64 = 1_048_576.0;
    const KIB: f64 = 1024.0;
    if bytes as f64 >= MIB {
        format!("{:.1} MiB", bytes as f64 / MIB)
    } else {
        let kib = (bytes as f64 / KIB).max(0.1);
        format!("{kib:.1} KiB")
    }
}

/// The exact text shown before a download. Sizes are rendered by `format_size`.
pub fn format_consent_prompt(request: &DownloadRequest) -> String {
    let files = request
        .files
        .iter()
        .map(|file| file.name)
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "{} needs the model \"{}\", which is not installed.\n  files    {} ({})\n  from     {}\n  license  {}\n  to       {}\nDownload now? [y/N] ",
        request.model.needed_by,
        request.model.dir_name,
        files,
        format_size(request.total_bytes),
        request.model.source,
        request.model.license,
        request.dir.display()
    )
}

fn is_yes(answer: &str) -> bool {
    matches!(answer.trim().to_ascii_lowercase().as_str(), "y" | "yes")
}

/// Asks on stderr and reads one line from the terminal. Anything but `y`/`yes` is a no.
pub struct TerminalConsent;

impl ConsentSource for TerminalConsent {
    fn ask(&self, request: &DownloadRequest) -> Result<bool, ModelStoreError> {
        let term = Term::stderr();
        term.write_str(&format_consent_prompt(request))
            .map_err(|source| ModelStoreError::Prompt { source })?;
        let answer = term
            .read_line()
            .map_err(|source| ModelStoreError::Prompt { source })?;
        Ok(is_yes(&answer))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model_store::{manifest, ModelId};

    #[test]
    fn ner_prompt_matches_the_spec() {
        let model = manifest(ModelId::NerMultilingualHrl);
        let request = DownloadRequest {
            model,
            dir: PathBuf::from(
                "/home/you/.cache/redacter/models/distilbert-base-multilingual-cased-ner-hrl",
            ),
            files: model.files.iter().collect(),
            total_bytes: model.files.iter().map(|f| f.size).sum(),
        };
        let expected = "The local-ner redacter needs the model \"distilbert-base-multilingual-cased-ner-hrl\", which is not installed.\n  files    model_uint8.onnx, tokenizer.json, config.json (131.6 MiB)\n  from     https://huggingface.co/Xenova/distilbert-base-multilingual-cased-ner-hrl (revision c2a4dbf)\n  license  Academic Free License 3.0 (AFL-3.0), see the model card\n  to       /home/you/.cache/redacter/models/distilbert-base-multilingual-cased-ner-hrl\nDownload now? [y/N] ";
        assert_eq!(format_consent_prompt(&request), expected);
    }

    #[test]
    fn prompt_lists_only_the_requested_files_and_their_size() {
        let model = manifest(ModelId::NerMultilingualHrl);
        let request = DownloadRequest {
            model,
            dir: PathBuf::from("/m"),
            files: vec![&model.files[2]],
            total_bytes: 927,
        };
        let prompt = format_consent_prompt(&request);
        assert!(
            prompt.contains("  files    config.json (0.9 KiB)\n"),
            "{prompt}"
        );
        assert!(prompt.contains("  to       /m\n"), "{prompt}");
    }

    #[test]
    fn ocrs_prompt_matches_the_spec() {
        let model = manifest(ModelId::Ocrs);
        let request = DownloadRequest {
            model,
            dir: PathBuf::from("/home/you/.cache/redacter/models/ocrs"),
            files: model.files.iter().collect(),
            total_bytes: model.files.iter().map(|f| f.size).sum(),
        };
        let expected = "OCR needs the model \"ocrs\", which is not installed.\n  files    text-detection.rten, text-recognition.rten (11.7 MiB)\n  from     https://ocrs-models.s3-accelerate.amazonaws.com/\n  license  ocrs, MIT OR Apache-2.0; weights trained on open, liberally licensed datasets\n  to       /home/you/.cache/redacter/models/ocrs\nDownload now? [y/N] ";
        assert_eq!(format_consent_prompt(&request), expected);
    }

    #[test]
    fn only_y_and_yes_are_consent() {
        for answer in ["y", "Y", "yes", "YES", " yes "] {
            assert!(is_yes(answer), "{answer:?}");
        }
        for answer in ["", "n", "no", "yes please", "ye"] {
            assert!(!is_yes(answer), "{answer:?}");
        }
    }
}
