use std::path::PathBuf;

/// A model the tool can install.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ModelId {
    // Only the ocr feature resolves this today; gated so a `--no-default-features` (or
    // pdf-render-only) build does not carry an unreachable variant.
    #[cfg(any(feature = "ocr", test))]
    Ocrs,
    // Only the local-ner feature resolves this one; gated for the same reason as `Ocrs`, so
    // a build without either feature carries an unreachable variant rather than an
    // `#[allow(dead_code)]` that would also hide a real regression.
    #[cfg(any(feature = "local-ner", test))]
    NerMultilingualHrl,
    // Only the local-gliner feature resolves this one; gated for the same reason as `Ocrs`.
    #[cfg(any(feature = "local-gliner", test))]
    GlinerMultiPii,
}

/// One file of a model: where it comes from and what it must look like on disk.
#[derive(Debug)]
pub struct ModelFile {
    pub name: &'static str,
    pub url: &'static str,
    pub size: u64,
    pub sha256: &'static str,
}

/// Everything the store knows about one model, declared once.
#[derive(Debug)]
pub struct ModelManifest {
    /// Directory name under the models root; also the model's name in messages.
    pub dir_name: &'static str,
    /// Subject of the consent prompt, e.g. "OCR" or "The local-ner redacter".
    pub needed_by: &'static str,
    /// The `from` line of the consent prompt.
    pub source: &'static str,
    /// The `license` line of the consent prompt.
    pub license: &'static str,
    pub files: &'static [ModelFile],
    /// Directories from earlier versions where a manual copy may still live; never verified.
    pub legacy_dirs: fn() -> Vec<PathBuf>,
}

#[cfg(any(feature = "local-ner", test))]
macro_rules! ner_url {
    ($path:literal) => {
        concat!(
            "https://huggingface.co/Xenova/distilbert-base-multilingual-cased-ner-hrl/resolve/",
            "c2a4dbf593c57f47004c5bc2d3770d311aee9c43/",
            $path
        )
    };
}

#[cfg(any(feature = "local-ner", test))]
static NER_MULTILINGUAL_HRL_FILES: [ModelFile; 3] = [
    ModelFile {
        name: "model_uint8.onnx",
        url: ner_url!("onnx/model_uint8.onnx"),
        size: 135_115_742,
        sha256: "5b54455882ff2f242af18932b9da308d1f902f9f65d1d8f56ce9eb9bfcd5bf9e",
    },
    ModelFile {
        name: "tokenizer.json",
        url: ner_url!("tokenizer.json"),
        size: 2_919_362,
        sha256: "bf1b59b7b11c95f194f51708d918eea378e09d05f84c0e1656dc5180e8117088",
    },
    ModelFile {
        name: "config.json",
        url: ner_url!("config.json"),
        size: 927,
        sha256: "38847be4dc6699b1218a749ed69f888c2ccc7b4deba98e3c4a1cac8cb34d54c8",
    },
];

/// A model that has never lived anywhere but the managed root.
#[cfg(any(feature = "local-ner", feature = "local-gliner", test))]
pub(super) fn no_legacy_dirs() -> Vec<PathBuf> {
    Vec::new()
}

#[cfg(any(feature = "local-gliner", test))]
macro_rules! gliner_url {
    ($path:literal) => {
        concat!(
            "https://huggingface.co/onnx-community/gliner_multi_pii-v1/resolve/",
            "2e0397a7e8a250d76c37122232b3cbde42c8d629/",
            $path
        )
    };
}

#[cfg(any(feature = "local-gliner", test))]
static GLINER_MULTI_PII_FILES: [ModelFile; 3] = [
    ModelFile {
        name: "model.onnx",
        url: gliner_url!("onnx/model.onnx"),
        size: 1_157_129_714,
        sha256: "7704865e414f24591da6aee08716a25677855bc8a84af81396d90e40df1e68d2",
    },
    ModelFile {
        name: "tokenizer.json",
        url: gliner_url!("tokenizer.json"),
        size: 16_331_948,
        sha256: "914bd3c8fb7b525af9e23b60d0ec7b1248ddb2b99014efd9c02ebeb022f8cab7",
    },
    ModelFile {
        name: "gliner_config.json",
        url: gliner_url!("gliner_config.json"),
        size: 732,
        sha256: "69e141f7fe1864e0d81ab0e542c68387d588db62393bcd93b471d54dcf0f5c16",
    },
];

#[cfg(any(feature = "ocr", test))]
static OCRS_FILES: [ModelFile; 2] = [
    ModelFile {
        name: "text-detection.rten",
        url: "https://ocrs-models.s3-accelerate.amazonaws.com/text-detection.rten",
        size: 2_510_284,
        sha256: "f15cfb56bd02c4bf478a20343986504a1f01e1665c2b3a0ad66340f054b1b5ca",
    },
    ModelFile {
        name: "text-recognition.rten",
        url: "https://ocrs-models.s3-accelerate.amazonaws.com/text-recognition.rten",
        size: 9_716_568,
        sha256: "e484866d4cce403175bd8d00b128feb08ab42e208de30e42cd9889d8f1735a6e",
    },
];

/// Where earlier versions of the tool looked for the OCR models; still honoured, never verified.
#[cfg(any(feature = "ocr", test))]
fn ocrs_legacy_dirs() -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    if let Some(share) = std::env::current_exe().ok().and_then(|exe| {
        exe.parent()
            .and_then(|bin| bin.parent())
            .map(|p| p.to_path_buf())
    }) {
        dirs.push(share.join("share").join("ocrs"));
    }
    if let Some(home) = dirs::home_dir() {
        dirs.push(home.join(".cache").join("ocrs"));
    }
    dirs
}

#[cfg(any(feature = "ocr", test))]
static OCRS: ModelManifest = ModelManifest {
    dir_name: "ocrs",
    needed_by: "OCR",
    source: "https://ocrs-models.s3-accelerate.amazonaws.com/",
    license: "ocrs, MIT OR Apache-2.0; weights trained on open, liberally licensed datasets",
    files: &OCRS_FILES,
    legacy_dirs: ocrs_legacy_dirs,
};

#[cfg(any(feature = "local-ner", test))]
static NER_MULTILINGUAL_HRL: ModelManifest = ModelManifest {
    dir_name: "distilbert-base-multilingual-cased-ner-hrl",
    needed_by: "The local-ner redacter",
    source:
        "https://huggingface.co/Xenova/distilbert-base-multilingual-cased-ner-hrl (revision c2a4dbf)",
    license: "Academic Free License 3.0 (AFL-3.0), see the model card",
    files: &NER_MULTILINGUAL_HRL_FILES,
    legacy_dirs: no_legacy_dirs,
};

#[cfg(any(feature = "local-gliner", test))]
static GLINER_MULTI_PII: ModelManifest = ModelManifest {
    dir_name: "gliner_multi_pii-v1",
    needed_by: "The local-gliner redacter",
    source: "https://huggingface.co/onnx-community/gliner_multi_pii-v1 (revision 2e0397a7)",
    license: "Apache-2.0 (model and training data), backbone microsoft/mdeberta-v3-base MIT",
    files: &GLINER_MULTI_PII_FILES,
    legacy_dirs: no_legacy_dirs,
};

pub fn manifest(id: ModelId) -> &'static ModelManifest {
    match id {
        #[cfg(any(feature = "ocr", test))]
        ModelId::Ocrs => &OCRS,
        #[cfg(any(feature = "local-ner", test))]
        ModelId::NerMultilingualHrl => &NER_MULTILINGUAL_HRL,
        #[cfg(any(feature = "local-gliner", test))]
        ModelId::GlinerMultiPii => &GLINER_MULTI_PII,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn ocrs_manifest_lists_both_rten_files_with_pinned_sizes() {
        let manifest = manifest(ModelId::Ocrs);
        assert_eq!(manifest.dir_name, "ocrs");
        assert_eq!(manifest.needed_by, "OCR");
        let files: Vec<(&str, u64)> = manifest.files.iter().map(|f| (f.name, f.size)).collect();
        assert_eq!(
            files,
            [
                ("text-detection.rten", 2_510_284),
                ("text-recognition.rten", 9_716_568)
            ]
        );
        for file in manifest.files {
            assert_eq!(
                file.url,
                format!(
                    "https://ocrs-models.s3-accelerate.amazonaws.com/{}",
                    file.name
                )
            );
            assert_eq!(file.sha256.len(), 64, "{}", file.name);
            assert!(
                file.sha256.chars().all(|c| c.is_ascii_hexdigit()),
                "{}",
                file.name
            );
        }
    }

    #[test]
    fn ocrs_legacy_dirs_are_share_ocrs_and_cache_ocrs() {
        let dirs = (manifest(ModelId::Ocrs).legacy_dirs)();
        assert!(
            dirs.iter()
                .any(|d| d.ends_with(Path::new("share").join("ocrs"))),
            "{dirs:?}"
        );
        assert!(
            dirs.iter()
                .any(|d| d.ends_with(Path::new(".cache").join("ocrs"))),
            "{dirs:?}"
        );
    }

    #[test]
    fn ner_manifest_lists_the_three_pinned_files() {
        let manifest = manifest(ModelId::NerMultilingualHrl);
        assert_eq!(
            manifest.dir_name,
            "distilbert-base-multilingual-cased-ner-hrl"
        );
        assert_eq!(manifest.needed_by, "The local-ner redacter");
        let names: Vec<&str> = manifest.files.iter().map(|f| f.name).collect();
        assert_eq!(names, ["model_uint8.onnx", "tokenizer.json", "config.json"]);
        let total: u64 = manifest.files.iter().map(|f| f.size).sum();
        assert_eq!(total, 138_036_031);
        assert!((manifest.legacy_dirs)().is_empty());
    }

    #[test]
    fn gliner_manifest_lists_the_three_pinned_files() {
        let manifest = manifest(ModelId::GlinerMultiPii);
        assert_eq!(manifest.dir_name, "gliner_multi_pii-v1");
        assert_eq!(manifest.needed_by, "The local-gliner redacter");
        let names: Vec<&str> = manifest.files.iter().map(|f| f.name).collect();
        assert_eq!(
            names,
            ["model.onnx", "tokenizer.json", "gliner_config.json"]
        );
        let total: u64 = manifest.files.iter().map(|f| f.size).sum();
        assert_eq!(total, 1_157_129_714 + 16_331_948 + 732);
        assert!((manifest.legacy_dirs)().is_empty());
    }

    #[test]
    fn gliner_urls_pin_the_revision_and_digests_are_hex() {
        let manifest = manifest(ModelId::GlinerMultiPii);
        for file in manifest.files {
            assert!(
                file.url.starts_with(
                    "https://huggingface.co/onnx-community/gliner_multi_pii-v1/resolve/2e0397a7e8a250d76c37122232b3cbde42c8d629/"
                ),
                "{}",
                file.url
            );
            assert!(file.url.ends_with(file.name), "{}", file.url);
            assert_eq!(file.sha256.len(), 64, "{}", file.name);
            assert!(
                file.sha256.chars().all(|c| c.is_ascii_hexdigit()),
                "{}",
                file.name
            );
        }
    }

    #[test]
    fn ner_urls_pin_the_revision_and_digests_are_hex() {
        let manifest = manifest(ModelId::NerMultilingualHrl);
        for file in manifest.files {
            assert!(
                file.url.starts_with(
                    "https://huggingface.co/Xenova/distilbert-base-multilingual-cased-ner-hrl/resolve/c2a4dbf593c57f47004c5bc2d3770d311aee9c43/"
                ),
                "{}",
                file.url
            );
            assert!(file.url.ends_with(file.name), "{}", file.url);
            assert_eq!(file.sha256.len(), 64, "{}", file.name);
            assert!(
                file.sha256.chars().all(|c| c.is_ascii_hexdigit()),
                "{}",
                file.name
            );
        }
    }
}
