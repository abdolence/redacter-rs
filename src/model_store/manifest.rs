use std::path::PathBuf;

/// An optional part of the tool that only `redacter setup-addons` downloads. Each variant
/// exists only in builds with its feature, so the subcommand never offers an addon the build
/// cannot use; test builds keep them all so every manifest is checked in every build.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Addon {
    /// The OCR models.
    #[cfg(any(feature = "ocr", test))]
    Ocr,
    /// The Pdfium library that renders PDF pages.
    #[cfg(any(feature = "pdf-render", test))]
    Pdf,
    /// The model of the local-ner redacter.
    #[cfg(any(feature = "local-ner", test))]
    LocalNer,
    /// The model of the local-gliner redacter.
    #[cfg(any(feature = "local-gliner", test))]
    LocalGliner,
}

impl Addon {
    /// The files this addon installs, or `None` on a platform it has no build for.
    pub fn manifest(self) -> Option<&'static ModelManifest> {
        match self {
            #[cfg(any(feature = "ocr", test))]
            Addon::Ocr => Some(&OCRS),
            #[cfg(any(feature = "pdf-render", test))]
            Addon::Pdf => super::pdfium::PDFIUM.as_ref(),
            #[cfg(any(feature = "local-ner", test))]
            Addon::LocalNer => Some(&NER_MULTILINGUAL_HRL),
            #[cfg(any(feature = "local-gliner", test))]
            Addon::LocalGliner => Some(&GLINER_MULTI_PII),
        }
    }
}

/// The name `setup-addons` takes.
impl std::fmt::Display for Addon {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = clap::ValueEnum::to_possible_value(self)
            .expect("no addon is hidden from the command line");
        f.write_str(value.get_name())
    }
}

/// One file of an addon: where it comes from and what it must look like on disk.
#[derive(Debug)]
pub struct ModelFile {
    pub name: &'static str,
    pub url: &'static str,
    pub size: u64,
    pub sha256: &'static str,
}

/// Everything the store knows about the files of one addon, declared once.
#[derive(Debug)]
pub struct ModelManifest {
    /// Directory name under the models root.
    pub dir_name: &'static str,
    pub files: &'static [ModelFile],
    /// Directories from earlier versions where a manual copy may still live; never verified.
    pub legacy_dirs: fn() -> Vec<PathBuf>,
    /// Native code the process loads and runs. A lookup checks its SHA-256 every time; a
    /// model is only data and is checked by size, so a run never hashes a gigabyte.
    pub native_code: bool,
}

#[cfg(any(feature = "local-ner", test))]
macro_rules! ner_url {
    ($path:literal) => {
        concat!(
            "https://github.com/abdolence/redacter-rs/releases/download/models-ner-hrl-1/",
            $path
        )
    };
}

#[cfg(any(feature = "local-ner", test))]
static NER_MULTILINGUAL_HRL_FILES: [ModelFile; 3] = [
    ModelFile {
        name: "model_uint8.onnx",
        url: ner_url!("model_uint8.onnx"),
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

/// An addon that has never lived anywhere but the models root.
#[cfg(any(
    feature = "local-ner",
    feature = "local-gliner",
    feature = "pdf-render",
    test
))]
pub(super) fn no_legacy_dirs() -> Vec<PathBuf> {
    Vec::new()
}

#[cfg(any(feature = "local-gliner", test))]
macro_rules! gliner_url {
    ($path:literal) => {
        concat!(
            "https://github.com/abdolence/redacter-rs/releases/download/models-gliner-multi-pii-1/",
            $path
        )
    };
}

#[cfg(any(feature = "local-gliner", test))]
static GLINER_MULTI_PII_FILES: [ModelFile; 3] = [
    ModelFile {
        name: "model.onnx",
        url: gliner_url!("model.onnx"),
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
        url: "https://github.com/abdolence/redacter-rs/releases/download/models-ocrs-1/text-detection.rten",
        size: 2_510_284,
        sha256: "f15cfb56bd02c4bf478a20343986504a1f01e1665c2b3a0ad66340f054b1b5ca",
    },
    ModelFile {
        name: "text-recognition.rten",
        url: "https://github.com/abdolence/redacter-rs/releases/download/models-ocrs-1/text-recognition.rten",
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
    files: &OCRS_FILES,
    legacy_dirs: ocrs_legacy_dirs,
    native_code: false,
};

#[cfg(any(feature = "local-ner", test))]
static NER_MULTILINGUAL_HRL: ModelManifest = ModelManifest {
    dir_name: "distilbert-base-multilingual-cased-ner-hrl",
    files: &NER_MULTILINGUAL_HRL_FILES,
    legacy_dirs: no_legacy_dirs,
    native_code: false,
};

#[cfg(any(feature = "local-gliner", test))]
static GLINER_MULTI_PII: ModelManifest = ModelManifest {
    dir_name: "gliner_multi_pii-v1",
    files: &GLINER_MULTI_PII_FILES,
    legacy_dirs: no_legacy_dirs,
    native_code: false,
};

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn ocrs_manifest_lists_both_rten_files_with_pinned_sizes() {
        let manifest = Addon::Ocr.manifest().unwrap();
        assert_eq!(manifest.dir_name, "ocrs");
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
                    "https://github.com/abdolence/redacter-rs/releases/download/models-ocrs-1/{}",
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
        let dirs = (Addon::Ocr.manifest().unwrap().legacy_dirs)();
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
        let manifest = Addon::LocalNer.manifest().unwrap();
        assert_eq!(
            manifest.dir_name,
            "distilbert-base-multilingual-cased-ner-hrl"
        );
        let names: Vec<&str> = manifest.files.iter().map(|f| f.name).collect();
        assert_eq!(names, ["model_uint8.onnx", "tokenizer.json", "config.json"]);
        let total: u64 = manifest.files.iter().map(|f| f.size).sum();
        assert_eq!(total, 138_036_031);
        assert!((manifest.legacy_dirs)().is_empty());
    }

    #[test]
    fn gliner_manifest_lists_the_three_pinned_files() {
        let manifest = Addon::LocalGliner.manifest().unwrap();
        assert_eq!(manifest.dir_name, "gliner_multi_pii-v1");
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
    fn gliner_urls_point_at_the_release_and_digests_are_hex() {
        let manifest = Addon::LocalGliner.manifest().unwrap();
        for file in manifest.files {
            assert!(
                file.url.starts_with(
                    "https://github.com/abdolence/redacter-rs/releases/download/models-gliner-multi-pii-1/"
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
    fn ner_urls_point_at_the_release_and_digests_are_hex() {
        let manifest = Addon::LocalNer.manifest().unwrap();
        for file in manifest.files {
            assert!(
                file.url.starts_with(
                    "https://github.com/abdolence/redacter-rs/releases/download/models-ner-hrl-1/"
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
    fn pdfium_manifest_pins_one_library_from_the_release_of_its_build() {
        let Some(manifest) = Addon::Pdf.manifest() else {
            return;
        };
        let build = manifest
            .dir_name
            .strip_prefix("pdfium-")
            .expect("the directory is named after the build");
        let [file] = manifest.files else {
            panic!("expected one library file, got {:?}", manifest.files);
        };
        assert_eq!(
            file.url
                .strip_prefix("https://github.com/abdolence/redacter-rs/releases/download/")
                .and_then(|rest| rest.split('/').next()),
            Some(format!("models-pdfium-{build}").as_str())
        );
        assert_eq!(file.sha256.len(), 64, "{}", file.name);
        assert!(file.sha256.chars().all(|c| c.is_ascii_hexdigit()));
        assert!((manifest.legacy_dirs)().is_empty());
    }
}
