use std::path::PathBuf;

/// An optional part of the tool that only `redacter addons install` downloads. Each variant
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
    /// The model of the local-credential-ml redacter.
    #[cfg(any(feature = "local-credential-ml", test))]
    LocalCredentialMl,
}

impl Addon {
    /// The files this addon installs, or `None` on a platform it has no build for.
    pub fn manifest(self) -> Option<&'static AddonManifest> {
        match self {
            #[cfg(any(feature = "ocr", test))]
            Addon::Ocr => Some(&OCRS),
            #[cfg(any(feature = "pdf-render", test))]
            Addon::Pdf => super::pdfium::PDFIUM.as_ref(),
            #[cfg(any(feature = "local-ner", test))]
            Addon::LocalNer => Some(&NER_MULTILINGUAL_HRL),
            #[cfg(any(feature = "local-gliner", test))]
            Addon::LocalGliner => Some(&GLINER_MULTI_PII),
            #[cfg(any(feature = "local-credential-ml", test))]
            Addon::LocalCredentialMl => Some(&CREDENTIAL_LINE),
        }
    }
}

/// The name `addons install` takes.
impl std::fmt::Display for Addon {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = clap::ValueEnum::to_possible_value(self)
            .expect("no addon is hidden from the command line");
        f.write_str(value.get_name())
    }
}

/// One file of an addon: where it comes from and what it must look like on disk.
#[derive(Debug)]
pub struct AddonFile {
    pub name: &'static str,
    pub url: &'static str,
    pub size: u64,
    pub sha256: &'static str,
}

/// Everything the store knows about the files of one addon, declared once.
#[derive(Debug)]
pub struct AddonManifest {
    /// Directory name under the addons root.
    pub dir_name: &'static str,
    pub files: &'static [AddonFile],
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
static NER_MULTILINGUAL_HRL_FILES: [AddonFile; 3] = [
    AddonFile {
        name: "model_uint8.onnx",
        url: ner_url!("model_uint8.onnx"),
        size: 135_115_742,
        sha256: "5b54455882ff2f242af18932b9da308d1f902f9f65d1d8f56ce9eb9bfcd5bf9e",
    },
    AddonFile {
        name: "tokenizer.json",
        url: ner_url!("tokenizer.json"),
        size: 2_919_362,
        sha256: "bf1b59b7b11c95f194f51708d918eea378e09d05f84c0e1656dc5180e8117088",
    },
    AddonFile {
        name: "config.json",
        url: ner_url!("config.json"),
        size: 927,
        sha256: "38847be4dc6699b1218a749ed69f888c2ccc7b4deba98e3c4a1cac8cb34d54c8",
    },
];

/// An addon that has never lived anywhere but the addons root.
#[cfg(any(
    feature = "local-ner",
    feature = "local-gliner",
    feature = "local-credential-ml",
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
static GLINER_MULTI_PII_FILES: [AddonFile; 3] = [
    AddonFile {
        name: "model.onnx",
        url: gliner_url!("model.onnx"),
        size: 1_157_129_714,
        sha256: "7704865e414f24591da6aee08716a25677855bc8a84af81396d90e40df1e68d2",
    },
    AddonFile {
        name: "tokenizer.json",
        url: gliner_url!("tokenizer.json"),
        size: 16_331_948,
        sha256: "914bd3c8fb7b525af9e23b60d0ec7b1248ddb2b99014efd9c02ebeb022f8cab7",
    },
    AddonFile {
        name: "gliner_config.json",
        url: gliner_url!("gliner_config.json"),
        size: 732,
        sha256: "69e141f7fe1864e0d81ab0e542c68387d588db62393bcd93b471d54dcf0f5c16",
    },
];

#[cfg(any(feature = "local-credential-ml", test))]
macro_rules! credential_line_url {
    ($path:literal) => {
        concat!(
            "https://github.com/abdolence/redacter-rs/releases/download/models-credential-line-1/",
            $path
        )
    };
}

#[cfg(any(feature = "local-credential-ml", test))]
static CREDENTIAL_LINE_FILES: [AddonFile; 4] = [
    AddonFile {
        name: "model.onnx",
        url: credential_line_url!("model.onnx"),
        size: 128_379_418,
        sha256: "bd08c443d1472304624ea0b16ef8e08685a9e8fbe34ddfb9a65cfd00b6b4c47e",
    },
    AddonFile {
        name: "tokenizer.json",
        url: credential_line_url!("tokenizer.json"),
        size: 3_583_485,
        sha256: "218484396a9d08293b108b0e5ea31e0a1b7c801dcbed35a821deb392d2fe9bb4",
    },
    AddonFile {
        name: "config.json",
        url: credential_line_url!("config.json"),
        size: 1_918,
        sha256: "1fb928ec87f399096fdd444914c15a6643c9a0c4a7a7fc17c1e20f8bd61f7ea6",
    },
    AddonFile {
        name: "policy.json",
        url: credential_line_url!("policy.json"),
        size: 754,
        sha256: "db74709c4c08dc5d20732ec34aea723194bfa347553e97e81240f896a8de9770",
    },
];

#[cfg(any(feature = "ocr", test))]
static OCRS_FILES: [AddonFile; 2] = [
    AddonFile {
        name: "text-detection.onnx",
        url: "https://github.com/abdolence/redacter-rs/releases/download/models-ocrs-2/text-detection.onnx",
        size: 2_499_479,
        sha256: "a917b23dbd9524b465df7e922641b3ff2981623df4ded5a0234004ef2fee7cfe",
    },
    AddonFile {
        name: "text-recognition.onnx",
        url: "https://github.com/abdolence/redacter-rs/releases/download/models-ocrs-2/text-recognition.onnx",
        size: 9_713_177,
        sha256: "86c145c2edb96c8caed5b1ebb8f44d706408922211451c309c625157dd6061c5",
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
static OCRS: AddonManifest = AddonManifest {
    dir_name: "ocrs-2",
    files: &OCRS_FILES,
    legacy_dirs: ocrs_legacy_dirs,
    native_code: false,
};

#[cfg(any(feature = "local-ner", test))]
static NER_MULTILINGUAL_HRL: AddonManifest = AddonManifest {
    dir_name: "distilbert-base-multilingual-cased-ner-hrl",
    files: &NER_MULTILINGUAL_HRL_FILES,
    legacy_dirs: no_legacy_dirs,
    native_code: false,
};

#[cfg(any(feature = "local-gliner", test))]
static GLINER_MULTI_PII: AddonManifest = AddonManifest {
    dir_name: "gliner_multi_pii-v1",
    files: &GLINER_MULTI_PII_FILES,
    legacy_dirs: no_legacy_dirs,
    native_code: false,
};

#[cfg(any(feature = "local-credential-ml", test))]
static CREDENTIAL_LINE: AddonManifest = AddonManifest {
    dir_name: "credential-line-32m-v2",
    files: &CREDENTIAL_LINE_FILES,
    legacy_dirs: no_legacy_dirs,
    native_code: false,
};

#[cfg(test)]
mod tests {
    use super::*;
    use clap::ValueEnum;
    use std::path::Path;

    const RELEASES: &str = "https://github.com/abdolence/redacter-rs/releases/download/";

    /// Every file is fetched from one release of this repository per addon and is pinned by
    /// a SHA-256 digest, whichever platform the table is compiled for.
    #[test]
    fn every_addon_pins_digests_of_files_hosted_in_one_release() {
        for &addon in Addon::value_variants() {
            let Some(manifest) = addon.manifest() else {
                continue;
            };
            let mut tags: Vec<&str> = manifest
                .files
                .iter()
                .map(|file| {
                    let (tag, _) = file
                        .url
                        .strip_prefix(RELEASES)
                        .and_then(|rest| rest.split_once('/'))
                        .unwrap_or_else(|| panic!("{addon}: {} is not a release asset", file.url));
                    tag
                })
                .collect();
            tags.dedup();
            assert_eq!(tags.len(), 1, "{addon}: {tags:?}");
            for file in manifest.files {
                assert_eq!(file.sha256.len(), 64, "{addon}: {}", file.name);
                assert!(
                    file.sha256.chars().all(|c| c.is_ascii_hexdigit()),
                    "{addon}: {}",
                    file.name
                );
            }
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

    /// The directory is named after the Pdfium build, and the models release that hosts the
    /// library after the same build, so replacing one without the other cannot go unnoticed.
    #[test]
    fn pdfium_release_tag_matches_the_directory_build() {
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
                .strip_prefix(RELEASES)
                .and_then(|rest| rest.split('/').next()),
            Some(format!("models-pdfium-{build}").as_str())
        );
    }
}
