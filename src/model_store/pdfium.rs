//! The Pdfium shared library as a managed download: one build per supported platform, taken
//! unchanged from bblanchon/pdfium-binaries and re-hosted as release assets of this
//! repository. The pinned build is the one `pdfium-render` is compiled against; moving the
//! `pdfium_*` feature of that crate means publishing a new models release and replacing this
//! table together.

use super::manifest::{no_legacy_dirs, ModelFile, ModelManifest};

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
static PDFIUM_FILES: [ModelFile; 1] = [ModelFile {
    name: "libpdfium.so",
    url: "https://github.com/abdolence/redacter-rs/releases/download/models-pdfium-7881/libpdfium-linux-x64.so",
    size: 7_645_184,
    sha256: "f728930966f503652b92acc89b9374a2eeca00ce42e26dccd3e4b5c5161b2d64",
}];

#[cfg(all(target_os = "linux", target_arch = "aarch64"))]
static PDFIUM_FILES: [ModelFile; 1] = [ModelFile {
    name: "libpdfium.so",
    url: "https://github.com/abdolence/redacter-rs/releases/download/models-pdfium-7881/libpdfium-linux-arm64.so",
    size: 7_834_256,
    sha256: "6252fce3da45e7f0dc5b27f4d4e1a1456ca3f7734cdb04f927967df772127478",
}];

#[cfg(all(target_os = "macos", target_arch = "x86_64"))]
static PDFIUM_FILES: [ModelFile; 1] = [ModelFile {
    name: "libpdfium.dylib",
    url: "https://github.com/abdolence/redacter-rs/releases/download/models-pdfium-7881/libpdfium-macos-x64.dylib",
    size: 7_471_800,
    sha256: "4eaad6c3e8d786cf6f66a45d7d014edf5c65f372f98c3070e66595ebb50e43d9",
}];

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
static PDFIUM_FILES: [ModelFile; 1] = [ModelFile {
    name: "libpdfium.dylib",
    url: "https://github.com/abdolence/redacter-rs/releases/download/models-pdfium-7881/libpdfium-macos-arm64.dylib",
    size: 7_732_336,
    sha256: "1bc45b15466b34cef96641ce25c77a876e70010c6b114f909dda2f5325fc5bd7",
}];

#[cfg(all(target_os = "windows", target_arch = "x86_64"))]
static PDFIUM_FILES: [ModelFile; 1] = [ModelFile {
    name: "pdfium.dll",
    url: "https://github.com/abdolence/redacter-rs/releases/download/models-pdfium-7881/pdfium-windows-x64.dll",
    size: 7_211_520,
    sha256: "79d4676b656cfb1abcea88f9ade3b4b0826c5200382db5f4ec72a636c598c118",
}];

#[cfg(all(target_os = "windows", target_arch = "aarch64"))]
static PDFIUM_FILES: [ModelFile; 1] = [ModelFile {
    name: "pdfium.dll",
    url: "https://github.com/abdolence/redacter-rs/releases/download/models-pdfium-7881/pdfium-windows-arm64.dll",
    size: 6_658_048,
    sha256: "267a6f08a9c854d9949754a53b7630f23c0b67de5c7ca273b6abf178b49158c2",
}];

#[cfg(all(target_os = "windows", target_arch = "x86"))]
static PDFIUM_FILES: [ModelFile; 1] = [ModelFile {
    name: "pdfium.dll",
    url: "https://github.com/abdolence/redacter-rs/releases/download/models-pdfium-7881/pdfium-windows-x86.dll",
    size: 6_710_272,
    sha256: "6c6a1efa0079a01395ae88c01b1f976223ee4d4018c4e5c077903d5f768e95e4",
}];

pub(super) static PDFIUM: ModelManifest = ModelManifest {
    dir_name: "pdfium-7881",
    needed_by: "PDF rendering",
    source: "https://github.com/abdolence/redacter-rs/releases/tag/models-pdfium-7881 (Pdfium 151.0.7881.0 from bblanchon/pdfium-binaries chromium/7881)",
    license: "Pdfium and the libraries it bundles, under their own licences, see the release",
    files: &PDFIUM_FILES,
    legacy_dirs: no_legacy_dirs,
};
