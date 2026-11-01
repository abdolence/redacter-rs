//! The Pdfium shared library as an addon: one build per supported platform, taken unchanged
//! from bblanchon/pdfium-binaries and re-hosted as release assets of this repository. The
//! pinned build is the one `pdfium-render` is compiled against; moving the `pdfium_*` feature
//! of that crate means publishing a new models release and replacing this table together. A
//! platform missing from the table, such as FreeBSD or a static musl build, has no download.

use super::manifest::{no_legacy_dirs, AddonFile, AddonManifest};

pub(super) static PDFIUM: Option<AddonManifest> = if LIBRARY.is_empty() {
    None
} else {
    Some(AddonManifest {
        dir_name: "pdfium-8125",
        files: LIBRARY,
        legacy_dirs: no_legacy_dirs,
        native_code: true,
    })
};

#[cfg(all(target_os = "linux", target_env = "gnu", target_arch = "x86_64"))]
const LIBRARY: &[AddonFile] = &[AddonFile {
    name: "libpdfium.so",
    url: "https://github.com/abdolence/redacter-rs/releases/download/models-pdfium-8125/libpdfium-linux-x64.so",
    size: 8_049_768,
    sha256: "1c251efce460d67437a4c76d4be9033ae2f17bf5603f5bff1af10209b94ca723",
}];

#[cfg(all(target_os = "linux", target_env = "gnu", target_arch = "aarch64"))]
const LIBRARY: &[AddonFile] = &[AddonFile {
    name: "libpdfium.so",
    url: "https://github.com/abdolence/redacter-rs/releases/download/models-pdfium-8125/libpdfium-linux-arm64.so",
    size: 8_127_624,
    sha256: "8cdc437dce84a6493b9ec6c4f5ad9ff2c299e505acfd7b0ad38cb440805b2c70",
}];

#[cfg(all(target_os = "macos", target_arch = "x86_64"))]
const LIBRARY: &[AddonFile] = &[AddonFile {
    name: "libpdfium.dylib",
    url: "https://github.com/abdolence/redacter-rs/releases/download/models-pdfium-8125/libpdfium-macos-x64.dylib",
    size: 7_809_288,
    sha256: "a9f428a16f46bab89646c8724b348e01c3fe707e3126d9356026cd876a74aecb",
}];

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
const LIBRARY: &[AddonFile] = &[AddonFile {
    name: "libpdfium.dylib",
    url: "https://github.com/abdolence/redacter-rs/releases/download/models-pdfium-8125/libpdfium-macos-arm64.dylib",
    size: 7_388_864,
    sha256: "a45c44645523968525c710462691dd974f4efb1013ce1c249919a0939cfb373a",
}];

#[cfg(all(target_os = "windows", target_arch = "x86_64"))]
const LIBRARY: &[AddonFile] = &[AddonFile {
    name: "pdfium.dll",
    url: "https://github.com/abdolence/redacter-rs/releases/download/models-pdfium-8125/pdfium-windows-x64.dll",
    size: 7_586_816,
    sha256: "4b0dec92555d69437bef0df37be630181ab3e83a6823ca264ede355a4f2ef994",
}];

#[cfg(all(target_os = "windows", target_arch = "aarch64"))]
const LIBRARY: &[AddonFile] = &[AddonFile {
    name: "pdfium.dll",
    url: "https://github.com/abdolence/redacter-rs/releases/download/models-pdfium-8125/pdfium-windows-arm64.dll",
    size: 6_898_176,
    sha256: "61b3500520d2f7ec2f8074851ba941b1470e85b8849e6efe5966ef2ce676c1f3",
}];

#[cfg(all(target_os = "windows", target_arch = "x86"))]
const LIBRARY: &[AddonFile] = &[AddonFile {
    name: "pdfium.dll",
    url: "https://github.com/abdolence/redacter-rs/releases/download/models-pdfium-8125/pdfium-windows-x86.dll",
    size: 7_011_328,
    sha256: "e90b1e3b598ff9a2aa5395dab4e4fea4fa97f0f36de0bc99dda8be3b5a0e6ca8",
}];

#[cfg(not(any(
    all(
        target_os = "linux",
        target_env = "gnu",
        any(target_arch = "x86_64", target_arch = "aarch64")
    ),
    all(
        target_os = "macos",
        any(target_arch = "x86_64", target_arch = "aarch64")
    ),
    all(
        target_os = "windows",
        any(target_arch = "x86_64", target_arch = "aarch64", target_arch = "x86")
    ),
)))]
const LIBRARY: &[AddonFile] = &[];
