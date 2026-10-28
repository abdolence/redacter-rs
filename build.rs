//! Sets `pdfium_download` on the targets that have a Pdfium build in the models release and
//! can load a shared library at run time. It is the one place that list lives for the cfg
//! checks in `src/model_store`; each target still needs its own entry in
//! `src/model_store/pdfium.rs`.

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rustc-check-cfg=cfg(pdfium_download)");

    let var = |name: &str| std::env::var(name).unwrap_or_default();
    let supported = matches!(
        (
            var("CARGO_CFG_TARGET_OS").as_str(),
            var("CARGO_CFG_TARGET_ENV").as_str(),
            var("CARGO_CFG_TARGET_ARCH").as_str(),
        ),
        ("linux", "gnu", "x86_64" | "aarch64")
            | ("macos", _, "x86_64" | "aarch64")
            | ("windows", _, "x86_64" | "aarch64" | "x86")
    );
    if supported {
        println!("cargo:rustc-cfg=pdfium_download");
    }
}
