use crate::addon_store::{
    Addon, AddonStatus, AddonStore, AddonStoreOptions, Fetcher, HttpFetcher, Installation,
};
use crate::errors::AppError;
use crate::AppResult;
use clap::ValueEnum;
use console::{pad_str, Alignment, Style, Term};
use indicatif::HumanBytes;

/// Installs each addon that is not already installed, one line per addon. Every addon is
/// tried even when an earlier one fails; the command fails when any of them did.
pub async fn command_addons_install(
    term: &Term,
    addons: &[Addon],
    options: &AddonStoreOptions,
) -> AppResult<()> {
    install_addons(
        term,
        addons,
        &AddonStore::new(options),
        &HttpFetcher::new()?,
    )
    .await
}

async fn install_addons(
    term: &Term,
    addons: &[Addon],
    store: &AddonStore,
    fetcher: &dyn Fetcher,
) -> AppResult<()> {
    let mut failed = 0;
    for &addon in addons {
        let line = match store.install(addon, fetcher).await {
            Ok(Installation::Found(dir)) => {
                format!("{addon}: already installed in {}", dir.display())
            }
            Ok(Installation::Downloaded(dir)) => {
                format!("{addon}: installed in {}", dir.display())
            }
            Err(err) => {
                failed += 1;
                format!("{addon}: {err}")
            }
        };
        term.write_line(&line)?;
    }
    if failed > 0 {
        return Err(AppError::AddonsNotInstalled {
            failed,
            requested: addons.len(),
        });
    }
    Ok(())
}

/// Lists the addons this build offers and where each stands. Looks only: nothing is
/// downloaded or written.
pub fn command_addons_ls(term: &Term, options: &AddonStoreOptions) -> AppResult<()> {
    let store = AddonStore::new(options);
    let dimmed = Style::new().dim();
    let highlighted = Style::new().bold().white();
    let rows = Addon::value_variants()
        .iter()
        .map(|&addon| Ok((addon, store.status(addon)?)))
        .collect::<AppResult<Vec<(Addon, AddonStatus)>>>()?;

    let header = |text: &str, width: usize| {
        dimmed.apply_to(pad_str(text, width, Alignment::Left, None).into_owned())
    };
    term.write_line(&format!(
        "\n  {} {} {} {}",
        header("Addon", ADDON_WIDTH),
        header("State", STATE_WIDTH),
        header("Size", SIZE_WIDTH),
        header("Location", 0),
    ))?;
    for (addon, status) in &rows {
        let size = addon
            .manifest()
            .map(|manifest| {
                HumanBytes(manifest.files.iter().map(|file| file.size).sum()).to_string()
            })
            .unwrap_or_default();
        let location = status
            .location()
            .map(|path| path.display().to_string())
            .unwrap_or_default();
        term.write_line(&format!(
            "- {} {} {} {}",
            highlighted.apply_to(pad_str(
                &addon.to_string(),
                ADDON_WIDTH,
                Alignment::Left,
                None
            )),
            pad_str(&status.to_string(), STATE_WIDTH, Alignment::Left, None),
            pad_str(&size, SIZE_WIDTH, Alignment::Left, None),
            location,
        ))?;
    }
    Ok(())
}

const ADDON_WIDTH: usize = 14;
const STATE_WIDTH: usize = 32;
const SIZE_WIDTH: usize = 12;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::addon_store::{AddonStoreError, ByteStream};
    use futures::future::BoxFuture;
    use tempfile::TempDir;

    struct UnreachableServer;

    impl Fetcher for UnreachableServer {
        fn fetch<'s>(&'s self, url: &'s str) -> BoxFuture<'s, Result<ByteStream, AddonStoreError>> {
            Box::pin(async move {
                Err(AddonStoreError::HttpStatus {
                    url: url.to_string(),
                    status: 503,
                })
            })
        }
    }

    #[tokio::test]
    async fn install_tries_every_addon_and_fails_when_any_failed() {
        let root = TempDir::new().unwrap();
        let store = AddonStore::new(&AddonStoreOptions {
            addons_dir: Some(root.path().to_path_buf()),
        });
        let err = install_addons(
            &Term::stdout(),
            &[Addon::Ocr, Addon::LocalGliner],
            &store,
            &UnreachableServer,
        )
        .await
        .unwrap_err();
        assert!(
            matches!(
                err,
                AppError::AddonsNotInstalled {
                    failed: 2,
                    requested: 2
                }
            ),
            "{err}"
        );
    }
}
