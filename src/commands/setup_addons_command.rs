use crate::errors::AppError;
use crate::model_store::{Addon, HttpFetcher, Installation, ModelStore, ModelStoreOptions};
use crate::AppResult;
use console::Term;

/// Installs each addon that is not already installed, one line per addon. Every addon is
/// tried even when an earlier one fails; the command fails when any of them did.
pub async fn command_setup_addons(
    term: &Term,
    addons: &[Addon],
    options: &ModelStoreOptions,
) -> AppResult<()> {
    let store = ModelStore::new(options);
    let fetcher = HttpFetcher::new()?;
    let mut failed = 0;
    for &addon in addons {
        let line = match store.install(addon, &fetcher).await {
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
