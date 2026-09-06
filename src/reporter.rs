use crate::AppResult;
use console::Term;
use indicatif::ProgressBar;

#[derive(Debug, Clone)]
pub struct AppReporter<'a> {
    inner: AppReporterInner<'a>,
}

impl<'a> AppReporter<'a> {
    /// Reports internal progress detail that is not part of the command's output: it
    /// goes to `tracing` (`RUST_LOG=debug`) instead of the terminal, so it never lands
    /// in the middle of the table a command is printing.
    pub fn report_debug<S>(&'a self, message: S)
    where
        S: AsRef<str>,
    {
        tracing::debug!("{}", message.as_ref());
    }

    pub fn report<S>(&'a self, message: S) -> AppResult<()>
    where
        S: AsRef<str>,
    {
        match &self.inner {
            AppReporterInner::Term(term) => Ok(term.write_line(message.as_ref())?),
            AppReporterInner::ProgressBar(progress_bar) => {
                progress_bar.println(message.as_ref());
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone)]
enum AppReporterInner<'a> {
    Term(&'a Term),
    ProgressBar(&'a ProgressBar),
}

impl<'a> From<&'a Term> for AppReporter<'a> {
    fn from(term: &'a Term) -> Self {
        AppReporter {
            inner: AppReporterInner::Term(term),
        }
    }
}

impl<'a> From<&'a ProgressBar> for AppReporter<'a> {
    fn from(progress_bar: &'a ProgressBar) -> Self {
        AppReporter {
            inner: AppReporterInner::ProgressBar(progress_bar),
        }
    }
}
