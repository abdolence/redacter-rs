use crate::errors::AppError;
use crate::file_systems::{AbsoluteFilePath, FileSystemConnection, FileSystemRef, ListFilesResult};
use crate::file_tools::FileMatcher;
use crate::reporter::AppReporter;
use crate::AppResult;
use bytes::Bytes;
use futures::Stream;
use rvstruct::ValueStruct;

pub struct NoopFileSystem<'a> {
    reporter: &'a AppReporter<'a>,
}

impl<'a> NoopFileSystem<'a> {
    #[allow(dead_code)]
    pub fn new(reporter: &'a AppReporter<'a>) -> Self {
        Self { reporter }
    }
}

impl<'a> FileSystemConnection<'a> for NoopFileSystem<'a> {
    async fn download(
        &mut self,
        _file_ref: Option<&FileSystemRef>,
    ) -> AppResult<(
        FileSystemRef,
        Box<dyn Stream<Item = AppResult<Bytes>> + Send + Sync + Unpin + 'static>,
    )> {
        Err(AppError::SystemError {
            message: "NoopFileSystem does not support download".to_string(),
        })
    }

    async fn upload<S: Stream<Item = AppResult<Bytes>> + Send + Unpin + Sync + 'static>(
        &mut self,
        _input: S,
        _file_ref: Option<&FileSystemRef>,
    ) -> AppResult<()> {
        Err(AppError::SystemError {
            message: "NoopFileSystem does not support upload".to_string(),
        })
    }

    async fn list_files(
        &mut self,
        _file_matcher: Option<&FileMatcher>,
        _max_files_limit: Option<usize>,
    ) -> AppResult<ListFilesResult> {
        self.reporter
            .report("NoopFileSystem does not support list_files")?;
        Ok(ListFilesResult::EMPTY)
    }

    async fn close(self) -> AppResult<()> {
        Ok(())
    }

    async fn has_multiple_files(&self) -> AppResult<bool> {
        Ok(false)
    }

    async fn accepts_multiple_files(&self) -> AppResult<bool> {
        Ok(false)
    }

    fn resolve(&self, file_ref: Option<&FileSystemRef>) -> AbsoluteFilePath {
        AbsoluteFilePath {
            file_path: file_ref
                .map(|fr| fr.relative_path.value().clone())
                .unwrap_or("".to_string()),
        }
    }
}
