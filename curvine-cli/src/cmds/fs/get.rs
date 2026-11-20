use super::common::copy_to;
use clap::Subcommand;
use curvine_core_error::CommonResult;
use curvine_fs_api::{CurvineURI, FileSystem, Reader};
use curvine_unified_fs::UnifiedFileSystem;
use std::path::{Path, PathBuf, MAIN_SEPARATOR};
use tokio::fs;

#[derive(Subcommand, Debug)]
pub enum GetCommand {
    /// Get file to local
    Get {
        #[clap(help = "Path of the file to get")]
        path: String,
        #[clap(help = "Local destination path")]
        local_path: PathBuf,
    },
}

/// Local file the download is written to. The remote file name is appended
/// when `local_path` is an existing directory or ends with a path separator,
/// and a bare file name is taken relative to `cwd`. The parent directory must
/// already exist. A symbolic link is followed, so that the file it points to is
/// replaced rather than the link. Nothing on the local side is changed here.
fn resolve_local_path(local_path: &Path, remote_name: &str, cwd: &Path) -> CommonResult<PathBuf> {
    let local = local_path.to_string_lossy();
    let mut dest = if local.ends_with(MAIN_SEPARATOR) || local_path.is_dir() {
        local_path.join(remote_name)
    } else if !local.contains(MAIN_SEPARATOR) {
        cwd.join(local_path)
    } else {
        local_path.to_path_buf()
    };

    if let Some(parent) = dest.parent() {
        if !parent.exists() {
            return Err(format!(
                "Parent directory '{}' does not exist. Please create it first.",
                parent.display()
            )
            .into());
        }
    }

    if dest.is_symlink() {
        // A dangling link has no target to resolve and is replaced as it is.
        if let Ok(target) = std::fs::canonicalize(&dest) {
            dest = target;
        }
    }

    if dest.is_dir() {
        return Err(format!("Cannot download to '{}': Is a directory", dest.display()).into());
    }

    Ok(dest)
}

/// Temporary file next to `dest` that a download is written to, so that the
/// final rename stays within one file system.
fn temp_path(dest: &Path) -> PathBuf {
    let name = dest
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    dest.with_file_name(format!(".{}.{}.cv-get.tmp", name, std::process::id()))
}

/// Copies `reader` to `dest` and returns the number of bytes copied. The data
/// goes to a temporary file that is renamed over `dest` only once the whole
/// file has been read and the read completed, so a failed download leaves an
/// existing `dest` and its contents untouched. A replaced file keeps its
/// permissions.
async fn download_to<R: Reader>(
    reader: &mut R,
    remote_path: &str,
    dest: &Path,
    chunk_size: usize,
) -> CommonResult<u64> {
    let tmp = temp_path(dest);
    let file = match fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp)
        .await
    {
        Ok(file) => file,
        Err(e) => {
            return Err(format!("Cannot create local file '{}': {}", tmp.display(), e).into());
        }
    };

    let result = write_and_replace(reader, remote_path, file, &tmp, dest, chunk_size).await;
    if result.is_err() {
        let _ = fs::remove_file(&tmp).await;
    }
    result
}

async fn write_and_replace<R: Reader>(
    reader: &mut R,
    remote_path: &str,
    mut file: fs::File,
    tmp: &Path,
    dest: &Path,
    chunk_size: usize,
) -> CommonResult<u64> {
    if let Ok(metadata) = fs::metadata(dest).await {
        file.set_permissions(metadata.permissions()).await?;
    }

    let bytes_read = match copy_to(reader, &mut file, chunk_size).await {
        Ok(bytes_read) => bytes_read,
        Err(e) => return Err(format!("Cannot download '{}': {}", remote_path, e).into()),
    };

    if let Err(e) = reader.complete().await {
        return Err(format!("Cannot complete read of '{}': {}", remote_path, e).into());
    }

    file.sync_all().await?;
    drop(file);

    if let Err(e) = fs::rename(tmp, dest).await {
        return Err(format!("Cannot move the download to '{}': {}", dest.display(), e).into());
    }

    Ok(bytes_read)
}

impl GetCommand {
    pub async fn execute(&self, client: UnifiedFileSystem) -> CommonResult<()> {
        match self {
            GetCommand::Get { path, local_path } => {
                let path = CurvineURI::new(path)?;

                // Check if the path exists and is a file
                let status = match client.get_status(&path).await {
                    Ok(status) => status,
                    Err(e) => {
                        return Err(format!("Cannot access '{}': {}", path.full_path(), e).into())
                    }
                };
                if status.is_dir {
                    return Err(
                        format!("Cannot download '{}': Is a directory", path.full_path()).into(),
                    );
                }

                let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
                let final_local_path = resolve_local_path(local_path, path.name(), &cwd)?;

                // Open the remote file before anything on the local side is created,
                // so that a failed open leaves the destination as it was.
                let mut reader = match client.open(&path).await {
                    Ok(reader) => reader,
                    Err(e) => {
                        return Err(format!(
                            "Cannot open remote file '{}': {}",
                            path.full_path(),
                            e
                        )
                        .into());
                    }
                };

                println!("Downloading to {}", final_local_path.display());

                let chunk_size = client.conf().client.read_chunk_size;
                let bytes_read =
                    download_to(&mut reader, path.full_path(), &final_local_path, chunk_size)
                        .await?;

                println!("Successfully downloaded {} bytes", bytes_read);
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{download_to, resolve_local_path, temp_path};
    use bytes::Bytes;
    use curvine_error::{FsError, FsResult};
    use curvine_fs_api::local::LocalReader;
    use curvine_fs_api::state::FileStatus;
    use curvine_fs_api::{Path as FsPath, Reader};
    use curvine_io::DataSlice;
    use std::fs;
    use std::path::{Path, PathBuf};
    use tempfile::TempDir;

    /// Serves `data` on the first read, then fails every later read while it
    /// still reports `len` bytes. Optionally fails `complete()` as well.
    struct FailingReader {
        path: FsPath,
        status: FileStatus,
        data: Option<Bytes>,
        len: i64,
        pos: i64,
        chunk: DataSlice,
        fail_complete: bool,
    }

    impl FailingReader {
        fn failing_read(data: &'static [u8], len: i64) -> Self {
            Self {
                path: FsPath::from_str("/data/keep.txt").unwrap(),
                status: FileStatus::default(),
                data: Some(Bytes::from_static(data)),
                len,
                pos: 0,
                chunk: DataSlice::Empty,
                fail_complete: false,
            }
        }

        fn failing_complete(data: &'static [u8]) -> Self {
            let mut reader = Self::failing_read(data, data.len() as i64);
            reader.fail_complete = true;
            reader
        }
    }

    impl Reader for FailingReader {
        fn status(&self) -> &FileStatus {
            &self.status
        }

        fn path(&self) -> &FsPath {
            &self.path
        }

        fn len(&self) -> i64 {
            self.len
        }

        fn chunk_mut(&mut self) -> &mut DataSlice {
            &mut self.chunk
        }

        fn chunk_size(&self) -> usize {
            self.len as usize
        }

        fn pos(&self) -> i64 {
            self.pos
        }

        fn pos_mut(&mut self) -> &mut i64 {
            &mut self.pos
        }

        async fn read_chunk0(&mut self) -> FsResult<DataSlice> {
            match self.data.take() {
                Some(data) => Ok(DataSlice::bytes(data)),
                None => Err(FsError::common("worker unavailable")),
            }
        }

        async fn seek(&mut self, _pos: i64) -> FsResult<()> {
            unimplemented!()
        }

        async fn complete(&mut self) -> FsResult<()> {
            if self.fail_complete {
                Err(FsError::common("complete failed"))
            } else {
                Ok(())
            }
        }
    }

    fn local_reader(dir: &TempDir, content: &[u8]) -> LocalReader {
        let file = dir.path().join("remote-source");
        fs::write(&file, content).unwrap();
        let path = FsPath::from_str(file.to_str().unwrap()).unwrap();
        LocalReader::new(&path, 4).unwrap()
    }

    /// Entries of `dir` other than the reader source used by the tests.
    fn entries(dir: &Path) -> Vec<PathBuf> {
        let mut entries: Vec<PathBuf> = fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| !path.ends_with("remote-source"))
            .collect();
        entries.sort();
        entries
    }

    #[test]
    fn resolves_an_absolute_file_path_as_is() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("out.txt");
        let resolved = resolve_local_path(&dest, "keep.txt", Path::new("/unused")).unwrap();
        assert_eq!(resolved, dest);
    }

    #[test]
    fn resolves_an_existing_directory_to_the_remote_file_name() {
        let dir = tempfile::tempdir().unwrap();
        let resolved = resolve_local_path(dir.path(), "keep.txt", Path::new("/unused")).unwrap();
        assert_eq!(resolved, dir.path().join("keep.txt"));
    }

    #[test]
    fn resolves_a_trailing_separator_to_the_remote_file_name() {
        let dir = tempfile::tempdir().unwrap();
        let local = PathBuf::from(format!(
            "{}{}",
            dir.path().display(),
            std::path::MAIN_SEPARATOR
        ));
        let resolved = resolve_local_path(&local, "keep.txt", Path::new("/unused")).unwrap();
        assert_eq!(resolved, dir.path().join("keep.txt"));
    }

    #[test]
    fn refuses_a_trailing_separator_for_a_missing_directory() {
        let dir = tempfile::tempdir().unwrap();
        let local = PathBuf::from(format!(
            "{}{}",
            dir.path().join("missing").display(),
            std::path::MAIN_SEPARATOR
        ));
        let err = resolve_local_path(&local, "keep.txt", Path::new("/unused")).unwrap_err();
        assert!(err.to_string().contains("does not exist"), "{}", err);
        assert!(!dir.path().join("missing").exists());
    }

    #[test]
    fn resolves_a_bare_file_name_against_the_current_directory() {
        let dir = tempfile::tempdir().unwrap();
        let resolved = resolve_local_path(Path::new("out.txt"), "keep.txt", dir.path()).unwrap();
        assert_eq!(resolved, dir.path().join("out.txt"));
    }

    #[test]
    fn refuses_a_missing_parent_directory() {
        let dir = tempfile::tempdir().unwrap();
        let local = dir.path().join("missing").join("out.txt");
        let err = resolve_local_path(&local, "keep.txt", Path::new("/unused")).unwrap_err();
        assert!(
            err.to_string().contains(&format!(
                "Parent directory '{}' does not exist",
                dir.path().join("missing").display()
            )),
            "{}",
            err
        );
    }

    #[test]
    fn refuses_a_destination_that_is_a_directory() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir(dir.path().join("keep.txt")).unwrap();
        let err = resolve_local_path(dir.path(), "keep.txt", Path::new("/unused")).unwrap_err();
        assert!(err.to_string().contains("Is a directory"), "{}", err);
    }

    #[cfg(unix)]
    #[test]
    fn resolves_a_symlink_to_its_target() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target.txt");
        fs::write(&target, b"old").unwrap();
        let link = dir.path().join("link.txt");
        std::os::unix::fs::symlink(&target, &link).unwrap();

        let resolved = resolve_local_path(&link, "keep.txt", Path::new("/unused")).unwrap();
        assert_eq!(resolved, fs::canonicalize(&target).unwrap());
    }

    #[test]
    fn temp_file_is_hidden_next_to_the_destination() {
        let tmp = temp_path(Path::new("/data/out/keep.txt"));
        assert_eq!(tmp.parent(), Some(Path::new("/data/out")));
        let name = tmp.file_name().unwrap().to_string_lossy().into_owned();
        assert!(name.starts_with(".keep.txt."), "{}", name);
        assert!(name.ends_with(".cv-get.tmp"), "{}", name);
    }

    #[tokio::test]
    async fn download_creates_a_new_file() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("keep.txt");
        let mut reader = local_reader(&dir, b"remote content");

        let copied = download_to(&mut reader, "/data/keep.txt", &dest, 4)
            .await
            .unwrap();
        assert_eq!(copied, 14);
        assert_eq!(fs::read(&dest).unwrap(), b"remote content");
        assert_eq!(entries(dir.path()), vec![dest]);
    }

    #[tokio::test]
    async fn successful_download_overwrites_an_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("keep.txt");
        fs::write(&dest, b"an older and longer local file").unwrap();
        let mut reader = local_reader(&dir, b"new");

        download_to(&mut reader, "/data/keep.txt", &dest, 4)
            .await
            .unwrap();
        assert_eq!(fs::read(&dest).unwrap(), b"new");
        assert_eq!(entries(dir.path()), vec![dest]);
    }

    #[tokio::test]
    async fn failed_read_keeps_the_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("keep.txt");
        fs::write(&dest, b"keep").unwrap();
        // Two bytes arrive, then the read fails with eight bytes to go.
        let mut reader = FailingReader::failing_read(b"ab", 10);

        let err = download_to(&mut reader, "/data/keep.txt", &dest, 4)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("Cannot download '/data/keep.txt'"),
            "{}",
            err
        );
        assert_eq!(fs::read(&dest).unwrap(), b"keep");
        assert_eq!(entries(dir.path()), vec![dest]);
    }

    #[tokio::test]
    async fn read_that_fails_at_once_keeps_the_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("keep.txt");
        fs::write(&dest, b"keep").unwrap();
        let mut reader = FailingReader::failing_read(b"", 10);
        reader.data = None;

        assert!(download_to(&mut reader, "/data/keep.txt", &dest, 4)
            .await
            .is_err());
        assert_eq!(fs::read(&dest).unwrap(), b"keep");
        assert_eq!(entries(dir.path()), vec![dest]);
    }

    #[tokio::test]
    async fn failed_complete_keeps_the_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("keep.txt");
        fs::write(&dest, b"keep").unwrap();
        let mut reader = FailingReader::failing_complete(b"remote content");

        let err = download_to(&mut reader, "/data/keep.txt", &dest, 4)
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("Cannot complete read of '/data/keep.txt'"),
            "{}",
            err
        );
        assert_eq!(fs::read(&dest).unwrap(), b"keep");
        assert_eq!(entries(dir.path()), vec![dest]);
    }

    #[tokio::test]
    async fn failed_download_creates_no_file() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("keep.txt");
        let mut reader = FailingReader::failing_read(b"ab", 10);

        assert!(download_to(&mut reader, "/data/keep.txt", &dest, 4)
            .await
            .is_err());
        assert!(entries(dir.path()).is_empty());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn overwrite_keeps_the_permissions_of_the_existing_file() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("keep.sh");
        fs::write(&dest, b"old").unwrap();
        fs::set_permissions(&dest, fs::Permissions::from_mode(0o750)).unwrap();
        let mut reader = local_reader(&dir, b"new");

        download_to(&mut reader, "/data/keep.sh", &dest, 4)
            .await
            .unwrap();
        let mode = fs::metadata(&dest).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o750);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn download_through_a_symlink_replaces_its_target() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target.txt");
        fs::write(&target, b"old").unwrap();
        let link = dir.path().join("link.txt");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        let mut reader = local_reader(&dir, b"new");

        let dest = resolve_local_path(&link, "keep.txt", Path::new("/unused")).unwrap();
        download_to(&mut reader, "/data/keep.txt", &dest, 4)
            .await
            .unwrap();
        assert!(link.is_symlink());
        assert_eq!(fs::read(&target).unwrap(), b"new");
    }
}
