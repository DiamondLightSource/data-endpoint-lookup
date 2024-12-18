// Copyright 2024 Diamond Light Source
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::fmt::{self, Display};
use std::io::Error;
use std::path::{Path, PathBuf};

#[cfg(test)]
pub use tests::TempTracker;
use tokio::fs as async_fs;
use tokio::sync::{Mutex, MutexGuard};
use tracing::{instrument, trace};

/// Central controller to access external directory trackers. Prevents concurrent access to the same
/// beamline's directory.
pub struct NumTracker {
    bl_locks: HashMap<String, Mutex<PathBuf>>,
}

impl NumTracker {
    /// Build a numtracker than will provide locked access to subdirectories that exists and no-op
    /// trackers for beamlines that do not have subdirectories.
    pub fn for_root_directory<P: AsRef<Path>>(root: Option<P>) -> Result<Self, Error> {
        let mut bl_locks: HashMap<String, Mutex<PathBuf>> = Default::default();
        if let Some(dir) = root {
            for entry in dir.as_ref().read_dir()? {
                let dir = entry?;
                if dir.file_type()?.is_dir() {
                    if let Ok(name) = dir.file_name().into_string() {
                        bl_locks.insert(name, Mutex::new(dir.path()));
                    }
                }
            }
        }

        Ok(Self { bl_locks })
    }

    /// Create a wrapper around a subdirectory if one exists for the given beamline, or a no-op
    /// tracker if a directory does not exist.
    pub async fn for_beamline<'nt, 'bl>(
        &'nt self,
        bl: &'bl str,
        ext: Option<&'bl str>,
    ) -> Result<DirectoryTracker<'nt, 'bl>, InvalidExtension> {
        if !ext.is_none_or(Self::valid_extension) {
            return Err(InvalidExtension);
        }
        Ok(match self.bl_locks.get(bl) {
            Some(dir) => DirectoryTracker::GdaDirectory(GdaNumTracker {
                ext: ext.unwrap_or(bl),
                directory: dir.lock().await,
            }),
            None => DirectoryTracker::NoDirectory,
        })
    }

    fn valid_extension(name: &str) -> bool {
        name.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    }
}

/// Number tracker for a directory that may or may not exist
pub enum DirectoryTracker<'nt, 'bl> {
    NoDirectory,
    GdaDirectory(GdaNumTracker<'nt, 'bl>),
}

impl DirectoryTracker<'_, '_> {
    pub async fn prev(&self) -> Result<Option<u32>, Error> {
        match self {
            DirectoryTracker::NoDirectory => Ok(None),
            DirectoryTracker::GdaDirectory(gnt) => Some(gnt.latest_scan_number().await).transpose(),
        }
    }

    pub async fn set(&self, num: u32) -> Result<(), Error> {
        match self {
            DirectoryTracker::NoDirectory => Ok(()),
            DirectoryTracker::GdaDirectory(gnt) => gnt.create_num_file(num).await,
        }
    }
}

#[derive(Debug)]
pub struct GdaNumTracker<'nt, 'bl> {
    ext: &'bl str,
    directory: MutexGuard<'nt, PathBuf>,
}

impl GdaNumTracker<'_, '_> {
    /// Build the path of the file that would correspond to the given number
    fn file_name(&self, num: u32) -> PathBuf {
        self.directory
            .join(num.to_string())
            .with_extension(self.ext)
    }

    /// Create a file named for the given number and, if present, remove the file for the previous
    /// number.
    #[instrument]
    async fn create_num_file(&self, num: u32) -> Result<(), Error> {
        trace!("Creating new scan number file: {num}.{}", self.ext);
        let next = self.file_name(num);
        async_fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(next)
            .await?;
        if let Some(prev) = num.checked_sub(1) {
            let prev = self.file_name(prev);
            let _ = async_fs::remove_file(prev).await;
        }
        Ok(())
    }

    /// Read the number corresponding to the given file if it is a valid file name
    ///
    /// Does not check that the file is a child of the current tracker's directory.
    fn file_num(&self, file: &Path) -> Option<u32> {
        if self.ext != file.extension()?.to_str()? {
            return None;
        }
        match file.file_stem()?.to_str()?.parse() {
            Ok(val) => Some(val),
            Err(_) => None,
        }
    }

    /// Find the highest number that has a corresponding number file in this tracker's directory
    async fn latest_scan_number(&self) -> Result<u32, Error> {
        let mut high = 0;
        let mut dir = async_fs::read_dir(&*self.directory).await?;
        while let Some(file) = dir.next_entry().await? {
            if !file.file_type().await?.is_file() {
                continue;
            }
            if let Some(val) = self.file_num(&file.path()) {
                high = high.max(val);
            }
        }
        Ok(high)
    }
}

/// Error returned when an extension would result in directory traversal - eg '.foo/../../bar'
#[derive(Debug, Clone, Copy)]
pub struct InvalidExtension;

impl Display for InvalidExtension {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Extension is not valid")
    }
}

impl std::error::Error for InvalidExtension {}

#[cfg(test)]
mod tests {
    use std::ops::Deref;
    use std::path::Path;
    use std::time::Duration;
    use std::{fs, io};

    use rstest::{fixture, rstest};
    use tempfile::{tempdir, TempDir};
    use tokio::time::timeout;

    use super::{InvalidExtension, NumTracker};

    /// Wrapper around a NumTracker to ensure the tempdir is not dropped while it is still required
    pub struct TempTracker(pub NumTracker, pub TempDir);
    impl TempTracker {
        pub fn new<F>(init: F) -> Self
        where
            F: for<'f> FnOnce(&'f Path) -> io::Result<()>,
        {
            let root = tempdir().unwrap();
            init(root.as_ref()).unwrap();
            Self(NumTracker::for_root_directory(Some(&root)).unwrap(), root)
        }
    }
    impl Deref for TempTracker {
        type Target = NumTracker;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    #[rstest::fixture]
    fn root() -> TempDir {
        let root = tempdir().unwrap();

        fs::create_dir(root.as_ref().join("i22")).unwrap();
        fs::File::create(root.as_ref().join("i22").join("122.i22")).unwrap();

        fs::create_dir(root.as_ref().join("b21")).unwrap();

        root
    }

    #[fixture]
    fn nt(root: TempDir) -> TempTracker {
        TempTracker(NumTracker::for_root_directory(Some(&root)).unwrap(), root)
    }

    #[rstest]
    #[tokio::test[]]
    async fn exclusive_locking(nt: TempTracker) {
        let i22 = nt.for_beamline("i22", None).await;

        // difficult to test but this should be locked until i22 is dropped
        nt.bl_locks.get("i22").unwrap().try_lock().unwrap_err();
        nt.bl_locks.get("i22").unwrap().try_lock().unwrap_err();
        nt.bl_locks.get("i22").unwrap().try_lock().unwrap_err();

        drop(i22);
        // lock should now be free
        _ = nt.bl_locks.get("i22").unwrap().try_lock().unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn multiple_beamlines_not_exclusive(nt: TempTracker) {
        // trackers for different beamlines can be held concurrently
        let _i22 = nt.for_beamline("i22", None).await.unwrap();
        let _b21 = nt.for_beamline("b21", None).await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn unmanaged_beamlines_not_locked(nt: TempTracker) {
        let i11 = nt.for_beamline("i11", None);
        let i11_2 = nt.for_beamline("i11", None);
        let i11_3 = nt.for_beamline("i11", None);
        let i11_4 = nt.for_beamline("i11", None);

        // This should never get near 1s but in case something deadlocks we want to exit early. The
        // test will still fail successfully in this case.
        timeout(Duration::from_secs(1), async {
            i11.await.unwrap();
            i11_2.await.unwrap();
            i11_3.await.unwrap();
            i11_4.await.unwrap();
        })
        .await
        .expect("Timed out waiting for unmanaged trackers");
    }

    #[rstest]
    #[tokio::test]
    async fn unmanaged_beamline_has_no_numbers(nt: TempTracker) {
        let i11 = nt.for_beamline("i11", None).await.unwrap();
        if let Some(num) = i11.prev().await.unwrap() {
            panic!("Unmanaged beamline returned previous number: {num}");
        }
        // setting an unmanaged beamline is a no-op
        i11.set(111).await.unwrap();
        if let Some(num) = i11.prev().await.unwrap() {
            panic!("Unmanaged beamline returned previous number: {num}");
        }
    }

    #[rstest]
    #[tokio::test]
    async fn bump_numbers(nt: TempTracker) {
        let i22 = nt.for_beamline("i22", None).await.unwrap();
        assert_eq!(i22.prev().await.unwrap(), Some(122));
        i22.set(123).await.unwrap();
        assert_eq!(i22.prev().await.unwrap(), Some(123));
        assert!(
            !fs::exists(nt.1.as_ref().join("i22").join("122.i22")).unwrap(),
            "previous number file not deleted"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn non_consecutive_files_left(nt: TempTracker) {
        let i22 = nt.for_beamline("i22", None).await.unwrap();
        assert_eq!(i22.prev().await.unwrap(), Some(122));
        i22.set(244).await.unwrap();
        assert_eq!(i22.prev().await.unwrap(), Some(244));
        assert!(
            fs::exists(nt.1.as_ref().join("i22").join("122.i22")).unwrap(),
            "Non-consecutive previous file was removed"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn alternative_extensions(nt: TempTracker) {
        let i22 = nt.for_beamline("i22", None).await.unwrap(); // default i22 extension
        assert_eq!(i22.prev().await.unwrap(), Some(122));
        drop(i22);
        let i22 = nt.for_beamline("i22", Some("alt")).await.unwrap();
        assert_eq!(i22.prev().await.unwrap(), Some(0));
        i22.set(1234).await.unwrap();
        assert!(
            fs::exists(nt.1.as_ref().join("i22").join("122.i22")).unwrap(),
            "Existing extension file was removed"
        );
        assert!(
            fs::exists(nt.1.as_ref().join("i22").join("1234.alt")).unwrap(),
            "New alternative extension file was not created"
        );
        drop(i22);
    }

    #[rstest]
    #[tokio::test]
    async fn invalid_extensions(nt: TempTracker) {
        let Err(InvalidExtension) = nt.for_beamline("i22", Some("ext space")).await else {
            panic!("Invalid extension was accepted");
        };

        let Err(InvalidExtension) = nt.for_beamline("i22", Some("in:valid@chars")).await else {
            panic!("Invalid extension was accepted");
        };

        let Err(InvalidExtension) = nt.for_beamline("i22", Some("i22/../beamline")).await else {
            panic!("Invalid extension was accepted");
        };
        assert_eq!(InvalidExtension.to_string(), "Extension is not valid");
    }

    #[rstest]
    #[tokio::test]
    async fn non_number_files(nt: TempTracker) {
        fs::File::create(nt.1.as_ref().join("i22").join("string.i22")).unwrap();
        let i22 = nt.for_beamline("i22", None).await.unwrap();
        assert_eq!(i22.prev().await.unwrap(), Some(122));
    }
}
