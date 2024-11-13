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

use std::io::Error;
use std::path::{Path, PathBuf};

use tokio::fs as async_fs;
use tracing::{instrument, trace};

#[derive(Debug)]
pub struct GdaNumTracker<'e> {
    ext: &'e str,
    directory: &'e Path,
}

impl<'e> GdaNumTracker<'e> {
    /// Create a new num tracker for the given directory and extension
    pub fn new<P: AsRef<Path>>(directory: &'e P, ext: &'e str) -> Result<Self, Error> {
        let directory = directory.as_ref();
        if ext.chars().all(char::is_alphanumeric) {
            Ok(Self { ext, directory })
        } else {
            Err(Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("{ext:?} is not a valid extension"),
            ))
        }
    }

    /// Build the path of the file that would correspond to the given number
    fn file_name(&self, num: u32) -> PathBuf {
        self.directory
            .join(num.to_string())
            .with_extension(self.ext)
    }

    /// Create a file named for the given number and, if present, remove the file for the previous
    /// number.
    #[instrument]
    pub async fn create_num_file(&self, num: u32) -> Result<(), Error> {
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
    pub async fn latest_scan_number(&self) -> Result<u32, Error> {
        let mut high = 0;
        let mut dir = async_fs::read_dir(&self.directory).await?;
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
