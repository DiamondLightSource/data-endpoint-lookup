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

use std::error::Error;
use std::fmt::Display;
use std::path::Path;

use futures::TryStreamExt as _;

use crate::db_service::{BeamlineConfiguration, NumtrackerConfig, SqliteScanPathService};
use crate::numtracker::GdaNumTracker;

pub async fn list_info(db: &Path, beamline: Option<&str>) {
    let db = SqliteScanPathService::connect(db)
        .await
        .expect("DB not available");
    if let Some(bl) = beamline {
        match db.current_configuration(bl).await {
            Ok(conf) => list_bl_info(conf).await,
            Err(e) => eprintln!("{e}"),
        }
    } else {
        let mut all = db.all_beamlines();
        while let Ok(Some(conf)) = all.try_next().await {
            list_bl_info(conf).await;
        }
    }
}

fn bl_field<F: Display, E: Error>(field: &str, value: Result<F, E>) {
    match value {
        Ok(value) => println!("    {field}: {value}"),
        Err(e) => println!("    {field} not available: {e}"),
    }
}

async fn list_bl_info(conf: BeamlineConfiguration) {
    println!("{}", conf.name());
    bl_field("Visit", conf.visit());
    bl_field("Scan", conf.scan());
    bl_field("Detector", conf.detector());
    println!("    Scan number: {}", conf.scan_number());
    if let Some(NumtrackerConfig {
        directory,
        extension,
    }) = conf.fallback()
    {
        match GdaNumTracker::new(&directory, extension) {
            Ok(nt) => match nt.latest_scan_number().await {
                Ok(latest) => println!("    Numtracker file: {directory}/{latest}.{extension}"),
                Err(e) => println!("    Numtracker file unavailable: {e}"),
            },
            Err(e) => println!("Invalid directory confiuration: {e}"),
        }
    } else {
        println!("    No fallback directory configured");
    }
}
