use std::fmt::Debug;
use std::path::PathBuf;

use sqlx::{query_as, query_scalar, FromRow, Pool, Sqlite};

use crate::{
    paths, BeamlineContext, DetectorPath, ScanPathService, ScanRequest, ScanSpec, Subdirectory,
    Visit, VisitRequest,
};

#[derive(Clone)]
pub struct SqliteScanPathService {
    pub pool: Pool<Sqlite>,
}

#[derive(Debug, FromRow)]
struct ScanTemplates {
    visit: String,
    scan: String,
    detector: String,
}

impl Debug for SqliteScanPathService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteScanPathService")
            .field("db", &self.pool.connect_options().get_filename())
            .finish()
    }
}

impl SqliteScanPathService {
    async fn next_scan_number(&self, beamline: &str) -> Result<usize, sqlx::Error> {
        let mut db = self.pool.begin().await?;
        let next = query_scalar!(r#"
            UPDATE scan_number
            SET last_number = number + 1
            FROM (
                SELECT beamline.id AS bl_id, beamline.name AS name, scan_number.last_number AS number
                FROM scan_number
                    JOIN beamline ON scan_number.beamline = beamline.id
                WHERE beamline.name=?
            )
            WHERE beamline = bl_id
            RETURNING last_number
            "#,
            beamline
        ).fetch_one(&mut *db)
            .await? as usize;
        db.commit().await?;
        Ok(next)
    }

    async fn vist_template(&self, beamline: &str) -> Result<String, sqlx::Error> {
        query_scalar!(
            "SELECT template FROM beamline_visit_template WHERE beamline = ?",
            beamline
        )
        .fetch_one(&self.pool)
        .await
    }

    async fn scan_templates(&self, beamline: &str) -> Result<ScanTemplates, sqlx::Error> {
        query_as!(
            ScanTemplates,
            "SELECT visit, scan, detector FROM beamline_template WHERE beamline = ?",
            beamline
        )
        .fetch_one(&self.pool)
        .await
    }
}

impl ScanPathService for SqliteScanPathService {
    type Err = sqlx::Error;

    async fn visit_directory(&self, req: VisitRequest) -> Result<PathBuf, Self::Err> {
        let template = self.vist_template(&req.instrument).await?;
        // TODO: invalid visit in request
        let visit: Visit = req.visit.parse().unwrap();
        // TODO: invalid template in db
        let template = paths::visit_path(&template).unwrap();
        // TODO: check instrument here?
        Ok(template.render(&BeamlineContext::new(req.instrument, visit)))
    }

    async fn scan_spec(&self, req: ScanRequest) -> Result<ScanSpec, Self::Err> {
        let templates = self.scan_templates(&req.instrument).await?;
        // TODO: invalid visit in request
        let visit = req.visit.parse().unwrap();
        // TODO: invalid instrument in request
        let beamline = req.instrument.as_str().try_into().unwrap();
        let ctx = BeamlineContext::new(&req.instrument, visit);
        // TODO: invalid visit template in db
        let visit_directory = paths::visit_path(&templates.visit).unwrap().render(&ctx);
        let mut scan_ctx = ctx.for_scan(self.next_scan_number(&req.instrument).await?);
        if let Some(sub) = req.subdirectory {
            // TODO: invalid subdirectory in request
            scan_ctx = scan_ctx.with_subdirectory(Subdirectory::new(sub).unwrap());
        }
        // TODO: invalid scan template in db
        let scan = paths::scan_path(&templates.scan).unwrap().render(&scan_ctx);
        // TODO: invalid detector template in db
        let det_temp = paths::detector_path(&templates.detector).unwrap();
        let detectors = req
            .detectors
            .into_iter()
            .map(|det| {
                let file = det_temp.render(&scan_ctx.for_detector(&det));
                DetectorPath(det, file)
            })
            .collect();
        let spec = ScanSpec {
            beamline,
            visit: scan_ctx.beamline.visit.clone(),
            visit_directory,

            scan_number: scan_ctx.scan_number,
            scan_file: scan,
            detector_files: detectors,
        };

        Ok(spec)
    }
}
