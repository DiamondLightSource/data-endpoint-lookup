SELECT template
FROM beamline
JOIN visit_template ON visit_template.id = visit
WHERE beamline.name = ?
