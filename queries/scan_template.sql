SELECT template
FROM beamline
JOIN scan_template ON scan_template.id = scan
WHERE name = ?
