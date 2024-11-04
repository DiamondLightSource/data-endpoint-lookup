SELECT template
FROM beamline
JOIN detector_template ON detector_template.id = detector
WHERE name = ?
