SELECT directory, extension
FROM beamline
JOIN scan_directory
    ON beamline.id = scan_directory.id
WHERE name = ?;
