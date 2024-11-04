CREATE TABLE beamline (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    scan_number INTEGER NOT NULL DEFAULT 1,
    visit INTEGER REFERENCES visit_template(id),
    scan INTEGER REFERENCES scan_template(id),
    detector INTEGER REFERENCES detector_template(id)
);

CREATE TABLE scan_directory (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    beamline INTEGER UNIQUE REFERENCES beamline(id),
    directory TEXT NOT NULL,
    extension TEXT NOT NULL,
    UNIQUE (directory, extension)
);

-- Templates for visit directories, scan files and detector files
CREATE TABLE visit_template (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    template TEXT UNIQUE NOT NULl
);
CREATE TABLE scan_template (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    template TEXT UNIQUE NOT NULl
);
CREATE TABLE detector_template (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    template TEXT UNIQUE NOT NULl
);
