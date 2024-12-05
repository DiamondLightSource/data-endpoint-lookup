CREATE TABLE beamline (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL CHECK (length(name) > 0),
    -- Default to 0 so scan numbering starts at 1
    scan_number INTEGER NOT NULL DEFAULT 0 CHECK (scan_number >= 0),

    -- Directory and file templates
    visit TEXT NOT NULL CHECK (length(visit) > 0),
    scan TEXT NOT NULL CHECK (length(scan) > 0),
    detector TEXT NOT NULL CHECK (length(detector) > 0),

    -- Override file tracker extension - defaults to beamline name
    fallback_extension TEXT
);
