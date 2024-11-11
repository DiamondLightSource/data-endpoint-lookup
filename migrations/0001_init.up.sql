CREATE TABLE beamline (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    -- Default to 0 so scan numbering starts at 1
    scan_number INTEGER NOT NULL DEFAULT 0 CHECK (scan_number >= 0),

    -- Directory and file templates
    visit TEXT NOT NULL CHECK (length(visit) > 0),
    scan TEXT NOT NULL CHECK (length(scan) > 0),
    detector TEXT NOT NULL CHECK (length(detector) > 0),

    fallback_directory TEXT,
    fallback_extension TEXT,

    -- Ensure fallback number files don't collide
    UNIQUE(fallback_directory, fallback_extension),

    -- Require a directory to be set if the extension is present
    CHECK (fallback_extension ISNULL OR fallback_directory NOTNULL)
);
