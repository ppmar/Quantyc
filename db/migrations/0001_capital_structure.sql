-- Capital structure snapshots from Appendix 2A parsing

CREATE TABLE IF NOT EXISTS capital_structure_snapshots (
    id                         INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_id                     TEXT NOT NULL REFERENCES documents(id),
    ticker                     TEXT NOT NULL,
    snapshot_date              DATE NOT NULL,
    source_profile             TEXT NOT NULL,          -- "appendix_2a"
    shares_basic               INTEGER NOT NULL,
    shares_fd_naive            INTEGER NOT NULL,
    options_outstanding        INTEGER NOT NULL,
    convertible_notes_face     INTEGER NOT NULL,
    performance_rights_count   INTEGER NOT NULL,
    parser_version             TEXT NOT NULL,
    parsed_at                  TIMESTAMP NOT NULL,
    UNIQUE(doc_id)
);

CREATE TABLE IF NOT EXISTS unquoted_instruments (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_id        INTEGER NOT NULL REFERENCES capital_structure_snapshots(id)
                                           ON DELETE CASCADE,
    asx_code           TEXT NOT NULL,
    instrument_type    TEXT NOT NULL CHECK (instrument_type IN
                         ('option','convertible_note','performance_right','other')),
    description        TEXT NOT NULL,
    total_on_issue     INTEGER NOT NULL,
    expiry_date        DATE,
    strike_aud         NUMERIC
);

CREATE INDEX IF NOT EXISTS idx_css_ticker_date
    ON capital_structure_snapshots(ticker, snapshot_date DESC);

CREATE INDEX IF NOT EXISTS idx_ui_snapshot
    ON unquoted_instruments(snapshot_id);
