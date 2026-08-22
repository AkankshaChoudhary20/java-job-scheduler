CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    run_at TEXT NOT NULL,              -- ISO-8601 instant, job is eligible once now >= run_at
    interval_seconds INTEGER,          -- NULL means one-shot job, otherwise reschedules on success
    status TEXT NOT NULL DEFAULT 'pending', -- pending | running | done | dead
    locked_by TEXT,                    -- worker id currently holding the claim
    locked_at TEXT,                    -- when the claim was taken, for stale-lock reclaim
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    last_error TEXT
);

CREATE INDEX IF NOT EXISTS idx_jobs_claimable ON jobs(status, run_at);
