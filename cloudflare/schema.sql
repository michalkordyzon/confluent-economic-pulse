CREATE TABLE IF NOT EXISTS latest_metrics (
  metric TEXT PRIMARY KEY,
  source TEXT NOT NULL,
  label TEXT NOT NULL,
  value REAL NOT NULL,
  unit TEXT NOT NULL,
  observed_at TEXT NOT NULL,
  collected_at TEXT NOT NULL,
  dimensions_json TEXT NOT NULL DEFAULT '{}',
  received_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_latest_metrics_source
ON latest_metrics(source);
