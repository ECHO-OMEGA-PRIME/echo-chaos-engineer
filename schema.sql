-- Echo Chaos Engineer D1 Schema
-- Chaos engineering experiments and resilience testing

CREATE TABLE IF NOT EXISTS experiments (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  description TEXT,
  target_service TEXT NOT NULL,
  experiment_type TEXT NOT NULL CHECK(experiment_type IN ('latency_injection','error_injection','timeout','partial_failure','load_spike','dependency_kill')),
  config TEXT NOT NULL DEFAULT '{}',
  status TEXT NOT NULL DEFAULT 'draft' CHECK(status IN ('draft','scheduled','running','completed','aborted')),
  safety_config TEXT NOT NULL DEFAULT '{"max_duration":60000,"abort_threshold":0.5,"blast_radius":0.1}',
  scheduled_for TEXT,
  started_at TEXT,
  completed_at TEXT,
  created_by TEXT DEFAULT 'system',
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS experiment_runs (
  id TEXT PRIMARY KEY,
  experiment_id TEXT NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
  run_number INTEGER NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','running','completed','aborted','failed')),
  started_at TEXT,
  completed_at TEXT,
  metrics_before TEXT DEFAULT '{}',
  metrics_during TEXT DEFAULT '{}',
  metrics_after TEXT DEFAULT '{}',
  recovery_time_ms INTEGER,
  passed INTEGER DEFAULT 0,
  failure_reason TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS experiment_results (
  id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES experiment_runs(id) ON DELETE CASCADE,
  metric_name TEXT NOT NULL,
  baseline_value REAL,
  chaos_value REAL,
  recovery_value REAL,
  degradation_pct REAL,
  recovered INTEGER DEFAULT 0,
  timestamp TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS safety_stops (
  id TEXT PRIMARY KEY,
  experiment_id TEXT NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
  run_id TEXT REFERENCES experiment_runs(id) ON DELETE SET NULL,
  reason TEXT NOT NULL,
  triggered_at TEXT NOT NULL DEFAULT (datetime('now')),
  auto_triggered INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS resilience_scores (
  id TEXT PRIMARY KEY,
  service_name TEXT NOT NULL UNIQUE,
  score REAL NOT NULL DEFAULT 0,
  latency_resilience REAL DEFAULT 0,
  error_resilience REAL DEFAULT 0,
  timeout_resilience REAL DEFAULT 0,
  dependency_resilience REAL DEFAULT 0,
  last_tested TEXT,
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_experiments_status ON experiments(status);
CREATE INDEX IF NOT EXISTS idx_experiments_target ON experiments(target_service);
CREATE INDEX IF NOT EXISTS idx_experiments_type ON experiments(experiment_type);
CREATE INDEX IF NOT EXISTS idx_runs_experiment ON experiment_runs(experiment_id);
CREATE INDEX IF NOT EXISTS idx_runs_status ON experiment_runs(status);
CREATE INDEX IF NOT EXISTS idx_results_run ON experiment_results(run_id);
CREATE INDEX IF NOT EXISTS idx_safety_experiment ON safety_stops(experiment_id);
CREATE INDEX IF NOT EXISTS idx_resilience_service ON resilience_scores(service_name);
