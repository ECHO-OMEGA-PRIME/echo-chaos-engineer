import { Hono } from 'hono';
import { cors } from 'hono/cors';

// ─── Types ───────────────────────────────────────────────────────────────────

interface Env {
  DB: D1Database;
  CACHE: KVNamespace;
  SHARED_BRAIN: Fetcher;
  ALERT_ROUTER: Fetcher;
  CIRCUIT_BREAKER: Fetcher;
}

type ExperimentType = 'latency_injection' | 'error_injection' | 'timeout' | 'partial_failure' | 'load_spike' | 'dependency_kill';
type ExperimentStatus = 'draft' | 'scheduled' | 'running' | 'completed' | 'aborted';
type RunStatus = 'pending' | 'running' | 'completed' | 'aborted' | 'failed';

interface SafetyConfig {
  max_duration: number;
  abort_threshold: number;
  blast_radius: number;
}

interface ExperimentConfig {
  latency_ms?: number;
  error_rate?: number;
  error_code?: number;
  timeout_ms?: number;
  failure_percentage?: number;
  load_multiplier?: number;
  target_dependency?: string;
  duration_ms?: number;
  ramp_up_ms?: number;
}

interface Experiment {
  id: string;
  name: string;
  description: string | null;
  target_service: string;
  experiment_type: ExperimentType;
  config: string;
  status: ExperimentStatus;
  safety_config: string;
  scheduled_for: string | null;
  started_at: string | null;
  completed_at: string | null;
  created_by: string;
  created_at: string;
}

interface ExperimentRun {
  id: string;
  experiment_id: string;
  run_number: number;
  status: RunStatus;
  started_at: string | null;
  completed_at: string | null;
  metrics_before: string;
  metrics_during: string;
  metrics_after: string;
  recovery_time_ms: number | null;
  passed: number;
  failure_reason: string | null;
  created_at: string;
}

interface ExperimentResult {
  id: string;
  run_id: string;
  metric_name: string;
  baseline_value: number | null;
  chaos_value: number | null;
  recovery_value: number | null;
  degradation_pct: number | null;
  recovered: number;
  timestamp: string;
}

interface SafetyStop {
  id: string;
  experiment_id: string;
  run_id: string | null;
  reason: string;
  triggered_at: string;
  auto_triggered: number;
}

interface ResilienceScore {
  id: string;
  service_name: string;
  score: number;
  latency_resilience: number;
  error_resilience: number;
  timeout_resilience: number;
  dependency_resilience: number;
  last_tested: string | null;
  updated_at: string;
}

interface GameDayRequest {
  name: string;
  services: string[];
  experiment_types: ExperimentType[];
  safety_config?: Partial<SafetyConfig>;
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

const VERSION = '1.0.0';
const startTime = Date.now();

function uid(): string {
  const ts = Date.now().toString(36);
  const rand = Math.random().toString(36).substring(2, 10);
  return `${ts}-${rand}`;
}

function now(): string {
  return new Date().toISOString();
}

function log(level: string, message: string, data?: Record<string, unknown>): void {
  const entry = {
    timestamp: now(),
    level,
    service: 'echo-chaos-engineer',
    version: VERSION,
    message,
    ...data,
  };
  if (level === 'error') {
    console.error(JSON.stringify(entry));
  } else {
    console.log(JSON.stringify(entry));
  }
}

function jsonOk(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'Content-Type': 'application/json' },
  });
}

function jsonErr(message: string, status = 400): Response {
  log('error', message, { status });
  return new Response(JSON.stringify({ error: message, status }), {
    status,
    headers: { 'Content-Type': 'application/json' },
  });
}

const VALID_TYPES: ExperimentType[] = ['latency_injection', 'error_injection', 'timeout', 'partial_failure', 'load_spike', 'dependency_kill'];

function isValidType(t: string): t is ExperimentType {
  return VALID_TYPES.includes(t as ExperimentType);
}

const DEFAULT_SAFETY: SafetyConfig = {
  max_duration: 60000,
  abort_threshold: 0.5,
  blast_radius: 0.1,
};

function parseSafety(raw: unknown): SafetyConfig {
  if (!raw || typeof raw !== 'object') return { ...DEFAULT_SAFETY };
  const obj = raw as Record<string, unknown>;
  return {
    max_duration: typeof obj['max_duration'] === 'number' ? obj['max_duration'] : DEFAULT_SAFETY.max_duration,
    abort_threshold: typeof obj['abort_threshold'] === 'number' ? obj['abort_threshold'] : DEFAULT_SAFETY.abort_threshold,
    blast_radius: typeof obj['blast_radius'] === 'number' ? obj['blast_radius'] : DEFAULT_SAFETY.blast_radius,
  };
}

// ─── Auth Middleware ─────────────────────────────────────────────────────────

function authMiddleware(apiKey: string | null | undefined, commanderOverride: string | null | undefined): Response | null {
  if (apiKey !== 'echo-omega-prime-forge-x-2026') {
    return jsonErr('Unauthorized: invalid or missing X-Echo-API-Key', 401);
  }
  return null;
}

function hasCommanderOverride(headers: Headers): boolean {
  return headers.get('X-Commander-Override') === 'true';
}

// ─── Experiment Execution Engine ─────────────────────────────────────────────

async function collectBaselineMetrics(env: Env, targetService: string): Promise<Record<string, number>> {
  const metrics: Record<string, number> = {};
  const startMs = Date.now();

  try {
    // Attempt to hit the target service health endpoint to get baseline latency
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 5000);
    const resp = await fetch(`https://${targetService}.bmcii1976.workers.dev/health`, {
      signal: controller.signal,
    });
    clearTimeout(timeoutId);
    metrics['health_latency_ms'] = Date.now() - startMs;
    metrics['health_status'] = resp.status;
    metrics['health_ok'] = resp.ok ? 1 : 0;
  } catch {
    metrics['health_latency_ms'] = Date.now() - startMs;
    metrics['health_status'] = 0;
    metrics['health_ok'] = 0;
  }

  try {
    const statsStart = Date.now();
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 5000);
    const resp = await fetch(`https://${targetService}.bmcii1976.workers.dev/stats`, {
      signal: controller.signal,
    });
    clearTimeout(timeoutId);
    metrics['stats_latency_ms'] = Date.now() - statsStart;
    if (resp.ok) {
      const data = await resp.json() as Record<string, unknown>;
      if (typeof data['error_rate'] === 'number') metrics['error_rate'] = data['error_rate'];
      if (typeof data['avg_latency_ms'] === 'number') metrics['avg_latency_ms'] = data['avg_latency_ms'];
      if (typeof data['request_count'] === 'number') metrics['request_count'] = data['request_count'];
    }
  } catch {
    metrics['stats_latency_ms'] = -1;
  }

  return metrics;
}

async function injectChaos(env: Env, experiment: Experiment, config: ExperimentConfig, safety: SafetyConfig): Promise<{
  metrics: Record<string, number>;
  aborted: boolean;
  abortReason?: string;
}> {
  const metrics: Record<string, number> = {};
  let aborted = false;
  let abortReason: string | undefined;

  const duration = config.duration_ms ?? safety.max_duration;
  const startMs = Date.now();
  const targetUrl = `https://${experiment.target_service}.bmcii1976.workers.dev`;
  let totalRequests = 0;
  let errorCount = 0;
  let totalLatency = 0;

  // Run chaos loop: send requests to target and track behavior
  const iterationLimit = Math.min(Math.ceil(duration / 200), 300); // Cap iterations
  for (let i = 0; i < iterationLimit; i++) {
    if (Date.now() - startMs > duration) break;

    try {
      const reqStart = Date.now();
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), Math.min(config.timeout_ms ?? 10000, 10000));

      // Apply experiment-specific chaos headers
      const headers: Record<string, string> = {
        'X-Chaos-Experiment': experiment.id,
        'X-Chaos-Type': experiment.experiment_type,
      };

      if (experiment.experiment_type === 'latency_injection' && config.latency_ms) {
        headers['X-Chaos-Latency'] = String(config.latency_ms);
      }
      if (experiment.experiment_type === 'error_injection' && config.error_rate) {
        headers['X-Chaos-Error-Rate'] = String(config.error_rate);
        headers['X-Chaos-Error-Code'] = String(config.error_code ?? 500);
      }
      if (experiment.experiment_type === 'partial_failure' && config.failure_percentage) {
        headers['X-Chaos-Failure-Pct'] = String(config.failure_percentage);
      }
      if (experiment.experiment_type === 'load_spike' && config.load_multiplier) {
        headers['X-Chaos-Load'] = String(config.load_multiplier);
      }
      if (experiment.experiment_type === 'dependency_kill' && config.target_dependency) {
        headers['X-Chaos-Kill-Dep'] = config.target_dependency;
      }

      const resp = await fetch(`${targetUrl}/health`, {
        headers,
        signal: controller.signal,
      });
      clearTimeout(timeoutId);

      const latency = Date.now() - reqStart;
      totalLatency += latency;
      totalRequests++;

      if (!resp.ok) {
        errorCount++;
      }
    } catch {
      totalRequests++;
      errorCount++;
    }

    // Check abort threshold
    if (totalRequests > 5) {
      const currentErrorRate = errorCount / totalRequests;
      if (currentErrorRate > safety.abort_threshold) {
        aborted = true;
        abortReason = `Error rate ${(currentErrorRate * 100).toFixed(1)}% exceeded abort threshold ${(safety.abort_threshold * 100).toFixed(1)}%`;
        log('warn', 'Safety abort triggered', { experimentId: experiment.id, currentErrorRate, threshold: safety.abort_threshold });
        break;
      }
    }

    // Small delay between requests
    await new Promise(r => setTimeout(r, 200));
  }

  metrics['total_requests'] = totalRequests;
  metrics['error_count'] = errorCount;
  metrics['error_rate'] = totalRequests > 0 ? errorCount / totalRequests : 0;
  metrics['avg_latency_ms'] = totalRequests > 0 ? totalLatency / totalRequests : 0;
  metrics['duration_ms'] = Date.now() - startMs;
  metrics['chaos_type_code'] = VALID_TYPES.indexOf(experiment.experiment_type);

  return { metrics, aborted, abortReason };
}

async function collectRecoveryMetrics(env: Env, targetService: string, maxWaitMs: number = 30000): Promise<{
  metrics: Record<string, number>;
  recoveryTimeMs: number;
}> {
  const metrics: Record<string, number> = {};
  const recoveryStart = Date.now();
  let recovered = false;
  let recoveryTimeMs = maxWaitMs;

  // Poll until service is healthy or timeout
  const pollInterval = 1000;
  const maxPolls = Math.ceil(maxWaitMs / pollInterval);
  for (let i = 0; i < maxPolls; i++) {
    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 5000);
      const resp = await fetch(`https://${targetService}.bmcii1976.workers.dev/health`, {
        signal: controller.signal,
      });
      clearTimeout(timeoutId);

      if (resp.ok) {
        recovered = true;
        recoveryTimeMs = Date.now() - recoveryStart;
        metrics['recovery_health_status'] = resp.status;
        break;
      }
    } catch {
      // Service still recovering
    }
    await new Promise(r => setTimeout(r, pollInterval));
  }

  metrics['recovered'] = recovered ? 1 : 0;
  metrics['recovery_time_ms'] = recoveryTimeMs;

  if (!recovered) {
    metrics['recovery_health_status'] = 0;
  }

  // Collect post-recovery stats
  try {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 5000);
    const resp = await fetch(`https://${targetService}.bmcii1976.workers.dev/stats`, {
      signal: controller.signal,
    });
    clearTimeout(timeoutId);
    if (resp.ok) {
      const data = await resp.json() as Record<string, unknown>;
      if (typeof data['error_rate'] === 'number') metrics['post_error_rate'] = data['error_rate'];
      if (typeof data['avg_latency_ms'] === 'number') metrics['post_avg_latency_ms'] = data['avg_latency_ms'];
    }
  } catch {
    // Stats not available
  }

  return { metrics, recoveryTimeMs };
}

async function executeExperiment(env: Env, experiment: Experiment): Promise<{
  runId: string;
  passed: boolean;
  recoveryTimeMs: number;
  failureReason?: string;
}> {
  const config: ExperimentConfig = JSON.parse(experiment.config || '{}');
  const safety: SafetyConfig = parseSafety(JSON.parse(experiment.safety_config || '{}'));

  // Get next run number
  const lastRun = await env.DB.prepare(
    'SELECT MAX(run_number) as max_run FROM experiment_runs WHERE experiment_id = ?'
  ).bind(experiment.id).first<{ max_run: number | null }>();
  const runNumber = (lastRun?.max_run ?? 0) + 1;

  const runId = uid();
  const startedAt = now();

  // Create run record
  await env.DB.prepare(
    'INSERT INTO experiment_runs (id, experiment_id, run_number, status, started_at) VALUES (?, ?, ?, ?, ?)'
  ).bind(runId, experiment.id, runNumber, 'running', startedAt).run();

  // Update experiment to running
  await env.DB.prepare(
    'UPDATE experiments SET status = ?, started_at = ? WHERE id = ?'
  ).bind('running', startedAt, experiment.id).run();

  log('info', 'Experiment run started', { experimentId: experiment.id, runId, runNumber, type: experiment.experiment_type, target: experiment.target_service });

  // Phase 1: Collect baseline
  const metricsBefore = await collectBaselineMetrics(env, experiment.target_service);
  await env.DB.prepare(
    'UPDATE experiment_runs SET metrics_before = ? WHERE id = ?'
  ).bind(JSON.stringify(metricsBefore), runId).run();

  // Phase 2: Inject chaos
  const chaosResult = await injectChaos(env, experiment, config, safety);
  await env.DB.prepare(
    'UPDATE experiment_runs SET metrics_during = ? WHERE id = ?'
  ).bind(JSON.stringify(chaosResult.metrics), runId).run();

  // If aborted, record safety stop
  if (chaosResult.aborted) {
    const safetyId = uid();
    await env.DB.prepare(
      'INSERT INTO safety_stops (id, experiment_id, run_id, reason, auto_triggered) VALUES (?, ?, ?, ?, 1)'
    ).bind(safetyId, experiment.id, runId, chaosResult.abortReason ?? 'Unknown', ).run();
  }

  // Phase 3: Collect recovery metrics
  const recoveryResult = await collectRecoveryMetrics(env, experiment.target_service);
  await env.DB.prepare(
    'UPDATE experiment_runs SET metrics_after = ? WHERE id = ?'
  ).bind(JSON.stringify(recoveryResult.metrics), runId).run();

  // Compute results per metric
  const metricNames = ['health_latency_ms', 'error_rate', 'avg_latency_ms'];
  for (const metricName of metricNames) {
    const baseline = metricsBefore[metricName] ?? null;
    const chaos = chaosResult.metrics[metricName] ?? null;
    const recovery = recoveryResult.metrics[`post_${metricName}`] ?? recoveryResult.metrics[metricName] ?? null;

    let degradation: number | null = null;
    if (baseline !== null && chaos !== null && baseline > 0) {
      degradation = ((chaos - baseline) / baseline) * 100;
    }

    const metricRecovered = recovery !== null && baseline !== null ?
      (Math.abs((recovery as number) - (baseline as number)) / Math.max(baseline as number, 0.001) < 0.5 ? 1 : 0) : 0;

    const resultId = uid();
    await env.DB.prepare(
      'INSERT INTO experiment_results (id, run_id, metric_name, baseline_value, chaos_value, recovery_value, degradation_pct, recovered) VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
    ).bind(resultId, runId, metricName, baseline, chaos, recovery, degradation, metricRecovered).run();
  }

  // Determine pass/fail
  const passed = !chaosResult.aborted && recoveryResult.metrics['recovered'] === 1;
  const failureReason = chaosResult.aborted
    ? chaosResult.abortReason
    : recoveryResult.metrics['recovered'] !== 1
      ? 'Service did not recover within timeout'
      : undefined;

  const completedAt = now();
  const finalStatus = chaosResult.aborted ? 'aborted' : 'completed';

  // Update run
  await env.DB.prepare(
    'UPDATE experiment_runs SET status = ?, completed_at = ?, recovery_time_ms = ?, passed = ?, failure_reason = ? WHERE id = ?'
  ).bind(finalStatus, completedAt, recoveryResult.recoveryTimeMs, passed ? 1 : 0, failureReason ?? null, runId).run();

  // Update experiment
  await env.DB.prepare(
    'UPDATE experiments SET status = ?, completed_at = ? WHERE id = ?'
  ).bind(finalStatus === 'aborted' ? 'aborted' : 'completed', completedAt, experiment.id).run();

  log('info', 'Experiment run completed', {
    experimentId: experiment.id,
    runId,
    passed,
    recoveryTimeMs: recoveryResult.recoveryTimeMs,
    status: finalStatus,
  });

  // Update resilience scores
  await updateResilienceScore(env, experiment.target_service);

  return { runId, passed, recoveryTimeMs: recoveryResult.recoveryTimeMs, failureReason };
}

async function updateResilienceScore(env: Env, serviceName: string): Promise<void> {
  // Get all completed runs for this service
  const runs = await env.DB.prepare(`
    SELECT er.experiment_type, xr.passed, xr.recovery_time_ms
    FROM experiment_runs xr
    JOIN experiments er ON xr.experiment_id = er.id
    WHERE er.target_service = ? AND xr.status IN ('completed', 'aborted')
    ORDER BY xr.completed_at DESC
    LIMIT 50
  `).bind(serviceName).all<{ experiment_type: ExperimentType; passed: number; recovery_time_ms: number | null }>();

  if (!runs.results || runs.results.length === 0) return;

  let latencyRuns = 0, latencyPass = 0;
  let errorRuns = 0, errorPass = 0;
  let timeoutRuns = 0, timeoutPass = 0;
  let depRuns = 0, depPass = 0;

  for (const run of runs.results) {
    const p = run.passed === 1;
    switch (run.experiment_type) {
      case 'latency_injection':
        latencyRuns++; if (p) latencyPass++;
        break;
      case 'error_injection':
      case 'partial_failure':
        errorRuns++; if (p) errorPass++;
        break;
      case 'timeout':
      case 'load_spike':
        timeoutRuns++; if (p) timeoutPass++;
        break;
      case 'dependency_kill':
        depRuns++; if (p) depPass++;
        break;
    }
  }

  const latencyResilience = latencyRuns > 0 ? (latencyPass / latencyRuns) * 100 : 0;
  const errorResilience = errorRuns > 0 ? (errorPass / errorRuns) * 100 : 0;
  const timeoutResilience = timeoutRuns > 0 ? (timeoutPass / timeoutRuns) * 100 : 0;
  const depResilience = depRuns > 0 ? (depPass / depRuns) * 100 : 0;

  const totalRuns = runs.results.length;
  const totalPass = runs.results.filter(r => r.passed === 1).length;
  const overallScore = totalRuns > 0 ? (totalPass / totalRuns) * 100 : 0;

  const scoreId = uid();
  await env.DB.prepare(`
    INSERT INTO resilience_scores (id, service_name, score, latency_resilience, error_resilience, timeout_resilience, dependency_resilience, last_tested, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(service_name) DO UPDATE SET
      score = excluded.score,
      latency_resilience = excluded.latency_resilience,
      error_resilience = excluded.error_resilience,
      timeout_resilience = excluded.timeout_resilience,
      dependency_resilience = excluded.dependency_resilience,
      last_tested = excluded.last_tested,
      updated_at = excluded.updated_at
  `).bind(scoreId, serviceName, overallScore, latencyResilience, errorResilience, timeoutResilience, depResilience, now(), now()).run();
}

// ─── App ─────────────────────────────────────────────────────────────────────

const app = new Hono<{ Bindings: Env }>();

// CORS
app.use('*', cors({
  origin: '*',
  allowHeaders: ['Content-Type', 'X-Echo-API-Key', 'X-Commander-Override', 'Authorization'],
  allowMethods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
}));

// Auth on all non-health routes
app.use('*', async (c, next) => {
  if (c.req.path === '/health' || c.req.method === 'OPTIONS') {
    return next();
  }
  const err = authMiddleware(c.req.header('X-Echo-API-Key'), c.req.header('X-Commander-Override'));
  if (err) return err;
  return next();
});

// ─── 1. GET /health ──────────────────────────────────────────────────────────

app.get('/health', async (c) => {
  let dbOk = false;
  try {
    await c.env.DB.prepare('SELECT 1').first();
    dbOk = true;
  } catch { /* db down */ }

  return c.json({
    status: 'operational',
    service: 'echo-chaos-engineer',
    version: VERSION,
    timestamp: now(),
    uptime_ms: Date.now() - startTime,
    dependencies: {
      d1: dbOk ? 'connected' : 'error',
      kv: 'connected',
    },
  });
});

// ─── 2. GET /stats ───────────────────────────────────────────────────────────

app.get('/stats', async (c) => {
  // Check cache first
  const cached = await c.env.CACHE.get('stats', 'json');
  if (cached) return c.json(cached);

  const [expCount, runCount, passRate, avgRecovery, resScores] = await Promise.all([
    c.env.DB.prepare('SELECT COUNT(*) as cnt FROM experiments').first<{ cnt: number }>(),
    c.env.DB.prepare('SELECT COUNT(*) as cnt FROM experiment_runs').first<{ cnt: number }>(),
    c.env.DB.prepare('SELECT AVG(CAST(passed AS REAL)) as rate FROM experiment_runs WHERE status IN (\'completed\',\'aborted\')').first<{ rate: number | null }>(),
    c.env.DB.prepare('SELECT AVG(recovery_time_ms) as avg_ms FROM experiment_runs WHERE recovery_time_ms IS NOT NULL').first<{ avg_ms: number | null }>(),
    c.env.DB.prepare('SELECT service_name, score FROM resilience_scores ORDER BY score DESC').all<{ service_name: string; score: number }>(),
  ]);

  const stats = {
    total_experiments: expCount?.cnt ?? 0,
    total_runs: runCount?.cnt ?? 0,
    pass_rate: passRate?.rate !== null ? Math.round((passRate?.rate ?? 0) * 100 * 10) / 10 : 0,
    avg_recovery_time_ms: Math.round(avgRecovery?.avg_ms ?? 0),
    resilience_scores: resScores.results ?? [],
    generated_at: now(),
  };

  await c.env.CACHE.put('stats', JSON.stringify(stats), { expirationTtl: 300 });
  return c.json(stats);
});

// ─── 3. GET /experiments ─────────────────────────────────────────────────────

app.get('/experiments', async (c) => {
  const status = c.req.query('status');
  const target = c.req.query('target');
  const type = c.req.query('type');
  const limit = Math.min(parseInt(c.req.query('limit') ?? '50', 10), 200);
  const offset = parseInt(c.req.query('offset') ?? '0', 10);

  let sql = 'SELECT * FROM experiments WHERE 1=1';
  const binds: unknown[] = [];

  if (status) {
    sql += ' AND status = ?';
    binds.push(status);
  }
  if (target) {
    sql += ' AND target_service = ?';
    binds.push(target);
  }
  if (type) {
    sql += ' AND experiment_type = ?';
    binds.push(type);
  }

  sql += ' ORDER BY created_at DESC LIMIT ? OFFSET ?';
  binds.push(limit, offset);

  const stmt = c.env.DB.prepare(sql);
  const result = await stmt.bind(...binds).all<Experiment>();

  return c.json({
    experiments: (result.results ?? []).map(e => ({
      ...e,
      config: JSON.parse(e.config || '{}'),
      safety_config: JSON.parse(e.safety_config || '{}'),
    })),
    count: result.results?.length ?? 0,
  });
});

// ─── 4. GET /experiments/:id ─────────────────────────────────────────────────

app.get('/experiments/:id', async (c) => {
  const id = c.req.param('id');
  const exp = await c.env.DB.prepare('SELECT * FROM experiments WHERE id = ?').bind(id).first<Experiment>();
  if (!exp) return jsonErr('Experiment not found', 404);

  const runs = await c.env.DB.prepare(
    'SELECT * FROM experiment_runs WHERE experiment_id = ? ORDER BY run_number DESC LIMIT 10'
  ).bind(id).all<ExperimentRun>();

  const safetyStops = await c.env.DB.prepare(
    'SELECT * FROM safety_stops WHERE experiment_id = ? ORDER BY triggered_at DESC LIMIT 5'
  ).bind(id).all<SafetyStop>();

  return c.json({
    experiment: {
      ...exp,
      config: JSON.parse(exp.config || '{}'),
      safety_config: JSON.parse(exp.safety_config || '{}'),
    },
    recent_runs: (runs.results ?? []).map(r => ({
      ...r,
      metrics_before: JSON.parse(r.metrics_before || '{}'),
      metrics_during: JSON.parse(r.metrics_during || '{}'),
      metrics_after: JSON.parse(r.metrics_after || '{}'),
      passed: r.passed === 1,
    })),
    safety_stops: safetyStops.results ?? [],
  });
});

// ─── 5. POST /experiments ────────────────────────────────────────────────────

app.post('/experiments', async (c) => {
  let body: Record<string, unknown>;
  try {
    body = await c.req.json();
  } catch {
    return jsonErr('Invalid JSON body', 400);
  }

  const name = body['name'];
  const targetService = body['target_service'];
  const experimentType = body['experiment_type'];
  const config = body['config'] ?? {};
  const safetyConfig = body['safety_config'] ?? {};
  const description = body['description'] ?? null;
  const createdBy = body['created_by'] ?? 'api';

  if (!name || typeof name !== 'string') return jsonErr('name is required', 400);
  if (!targetService || typeof targetService !== 'string') return jsonErr('target_service is required', 400);
  if (!experimentType || typeof experimentType !== 'string' || !isValidType(experimentType)) {
    return jsonErr(`experiment_type must be one of: ${VALID_TYPES.join(', ')}`, 400);
  }

  const id = uid();
  const safety = parseSafety(safetyConfig);

  await c.env.DB.prepare(`
    INSERT INTO experiments (id, name, description, target_service, experiment_type, config, status, safety_config, created_by)
    VALUES (?, ?, ?, ?, ?, ?, 'draft', ?, ?)
  `).bind(
    id,
    name as string,
    description as string | null,
    targetService as string,
    experimentType,
    JSON.stringify(config),
    JSON.stringify(safety),
    createdBy as string
  ).run();

  log('info', 'Experiment created', { id, name: name as string, type: experimentType, target: targetService as string });

  // Invalidate stats cache
  await c.env.CACHE.delete('stats');

  const exp = await c.env.DB.prepare('SELECT * FROM experiments WHERE id = ?').bind(id).first<Experiment>();
  return c.json({
    experiment: exp ? {
      ...exp,
      config: JSON.parse(exp.config || '{}'),
      safety_config: JSON.parse(exp.safety_config || '{}'),
    } : null,
  }, 201);
});

// ─── 6. PUT /experiments/:id ─────────────────────────────────────────────────

app.put('/experiments/:id', async (c) => {
  const id = c.req.param('id');
  const exp = await c.env.DB.prepare('SELECT * FROM experiments WHERE id = ?').bind(id).first<Experiment>();
  if (!exp) return jsonErr('Experiment not found', 404);
  if (exp.status === 'running') return jsonErr('Cannot update a running experiment', 409);

  let body: Record<string, unknown>;
  try {
    body = await c.req.json();
  } catch {
    return jsonErr('Invalid JSON body', 400);
  }

  const updates: string[] = [];
  const binds: unknown[] = [];

  if (body['name'] !== undefined) { updates.push('name = ?'); binds.push(body['name']); }
  if (body['description'] !== undefined) { updates.push('description = ?'); binds.push(body['description']); }
  if (body['target_service'] !== undefined) { updates.push('target_service = ?'); binds.push(body['target_service']); }
  if (body['experiment_type'] !== undefined) {
    if (!isValidType(body['experiment_type'] as string)) return jsonErr(`Invalid experiment_type`, 400);
    updates.push('experiment_type = ?'); binds.push(body['experiment_type']);
  }
  if (body['config'] !== undefined) { updates.push('config = ?'); binds.push(JSON.stringify(body['config'])); }
  if (body['safety_config'] !== undefined) {
    updates.push('safety_config = ?');
    binds.push(JSON.stringify(parseSafety(body['safety_config'])));
  }

  if (updates.length === 0) return jsonErr('No fields to update', 400);

  binds.push(id);
  await c.env.DB.prepare(`UPDATE experiments SET ${updates.join(', ')} WHERE id = ?`).bind(...binds).run();

  await c.env.CACHE.delete('stats');

  const updated = await c.env.DB.prepare('SELECT * FROM experiments WHERE id = ?').bind(id).first<Experiment>();
  return c.json({
    experiment: updated ? {
      ...updated,
      config: JSON.parse(updated.config || '{}'),
      safety_config: JSON.parse(updated.safety_config || '{}'),
    } : null,
  });
});

// ─── 7. DELETE /experiments/:id ──────────────────────────────────────────────

app.delete('/experiments/:id', async (c) => {
  const id = c.req.param('id');
  const exp = await c.env.DB.prepare('SELECT * FROM experiments WHERE id = ?').bind(id).first<Experiment>();
  if (!exp) return jsonErr('Experiment not found', 404);
  if (exp.status === 'running' || exp.status === 'scheduled') {
    return jsonErr('Cannot delete running or scheduled experiments. Abort first.', 409);
  }

  await c.env.DB.prepare('DELETE FROM experiment_results WHERE run_id IN (SELECT id FROM experiment_runs WHERE experiment_id = ?)').bind(id).run();
  await c.env.DB.prepare('DELETE FROM experiment_runs WHERE experiment_id = ?').bind(id).run();
  await c.env.DB.prepare('DELETE FROM safety_stops WHERE experiment_id = ?').bind(id).run();
  await c.env.DB.prepare('DELETE FROM experiments WHERE id = ?').bind(id).run();

  await c.env.CACHE.delete('stats');
  log('info', 'Experiment deleted', { id });

  return c.json({ deleted: true, id });
});

// ─── 8. POST /experiments/:id/run ────────────────────────────────────────────

app.post('/experiments/:id/run', async (c) => {
  const id = c.req.param('id');
  const exp = await c.env.DB.prepare('SELECT * FROM experiments WHERE id = ?').bind(id).first<Experiment>();
  if (!exp) return jsonErr('Experiment not found', 404);
  if (exp.status === 'running') return jsonErr('Experiment is already running', 409);

  // Safety: Check no other experiments are running against same or other services (unless commander override)
  if (!hasCommanderOverride(c.req.raw.headers)) {
    const running = await c.env.DB.prepare(
      "SELECT COUNT(*) as cnt FROM experiments WHERE status = 'running'"
    ).first<{ cnt: number }>();
    if ((running?.cnt ?? 0) > 0) {
      return jsonErr('Another experiment is currently running. Use X-Commander-Override to bypass.', 409);
    }
  }

  // Execute experiment
  const result = await executeExperiment(c.env, exp);

  await c.env.CACHE.delete('stats');

  return c.json({
    run_id: result.runId,
    passed: result.passed,
    recovery_time_ms: result.recoveryTimeMs,
    failure_reason: result.failureReason ?? null,
  });
});

// ─── 9. POST /experiments/:id/abort ──────────────────────────────────────────

app.post('/experiments/:id/abort', async (c) => {
  const id = c.req.param('id');
  const exp = await c.env.DB.prepare('SELECT * FROM experiments WHERE id = ?').bind(id).first<Experiment>();
  if (!exp) return jsonErr('Experiment not found', 404);
  if (exp.status !== 'running' && exp.status !== 'scheduled') {
    return jsonErr('Experiment is not running or scheduled', 409);
  }

  const abortedAt = now();

  // Abort any running runs
  await c.env.DB.prepare(
    "UPDATE experiment_runs SET status = 'aborted', completed_at = ?, failure_reason = 'Manual abort' WHERE experiment_id = ? AND status = 'running'"
  ).bind(abortedAt, id).run();

  // Update experiment
  await c.env.DB.prepare(
    "UPDATE experiments SET status = 'aborted', completed_at = ? WHERE id = ?"
  ).bind(abortedAt, id).run();

  // Record safety stop
  const safetyId = uid();
  await c.env.DB.prepare(
    'INSERT INTO safety_stops (id, experiment_id, reason, triggered_at, auto_triggered) VALUES (?, ?, ?, ?, 0)'
  ).bind(safetyId, id, 'Manual abort by operator', abortedAt).run();

  await c.env.CACHE.delete('stats');
  log('info', 'Experiment aborted', { id });

  return c.json({ aborted: true, id, aborted_at: abortedAt });
});

// ─── 10. GET /experiments/:id/runs ───────────────────────────────────────────

app.get('/experiments/:id/runs', async (c) => {
  const id = c.req.param('id');
  const exp = await c.env.DB.prepare('SELECT id FROM experiments WHERE id = ?').bind(id).first();
  if (!exp) return jsonErr('Experiment not found', 404);

  const limit = Math.min(parseInt(c.req.query('limit') ?? '50', 10), 200);
  const offset = parseInt(c.req.query('offset') ?? '0', 10);

  const runs = await c.env.DB.prepare(
    'SELECT * FROM experiment_runs WHERE experiment_id = ? ORDER BY run_number DESC LIMIT ? OFFSET ?'
  ).bind(id, limit, offset).all<ExperimentRun>();

  return c.json({
    runs: (runs.results ?? []).map(r => ({
      ...r,
      metrics_before: JSON.parse(r.metrics_before || '{}'),
      metrics_during: JSON.parse(r.metrics_during || '{}'),
      metrics_after: JSON.parse(r.metrics_after || '{}'),
      passed: r.passed === 1,
    })),
    count: runs.results?.length ?? 0,
  });
});

// ─── 11. GET /runs/:id ───────────────────────────────────────────────────────

app.get('/runs/:id', async (c) => {
  const id = c.req.param('id');
  const run = await c.env.DB.prepare('SELECT * FROM experiment_runs WHERE id = ?').bind(id).first<ExperimentRun>();
  if (!run) return jsonErr('Run not found', 404);

  const results = await c.env.DB.prepare(
    'SELECT * FROM experiment_results WHERE run_id = ? ORDER BY metric_name'
  ).bind(id).all<ExperimentResult>();

  return c.json({
    run: {
      ...run,
      metrics_before: JSON.parse(run.metrics_before || '{}'),
      metrics_during: JSON.parse(run.metrics_during || '{}'),
      metrics_after: JSON.parse(run.metrics_after || '{}'),
      passed: run.passed === 1,
    },
    results: (results.results ?? []).map(r => ({
      ...r,
      recovered: r.recovered === 1,
    })),
  });
});

// ─── 12. GET /runs/:id/results ───────────────────────────────────────────────

app.get('/runs/:id/results', async (c) => {
  const id = c.req.param('id');
  const run = await c.env.DB.prepare('SELECT id FROM experiment_runs WHERE id = ?').bind(id).first();
  if (!run) return jsonErr('Run not found', 404);

  const results = await c.env.DB.prepare(
    'SELECT * FROM experiment_results WHERE run_id = ? ORDER BY metric_name'
  ).bind(id).all<ExperimentResult>();

  return c.json({
    results: (results.results ?? []).map(r => ({
      ...r,
      recovered: r.recovered === 1,
    })),
    count: results.results?.length ?? 0,
  });
});

// ─── 13. POST /experiments/:id/schedule ──────────────────────────────────────

app.post('/experiments/:id/schedule', async (c) => {
  const id = c.req.param('id');
  const exp = await c.env.DB.prepare('SELECT * FROM experiments WHERE id = ?').bind(id).first<Experiment>();
  if (!exp) return jsonErr('Experiment not found', 404);
  if (exp.status === 'running') return jsonErr('Cannot schedule a running experiment', 409);

  let body: Record<string, unknown>;
  try {
    body = await c.req.json();
  } catch {
    return jsonErr('Invalid JSON body', 400);
  }

  const scheduledFor = body['scheduled_for'];
  if (!scheduledFor || typeof scheduledFor !== 'string') {
    return jsonErr('scheduled_for is required (ISO 8601 datetime)', 400);
  }

  // Validate date
  const scheduledDate = new Date(scheduledFor as string);
  if (isNaN(scheduledDate.getTime())) {
    return jsonErr('scheduled_for must be a valid ISO 8601 datetime', 400);
  }
  if (scheduledDate.getTime() < Date.now()) {
    return jsonErr('scheduled_for must be in the future', 400);
  }

  await c.env.DB.prepare(
    "UPDATE experiments SET status = 'scheduled', scheduled_for = ? WHERE id = ?"
  ).bind(scheduledFor as string, id).run();

  await c.env.CACHE.delete('stats');
  log('info', 'Experiment scheduled', { id, scheduledFor });

  return c.json({ scheduled: true, id, scheduled_for: scheduledFor });
});

// ─── 14. GET /resilience ─────────────────────────────────────────────────────

app.get('/resilience', async (c) => {
  const scores = await c.env.DB.prepare(
    'SELECT * FROM resilience_scores ORDER BY score DESC'
  ).all<ResilienceScore>();

  return c.json({
    scores: scores.results ?? [],
    count: scores.results?.length ?? 0,
    generated_at: now(),
  });
});

// ─── 15. GET /resilience/:service ────────────────────────────────────────────

app.get('/resilience/:service', async (c) => {
  const service = c.req.param('service');
  const score = await c.env.DB.prepare(
    'SELECT * FROM resilience_scores WHERE service_name = ?'
  ).bind(service).first<ResilienceScore>();

  if (!score) return jsonErr('No resilience data for this service', 404);

  // Get recent experiment history for this service
  const recentRuns = await c.env.DB.prepare(`
    SELECT e.experiment_type, er.passed, er.recovery_time_ms, er.completed_at
    FROM experiment_runs er
    JOIN experiments e ON er.experiment_id = e.id
    WHERE e.target_service = ? AND er.status IN ('completed','aborted')
    ORDER BY er.completed_at DESC
    LIMIT 20
  `).bind(service).all<{ experiment_type: string; passed: number; recovery_time_ms: number | null; completed_at: string | null }>();

  return c.json({
    score,
    recent_tests: (recentRuns.results ?? []).map(r => ({
      ...r,
      passed: r.passed === 1,
    })),
  });
});

// ─── 16. POST /resilience/recalculate ────────────────────────────────────────

app.post('/resilience/recalculate', async (c) => {
  const services = await c.env.DB.prepare(
    'SELECT DISTINCT target_service FROM experiments'
  ).all<{ target_service: string }>();

  const recalculated: string[] = [];
  for (const row of (services.results ?? [])) {
    await updateResilienceScore(c.env, row.target_service);
    recalculated.push(row.target_service);
  }

  await c.env.CACHE.delete('stats');
  log('info', 'Resilience scores recalculated', { services: recalculated });

  return c.json({ recalculated, count: recalculated.length });
});

// ─── 17. GET /safety ─────────────────────────────────────────────────────────

app.get('/safety', async (c) => {
  const limit = Math.min(parseInt(c.req.query('limit') ?? '50', 10), 200);
  const offset = parseInt(c.req.query('offset') ?? '0', 10);

  const stops = await c.env.DB.prepare(
    'SELECT s.*, e.name as experiment_name, e.target_service FROM safety_stops s JOIN experiments e ON s.experiment_id = e.id ORDER BY s.triggered_at DESC LIMIT ? OFFSET ?'
  ).bind(limit, offset).all<SafetyStop & { experiment_name: string; target_service: string }>();

  return c.json({
    safety_stops: (stops.results ?? []).map(s => ({
      ...s,
      auto_triggered: s.auto_triggered === 1,
    })),
    count: stops.results?.length ?? 0,
  });
});

// ─── 18. POST /gameday ───────────────────────────────────────────────────────

app.post('/gameday', async (c) => {
  let body: GameDayRequest;
  try {
    body = await c.req.json();
  } catch {
    return jsonErr('Invalid JSON body', 400);
  }

  if (!body.name || typeof body.name !== 'string') return jsonErr('name is required', 400);
  if (!Array.isArray(body.services) || body.services.length === 0) return jsonErr('services array is required', 400);
  if (!Array.isArray(body.experiment_types) || body.experiment_types.length === 0) return jsonErr('experiment_types array is required', 400);

  for (const t of body.experiment_types) {
    if (!isValidType(t)) return jsonErr(`Invalid experiment type: ${t}`, 400);
  }

  const safety = parseSafety(body.safety_config ?? {});
  const createdExperiments: Array<{ id: string; name: string; target: string; type: ExperimentType }> = [];

  for (const service of body.services) {
    for (const expType of body.experiment_types) {
      const id = uid();
      const expName = `${body.name} - ${service} - ${expType}`;

      const defaultConfig: ExperimentConfig = {};
      switch (expType) {
        case 'latency_injection':
          defaultConfig.latency_ms = 2000;
          defaultConfig.duration_ms = 10000;
          break;
        case 'error_injection':
          defaultConfig.error_rate = 0.3;
          defaultConfig.error_code = 500;
          defaultConfig.duration_ms = 10000;
          break;
        case 'timeout':
          defaultConfig.timeout_ms = 100;
          defaultConfig.duration_ms = 10000;
          break;
        case 'partial_failure':
          defaultConfig.failure_percentage = 0.2;
          defaultConfig.duration_ms = 10000;
          break;
        case 'load_spike':
          defaultConfig.load_multiplier = 5;
          defaultConfig.duration_ms = 10000;
          break;
        case 'dependency_kill':
          defaultConfig.target_dependency = 'shared-brain';
          defaultConfig.duration_ms = 10000;
          break;
      }

      await c.env.DB.prepare(`
        INSERT INTO experiments (id, name, description, target_service, experiment_type, config, status, safety_config, created_by)
        VALUES (?, ?, ?, ?, ?, ?, 'draft', ?, 'gameday')
      `).bind(
        id,
        expName,
        `Game Day: ${body.name}`,
        service,
        expType,
        JSON.stringify(defaultConfig),
        JSON.stringify(safety),
      ).run();

      createdExperiments.push({ id, name: expName, target: service, type: expType });
    }
  }

  await c.env.CACHE.delete('stats');
  log('info', 'Game Day created', { name: body.name, experimentCount: createdExperiments.length });

  return c.json({
    gameday: body.name,
    experiments_created: createdExperiments.length,
    experiments: createdExperiments,
    note: 'Experiments created in draft status. Run each individually or schedule them.',
  }, 201);
});

// ─── 19. GET /report ─────────────────────────────────────────────────────────

app.get('/report', async (c) => {
  // Check cache
  const cached = await c.env.CACHE.get('report', 'json');
  if (cached) return c.json(cached);

  const [
    totalExp,
    totalRuns,
    passedRuns,
    failedRuns,
    abortedRuns,
    avgRecovery,
    typeBreakdown,
    recentSafetyStops,
    resilienceScores,
    topTargets,
  ] = await Promise.all([
    c.env.DB.prepare('SELECT COUNT(*) as cnt FROM experiments').first<{ cnt: number }>(),
    c.env.DB.prepare('SELECT COUNT(*) as cnt FROM experiment_runs').first<{ cnt: number }>(),
    c.env.DB.prepare("SELECT COUNT(*) as cnt FROM experiment_runs WHERE passed = 1").first<{ cnt: number }>(),
    c.env.DB.prepare("SELECT COUNT(*) as cnt FROM experiment_runs WHERE passed = 0 AND status = 'completed'").first<{ cnt: number }>(),
    c.env.DB.prepare("SELECT COUNT(*) as cnt FROM experiment_runs WHERE status = 'aborted'").first<{ cnt: number }>(),
    c.env.DB.prepare('SELECT AVG(recovery_time_ms) as avg_ms FROM experiment_runs WHERE recovery_time_ms IS NOT NULL').first<{ avg_ms: number | null }>(),
    c.env.DB.prepare(`
      SELECT experiment_type, COUNT(*) as cnt,
             SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
             SUM(CASE WHEN status = 'aborted' THEN 1 ELSE 0 END) as aborted
      FROM experiments GROUP BY experiment_type
    `).all<{ experiment_type: string; cnt: number; completed: number; aborted: number }>(),
    c.env.DB.prepare(
      'SELECT * FROM safety_stops ORDER BY triggered_at DESC LIMIT 10'
    ).all<SafetyStop>(),
    c.env.DB.prepare('SELECT * FROM resilience_scores ORDER BY score DESC').all<ResilienceScore>(),
    c.env.DB.prepare(`
      SELECT target_service, COUNT(*) as experiment_count
      FROM experiments GROUP BY target_service ORDER BY experiment_count DESC LIMIT 10
    `).all<{ target_service: string; experiment_count: number }>(),
  ]);

  const totalRunsCnt = totalRuns?.cnt ?? 0;
  const passedCnt = passedRuns?.cnt ?? 0;

  const report = {
    title: 'ECHO Chaos Engineering Report',
    generated_at: now(),
    summary: {
      total_experiments: totalExp?.cnt ?? 0,
      total_runs: totalRunsCnt,
      passed: passedCnt,
      failed: failedRuns?.cnt ?? 0,
      aborted: abortedRuns?.cnt ?? 0,
      pass_rate_pct: totalRunsCnt > 0 ? Math.round((passedCnt / totalRunsCnt) * 100 * 10) / 10 : 0,
      avg_recovery_time_ms: Math.round(avgRecovery?.avg_ms ?? 0),
    },
    experiment_type_breakdown: typeBreakdown.results ?? [],
    resilience_scores: resilienceScores.results ?? [],
    most_tested_services: topTargets.results ?? [],
    recent_safety_stops: (recentSafetyStops.results ?? []).map(s => ({
      ...s,
      auto_triggered: s.auto_triggered === 1,
    })),
  };

  await c.env.CACHE.put('report', JSON.stringify(report), { expirationTtl: 600 });
  return c.json(report);
});

// ─── 404 ─────────────────────────────────────────────────────────────────────

app.notFound((c) => {
  return c.json({ error: 'Not found', path: c.req.path }, 404);
});

// ─── Error Handler ───────────────────────────────────────────────────────────

app.onError((err, c) => {
  log('error', 'Unhandled error', { error: err.message, path: c.req.path, method: c.req.method });
  return c.json({ error: 'Internal server error', message: err.message }, 500);
});

// ─── Cron Handler ────────────────────────────────────────────────────────────

async function handleCron(env: Env): Promise<void> {
  log('info', 'Nightly chaos cron started');

  // Run scheduled experiments
  const scheduled = await env.DB.prepare(
    "SELECT * FROM experiments WHERE status = 'scheduled' AND scheduled_for <= ?"
  ).bind(now()).all<Experiment>();

  let ranCount = 0;
  for (const exp of (scheduled.results ?? [])) {
    try {
      log('info', 'Running scheduled experiment', { id: exp.id, name: exp.name });
      await executeExperiment(env, exp);
      ranCount++;
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Unknown error';
      log('error', 'Scheduled experiment failed', { id: exp.id, error: msg });
      await env.DB.prepare(
        "UPDATE experiments SET status = 'aborted', completed_at = ? WHERE id = ?"
      ).bind(now(), exp.id).run();
    }
  }

  // Generate nightly report and cache it
  const [totalExp, totalRuns, passRate, avgRecovery] = await Promise.all([
    env.DB.prepare('SELECT COUNT(*) as cnt FROM experiments').first<{ cnt: number }>(),
    env.DB.prepare('SELECT COUNT(*) as cnt FROM experiment_runs').first<{ cnt: number }>(),
    env.DB.prepare("SELECT AVG(CAST(passed AS REAL)) as rate FROM experiment_runs WHERE status IN ('completed','aborted')").first<{ rate: number | null }>(),
    env.DB.prepare('SELECT AVG(recovery_time_ms) as avg_ms FROM experiment_runs WHERE recovery_time_ms IS NOT NULL').first<{ avg_ms: number | null }>(),
  ]);

  const nightlyReport = {
    type: 'nightly_chaos_report',
    timestamp: now(),
    scheduled_experiments_run: ranCount,
    total_experiments: totalExp?.cnt ?? 0,
    total_runs: totalRuns?.cnt ?? 0,
    pass_rate_pct: passRate?.rate !== null ? Math.round((passRate?.rate ?? 0) * 100 * 10) / 10 : 0,
    avg_recovery_time_ms: Math.round(avgRecovery?.avg_ms ?? 0),
  };

  await env.CACHE.put('nightly_report', JSON.stringify(nightlyReport), { expirationTtl: 86400 });
  log('info', 'Nightly chaos cron completed', nightlyReport);
}

// ─── Export ──────────────────────────────────────────────────────────────────

export default {
  fetch: app.fetch,
  scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext): void {
    ctx.waitUntil(handleCron(env));
  },
};
