/** Env config, KISS: plain process.env with defaults. DB_PATH is the only
 *  shared state (every process opens the same SQLite file); ports and ids
 *  are per-process. Poc-module timing constants (EXEC_DELAY_MS,
 *  STALE_HELD_MS, CLAIM_TTL_MS, ...) are read directly where they live. */

function str(name: string, dflt: string): string {
  const v = process.env[name];
  return v === undefined || v === "" ? dflt : v;
}
function num(name: string, dflt: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw === "") return dflt;
  const v = Number(raw);
  return Number.isFinite(v) ? v : dflt;
}
function bool(name: string): boolean {
  return process.env[name] === "1" || process.env[name] === "true";
}

export const config = {
  port: num("PORT", 3000),
  bankPort: num("BANK_PORT", 4000),
  bankUrl: str("BANK_URL", "http://127.0.0.1:4000"),
  dbPath: str("DB_PATH", "run/orchestrator.db"),
  apiKey: str("API_KEY", "devkey"),
  relayIntervalMs: num("RELAY_INTERVAL_MS", 100),
  syncIntervalMs: num("SYNC_INTERVAL_MS", 2000),
  sweepIntervalMs: num("SWEEP_INTERVAL_MS", 1000),
  workerHealthPort: num("WORKER_HEALTH_PORT", 0),
  workerId: str("WORKER_ID", "w-" + process.pid + "-" + Math.random().toString(36).slice(2, 6)),
  drainDeadlineMs: num("DRAIN_DEADLINE_MS", 10_000),
  faultCrashAfterHold: bool("FAULT_CRASH_AFTER_HOLD"),
  faultCrashAfterSubmit: bool("FAULT_CRASH_AFTER_SUBMIT"),
};
