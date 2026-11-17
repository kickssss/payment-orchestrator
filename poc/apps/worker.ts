/** Worker process (the plan's worker role): drives the three loops — outbox
 *  relay, bank statement sync, master sweeper — plus graceful drain and the
 *  crash fault hooks. N replicas are safe BY CONSTRUCTION: DB claims and bank
 *  gates do the correctness work; this file only supplies the ticks. */

import { config } from "../src/platform/config.ts";
import { makeHttpBank } from "../src/platform/connector.ts";
import { createRuntime } from "../src/platform/runtime.ts";

const iso = () => new Date().toISOString();
const runtime = createRuntime({
  dbPath: config.dbPath,
  instanceId: config.workerId,
  bank: makeHttpBank(config.bankUrl),
  faults: { crashAfterHold: config.faultCrashAfterHold, crashAfterSubmit: config.faultCrashAfterSubmit },
});

console.log(
  iso(), "[worker]", config.workerId,
  `up (relay ${config.relayIntervalMs / 1000}s, sync ${config.syncIntervalMs / 1000}s, sweep ${config.sweepIntervalMs / 1000}s)`
);

let draining = false;
let lastBusyLog = 0;
function onTickError(e: unknown): void {
  if (runtime.isBusy(e)) {
    // SQLITE_BUSY can still surface past busy_timeout; the next tick IS the backoff
    if (Date.now() - lastBusyLog >= 5_000) {
      lastBusyLog = Date.now();
      console.log(iso(), "[worker] busy, skipping tick");
    }
    return;
  }
  console.error(iso(), "[worker] tick error:", e); // a worker NEVER exits on loop errors
}

type Loop = { stop: () => void; inFlight: () => Promise<void> };
function loop(ms: number, tick: () => Promise<void>): Loop {
  let running = false; // a slow tick must never overlap itself
  let current = Promise.resolve();
  const h = setInterval(() => {
    if (draining || running) return; // draining: stop taking new ticks
    running = true;
    current = tick().catch(onTickError).finally(() => {
      running = false;
    });
  }, ms);
  return { stop: () => clearInterval(h), inFlight: () => current };
}

const loops: Loop[] = [
  loop(config.relayIntervalMs, async () => {
    const r = await runtime.outbox.relay();
    if (r.dispatched > 0) console.log(iso(), "[worker] relay dispatched=" + r.dispatched + " failed=" + r.failed + " claimed=" + r.claimed);
  }),
  loop(config.syncIntervalMs, async () => {
    const r = await runtime.sync.syncDue(config.syncIntervalMs, config.syncIntervalMs);
    if (r.synced.length > 0 || r.failed > 0)
      console.log(iso(), "[worker] sync due=" + r.due + " synced=[" + r.synced.join(",") + "] busy=" + r.busy + " failed=" + r.failed);
  }),
  loop(config.sweepIntervalMs, async () => {
    const r = await runtime.sweeper.tick();
    const changes =
      r.payin.expired + r.payin.claimDeadlines + r.payin.rematched + r.payin.oldCredits +
      r.payouts.drove + r.payouts.heldStale + r.payouts.reconciled;
    if (changes > 0)
      console.log(
        iso(), "[worker] sweep",
        "expired=" + r.payin.expired, "claims=" + r.payin.claimDeadlines, "rematched=" + r.payin.rematched,
        "oldCredits=" + r.payin.oldCredits, "drove=" + r.payouts.drove, "deferred=" + r.payouts.deferred,
        "heldStale=" + r.payouts.heldStale, "reconciled=" + r.payouts.reconciled, "staleGates=" + r.staleGates
      );
  }),
];

const health = Bun.serve({
  hostname: "127.0.0.1",
  port: config.workerHealthPort, // 0 -> ephemeral
  development: false, // no dev error pages leaking source lines
  fetch: (req) =>
    new URL(req.url).pathname === "/healthz"
      ? Response.json({ ok: true, role: "worker", id: config.workerId })
      : new Response("not found", { status: 404 }),
});
console.log(iso(), "[worker] health on port " + health.port);

function drain(): void {
  if (draining) return;
  draining = true;
  console.log(iso(), "[worker] draining...");
  for (const l of loops) l.stop();
  health.stop(); // no server.shutdown() in Bun 1.4.4 — stop() is graceful; health-only traffic, exit after is fine
  // orphaned gate rows self-expire (GATE_TTL_MS, expired = free) — nothing else to do
  setTimeout(() => process.exit(0), config.drainDeadlineMs).unref?.(); // hard exit if a bank call wedges
  void Promise.all(loops.map((l) => l.inFlight())).then(() => {
    console.log(iso(), "[worker] drained");
    process.exit(0);
  });
}
process.on("SIGTERM", drain);
process.on("SIGINT", drain);
