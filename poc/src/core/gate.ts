import { q, run, type Db } from "./db.ts";
import type { Clock } from "./clock.ts";

/** bank_gates: one authoritative compare-and-set row per (scope, entity) —
 *  the plan's single gate authority (§8). Acquire is a single conditional
 *  upsert: a fresh row plain-INSERTs; an existing row is taken only if it is
 *  expired ("expired = free") or already ours. Release is conditional on the
 *  holder — a stale holder can never free a newer holder's gate.
 *  Fencing: the holder re-verifies ownership before every bank step.
 *  All timestamps come from the injected clock, never SQLite's. */

export const GATE_TTL_MS = Number(process.env.GATE_TTL_MS ?? 60_000);

export type Scope = "account" | "connection";

export function makeGate({ db, clock }: { db: Db; clock: Clock }) {
  function acquire(scope: Scope, entity: number, op: "statement" | "payout", holderId: string, ttlMs: number = GATE_TTL_MS): boolean {
    const now = clock.now();
    const changed = run(
      db,
      "INSERT INTO bank_gates (scope, entity_id, holder_op, holder_id, acquired_at, expires_at) VALUES (?, ?, ?, ?, ?, ?) " +
        "ON CONFLICT (scope, entity_id) DO UPDATE SET holder_op = excluded.holder_op, holder_id = excluded.holder_id, " +
        "acquired_at = excluded.acquired_at, expires_at = excluded.expires_at " +
        "WHERE bank_gates.expires_at < ? OR bank_gates.holder_id = excluded.holder_id OR bank_gates.holder_id IS NULL",
      scope,
      entity,
      op,
      holderId,
      now,
      now + ttlMs,
      now
    );
    return changed === 1;
  }

  function release(scope: Scope, entity: number, holderId: string): boolean {
    return run(db, "DELETE FROM bank_gates WHERE scope = ? AND entity_id = ? AND holder_id = ?", scope, entity, holderId) === 1;
  }

  /** Fencing check: do I still hold it, unexpired? Call before every bank step. */
  function heldBy(scope: Scope, entity: number, holderId: string): boolean {
    return (
      q(
        db,
        "SELECT 1 AS ok FROM bank_gates WHERE scope = ? AND entity_id = ? AND holder_id = ? AND expires_at > ? LIMIT 1",
        scope,
        entity,
        holderId,
        clock.now()
      ).length === 1
    );
  }

  /** Gates held past expiry x2 — ops visibility, never auto-cleared (plan §4). */
  function stale(): { scope: string; entity_id: number; holder_id: string; expires_at: number }[] {
    const grace = clock.now() - 2 * GATE_TTL_MS;
    return q(
      db,
      "SELECT scope, entity_id, holder_id, expires_at FROM bank_gates WHERE holder_id IS NOT NULL AND expires_at < ?",
      grace
    );
  }

  function holder(scope: Scope, entity: number): string | null {
    return one_holder(db, scope, entity);
  }

  return { acquire, release, heldBy, stale, holder };
}

function one_holder(db: Db, scope: Scope, entity: number): string | null {
  const row = q<{ holder_id: string | null }>(db, "SELECT holder_id FROM bank_gates WHERE scope = ? AND entity_id = ?", scope, entity);
  return row.length === 1 ? row[0].holder_id : null;
}

export type Gate = ReturnType<typeof makeGate>;
