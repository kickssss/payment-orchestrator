import { one, q, run, withTxn, type Db } from "./db.ts";
import type { Clock } from "./clock.ts";

/** Transactional outbox + in-process relay (stands in for the plan's
 *  outbox -> Redis XADD -> consumer-group pipeline).
 *  - emit() is called INSIDE the effect transaction: state change + event row
 *    commit together, or neither does.
 *  - relay() claims undispatched rows with a self-expiring claim, dispatches to
 *    registered handlers, then marks dispatched. A handler that throws leaves
 *    the row claimed-but-unmarked; after CLAIM_TTL the row is re-claimable —
 *    at-least-once delivery. Handlers must be idempotent: the guards are the
 *    dedupe (no processed_events table, exactly like the plan). */

export type OutboxEvent = {
  event_id: string;
  tenant_id: number | null;
  aggregate_type: string;
  aggregate_id: number;
  event_type: string;
  payload: string;
  occurred_at: number;
};

export type Handler = (evt: OutboxEvent) => void | Promise<void>;

const CLAIM_TTL_MS = Number(process.env.CLAIM_TTL_MS ?? 60_000);

export function makeOutbox({ db, clock, instanceId = "relay" }: { db: Db; clock: Clock; instanceId?: string }) {
  const handlers = new Map<string, Handler[]>();
  let relaySeq = 0;

  function emit(e: {
    type: string;
    tenantId: number | null;
    aggregateType: string;
    aggregateId: number;
    payload?: unknown;
  }): void {
    run(
      db,
      "INSERT INTO outbox (event_id, tenant_id, aggregate_type, aggregate_id, event_type, payload, occurred_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
      crypto.randomUUID(),
      e.tenantId,
      e.aggregateType,
      e.aggregateId,
      e.type,
      JSON.stringify(e.payload ?? {}),
      clock.now()
    );
  }

  function on(type: string, handler: Handler): void {
    handlers.set(type, [...(handlers.get(type) ?? []), handler]);
  }

  async function relay(): Promise<{ dispatched: number; failed: number; claimed: number }> {
    // an api process must never run the relay: a zero-handler registry would
    // mark events dispatched while doing nothing with them
    if (handlers.size === 0) return { dispatched: 0, failed: 0, claimed: 0 };
    const token = instanceId + "-" + ++relaySeq; // process-unique: two relays must never share claim tokens
    const now = clock.now();
    withTxn(db, () => {
      run(
        db,
        "UPDATE outbox SET claimed_by = ?, claimed_at = ? WHERE event_id IN (" +
          "SELECT event_id FROM outbox WHERE dispatched_at IS NULL" +
          " AND (claimed_at IS NULL OR claimed_at <= ?) ORDER BY occurred_at, event_id LIMIT 100)",
        token,
        now,
        now - CLAIM_TTL_MS
      );
    });

    const claimed = q<OutboxEvent>(
      db,
      "SELECT event_id, tenant_id, aggregate_type, aggregate_id, event_type, payload, occurred_at " +
        "FROM outbox WHERE claimed_by = ? AND dispatched_at IS NULL ORDER BY occurred_at, event_id",
      token
    );

    let dispatched = 0;
    let failed = 0;
    for (const evt of claimed) {
      try {
        for (const h of handlers.get(evt.event_type) ?? []) await h(evt);
        withTxn(db, () => {
          run(db, "UPDATE outbox SET dispatched_at = ? WHERE event_id = ? AND dispatched_at IS NULL", clock.now(), evt.event_id);
        });
        dispatched++;
      } catch {
        failed++; // row stays claimed-but-unmarked; re-claimable after CLAIM_TTL
      }
    }
    return { dispatched, failed, claimed: claimed.length };
  }

  function pending(): number {
    return one<{ n: number }>(db, "SELECT COUNT(*) AS n FROM outbox WHERE dispatched_at IS NULL")!.n;
  }

  function events(): { event_type: string; dispatched_at: number | null }[] {
    return q(db, "SELECT event_type, dispatched_at FROM outbox ORDER BY occurred_at, event_id");
  }

  return { emit, on, relay, pending, events };
}

export type Outbox = ReturnType<typeof makeOutbox>;
