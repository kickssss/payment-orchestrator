import { q, run, withTxn, type Db } from "./db.ts";
import type { Clock } from "./clock.ts";
import type { Gate } from "./gate.ts";
import type { BankConnector } from "../platform/connector.ts";
import type { Payin } from "./payin.ts";

/** Bank sync behind the gate (plan S8): statement reads hold the gate first -
 *  "no payout while seeking a statement", statements win, payouts defer.
 *  Ingest is ONE transaction: dedupe, escrow park, inline two-sided match.
 *  Multi-process: holder ids carry the instance id so two workers can never
 *  mistake each other's lease for their own. */

export function makeSync({ db, clock, gate, bank, payin, instanceId = "" }: { db: Db; clock: Clock; gate: Gate; bank: BankConnector; payin: Payin; instanceId?: string }) {
  const holderFor = (accountId: number) => "statement:sync:" + accountId + (instanceId ? ":" + instanceId : "");

  async function syncAccount(accountId: number, fromMs: number, toMs?: number) {
    const holder = holderFor(accountId);
    let acquired = false;
    withTxn(db, () => {
      acquired = gate.acquire("account", accountId, "statement", holder);
    });
    if (!acquired) return { result: "busy" as const };
    try {
      const txns = await bank.fetchTransactions(accountId, fromMs, toMs ?? clock.now());
      const r = payin.ingest(accountId, txns);
      return { result: "ok" as const, ...r };
    } finally {
      withTxn(db, () => {
        gate.release("account", accountId, holder);
      });
    }
  }

  /** Worker loop entry: due-scan over accounts with a watermark (sync_state).
   *  Correctness never depends on the watermark - the gate serializes
   *  concurrent syncs and ingest dedupe makes re-fetches no-ops; the cursor
   *  is efficiency (fetch [cursor - overlap, now], not [0, now] forever). */
  async function syncDue(intervalMs: number, overlapMs: number, limit = 4): Promise<{ due: number; synced: number[]; busy: number; failed: number }> {
    const now = clock.now();
    const due = q<{ id: number; last_cursor: number }>(
      db,
      "SELECT a.id AS id, COALESCE(s.last_cursor, 0) AS last_cursor " +
        "FROM bank_accounts a LEFT JOIN sync_state s ON s.account_id = a.id " +
        "WHERE a.status = 'active' AND (s.last_fetch_started IS NULL OR s.last_fetch_started <= ?) " +
        "ORDER BY a.id LIMIT ?",
      now - intervalMs,
      limit
    );
    const out: { due: number; synced: number[]; busy: number; failed: number } = { due: due.length, synced: [], busy: 0, failed: 0 };
    for (const acct of due) {
      const holder = holderFor(acct.id);
      let acquired = false;
      withTxn(db, () => {
        acquired = gate.acquire("account", acct.id, "statement", holder);
      });
      if (!acquired) {
        out.busy++;
        continue;
      }
      try {
        withTxn(db, () => {
          run(
            db,
            "INSERT INTO sync_state (account_id, last_cursor, last_fetch_started) VALUES (?, 0, ?) " +
              "ON CONFLICT (account_id) DO UPDATE SET last_fetch_started = excluded.last_fetch_started",
            acct.id,
            now
          );
        });
        const to = clock.now();
        const from = Math.max(0, acct.last_cursor - overlapMs);
        const txns = await bank.fetchTransactions(acct.id, from, to);
        payin.ingest(acct.id, txns);
        withTxn(db, () => {
          run(db, "UPDATE sync_state SET last_cursor = ? WHERE account_id = ?", to, acct.id);
        });
        out.synced.push(acct.id);
      } catch {
        out.failed++; // bank down or ingest error: gate released below, retried next interval
      } finally {
        withTxn(db, () => {
          gate.release("account", acct.id, holder);
        });
      }
    }
    return out;
  }

  return { syncAccount, syncDue };
}

export type Sync = ReturnType<typeof makeSync>;
