import { createDb, isBusy, TENANT_ID } from "../core/db.ts";
import type { Clock } from "../core/clock.ts";
import { makeOutbox } from "../core/outbox.ts";
import { makeGate } from "../core/gate.ts";
import { makePayin } from "../core/payin.ts";
import { makePayout } from "../core/payout.ts";
import { makeSync } from "../core/sync.ts";
import { makeSweeper } from "../core/sweeper.ts";
import { openingDeposit, verifyLedger } from "../core/ledger.ts";
import type { BankConnector } from "./connector.ts";

/** Wiring for REAL processes (api / worker entrypoints): a file-backed DB from
 *  the caller, the REAL wall clock (never the test fixture epoch), instance-
 *  scoped claims and gate holders. The only event consumer is the payout
 *  executor (payout.created -> execute). The api role calls the same modules
 *  but NEVER runs the relay (zero handlers = no dispatch, by design). */

export function createRuntime(opts: { dbPath: string; instanceId: string; bank: BankConnector; faults?: { crashAfterHold?: boolean; crashAfterSubmit?: boolean } }) {
  const db = createDb(opts.dbPath);
  // wall clock: createClock FREEZES time (test fixture semantics) - deadlines
  // in a real process must actually come due, so the runtime rolls its own.
  const clock: Clock = { now: () => Date.now(), setNow: () => {}, advance: () => {} };
  const outbox = makeOutbox({ db, clock, instanceId: opts.instanceId });
  const gate = makeGate({ db, clock });
  const bank = opts.bank;
  const payin = makePayin({ db, clock, outbox });
  const payout = makePayout({ db, clock, outbox, gate, bank, instanceId: opts.instanceId });
  // the executor — also the fault seam: a marker error kills the process for
  // real, leaving a claimed-but-unmarked outbox row: exactly the wedge the
  // sweeper + CLAIM_TTL re-claim must clear (only when faults are requested).
  outbox.on("payout.created", async (evt) => {
    try {
      await payout.execute(evt.aggregate_id, {
        crashAfterHold: opts.faults?.crashAfterHold === true,
        crashAfterSubmit: opts.faults?.crashAfterSubmit === true,
      });
    } catch (e) {
      if (opts.faults && e instanceof Error && (e.message === "WORKER_CRASH_AFTER_HOLD" || e.message === "WORKER_CRASH_AFTER_SUBMIT")) {
        console.error("[fault] dying on marker:", e.message);
        process.exit(1);
      }
      throw e; // relay counts it failed; the row re-claims after CLAIM_TTL
    }
  });
  const sync = makeSync({ db, clock, gate, bank, payin, instanceId: opts.instanceId });
  const sweeper = makeSweeper({ clock, payin, payout, gate });

  return {
    db,
    clock,
    outbox,
    gate,
    bank,
    payin,
    payout,
    sync,
    sweeper,
    fund: (accountId: number, amountPaise: number) => openingDeposit(db, TENANT_ID, accountId, amountPaise, clock.now()),
    verify: () => verifyLedger(db),
    isBusy, // loop-level skip policy: busy_timeout can still throw - skip this tick
  };
}

export type Runtime = ReturnType<typeof createRuntime>;
