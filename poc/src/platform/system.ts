import { createDb, TENANT_ID } from "../core/db.ts";
import { createClock } from "../core/clock.ts";
import { makeOutbox } from "../core/outbox.ts";
import { makeGate } from "../core/gate.ts";
import { makeFakeBank } from "./fakebank.ts";
import { makePayin } from "../core/payin.ts";
import { makePayout } from "../core/payout.ts";
import { makeSync } from "../core/sync.ts";
import { makeSweeper } from "../core/sweeper.ts";
import { openingDeposit, verifyLedger } from "../core/ledger.ts";

/** Wiring: one fresh in-memory system per test / demo run — the plan's two
 *  process roles collapsed into one Bun process. The only event consumer is
 *  the payout executor (payout.created), exactly like the plan's worker role
 *  minus the cut webhook dispatcher. */

export function createSystem(opts: { startAt?: number } = {}) {
  const db = createDb();
  const clock = createClock(opts.startAt ?? 1_700_000_000_000);
  const outbox = makeOutbox({ db, clock });
  const gate = makeGate({ db, clock });
  const bank = makeFakeBank(clock);
  const payin = makePayin({ db, clock, outbox });
  const payout = makePayout({ db, clock, outbox, gate, bank });
  outbox.on("payout.created", (evt) => payout.execute(evt.aggregate_id));
  const sync = makeSync({ db, clock, gate, bank, payin });
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
    async tick() {
      const relay = await outbox.relay();
      const sweep = await sweeper.tick();
      return { relay, sweep };
    },
  };
}

export type System = ReturnType<typeof createSystem>;
