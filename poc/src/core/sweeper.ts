import type { Clock } from "./clock.ts";
import type { Payin } from "./payin.ts";
import type { Payout } from "./payout.ts";
import type { Gate } from "./gate.ts";

/** Master sweeper (plan §10, v2: 7 loops -> 1): every non-terminal row carries a
 *  deadline; this escalates staleness. Driven explicitly via system.tick() —
 *  no daemon: the deadline mechanics are the claim, not the timer. */

export function makeSweeper({ clock, payin, payout, gate }: { clock: Clock; payin: Payin; payout: Payout; gate: Gate }) {
  async function tick() {
    const now = clock.now();
    const payinSweep = payin.sweep(now);
    const payouts = await payout.sweep();
    const staleGates = gate.stale().length;
    return { payin: payinSweep, payouts, staleGates };
  }
  return { tick };
}

export type Sweeper = ReturnType<typeof makeSweeper>;
