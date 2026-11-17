/** Injectable clock: every timestamp in the POC comes from here, so deadline
 *  tests time-travel deterministically instead of sleeping. Never use SQLite's
 *  own clock (second granularity, untestable). */

export type Clock = {
  now(): number;
  setNow(ms: number): void;
  advance(ms: number): void;
};

export function createClock(startMs: number = Date.now()): Clock {
  let t = startMs;
  return {
    now: () => t,
    setNow: (ms: number) => {
      t = ms;
    },
    advance: (ms: number) => {
      t += ms;
    },
  };
}
