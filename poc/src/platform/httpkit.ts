/** httpkit.ts — the whole HTTP "framework" (plan: Fastify → POC: this file).
 *  No deps, no router library: an error type, JSON body reading, validators
 *  that throw HttpError(400, <field>), a JSON response helper, a URL splitter.
 *  Handlers are plain functions; everything else is guarded SQL. */

export class HttpError extends Error {
  constructor(public status: number, public code: string) {
    super(code);
  }
}

/** Parse the request body as JSON — a malformed body is a client error, never a crash. */
export async function readBody(req: Request): Promise<any> {
  try {
    return await req.json();
  } catch {
    throw new HttpError(400, "bad_json");
  }
}

/** Positive safe integer: every id and every money amount (integer paise). */
export function asPosInt(v: unknown, name: string): number {
  if (typeof v !== "number" || !Number.isSafeInteger(v) || v <= 0) throw new HttpError(400, name);
  return v;
}

/** Non-empty string within maxLen: client order ids, UTRs, idempotency keys. */
export function asString(v: unknown, name: string, maxLen = 64): string {
  if (typeof v !== "string" || v.length === 0 || v.length > maxLen) throw new HttpError(400, name);
  return v;
}

/** A JSON object — the required top level of every request body. */
export function asObj(v: unknown, name: string): Record<string, unknown> {
  if (typeof v !== "object" || v === null || Array.isArray(v)) throw new HttpError(400, name);
  return v as Record<string, unknown>;
}

export function json(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
}

/** URL → path segments + query; null only for a malformed URL (Bun never sends one). */
export function route(req: Request): { segments: string[]; query: URLSearchParams } | null {
  try {
    const url = new URL(req.url);
    return { segments: url.pathname.split("/").filter((s) => s !== ""), query: url.searchParams };
  } catch {
    return null;
  }
}
