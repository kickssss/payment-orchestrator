import { insert, one, run, type Db } from "./db.ts";

/** Reviews (plan §4): one OPEN review per (kind, ref) — the partial unique
 *  index is the backstop; duplicate escalations no-op onto the existing row.
 *  Resolutions are guarded (status = 'open' required) so a double resolve is a
 *  no-op; every money-moving effect additionally sits behind the referenced
 *  entity's own conditional transition (the real dedupe). */

export type ReviewKind =
  | "utr_mismatch"
  | "utr_reuse"
  | "no_txn_found"
  | "late_claim"
  | "unknown_credit"
  | "unknown_debit"
  | "payout_ambiguous"
  | "reversal_overdraw";

export function createReview(
  db: Db,
  e: { tenantId: number; kind: ReviewKind; refType: string; refId: number; payload: unknown; suggestedRefId?: number | null; at: number }
): { reviewId: number; created: boolean } {
  try {
    const reviewId = insert(
      db,
      "INSERT INTO reviews (tenant_id, kind, ref_type, ref_id, payload, status, suggested_ref_id, created_at) VALUES (?, ?, ?, ?, ?, 'open', ?, ?)",
      e.tenantId,
      e.kind,
      e.refType,
      e.refId,
      JSON.stringify(e.payload),
      e.suggestedRefId ?? null,
      e.at
    );
    return { reviewId, created: true };
  } catch (err) {
    if (
      err instanceof Error &&
      (err as { code?: string }).code === "SQLITE_CONSTRAINT_UNIQUE" &&
      err.message.includes("reviews.kind")
    ) {
      const existing = one<{ id: number }>(
        db,
        "SELECT id FROM reviews WHERE kind = ? AND ref_type = ? AND ref_id = ? AND status = 'open'",
        e.kind,
        e.refType,
        e.refId
      );
      return { reviewId: existing!.id, created: false };
    }
    throw err;
  }
}

export function resolveReviewRow(db: Db, reviewId: number, action: string): boolean {
  return run(db, "UPDATE reviews SET status = 'resolved', resolved_action = ? WHERE id = ? AND status = 'open'", action, reviewId) === 1;
}
