# Payment Orchestrator

Multi-tenant payment orchestration system — UTR-claim pay-in matching, admin payouts with account selection and per-window limits, bank sync under locking, rate limits, and a statement-vs-payout gate. Event-driven modular monolith: Node.js 22 + TypeScript · MySQL 8 (source of truth) · Redis 7 (coordination + event streams).

- [PLAN.md](PLAN.md) — architecture & implementation plan (v2, reviewed by 4 independent agents: money-safety, simplicity/ops, requirements-fit, data-model)
- [PLAN-VISUAL.html](PLAN-VISUAL.html) — visual companion: open in a browser, click the tabs
