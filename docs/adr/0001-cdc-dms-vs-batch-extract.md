# ADR 0001 — CDC via AWS DMS vs. nightly batch extract

- **Status:** Accepted
- **Date:** 2026-10-03
- **Deciders:** Lead Data Engineer (portfolio)

## Context
The legacy wagering/settlement system runs on SQL Server with high transaction
volume. We must land source data in S3 Bronze for the lakehouse. Two options:
nightly full/batch extracts, or continuous change data capture (CDC).

## Decision
Use **AWS DMS CDC** for the transactional tables (customers, wagers, settlements)
and keep **batch** only for small, slowly‑changing reference data.

## Rationale
- **Freshness:** settlement reconciliation benefits from near‑continuous capture;
  nightly batch widens the window where legacy and modern diverge.
- **Load on source:** CDC reads the transaction log, lighter than repeated full
  scans of large bet tables.
- **Replayability:** Bronze stays append‑only; DMS LSN/commit timestamps give a
  deterministic watermark for point‑in‑time reconciliation (see ADR‑0003).

## Consequences
**Positive:** fresher data, lower source impact, clean watermark for parity.
**Negative / trade‑offs:**
- DMS ongoing replication adds operational surface (task monitoring, DDL handling).
- Schema drift must be handled explicitly in Bronze.
- Streaming odds use **Kinesis/MSK**, a separate path — two ingestion patterns to
  operate (batch CDC + stream).

## Alternatives considered
- **Nightly batch only:** simplest, but stale and heavy on the source.
- **Trigger‑based CDC in SQL Server:** invasive to the legacy system; rejected.
