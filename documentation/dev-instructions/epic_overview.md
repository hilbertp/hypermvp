# HyperMVP – Development Backlog (Markdown‑Friendly)

This backlog converts the technical, functional, and algorithmic specs into **Epics** and **User Stories** with clear acceptance criteria – all without HTML line‑breaks or tables.

---

## Epic 1 – Provider Data Pipeline

*Provide reliable, deduplicated, and price‑normalised provider bids in DuckDB on rolling 4‑hour intervals.*

* **P‑01** — *As a data engineer* I want the system to load only `.xlsx` files from the provider input folder so that inconsistent formats never enter the pipeline.

  * Files without `.xlsx` extension are rejected with a logged error.
  * Successful loads are logged with timestamp and row count.
* **P‑02** — *As the pipeline* I need to validate required columns and drop rows whose `PRODUCT` begins with `POS_` so that only negative aFRR bids are processed.

  * Missing columns raise a descriptive exception.
  * Rows with `POS_*` are absent from the output dataset.
* **P‑03** — *As the pipeline* I must normalise `ENERGY_PRICE_[EUR/MWh]` by making prices negative when `PAYMENT_DIRECTION = PROVIDER_TO_GRID` and leaving them positive otherwise.

  * For each transformed row, sign matches rule.
  * Unit tests cover both directions.
* **P‑04** — *As the scheduler* I want cleaned bids grouped into 4‑hour buckets (00‑04, 04‑08, …) so that downstream analytics use consistent time slices.

  * Grouping uses `DELIVERY_DATE` start hour modulo 4.
  * Each group’s start/end timestamps are stored.
* **P‑05** — *As the database layer* I need to delete existing rows for a 4‑hour period before inserting the refreshed dataset so that duplicates cannot occur.

  * Upsert transaction deletes then inserts within a single commit.
  * Idempotent re‑imports produce identical row counts.

---

## Epic 2 – aFRR Data Pipeline

*Maintain a single, correct adjustment delta per 15‑minute interval for negative aFRR.*

* **A‑01** — *As a data engineer* I want to load raw aFRR CSV files and convert the `Datum` column to ISO‑8601 datetime so that temporal joins are accurate.

  * Datetime includes timezone.
  * Invalid dates raise errors.
* **A‑02** — *As the cleaner* I must trim column names, select `Datum`, `von`, `bis`, `50Hertz (Negativ)` and filter to negative deltas only.

  * Output contains exactly four columns.
  * No positive values remain.
* **A‑03** — *As the persister* I need to overwrite an existing delta when re‑importing the same 15‑minute interval so that corrections propagate historically.

  * Primary key on interval guarantees one row.
  * Subsequent import replaces value, not duplicates.

---

## Epic 3 – DuckDB Storage & Governance

*Provide a single source of truth for provider bids and aFRR adjustments.*

* **D‑01** — *As a data analyst* I want both pipelines to persist to DuckDB tables with defined schemas so that queries are performant and self‑documented.

  * DDL executed via migrations.
  * Column comments describe units.
* **D‑02** — *As the system* I must add ingest and validity timestamps plus audit columns to each table so that data lineage is transparent.

  * Columns `ingested_at`, `effective_from`, `effective_to` exist and are populated.

---

## Epic 4 – Data Alignment & Integration

*Merge provider bids with aFRR demand for unified analysis.*

* **I‑01** — *As the integration job* I need to join provider buckets (4 h) to aFRR deltas (15 min) by overlapping time ranges so that each delta sees the full bid stack valid during its interval.

  * Join logic validated with edge‑case unit tests.
  * Resulting view lists bids active at interval end.
* **I‑02** — *As a data scientist* I want a materialised view exposing joined data for algorithmic pricing so that analytics does not recompute joins ad‑hoc.

  * View refreshes nightly or on data change.
  * Query returns in < 500 ms for a one‑day window.

---

## Epic 5 – Marginal Price Calculation Engine

*Compute the highest activated bid price per 15‑minute interval using merit‑order stacking.*

* **M‑01** — *As the engine* I need to sort bids by adjusted price (negative first) within each interval so that the cheapest energy is activated first.

  * Sorting accounts for `PAYMENT_DIRECTION` rule.
* **M‑02** — *As the engine* I must accumulate `offered_mw` until cumulative ≥ demand (`delta_mw`) and capture the price of the last activated bid as `marginal_price`.

  * Unit test reproduces example (–3.87 EUR/MWh at 80.528 MW).
* **M‑03** — *As the analyst* I want the algorithm to support what‑if scenarios with modified demand so that pricing strategy can be simulated.

  * Demand input parameterised.
  * Outputs marginal‑price graph.

---

## Epic 6 – Validation & Reporting

*Guarantee data quality and share insights.*

* **V‑01** — *As QA* I need automated tests that compare row counts and primary‑key uniqueness after each load.

  * CI pipeline fails on any anomaly.
* **V‑02** — *As management* I want daily reports of marginal prices, outliers, and pipeline health.

  * Report delivered 08:00 CET via email & dashboard.

---

## Epic 7 – API & Self‑Service Query Layer

*Expose curated data and metrics to external consumers.*

* **Q‑01** — *As a developer* I want a REST/GraphQL endpoint to fetch marginal prices for a date range so that other services can use them.

  * Endpoint responds ≤ 300 ms for a one‑month window.
  * Swagger/OpenAPI docs auto‑generated.
* **Q‑02** — *As a BI analyst* I need read‑only DuckDB access with role‑based auth so that I can build custom dashboards.

  * Connection via ODBC.
  * Roles `analyst`, `engineer` created.

---

## Epic 8 – Monitoring & Observability *(Stretch Goal)*

*Detect and react to anomalies in data flow and algorithm outputs.*

* **O‑01** — *As SRE* I want pipeline DAGs instrumented with metrics (latency, row counts, error rate) so that alerts trigger on deviations.

  * Prometheus metrics exported.
  * Alert rules defined in Grafana.

---

## Epic 9 – Documentation & Knowledge Transfer

*Keep the team aligned and onboard newcomers fast.*

* **K‑01** — *As a new developer* I need up‑to‑date README and code comments explaining each module so that I can contribute within one day.

  * Docs built with MkDocs.
  * Pipeline diagrams embedded.
