# Azure Data Lakehouse — OTT Search Analytics Platform

> End-to-end data lakehouse on Azure analyzing user search behavior on a Vietnamese OTT/Internet-TV platform (FPT Play), featuring **incremental ingestion with Auto Loader**, **event-driven orchestration (file-arrival trigger)**, **ML-powered Vietnamese keyword normalization**, a **star-schema warehouse**, and a **full analytics & monitoring stack**.

**Ported from an AWS design to Azure** · 81K search events · 3-day window (June 2022)

---

## 📐 Architecture

```
                     ┌─────────────────────────────────────────────┐
                     │                AZURE CLOUD                   │
                     │                                              │
 Raw Parquet ──────▶ │  ADLS Gen2 (HNS)          Azure Databricks  │
 (batches drop       │  ┌──────────────┐        ┌───────────────┐  │
  into landing/)     │  │ landing/  ●──┼─trigger│ Bronze        │  │
                     │  │ bronze/      │ Auto   │   ▼           │  │
                     │  │ silver/      │ Loader │ Silver ◀── ML │  │
                     │  │ gold/        │        │   ▼           │  │
                     │  │ gold_export/ │◀───────│ Gold (star)   │  │
                     │  └──────┬───────┘        └───────────────┘  │
                     │         │                 Unity Catalog      │
                     │         ▼                                    │
                     │  Synapse Serverless SQL                      │
                     │  (OPENROWSET on Delta, UTF-8 collation)      │
                     └─────────┬────────────────────────────────────┘
                               │
              ┌────────────────┼──────────────────────┐
              ▼                ▼                       ▼
        Apache Superset   Grafana ◀── Prometheus ◀── Pushgateway
        (BI dashboards)   (pipeline monitoring)   ▲ (ngrok tunnel)
              [ local Docker stack ]              └── Databricks push
```

### Event-driven pipeline

A **Databricks Job** (`lakehouse_pipeline`) chains four notebook tasks and fires
automatically on **file arrival** in `landing/search_log/`:

```
new file lands  ──▶  bronze_ingest ──▶ silver ──▶ gold_build ──▶ push_metrics
(file-arrival        Auto Loader:      clean +     star schema    row counts →
 trigger)            new files only    ML MERGE    + export       Grafana
```

- Auto Loader's checkpoint guarantees **exactly-once file processing** — re-runs don't duplicate rows.
- `trigger(availableNow=True)` drains new files then stops: batch economics, streaming plumbing.
- Routine operation is fully hands-off; `00_cleanup` exists only for full resets.

**Medallion layers**

| Layer | Table | Rows | Purpose |
|---|---|---|---|
| Bronze | `customer_search` | 81,483 | Incremental append via Auto Loader, minimal typing |
| Silver | `customer_search_keynormalize` | 81,469 | Cleaned + ML-normalized keywords |
| Silver | `user_plans_map` | 14,778 | Exploded subscription plans |
| Gold | `fact_customer_search` + 6 dims + 1 bridge | 81,469 fact | Star schema for analytics |

---

## 🛠️ Tech Stack

| Concern | Technology | Notes |
|---|---|---|
| Storage | **ADLS Gen2** (Hierarchical Namespace) | Delta Lake format |
| Ingestion | **Databricks Auto Loader** (`cloudFiles`) | `trigger(availableNow)` — incremental batch |
| Orchestration | **Databricks Jobs** — file-arrival trigger | 4 chained tasks; see "why not Airflow" below |
| Compute | **Azure Databricks** (Runtime 17.3 LTS, single-node) | PySpark transforms |
| Governance | **Unity Catalog** | bronze/silver/gold schemas, External Location w/ Managed Identity |
| Warehouse | **Azure Synapse Serverless** | Query Delta directly via `OPENROWSET` — pay-per-scan, zero idle cost |
| BI | **Apache Superset** (Docker) | Custom image w/ MS ODBC Driver 18 |
| Monitoring | **Grafana + Prometheus + Pushgateway** (Docker) | Databricks pushes metrics through an ngrok tunnel |
| ML / NLP | **Qwen2.5-14B** (local, LM Studio) + **rapidfuzz** | Vietnamese keyword → canonical movie title |

> **Why not Airflow?** Considered and rejected: the pipeline lives entirely
> inside Databricks, so native Jobs with a file-arrival trigger cover
> scheduling, dependencies, and retries with zero extra infrastructure.
> Airflow earns its place when orchestrating *across* systems.
>
> **Infrastructure** was provisioned manually via the Azure Portal —
> IaC with Terraform is demonstrated in my separate CI/CD project.

---

## 📥 Incremental Ingestion (Auto Loader)

Bronze uses Auto Loader instead of manual batch splitting:

```python
spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", CHECKPOINT)
    .load(LANDING_PATH)
    ...
    .writeStream
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)      # process all new files, then stop
    .toTable("bronze.customer_search")
```

Why this over splitting a 50K batch into small chunks manually:

- **Small-files problem avoided** — Spark controls output file sizes; manual 1K-row micro-batches would create dozens of tiny Parquet files and bloat the Delta log.
- **Exactly-once semantics for free** — the checkpoint tracks processed files; re-runs are no-ops.
- **Pairs with the file-arrival trigger** — a new batch landing in `landing/` starts the job, and Auto Loader reads exactly that batch.

---

## 🤖 ML Keyword Normalization

Users type movie searches with typos, missing diacritics, and noise
(`"doremon"` → **Doraemon**, `"vượt bguc"` → **Vượt Ngục**). The pipeline maps raw keywords to canonical titles:

### Approach

1. **Pareto scoping** — keywords with freq ≥ 2 (7,646 unique) cover **~72% of traffic**; the 21K freq=1 long-tail is handled separately with cheap fuzzy matching.
2. **Local LLM classification** — Qwen2.5-14B-Instruct via LM Studio (OpenAI-compatible API, JSON-schema output, 4 worker threads) labels each keyword: *is it a movie? what's the canonical title? confidence?*
3. **Empirical confidence calibration** — a hand-reviewed stratified sample (104 rows) showed the model is **not well-calibrated** (weighted linear fit: `accuracy = 1.41·conf − 0.836`, **R² = 0.405**). Accuracy at the 0.85 confidence bucket was only **17.4%** vs **65.3%** at ≥ 0.95 → only mappings with **conf ≥ 0.95** are trusted; the rest are kept as-is (`low_confidence`).
4. **Fuzzy clustering** — rapidfuzz (threshold 92, with rules protecting sequels like *"Part 2"*) merges near-duplicate canonical titles: 3,173 → 3,006.
5. **Long-tail matching** — a rapidfuzz UDF in Databricks (Vietnamese diacritic stripping + min-length ≥ 6 guard) matches freq=1 keywords against the LLM-confirmed title list.

All normalization results are applied to Silver via **idempotent `MERGE`** keyed on
`keyword` + `_kw_match_method = 'raw'` — safe to re-run.

### Result (Silver distribution)

| `_kw_match_method` | Rows | Meaning |
|---|---|---|
| `llm_fuzzy_clustered` | 30,703 | LLM-confirmed movie, conf ≥ 0.95, clustered |
| `raw` | 27,162 | Long-tail, left as-is (demo scope) |
| `llm_not_movie` | 20,867 | Not a movie title (channels, apps, noise) |
| `rapidfuzz` | ~1,445 | Long-tail fuzzy-matched (4K-keyword demo run) |
| `low_confidence` | 1,292 | Suspected movie, conf < 0.95 — kept raw |

### Known limitations

- Overall raw LLM accuracy was **40.4%** — hence the strict threshold instead of blind trust.
- Hallucination on heavily misspelled Vietnamese; ambiguous titles (e.g. *"anh hùng kiên"*) can map to the wrong film.
- Fuzzy matching truncates episode/season qualifiers (`"fairy tail tập 225"` → *Fairy Tail*), acceptable for title-level analytics.

---

## ⭐ Gold Star Schema

```
                    dim_date
                        │
 dim_platform ──┐       │      ┌── dim_category
                ▼       ▼      ▼
            fact_customer_search ──── dim_network
                        │
                    dim_user (SCD Type 1)
                        │
                 bridge_user_plan  ◀── many-to-many
                        │
                 dim_subscription
```

- Surrogate keys throughout; `hour_of_day` on the fact enables intraday analysis.
- `dim_user` uses **SCD Type 1** (dataset spans 3 days; Type 2 documented as the evolution path).
- `bridge_user_plan` resolves the user ↔ subscription many-to-many with `first_seen / last_seen / event_count`.
- Gold is exported to a plain ADLS path (`gold_export/`) because Synapse cannot read Unity Catalog's internal `__unitystorage` layout.

---

## 📊 Dashboards

### Superset — "OTT Search Analytics" (6 charts)

| Chart | Insight |
|---|---|
| Top 20 Movies | Fairy Tail, Naruto, Doraemon dominate — anime is king |
| Searches by Hour | Trough at 3–4 AM, lunch bump 11–14h, **prime-time peak 20–21h** |
| Match Method Distribution | Visual proof of ML coverage |
| Device Type | Mobile ≈ 45% > SmartTV > Web |
| ISP | Viettel > VNPT > FPT |
| Subscription Plans | Promotion/giftcode dwarf direct purchases |

### Grafana — "Lakehouse Pipeline Monitoring"

Row counts per table/layer pushed **from Databricks** after each run — the
bronze→silver delta of exactly 14 rows (dropped malformed dates) is visible at a
glance. Total: **302,794 rows** across 11 tables.

---

## 🔧 Engineering Decisions & Trade-offs

| Decision | Rationale |
|---|---|
| **Auto Loader over manual batch splitting** | Incremental by file arrival, exactly-once via checkpoint; hand-chunking rows causes the small-files problem |
| **Databricks Jobs over Airflow** | Single-system pipeline — native file-arrival trigger covers it with zero extra infra |
| **ELT over ETL** | Raw preserved in Bronze → reprocessable; transforms run on Databricks compute |
| **PySpark instead of dbt** | Pipeline has ML steps (LLM, fuzzy UDF) dbt can't express; avoids over-engineering |
| **Synapse Serverless over Dedicated Pool** | Pay-per-TB-scanned, zero idle cost — right-sized for a portfolio on student credits |
| **Local LLM over paid APIs** | Gemini/OpenAI billing blocked → Qwen2.5-14B on RTX 4070 SUPER, free & private |
| **Threshold 0.95, not raw confidence** | Calibration analysis (R²=0.405) proved confidence can't be trusted as-is |
| **ngrok tunnel for metrics** | Databricks (cloud) can't reach a home PC behind CGNAT; production answer = Pushgateway in the same VNet (ACI) |

## 🐛 Notable Bugs Fixed (the fun part)

| Symptom | Root cause | Fix |
|---|---|---|
| `cannot be cast to TIMESTAMP` | Vietnamese locale AM/PM suffixes (`SA`/`CH`) mixed with 24h strings | regexp SA/CH→AM/PM + `coalesce(try_to_timestamp(24h), try_to_timestamp(12h))` |
| Dates in year **2565** | Buddhist Era timestamps | `add_months(-543*12)` in Silver |
| `input_file_name() not supported` | Unity Catalog restriction | `_metadata.file_path` |
| Vietnamese shows as `?` in Superset | **Synapse default collation CP1252** (not a driver issue — hours were spent on pymssql/FreeTDS first) | `ALTER DATABASE gold_db COLLATE Latin1_General_100_BIN2_UTF8` |
| `web-playfpt` classified as `unknown` device | `isin("web")` doesn't match compound platform names | `contains("web")` in dim_platform |
| Short keywords matching nonsense (`"fre"`→Free Fire) | Fuzzy matcher too eager | min-length ≥ 6 guard in UDF |
| MERGE updated 0 rows | Lazy temp view not materialized | `.cache()` + `.count()` before `MERGE` |

---

## 📁 Repository Layout

```
├── notebooks/
│   ├── 00_cleanup.ipynb             # one-time reset (tables + checkpoints)
│   ├── 01_bronze_autoloader.ipynb   # Auto Loader incremental ingest
│   ├── 02_silver.ipynb              # clean + ML MERGE + plans explode
│   ├── 03_gold.ipynb                # star schema + export
│   └── 04_push_metrics.ipynb        # observability
├── ml_normalization/                # local LLM pipeline + calibration analysis
├── docker/
│   ├── docker-compose.yml           # Superset, Grafana, Prometheus, Pushgateway
│   ├── Dockerfile.superset          # + MS ODBC Driver 18 / pyodbc
│   └── prometheus.yml
├── synapse/                         # DDL: credential, external data source, views
└── docs/screenshots/
```

---

## 🚀 Reproducing

1. **Azure**: ADLS Gen2 (enable HNS at creation — irreversible), Databricks workspace, Access Connector; grant `Storage Blob Data Contributor` to the connector's Managed Identity.
2. **Unity Catalog**: metastore → storage credential → external location → `CREATE SCHEMA ... MANAGED LOCATION`.
3. **Databricks Job**: 4 chained notebook tasks (`01 → 04`) with a file-arrival trigger on `landing/search_log/`. Drop a new Parquet batch there and the whole pipeline runs itself.
4. **Synapse**: create `gold_db`, **switch collation to UTF-8**, master key → MI credential → external data source → views.
5. **Local stack**: `docker compose up -d`, connect Superset to the Synapse serverless endpoint, import dashboards.
6. **Monitoring**: `ngrok http 9091`, set the tunnel URL in notebook 04, run it, watch Grafana light up.

---

*Built as a portfolio project on Azure for Students credits — every architectural choice optimizes for demonstrable engineering judgment per dollar.* 😄
