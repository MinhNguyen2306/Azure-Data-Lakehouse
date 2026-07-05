<div align="center">

# FPT Play Lakehouse — Azure Medallion Data Pipeline

An end-to-end **Azure lakehouse** that turns 81K raw OTT search-log records into a star-schema warehouse with BI dashboards and pipeline monitoring — built solo, from cloud infrastructure to visualization.

Data flows through a **medallion architecture** (Bronze → Silver → Gold) on Delta Lake, governed by Unity Catalog, with **ML-powered Vietnamese keyword normalization** in the Silver layer.

<br>

![Microsoft Azure](https://img.shields.io/badge/Microsoft%20Azure-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD4?style=for-the-badge&logo=delta&logoColor=white)
![Azure Synapse](https://img.shields.io/badge/Azure%20Synapse-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white)
![Apache Superset](https://img.shields.io/badge/Apache%20Superset-20A6C9?style=for-the-badge&logo=apache&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-F46800?style=for-the-badge&logo=grafana&logoColor=white)
![Prometheus](https://img.shields.io/badge/Prometheus-E6522C?style=for-the-badge&logo=prometheus&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

</div>

---

## Stack

| Layer | Technology |
|---|---|
| Object storage | ADLS Gen2 (Hierarchical Namespace) |
| Table format | Delta Lake |
| Catalog / governance | Unity Catalog (Managed Identity via Access Connector) |
| Processing | PySpark on Azure Databricks (Runtime 17.3 LTS) |
| Ingestion | Databricks Auto Loader (`cloudFiles`, incremental) |
| Orchestration | Databricks Jobs — **file-arrival trigger** |
| Warehouse | Azure Synapse Serverless SQL (`OPENROWSET` on Delta) |
| ML / NLP | Qwen2.5-14B (local, LM Studio) + rapidfuzz |
| Dashboard | Apache Superset |
| Monitoring | Grafana + Prometheus + Pushgateway |
| Containerization | Docker Compose (local analytics stack) |

---

## Architecture

<!-- TODO: replace with docs/screenshots/architecture.png -->
<img width="1326" alt="architecture" src="docs/screenshots/architecture.png" />

```
new file lands in landing/  ──▶  Databricks Job (file-arrival trigger)
     bronze_ingest ──▶ silver ──▶ gold_build ──▶ push_metrics
     Auto Loader:      clean +     star schema    row counts →
     new files only    ML MERGE    + export       Grafana
```

Key points: **Unity Catalog is not in the data path** — it governs where each Delta table lives and who can access it; the data itself sits on ADLS Gen2. **Synapse Serverless holds no data** — it queries the exported Delta files in place, billed per TB scanned.

---

## Medallion layers

**Bronze — raw capture (`notebooks/01_bronze_autoloader`)**
Auto Loader (`cloudFiles`) incrementally picks up new Parquet files from `landing/search_log/`, fixes a two-format datetime column (24h strings mixed with Vietnamese-locale 12h — `SA`/`CH` suffixes), adds `_ingested_at` / `_source_file`, and appends to `bronze.customer_search` (81,483 rows). The checkpoint guarantees exactly-once file processing.

**Silver — clean & normalize (`notebooks/02_silver`)**
Reads bronze, fixes Buddhist-Era timestamps (year 2565 → −543), drops 14 malformed-date rows, lowercases/trims `networkType`/`keyword`/`category`, then applies **ML keyword normalization via idempotent `MERGE`** (LLM mapping + rapidfuzz long-tail). A second branch explodes the `userPlansMap` array into `silver.user_plans_map` (14,778 rows).

**Gold — star schema (`notebooks/03_gold`)**
Builds `fact_customer_search` (81,469 rows, incl. `hour_of_day`) plus `dim_date`, `dim_platform` (device-type classification), `dim_category`, `dim_network`, `dim_user` (35,038 rows, SCD Type 1), `dim_subscription` (35), and the many-to-many `bridge_user_plan` (8,437). Exports all tables to `gold_export/` — Synapse cannot read Unity Catalog's internal `__unitystorage` layout.

**Serving (`synapse/`)**
Synapse Serverless views (`dbo.*`) wrap `OPENROWSET(FORMAT='DELTA')` over the export path. Database collation is switched to `Latin1_General_100_BIN2_UTF8` — the default CP1252 collation renders Vietnamese as `?`.

**Observability (`notebooks/04_push_metrics`)**
Pushes per-table row counts from Databricks → ngrok tunnel → Pushgateway → Prometheus → Grafana. The bronze→silver delta of exactly 14 rows is visible on the dashboard.

---

## Incremental loading (Auto Loader)

Bronze ingests by **file arrival** rather than manual batch splitting. Auto Loader's checkpoint stores which files were processed; each run ingests only new files, and `trigger(availableNow=True)` drains the queue then stops — batch economics with streaming plumbing.

```python
spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", CHECKPOINT)
    .load(LANDING_PATH)
    .writeStream
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .toTable("bronze.customer_search")
```

> **Note:** hand-chunking a large batch into small row groups was considered and rejected — it creates the small-files problem (dozens of tiny Parquet files, bloated Delta log) while Auto Loader gives exactly-once semantics for free. Re-running the notebook does not duplicate rows.

---

## ML keyword normalization

Users type movie searches with typos and missing diacritics (`"doremon"` → **Doraemon**, `"vượt bguc"` → **Vượt Ngục**). The Silver layer maps raw keywords to canonical titles:

1. **Pareto scoping** — keywords with freq ≥ 2 (7,646 unique) cover ~72% of traffic; the 21K freq=1 long-tail is handled separately.
2. **Local LLM classification** — Qwen2.5-14B via LM Studio (JSON-schema output, 4 worker threads) labels each keyword: movie or not, canonical title, confidence.
3. **Confidence calibration** — a hand-reviewed stratified sample showed the model is *not well-calibrated* (weighted fit `accuracy = 1.41·conf − 0.836`, R² = 0.405). Accuracy at the 0.85 bucket: **17.4%**; at ≥ 0.95: **65.3%** → only conf ≥ 0.95 mappings are trusted.
4. **Fuzzy clustering** — rapidfuzz (threshold 92, sequel-protection rules) merges near-duplicate canonical titles: 3,173 → 3,006.
5. **Long-tail matching** — rapidfuzz UDF in Databricks with Vietnamese diacritic stripping and a min-length ≥ 6 guard.

| `_kw_match_method` | Rows | Meaning |
|---|---|---|
| `llm_fuzzy_clustered` | 30,703 | LLM-confirmed movie, conf ≥ 0.95, clustered |
| `raw` | 27,162 | Long-tail, left as-is (demo scope) |
| `llm_not_movie` | 20,867 | Not a movie title |
| `rapidfuzz` | ~1,445 | Long-tail fuzzy-matched |
| `low_confidence` | 1,292 | Suspected movie, conf < 0.95 — kept raw |

Known limits: raw LLM accuracy was 40.4% (hence the strict threshold); heavy misspellings can hallucinate; episode qualifiers are truncated (`"fairy tail tập 225"` → *Fairy Tail*).

---

## Data quality notes

- **Buddhist-Era timestamps** — 108 rows landed in year 2565 (Thai BE calendar); corrected by −543 years in Silver.
- **Vietnamese-locale datetimes** — `SA`/`CH` (AM/PM) suffixes mixed with 24h strings in the same column; parsed with a two-format `coalesce(try_to_timestamp(...))`.
- **14 unparseable rows** (years 0004/0034/NULL) are dropped at Silver — the bronze→silver delta on the Grafana dashboard is the visible audit trail.
- **`web-playfpt` device classification** — compound platform names broke an `isin("web")` check; fixed with `contains("web")` in `dim_platform`.

---

## Project structure

```
azure-lakehouse-fptplay/
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
├── docs/screenshots/
└── README.md
```

---

## Quick start

**1. Azure infrastructure (Portal)**
ADLS Gen2 storage account (**enable HNS at creation — irreversible**) → container `lakehouse` with `landing/ bronze/ silver/ gold/` → Databricks workspace (Premium) + Access Connector → grant `Storage Blob Data Contributor` to the connector's Managed Identity.

**2. Unity Catalog**
Metastore → storage credential (Access Connector) → external location (`abfss://lakehouse@...`) → `CREATE SCHEMA bronze/silver/gold ... MANAGED LOCATION`.

**3. Databricks Job (event-driven pipeline)**
Create job `lakehouse_pipeline` with 4 chained notebook tasks (`01 → 04`) and a **file-arrival trigger** on `landing/search_log/`. Drop a Parquet batch there — the whole pipeline runs itself.

**4. Synapse Serverless**
```sql
CREATE DATABASE gold_db;
ALTER DATABASE gold_db COLLATE Latin1_General_100_BIN2_UTF8;  -- Vietnamese!
-- then: master key → MI credential → external data source → dbo.* views
```

**5. Local analytics stack**
```bash
docker compose build superset   # custom image: ODBC Driver 18 + pyodbc
docker compose up -d
```
Connect Superset to the Synapse serverless endpoint (`mssql+pymssql://...`), build datasets from `dbo.*` views.

**6. Monitoring**
```bash
ngrok http 9091                 # expose Pushgateway
# set the tunnel URL in notebooks/04_push_metrics, run it
```

---

## Service endpoints

| Service | URL | Login |
|---|---|---|
| Superset | http://localhost:8088 | admin / admin |
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | — |
| Pushgateway | http://localhost:9091 | — |
| Synapse Serverless | `lakehouse-synapse-ondemand.sql.azuresynapse.net` | SQL admin |

---

## Dashboards

**Superset — "OTT Search Analytics" (6 charts)**

| Chart | Insight |
|---|---|
| Top 20 Movies | Fairy Tail, Naruto, Doraemon dominate — anime is king |
| Searches by Hour | Trough 3–4 AM, lunch bump 11–14h, **prime-time peak 20–21h** |
| Match Method Distribution | Visual proof of ML coverage |
| Device Type | Mobile ≈ 45% > SmartTV > Web |
| ISP | Viettel > VNPT > FPT |
| Subscription Plans | Promotion/giftcode dwarf direct purchases |

**Grafana — "Lakehouse Pipeline Monitoring"**
Per-table row counts pushed from Databricks after each run — 302,794 rows across 11 tables, with the 14-row bronze→silver drop visible at a glance.

---

## Design decisions & trade-offs

This is a portfolio build on Azure for Students credits; choices favor demonstrable
engineering judgment per dollar, with a clear production upgrade path:

| Decision | Rationale | Production upgrade |
|---|---|---|
| Auto Loader over manual batch splitting | Exactly-once via checkpoint; avoids small-files problem | (keep — correct pattern) |
| Databricks Jobs over Airflow | Single-system pipeline; native file-arrival trigger, zero extra infra | Airflow/Dagster when orchestrating *across* systems |
| PySpark over dbt | ML steps (LLM, fuzzy UDF) don't fit dbt's SQL model | dbt for the SQL-only Gold marts |
| Synapse Serverless over Dedicated Pool | Pay-per-TB-scanned, zero idle cost | Dedicated pool / Fabric for heavy concurrency |
| Local LLM over paid APIs | Billing blocked in region; Qwen2.5-14B on RTX 4070 SUPER is free & private | Hosted LLM API with batch endpoint |
| Confidence threshold 0.95 | Calibration analysis (R²=0.405) proved raw confidence untrustworthy | Larger labeled set → recalibration / fine-tune |
| ngrok tunnel for metrics | Home PC sits behind CGNAT — Databricks can't reach it | Pushgateway on ACI in the same VNet |
| SCD Type 1 on dim_user | Dataset spans 3 days | SCD Type 2 with change tracking |

---

## Roadmap / future work

- **Process the remaining long-tail** — 27K `raw` keywords await a full rapidfuzz pass (demo ran 4K).
- **SCD Type 2** on `dim_user` once multi-week history exists.
- **Pushgateway on Azure Container Instances** — replace the ngrok tunnel with an in-VNet endpoint.
- **Data quality gates** — Delta constraints / expectations between layers, alerting via Grafana.
- **CI/CD for notebooks** — Databricks Asset Bundles; IaC with Terraform is demonstrated in my separate CI/CD project.

---

## Notes

Built end-to-end by a single developer to understand the full data lifecycle on Azure —
from storage and governance through incremental processing, ML enrichment,
dimensional modeling, serving, and observability. Every bug in the
"data quality notes" section was hit and fixed for real: Vietnamese locale
datetimes, Buddhist-Era calendars, and a CP1252 collation that spent hours
disguised as a driver problem.
