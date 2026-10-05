# main\_pipeline

This folder defines all source code for the `main_pipeline` Lakeflow Spark Declarative Pipeline.

## Pipeline Settings

| Setting | Value |
| --- | --- |
| Catalog | `rearc` |
| Schema | `raw` |
| Compute | Serverless + Photon |
| Libraries glob | `transformations/**` |

Datasets publish to `rearc.<schema>.<table>` (e.g., `rearc.bronze.series`, `rearc.silver.all_data`, `rearc.gold.population_stats_2013_2018`).

## Directory Structure

```
main_pipeline/
├── transformations/
│   ├── bronze/        # Auto Loader ingestion (streaming tables)
│   ├── silver/        # Cleansing + de-duplication (materialized views)
│   └── gold/          # Analytical outputs (materialized views)
├── Alternative_pipelines/  # SQL/Python alternatives — excluded from glob
├── explorations/           # Ad-hoc analysis notebooks
└── README.md
```

## Medallion Architecture

### Bronze — Raw Ingestion

Bronze tables are **streaming tables** that use Auto Loader (`cloudFiles`) to incrementally ingest files from UC volumes as they arrive.

* **`bronze_bls.py`** — 10 streaming tables for BLS Productivity & Costs data. Each reads tab-delimited CSV files with `cloudFiles.format=csv`, `sep=\t`, `header=true`, and `inferColumnTypes=false` (every column is a string; silver does the typing) from `/Volumes/rearc/raw/volume/bls/pr/`. Each file's header is checked against the expected columns (trimmed, case-insensitive) before columns are renamed to clean names, and `_metadata.file_name` is captured as `source_file` for downstream de-duplication. `expect_all_or_fail` expectations reject rescued data and malformed keys, years, periods and values.
* **`bronze_population.py`** — 1 streaming table for DataUSA population data. Reads multi-line JSON (`cloudFiles.format=json`, `multiLine=true`) from `/Volumes/rearc/raw/volume/datausa` with an explicit schema (`annotations`, `columns`, `data`, `page`). Fields outside the schema are rescued, and expectations fail the update on rescued data, an empty `data` array, or an unexpected column list.

### Silver — Cleansing & De-duplication

Silver tables are **materialized views** that clean, type-cast, and de-duplicate bronze data.

* **`silver_bls.py`** — 10 materialized views. Each applies `TRIM` to string columns, casts numeric columns, and de-duplicates by keeping only the row from the **latest source file**. The de-duplication logic works by:
  1. Parsing a timestamp from the source filename using `regexp_extract(source_file, '_([0-9]{8}_[0-9]{6})$', 1)` with format `yyyyMMdd_HHmmss`.
  2. Using `row_number() OVER (PARTITION BY <key_cols> ORDER BY _source_ts DESC)` to rank rows per key.
  3. Keeping only `rank = 1` (the row from the most recent file).

  This ensures that when a source file is re-ingested or a newer version arrives, only the latest version's rows are retained in silver.

* **`silver_population.py`** — 1 materialized view. Explodes the nested `data` array from the bronze JSON into individual rows, extracting `Nation`, `Nation ID`, `Population`, and `Year` fields with type casting.

### Gold — Analytical Outputs

Gold tables are **materialized views** written in SQL.

* **`Q1.sql`** — `rearc.gold.population_stats_2013_2018`: Mean and standard deviation of annual US population (2013–2018).
* **`Q2.sql`** — `rearc.gold.productivity_costs_index`: Highest annual total per BLS series (summing quarters Q01–Q04; Q05 is the annual average and is excluded) with dimension lookups (seasonal, sector, class, measure, duration). Parses `series_id` into component codes via `SUBSTRING` and joins with silver lookup tables.
* **`Q3.sql`** — `rearc.gold.series_population`: BLS series `PRS30006032` (Q01 period) joined with US population by year.

## Alternative Pipelines

The `Alternative_pipelines/` folder contains SQL and Python alternative implementations of the bronze, silver, and gold datasets:

* `(Alternative) bronze_bls.sql` / `bronze_population.sql` — SQL Auto Loader versions
* `(Alternative) silver_bls.sql` / `silver_population.sql` — SQL materialized view versions
* `(Alternative) Q1.py` / `Q2.py` / `Q3.py` — Python materialized view versions

These files are **excluded from the pipeline libraries glob** (`transformations/**` only covers the `transformations/` folder, not `Alternative_pipelines/`). This prevents duplicate dataset definitions from interfering with dry runs and pipeline updates. To switch to an alternative implementation, update the pipeline's `libraries` setting to point at the desired folder.

## Getting Started

To get started, go to the `transformations` folder — most of the relevant source code lives there:

* By convention, every dataset under `transformations` is in a separate file.
* Use `Run file` to run and preview a single transformation.
* Use `Run pipeline` to run _all_ transformations in the entire pipeline.
* Use `+ Add` in the file browser to add a new data set definition.
* Use `Schedule` to run the pipeline on a schedule!

For more tutorials and reference material, see https://docs.databricks.com/ldp.