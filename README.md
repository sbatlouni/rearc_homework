# Rearc Data Quest: Databricks Edition

This repo pulls the BLS productivity time series (`pr`) and DataUSA population data into a Databricks Volume. It then models them as a Bronze/Silver/Gold Spark Declarative Pipeline that answers three analytical questions. Everything is deployed with a Databricks Asset Bundle.

- **Design, trade-offs and retrospective:** [PROCESS.md](PROCESS.md)
- **Answers:** [Screenshots/Answers.html](Screenshots/Answers.html). Download it and open it in a browser; GitHub shows `.html` files as source. The headline numbers are also in [PROCESS.md](PROCESS.md#results).

## Where to find everything

| What | Where |
|---|---|
| Bundle definition (workspace target) | [databricks.yml](databricks.yml) |
| Job and pipeline definitions | [resources/](resources/): [init.job.yml](resources/init.job.yml), [rearc_primary_job.job.yml](resources/rearc_primary_job.job.yml), [main_pipeline.pipeline.yml](resources/main_pipeline.pipeline.yml) |
| **Step 1: Sourcing** | |
| One-time setup: catalog, schemas, volume, metadata table | [src/rearc_primary_job/CREATE NOTEBOOK.ipynb](src/rearc_primary_job/CREATE%20NOTEBOOK.ipynb) |
| BLS ingestion (new or changed files only, versioned) | [src/rearc_primary_job/BLS INGESTION.ipynb](src/rearc_primary_job/BLS%20INGESTION.ipynb) |
| Population API ingestion | [src/rearc_primary_job/POPULATION INGESTION.ipynb](src/rearc_primary_job/POPULATION%20INGESTION.ipynb) |
| **Step 2: Pipeline** | |
| Bronze: Auto Loader streaming tables, with schema expectations | [src/main_pipeline/transformations/bronze/](src/main_pipeline/transformations/bronze/) |
| Silver: cleaned, typed, deduplicated materialized views | [src/main_pipeline/transformations/silver/](src/main_pipeline/transformations/silver/) |
| Gold: the three answers (primary, Spark SQL) | [src/main_pipeline/transformations/gold/](src/main_pipeline/transformations/gold/): [Q1.sql](src/main_pipeline/transformations/gold/Q1.sql), [Q2.sql](src/main_pipeline/transformations/gold/Q2.sql), [Q3.sql](src/main_pipeline/transformations/gold/Q3.sql) |
| Documented alternatives (gold in PySpark, bronze and silver in SQL) | [src/main_pipeline/Alternative_pipelines/](src/main_pipeline/Alternative_pipelines/) |
| Pipeline reference (tables, dedup logic, alternatives) | [src/main_pipeline/README.md](src/main_pipeline/README.md) |
| **Results** | |
| Notebook that queries the gold tables | [notebooks/Answers.ipynb](notebooks/Answers.ipynb) |
| Exported results and screenshots | [Screenshots/](Screenshots/) |

### The three gold tables

| Question | Table | SQL (primary) | PySpark (alternative) |
|---|---|---|---|
| Mean and standard deviation of the US population, 2013–2018 | `rearc.gold.population_stats_2013_2018` | [Q1.sql](src/main_pipeline/transformations/gold/Q1.sql) | [(Alternative) Q1.py](src/main_pipeline/Alternative_pipelines/(Alternative)%20Q1.py) |
| Best year per `series_id`, with readable labels | `rearc.gold.productivity_costs_index` | [Q2.sql](src/main_pipeline/transformations/gold/Q2.sql) | [(Alternative) Q2.py](src/main_pipeline/Alternative_pipelines/(Alternative)%20Q2.py) |
| `PRS30006032` `Q01` value by year, with population | `rearc.gold.series_population` | [Q3.sql](src/main_pipeline/transformations/gold/Q3.sql) | [(Alternative) Q3.py](src/main_pipeline/Alternative_pipelines/(Alternative)%20Q3.py) |

## Running it

### Prerequisites

- A Databricks workspace with Unity Catalog and serverless compute. The jobs and pipeline run on serverless.
- Permission to create a catalog. Everything is created in a catalog named `rearc`.
- The [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) with bundle support, authenticated to that workspace (`databricks auth login --host <workspace-url>`).

### 1. Configure

1. In [databricks.yml](databricks.yml), set `targets.dev.workspace.host` to your workspace URL.
2. In [BLS INGESTION.ipynb](src/rearc_primary_job/BLS%20INGESTION.ipynb), set `bls_email` to your own address. BLS blocks requests whose User-Agent has no contact details (that's the 403), and the contact should be the person running the job.

### 2. Deploy

```bash
databricks bundle validate
databricks bundle deploy
```

This creates two jobs, `init` and `rearc_primary_job`, and the `main_pipeline` pipeline. In development mode, the deployed names are prefixed with `[dev <your user>]`.

### 3. Set up the catalog (once)

```bash
databricks bundle run init
```

This creates the `rearc` catalog, the `raw`, `bronze`, `silver` and `gold` schemas, the volume `rearc.raw.volume`, and the table `rearc.raw.bls_metadata`. Every statement is `IF NOT EXISTS`, so running it again does nothing.

### 4. Ingest and build the tables

```bash
databricks bundle run rearc_primary_job
```

The job runs three tasks:

1. **BLS_Ingestion:** downloads the BLS files that are new or have changed since the last run.
2. **Population_ingestion:** saves the population API response. It runs in parallel with BLS ingestion.
3. **Update_main_pipeline:** refreshes bronze, silver and gold. It starts once both ingestion tasks have succeeded.

The job is scheduled daily at 1:00 AM America/New_York. Development-mode deployments normally pause schedules, so run it manually as above, or unpause it in the workspace.

### 5. Look at the results

Open [notebooks/Answers.ipynb](notebooks/Answers.ipynb) in the workspace and run it, or query the gold tables directly:

```sql
SELECT * FROM rearc.gold.population_stats_2013_2018;
SELECT * FROM rearc.gold.productivity_costs_index;
SELECT * FROM rearc.gold.series_population ORDER BY year;
```

### Re-running

Running `rearc_primary_job` again is safe:

- BLS files are only downloaded when they're new or their `last_modified` has changed.
- Auto Loader only ingests files bronze hasn't seen.
- Silver keeps the newest version of each row.

If the pipeline code changes the bronze schema, run a **full refresh** of `main_pipeline` from the pipeline page. The raw files stay in the volume, so bronze rebuilds from them.

## Where the data lands

| Data | Location |
|---|---|
| BLS raw files, one folder per file, with every version kept | `/Volumes/rearc/raw/volume/bls/pr/<file_name>/<file_name>_<YYYYMMDD_HHMMSS>` |
| Population raw JSON, one file per fetch | `/Volumes/rearc/raw/volume/datausa/population/population_<YYYYMMDD_HHMMSS>.json` |
| BLS ingestion state (one row per file) | `rearc.raw.bls_metadata` |
| Pipeline tables | `rearc.bronze.*`, `rearc.silver.*`, `rearc.gold.*` |
