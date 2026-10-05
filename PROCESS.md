# Process

How I built the Rearc Data Quest (Databricks Edition): the architecture, the trade-offs I made, and what was hardest to get right.

## Overview

```
BLS pr/ folder ──┐                                  ┌─ bronze (10 streaming tables) ─┐
                 ├─ ingestion notebooks ─► Volume ──┤                                ├─ silver (11 MVs) ─► gold (3 MVs)
DataUSA API ─────┘   (rearc_primary_job)            └─ bronze.population ────────────┘
```

| Layer | What it holds | Where |
|---|---|---|
| Raw | Source files exactly as downloaded, one folder per file, with each version kept | `/Volumes/rearc/raw/volume/` |
| Raw metadata | One row per BLS file: its `last_modified`, size and URL as of the last successful download | `rearc.raw.bls_metadata` |
| Bronze | Raw rows plus the name of the file they came from | `rearc.bronze.*` |
| Silver | Trimmed, typed, deduplicated rows with non-null keys enforced | `rearc.silver.*` |
| Gold | The three answers | `rearc.gold.*` |

Everything is deployed with a Databricks Asset Bundle ([databricks.yml](databricks.yml), [resources/](resources/)):

- `init` creates the catalog, schemas, volume and metadata table. Run it once.
- `rearc_primary_job` runs BLS ingestion and population ingestion in parallel, then refreshes `main_pipeline`.

## Architecture

### Step 1: Sourcing

**BLS.** [BLS INGESTION.ipynb](src/rearc_primary_job/BLS%20INGESTION.ipynb) reads the `pr/` directory listing instead of a hard-coded list of files. The page is an IIS listing: every file has its own line with a date, time, size and link. I parse each line into `file_name`, `last_modified`, `size_bytes` and `url`. Every request sends a User-Agent with my contact email, which is what BLS's access policy asks for and is what avoids the 403.

Re-runs are safe because of three choices:

1. **A file is only downloaded when it's new or changed.** The notebook reads `rearc.raw.bls_metadata` once into a `{file_name: last_modified}` dictionary. It downloads a file only if the name isn't there yet, or if the listing's `last_modified` is later than the stored one. Unchanged files are skipped, so a re-run with no source changes downloads nothing.
2. **Every version is kept.** Each file is saved as `<file_name>/<file_name>_<YYYYMMDD_HHMMSS>`, where the timestamp is the source's `last_modified`. An updated file becomes a new file next to the old one rather than replacing it. This gives an audit trail, and it lets Auto Loader treat each version as a new file.
3. **Metadata is written only after a download succeeds.** After the downloads, a single `MERGE` on `file_name` updates `bls_metadata`, and only for files that downloaded successfully. If a download fails, its old `last_modified` stays in the table, so the next run picks the file up again. The notebook raises an error at the end if anything failed, so the job run shows as failed rather than silently partial.

**Population.** [POPULATION INGESTION.ipynb](src/rearc_primary_job/POPULATION%20INGESTION.ipynb) calls the DataUSA API, checks that the response is valid JSON, and saves it as `datausa/population/population_<fetch time UTC>.json`. The API has no last-modified date, so I timestamp each copy with the fetch time.

The two ingestion notebooks are separate job tasks, so a BLS failure doesn't stop the population fetch.

### Step 2: The pipeline

**Bronze: streaming tables with Auto Loader.** There is one streaming table per BLS file type, plus one for population. Auto Loader tracks which files it has already read, so each pipeline update only ingests new file versions and never re-reads old ones. Each row keeps `_metadata.file_name` as `source_file`, because silver needs it to tell versions apart. Bronze stays close to raw: the only changes are tidier column names.

**Silver: materialized views.** Silver is where the data gets cleaned:

- strings are `TRIM`med, because BLS pads its fields with whitespace and joins fail otherwise
- columns are cast to proper types
- `-` placeholders are turned into nulls
- duplicate rows are removed

Because every version of a file lands in bronze, the same key can appear several times. Silver reads the timestamp from the source file name and keeps the row from the newest version of each key. Population works the same way, keyed on `(nation_id, year)`. Expectations with `expect_or_fail` enforce non-null keys on every silver table. I chose *fail* rather than *drop* because a null key here means the parsing is wrong, not that one row is bad, and I'd rather stop the update than publish partial data.

**Gold: materialized views in SQL.**

| Table | Question |
|---|---|
| `gold.population_stats_2013_2018` | Mean and standard deviation of the annual US population, 2013–2018 inclusive |
| `gold.productivity_costs_index` | Best year per `series_id` (the year with the highest summed value), with readable labels |
| `gold.series_population` | `PRS30006032`, `Q01`: the value for each year, left-joined to that year's population |

For the readable labels, I followed the series ID layout in `pr.txt`: survey (2), seasonal (1), sector (4), class (1), measure (2), duration (1). I split `series_id` into those codes and joined the silver lookup tables (seasonal, sector, class, measure, duration), so each row says in words what is being measured. The population join is a `LEFT JOIN`, because the question asks for population "where available" and BLS goes back further than the API.

### SQL vs. PySpark

All three gold questions exist in both languages:

- **Primary:** SQL, in [transformations/gold/](src/main_pipeline/transformations/gold/)
- **Documented alternative:** PySpark, in [Alternative_pipelines/](src/main_pipeline/Alternative_pipelines/)

Bronze and silver have SQL alternatives too. The alternatives are outside the pipeline's `transformations/**` path, so they don't create duplicate tables in the same pipeline.

I chose SQL as primary for gold because those tables are aggregations and joins, which read most clearly in SQL. They're also what an analyst is most likely to read or change. Bronze and silver are primary in PySpark. There I wanted helper functions shared across all ten BLS tables (one ingest function, one dedup function), which is much less repetitive than ten near-identical SQL blocks.

## Trade-offs

What I'd handle differently for a real client:

- **Schema drift.** Bronze BLS uses `inferColumnTypes` and renames columns by position. If BLS added or reordered a column, rows would load under the wrong names without any error. In production I'd:
  - pin an explicit schema per file
  - send unexpected columns to `_rescued_data`
  - add an expectation that `_rescued_data IS NULL`, so drift stops the pipeline instead of corrupting it
- **Data removed at the source.** Silver keeps the newest row *per key*. If BLS deletes a row in a new version of a file, the old version's row survives. Whole files removed from the folder also stay in the volume and in `bls_metadata`. In the current architecture, we capture those deleted files for audit purposes.
- **New data at the source.** New data will automatically be ingested into bronze but not silver and gold. In a production setting, I would create an alert cross referencing the bronze tables to the silver tables to be automatically notified if any new pipelines need to be created.
- **Re-fetching unchanged population data.** Every run saves a new copy of the population JSON even when nothing has changed, and bronze ingests it again. Silver's dedup keeps the answers right, but it's wasted work. I'd hash the response and skip writing when it matches the last copy.
- **Data volume and cost.** The silver views recompute from all of bronze, and bronze grows with every version kept. At BLS `pr` scale (a few MB) this costs nothing. For larger sources I would:
  - use `APPLY CHANGES` / AUTO CDC into silver instead of window-based dedup
  - set a retention policy on old raw versions
  - check that serverless incremental refresh actually applies to the silver views
- **Access control.** Everything is owned by one user. For a client I'd give analysts `USE CATALOG`, `USE SCHEMA` and `SELECT` on `rearc.gold` only, keep `raw`/`bronze`/`silver` to the pipeline's service principal, and run the job as that principal instead of a person.
- **Monitoring.** Right now a failed job run is the only signal. I'd add:
  - job failure notifications
  - alerts on expectation metrics from the pipeline event log
  - a freshness check that warns if `bls_metadata` hasn't changed in longer than BLS's release schedule would explain
- **Contact details in code.** The BLS User-Agent email is in the notebook. In production it would come from a secret or a job parameter.

## Retrospective

What was hardest to get right:

- **Deciding what "already ingested" means.** My first version overwrote files in place. That broke the audit trail, and it meant Auto Loader couldn't tell that a file had changed. Keeping every version fixed both problems, but it moved the hard part downstream: silver then has to pick one version out of many. That is where most of my data-quality bugs came from:
  - population duplicated a row for every fetch
  - per-key dedup can't see rows that were deleted
- **Making the metadata table trustworthy.** It only stays correct if it's updated *after* a successful download and keeps exactly one row per file. Getting the order right (download, then `MERGE` only the successes) is what makes a re-run after a partial failure correct.
- **Reading the BLS files correctly.** The data files aren't self-describing: values are padded with whitespace, `-` stands for "none", and series IDs pack five dimensions into one string. Making the gold table readable meant using `pr.txt` to map the codes and the lookup files.
- **Moving to Asset Bundles.** Converting UI-built jobs to a bundle surfaced hard-coded workspace paths and a hard-coded pipeline ID that would only ever have worked in my workspace.
- **Switching from Databricks Free Edition to my own workspace.** I started on Free Edition but hit its resource limits, so I moved to a personal Databricks workspace part-way through. A new workspace starts empty. The catalog, schemas, volume and metadata table had to be recreated, the raw files ingested again, and the pipeline and job got new IDs. This is what made the bundle and the `init` job worth having. With them, the move came down to changing the workspace host in `databricks.yml`, deploying, running `init` once, and running the main job, which repopulated the volume from scratch. Because ingestion only downloads files missing from `bls_metadata`, the first run in an empty workspace is just a full load, with no special case needed.

## AI usage

I used two AI tools, and reviewed everything they produced before committing it.

**Databricks Genie Code: pipeline scaffolding.** I used it for the general structure of the pipeline: the bronze, silver and gold table definitions and their SQL and PySpark versions. I reviewed what it generated and changed it. In particular, I wrote the deduplication logic myself. Silver keeps only the row from the newest file version for each key, using the timestamp in the source file name.

**Claude (via Claude Code in VS Code): ingestion notebooks, review and this document.** I used it as a pair programmer on the ingestion notebooks. It drafted:

- the `bls_metadata` DDL
- the new-or-changed file check
- the versioned download loop and the `MERGE` of successful downloads
- the population ingestion notebook

It also reviewed the repo against the brief and pointed out gaps, such as duplicated population rows and data removed at the source. I steered the design and corrected it where it was wrong; for example, it initially added a `groupBy` to the metadata read that wasn't needed because `file_name` is unique.

I also used Claude to structure this PROCESS.md. It proposed the sections and a first draft based on the repo and our working session, which I then reviewed and edited.

## Screenshots

See [Screenshots/](Screenshots/) for the bronze, silver and gold tables in Catalog Explorer.
