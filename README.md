# Enterprise Lakehouse & Incremental Ingestion

**Architecture:** JSONL / REST export → Bronze → Silver (deduplicated upserts) → Gold (customer aggregates).

## Local quick start
```bash
python -m pip install -e '.[dev]'
python -m src.pipeline
pytest -q
```
Runs on SQLite locally so no cloud account is required. Repeat the command to see watermark-based idempotence. Change event timestamps to test updates.

## Databricks deployment
The `databricks/delta_pipeline.py` script is a **separate cloud implementation** using Spark and Delta MERGE. Configure `portfolio.source_path` and optional table names in Spark config, attach compute with Unity Catalog permissions, then run the notebook. Use a Databricks Job for scheduling. This sample is not an automatically deployed Azure pipeline.

## Engineering notes
The local implementation uses a timestamp watermark and quarantines invalid records. For production, use a compound `(updated_at, event_id)` cursor or an overlap window to avoid missing equal-timestamp or late-arriving records; maintain the watermark in the same transaction as the writes. Gold is rebuilt for correctness in this small demo.
