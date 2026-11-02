# Streamify-local

A streaming data pipeline you can run **on a laptop in one command**, extended with
**feature engineering, a churn-prediction model and a live dashboard**.

Derived from [ankurchavda/streamify](https://github.com/ankurchavda/streamify)
(Kafka + Spark Streaming + dbt + Airflow + Terraform on GCP). The original README is kept in
[`docs/ORIGINAL_README.md`](docs/ORIGINAL_README.md); git history preserves the original commits.

## What I added

| Area | Change |
|---|---|
| Local mode | Spark runs on `local[*]`, writes parquet to disk; Kafka is optional (`SOURCE=file`). Config via env vars (`SPARK_MASTER`, `STORAGE_PATH`, `TRIGGER_INTERVAL`, `RUN_SECONDS`) - defaults still match the original GCP/YARN setup |
| Data | `scripts/generate_fake_events.py`: eventsim-style events where users have a hidden churn propensity (so there is something to learn) |
| Bug fix | `string_decode` UDF crashed the whole stream on real unicode; now falls back safely |
| ML | `ml/features.py` (per-user features from events *before* a cutoff, label from events *after* it - no leakage) and `ml/train.py` (logistic regression vs gradient boosting, hold-out + 5-fold CV, out-of-fold scores) |
| Dashboard | `dashboard/app.py` (Streamlit): KPIs, listens per day/hour, top songs, page mix, churn-risk distribution, top features, highest-risk users |
| Quality | `tests/` (pytest) and a `Makefile` |

```
simulate -> JSON lines -> Spark Structured Streaming -> parquet (month/day/hour)
                                                   |-> ml: features -> churn model -> scores
                                                   '-> Streamlit dashboard
```

## Run it locally (no Docker, no GCP)

Needs Python 3.9+ and Java 11/17.

```bash
python3 -m venv .venv && source .venv/bin/activate
make install
unset SPARK_HOME          # if Homebrew Spark is installed it clashes with pip's pyspark
make all                  # simulate -> stream (~1 min) -> train
make dashboard            # http://localhost:8501
make test
```

## Results (synthetic data!)

On 300 simulated users / 14 days the best model reaches ROC-AUC ~0.94 (hold-out) / ~0.93 (5-fold CV).
These numbers only show the pipeline works end to end: the churn signal is built into the
generator, so they say nothing about real-world accuracy.

## Kafka and GCP

* **Kafka:** drop `SOURCE=file`, start a broker (`kafka/docker-compose.yml`) and submit with
  `--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3`. Something has to produce events into
  the `listen_events`, `page_view_events`, `auth_events` topics (eventsim, as in the original).
* **GCP:** with `STORAGE_PATH` unset the Spark job writes to `gs://$GCP_GCS_BUCKET` as in the original;
  `pip install -r requirements-gcp.txt` lets `ml/` and `dashboard/` read `gs://` paths
  (`make train OUT=gs://bucket`). Terraform / Airflow / dbt are unchanged from upstream.
  *The GCP and Kafka paths have not been exercised in this fork yet - only the local path is tested.*

## Ideas next

Kafka producer + docker-compose slimmed to broker only; dbt on DuckDB; scheduled retraining; model registry.
