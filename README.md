# churnstream

I took [ankurchavda/streamify](https://github.com/ankurchavda/streamify) (Kafka, Spark Streaming, dbt, Airflow and Terraform on GCP) and reworked it into something I can run on my laptop, then added a churn model and a dashboard on top.
The original README is in [`docs/ORIGINAL_README.md`](docs/ORIGINAL_README.md) and the upstream commit history is kept.
I built this with help from an AI assistant (Claude).

## What I changed

- **Runs locally.** Spark runs on `local[*]` and writes parquet to disk. Kafka is optional: `SOURCE=file` reads JSON lines instead. Everything is driven by env vars (`SPARK_MASTER`, `STORAGE_PATH`, `TRIGGER_INTERVAL`, `RUN_SECONDS`), and the defaults still match the original GCP/YARN setup.
- **Fake data with a signal.** `scripts/generate_fake_events.py` simulates users who slowly disengage before cancelling, so the model has something real to learn.
- **Bug fix.** The `string_decode` UDF crashed the whole stream on real unicode characters. It now falls back instead of failing.
- **Churn model** (`ml/`). Features come from events before a cutoff and the label from events after it, so there is no leakage. I compare logistic regression and gradient boosting, with a hold-out set and 5-fold CV.
- **Dashboard** (`dashboard/app.py`, Streamlit). KPIs, listens by day/hour, top songs, page mix, churn-risk distribution and the highest-risk users.
- **Tests and Makefile.** `make all` runs the whole thing.

```
simulate -> JSON lines -> Spark Structured Streaming -> parquet
                                                    |-> features -> churn model -> scores
                                                    '-> Streamlit dashboard
```

## Run it (no Docker, no GCP)

Python 3.9+ and Java 11/17.

```bash
python3 -m venv .venv && source .venv/bin/activate
make install
unset SPARK_HOME      # Homebrew's Spark clashes with pip's pyspark
make all              # simulate -> stream (~1 min) -> train
make dashboard        # http://localhost:8501
make test
```

## Results

With 300 simulated users over 14 days the best model gets ROC-AUC around 0.94 on a hold-out set and 0.93 in 5-fold CV.
The churn signal is built into the generator, so this only shows the pipeline works. It says nothing about real accuracy.
My first version scored AUC 1.0, which meant the fake data was too easy, so I added noise and softened the signal.

## Kafka and GCP

- **Kafka:** drop `SOURCE=file`, start a broker with `kafka/docker-compose.yml`, and submit with `--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3`. Something still has to produce events into `listen_events`, `page_view_events` and `auth_events` (eventsim, as upstream).
- **GCP:** with `STORAGE_PATH` unset the job writes to `gs://$GCP_GCS_BUCKET` like upstream. `pip install -r requirements-gcp.txt` lets `ml/` and the dashboard read `gs://`. Terraform, Airflow and dbt are unchanged.
- **Not tested yet:** the Kafka and GCP paths. Only the local path has been run.

## Next

Kafka producer with a slimmed docker-compose, dbt on DuckDB, scheduled retraining.
