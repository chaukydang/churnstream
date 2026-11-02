# churnstream

A data pipeline with Kafka, Spark Streaming, dbt, Docker, Airflow, Terraform, GCP and much more!

> Forked from [ankurchavda/streamify](https://github.com/ankurchavda/streamify). All sections are from the original project except "What I added in churnstream", which describes my extensions: a local run mode, a churn model and a dashboard. Developed with AI assistance (Claude).

## Description

### Objective

The project will stream events generated from a fake music streaming service (like Spotify) and create a data pipeline that consumes the real-time data. The data coming in would be similar to an event of a user listening to a song, navigating on the website, authenticating. The data would be processed in real-time and stored to the data lake periodically (every two minutes). The hourly batch job will then consume this data, apply transformations, and create the desired tables for our dashboard to generate analytics. We will try to analyze metrics like popular songs, active users, user demographics etc.

### Dataset

[Eventsim](https://github.com/Interana/eventsim) is a program that generates event data to replicate page requests for a fake music web site. The results look like real use data, but are totally fake. The docker image is borrowed from [viirya's fork](https://github.com/viirya/eventsim) of it, as the original project has gone without maintenance for a few years now.

Eventsim uses song data from [Million Songs Dataset](http://millionsongdataset.com) to generate events. I have used a [subset](http://millionsongdataset.com/pages/getting-dataset/#subset) of 10000 songs.

### Tools & Technologies

- Cloud - [**Google Cloud Platform**](https://cloud.google.com)
- Infrastructure as Code software - [**Terraform**](https://www.terraform.io)
- Containerization - [**Docker**](https://www.docker.com), [**Docker Compose**](https://docs.docker.com/compose/)
- Stream Processing - [**Kafka**](https://kafka.apache.org), [**Spark Streaming**](https://spark.apache.org/docs/latest/streaming-programming-guide.html)
- Orchestration - [**Airflow**](https://airflow.apache.org)
- Transformation - [**dbt**](https://www.getdbt.com)
- Data Lake - [**Google Cloud Storage**](https://cloud.google.com/storage)
- Data Warehouse - [**BigQuery**](https://cloud.google.com/bigquery)
- Data Visualization - [**Data Studio**](https://datastudio.google.com/overview)
- Language - [**Python**](https://www.python.org)

### Architecture

![streamify-architecture](images/Streamify-Architecture.jpg)

### Final Result

![dashboard](images/dashboard.png)

## What I added in churnstream

On top of the original pipeline I made it runnable on a laptop (no Docker, no GCP) and added a machine-learning and dashboard layer.

### New architecture

![churnstream-architecture](images/churnstream-architecture.png)

### Changes

- **Local mode** - Spark runs on `local[*]` and writes parquet to disk. Kafka is optional (`SOURCE=file`). Config via env vars (`SPARK_MASTER`, `STORAGE_PATH`, `TRIGGER_INTERVAL`, `RUN_SECONDS`); defaults still match the original GCP/YARN setup.
- **Data generator** (`scripts/generate_fake_events.py`) - simulated users who gradually disengage before cancelling, so the model has a real signal to learn.
- **Churn model** (`ml/`) - features from events *before* a cutoff, label from events *after* it (no leakage). Logistic regression vs gradient boosting, hold-out + 5-fold CV.
- **Dashboard** (`dashboard/app.py`, Streamlit) - KPIs, listens by day/hour, top songs, page mix, churn-risk distribution, highest-risk users.
- **Bug fix** - the `string_decode` UDF crashed the stream on real unicode; it now falls back safely.
- **Tests + Makefile** - `make all` runs generate -> stream -> train.

### Run it locally (no Docker, no GCP)

Python 3.9+ and Java 11/17.

```bash
python3 -m venv .venv && source .venv/bin/activate
make install
unset SPARK_HOME      # Homebrew's Spark clashes with pip's pyspark
make all              # generate -> stream (~1 min) -> train
make dashboard        # http://localhost:8501
make test
```

### Dashboard demo (Streamlit)

Live demo: **[churnstream-dashboard.streamlit.app](https://churnstream-dashboard.streamlit.app/)**

![churnstream-dashboard](images/churnstream-dashboard.png)

The dashboard falls back to a small committed sample in `demo_data/` when no live data exists, so it works on a fresh clone and on Streamlit Community Cloud.

```bash
make dashboard                         # live data from out/ and ml/artifacts/
python scripts/make_demo_data.py       # refresh demo_data/ from the latest run
```

To deploy it yourself (free): open [share.streamlit.io](https://share.streamlit.io), sign in with GitHub, click **Create app**, pick this repo, branch `main`, main file `dashboard/app.py`, then **Deploy**. Dependencies come from `dashboard/requirements.txt`.

### Results

On 300 simulated users over 14 days the best model reaches ROC-AUC about 0.94 (hold-out) and 0.93 (5-fold CV). The churn signal is built into the generator, so this shows the pipeline works, not real-world accuracy.

### Not tested yet

The Kafka and GCP paths of this fork. Only the local path has been run.

---

## Setup

**WARNING: You will be charged for all the infra setup. You can avail 300$ in credit by creating a new account on GCP.**
### Pre-requisites

If you already have a Google Cloud account and a working terraform setup, you can skip the pre-requisite steps.

- Google Cloud Platform. 
  - [GCP Account and Access Setup](setup/gcp.md)
  - [gcloud alternate installation method - Windows](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_1_basics_n_setup/1_terraform_gcp/windows.md#google-cloud-sdk)
- Terraform
  - [Setup Terraform](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_1_basics_n_setup/1_terraform_gcp/windows.md#terraform)


### Get Going!

A video walkthrough of how I run my project - [YouTube Video](https://youtu.be/vzoYhI8KTlY)

- Procure infra on GCP with Terraform - [Setup](setup/terraform.md)
- (Extra) SSH into your VMs, Forward Ports - [Setup](setup/ssh.md)
- Setup Kafka Compute Instance and start sending messages from Eventsim - [Setup](setup/kafka.md)
- Setup Spark Cluster for stream processing - [Setup](setup/spark.md)
- Setup Airflow on Compute Instance to trigger the hourly data pipeline - [Setup](setup/airflow.md)


### Debug

If you run into issues, see if you find something in this debug [guide](setup/debug.md).
### How can I make this better?!
A lot can still be done :).
- Choose managed Infra
  - Cloud Composer for Airflow
  - Confluent Cloud for Kafka
- Create your own VPC network
- Build dimensions and facts incrementally instead of full refresh
- Write data quality tests
- Create dimensional models for additional business processes
- Include CI/CD
- Add more visualizations

### Special Mentions
I'd like to thank the [DataTalks.Club](https://datatalks.club) for offering this Data Engineering course for completely free. All the things I learnt there, enabled me to come up with this project. If you want to upskill on Data Engineering technologies, please check out the [course](https://github.com/DataTalksClub/data-engineering-zoomcamp). :)