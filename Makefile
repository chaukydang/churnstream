# One-command local run (no Docker / GCP):  make install && make all
PY      ?= python3
SIM_IN  ?= sample_input
OUT     ?= out
ML_DIR  ?= ml/artifacts
USERS   ?= 300
DAYS    ?= 14
# Homebrew's SPARK_HOME clashes with pip's pyspark -> always unset it
SPARK_ENV = env -u SPARK_HOME SOURCE=file INPUT_PATH=$(abspath $(SIM_IN)) \
	STORAGE_PATH=$(abspath $(OUT)) SPARK_MASTER='local[4]' TRIGGER_INTERVAL='3 seconds' \
	RUN_SECONDS=$${RUN_SECONDS:-60}

.PHONY: install simulate stream train dashboard test all clean
install:
	$(PY) -m pip install -r requirements-local.txt

simulate:   ## fake eventsim-style JSON lines
	$(PY) scripts/generate_fake_events.py $(SIM_IN) $(USERS) $(DAYS)

stream:     ## Spark Structured Streaming -> parquet (local files)
	cd spark_streaming && $(SPARK_ENV) $(PY) stream_all_events.py

train:      ## features + churn model -> $(ML_DIR)
	$(PY) -m ml.train $(OUT) $(ML_DIR)

dashboard:
	DATA_DIR=$(OUT) ML_DIR=$(ML_DIR) $(PY) -m streamlit run dashboard/app.py

test:
	$(PY) -m pytest -q tests

all: clean simulate stream train
clean:
	rm -rf $(SIM_IN) $(OUT) $(ML_DIR)
