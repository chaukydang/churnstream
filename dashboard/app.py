"""Streamlit dashboard over the parquet written by Spark + ML churn scores.

Run:  DATA_DIR=out ML_DIR=ml/artifacts streamlit run dashboard/app.py
(DATA_DIR may also be gs://bucket if gcsfs is installed.)
"""
import json
import os

import pandas as pd
import plotly.express as px
import streamlit as st

from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEMO_DIR = ROOT / "demo_data"          # small committed sample, used when no live data exists
DATA_DIR = os.getenv("DATA_DIR", str(ROOT / "out"))
ML_DIR = os.getenv("ML_DIR", str(ROOT / "ml" / "artifacts"))
DEMO_MODE = not (Path(DATA_DIR) / "listen_events").exists() and DEMO_DIR.exists()
if DEMO_MODE:  # e.g. Streamlit Community Cloud: out/ is not in git
    DATA_DIR, ML_DIR = str(DEMO_DIR), str(DEMO_DIR)

st.set_page_config(page_title="churnstream", layout="wide")
st.title("churnstream - streaming analytics & churn risk")



@st.cache_data
def load(topic):
    base = f"{DATA_DIR.rstrip('/')}/{topic}"
    df = pd.read_parquet(base + ".parquet" if DEMO_MODE else base)
    df["ts"] = pd.to_datetime(df["ts"])
    return df


try:
    listen, pages, auth = load("listen_events"), load("page_view_events"), load("auth_events")
except Exception as e:  # missing data is the common first-run problem
    st.error(f"Could not read parquet from '{DATA_DIR}': {e}\n\n"
             "Run the Spark job first (see README).")
    st.stop()

# ---- sidebar filter
min_d, max_d = listen.ts.min().date(), listen.ts.max().date()
start, end = st.sidebar.date_input("Date range", (min_d, max_d), min_value=min_d, max_value=max_d) \
    if min_d != max_d else (min_d, max_d)
levels = st.sidebar.multiselect("Level", ["free", "paid"], ["free", "paid"])
mask = lambda df: (df.ts.dt.date >= start) & (df.ts.dt.date <= end) & df.level.isin(levels)
listen, pages, auth = listen[mask(listen)], pages[mask(pages)], auth[mask(auth)]

# ---- KPIs
c1, c2, c3, c4 = st.columns(4)
c1.metric("Active users", f"{listen.userId.nunique():,}")
c2.metric("Listens", f"{len(listen):,}")
c3.metric("Avg listens / user", f"{len(listen) / max(listen.userId.nunique(), 1):.1f}")
c4.metric("Login failure rate", f"{1 - auth.success.mean():.1%}" if len(auth) else "n/a")

# ---- activity
left, right = st.columns(2)
daily = listen.groupby(listen.ts.dt.date).size().rename_axis("day").reset_index(name="listens")
left.plotly_chart(px.line(daily, x="day", y="listens", title="Listens per day"),
                  width="stretch")
hourly = listen.groupby(listen.ts.dt.hour).size().rename_axis("hour").reset_index(name="listens")
right.plotly_chart(px.bar(hourly, x="hour", y="listens", title="Listens by hour of day"),
                   width="stretch")

left, right = st.columns(2)
top_songs = listen.groupby(["artist", "song"]).size().rename("listens").reset_index() \
    .sort_values("listens", ascending=False).head(10)
top_songs["track"] = top_songs.artist + " - " + top_songs.song
left.plotly_chart(px.bar(top_songs, x="listens", y="track", orientation="h", title="Top songs")
                  .update_yaxes(autorange="reversed"), width="stretch")
page_mix = pages.page.value_counts().rename_axis("page").reset_index(name="views")
right.plotly_chart(px.bar(page_mix, x="views", y="page", orientation="h", title="Page views")
                   .update_yaxes(autorange="reversed"), width="stretch")

# ---- ML section
st.header("Churn risk (ML)")
scores_path, metrics_path = f"{ML_DIR}/churn_scores.parquet", f"{ML_DIR}/metrics.json"
if not (os.path.exists(scores_path) and os.path.exists(metrics_path)):
    st.info("No model artifacts yet. Run `make train` first.")
else:
    scores = pd.read_parquet(scores_path)
    metrics = json.load(open(metrics_path))
    best = metrics["best_model"]
    m = metrics["models"][best]
    a, b, c = st.columns(3)
    a.metric("Best model", best)
    b.metric("ROC-AUC (hold-out)", f"{m['roc_auc']:.3f}")
    c.metric("ROC-AUC (5-fold CV)", f"{m.get('cv_roc_auc', float('nan')):.3f}")
    st.caption("Metrics come from SYNTHETIC data with a built-in churn signal - they show the "
               "pipeline works, not real-world accuracy.")
    left, right = st.columns(2)
    left.plotly_chart(px.histogram(scores, x="churn_risk", nbins=20, title="Churn risk distribution"),
                      width="stretch")
    if "feature_importance" in metrics:
        fi = pd.Series(metrics["feature_importance"]).head(10).rename_axis("feature").reset_index(name="importance")
        right.plotly_chart(px.bar(fi, x="importance", y="feature", orientation="h",
                                  title="Top features").update_yaxes(autorange="reversed"),
                           width="stretch")
    st.subheader("Highest-risk users")
    st.dataframe(scores.sort_values("churn_risk", ascending=False).head(20)
                 .rename(columns={"churned": "actually_churned"}), width="stretch")
