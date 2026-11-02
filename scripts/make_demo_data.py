"""Trim the Spark output + ML artifacts into small files that are safe to commit
(used by the hosted Streamlit demo).  Usage: python scripts/make_demo_data.py [out_dir] [ml_dir] [demo_dir]"""
import os
import shutil
import sys

import pandas as pd

out = sys.argv[1] if len(sys.argv) > 1 else "out"
ml = sys.argv[2] if len(sys.argv) > 2 else "ml/artifacts"
demo = sys.argv[3] if len(sys.argv) > 3 else "demo_data"
os.makedirs(demo, exist_ok=True)

COLS = {  # only what the dashboard reads
    "listen_events": ["ts", "userId", "level", "artist", "song"],
    "page_view_events": ["ts", "userId", "level", "page"],
    "auth_events": ["ts", "userId", "level", "success"],
}
for topic, cols in COLS.items():
    df = pd.read_parquet(f"{out}/{topic}", columns=cols)
    df.to_parquet(f"{demo}/{topic}.parquet", index=False, compression="zstd")
for f in ("churn_scores.parquet", "metrics.json"):
    shutil.copy(f"{ml}/{f}", f"{demo}/{f}")
print({f: os.path.getsize(f"{demo}/{f}") // 1024 for f in os.listdir(demo)}, "KB")
