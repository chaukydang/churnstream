"""Feature engineering for the churn model.

Reads the parquet written by spark_streaming (local dir or gs:// path) and builds
ONE ROW PER USER:
  * features come only from events BEFORE the cutoff
  * label  = user hit "Cancellation Confirmation" AT/AFTER the cutoff
so there is no label leakage.  cutoff = max(ts) - label_days.
"""
import pandas as pd

LISTEN, PAGES, AUTH = "listen_events", "page_view_events", "auth_events"

FEATURE_COLUMNS = [
    "is_paid", "tenure_days", "listens_total", "listens_last3d", "listen_trend",
    "active_days", "days_since_last_event", "distinct_songs", "distinct_artists",
    "avg_listen_duration", "thumbs_up", "thumbs_down", "thumbs_ratio",
    "help_visits", "downgrade_visits", "error_pages", "auth_fail_rate",
]


def load_topic(data_dir: str, topic: str) -> pd.DataFrame:
    # pandas can read a partitioned parquet dir; gs:// needs `pip install gcsfs`
    df = pd.read_parquet(f"{data_dir.rstrip('/')}/{topic}")
    df["ts"] = pd.to_datetime(df["ts"])
    return df


def build_features(listen: pd.DataFrame, pages: pd.DataFrame, auth: pd.DataFrame,
                   label_days: int = 4):
    """Return (features_df indexed by userId, cutoff Timestamp)."""
    all_ts = pd.concat([listen["ts"], pages["ts"], auth["ts"]])
    cutoff = all_ts.max() - pd.Timedelta(days=label_days)

    listen_f = listen[listen["ts"] < cutoff]
    pages_f = pages[pages["ts"] < cutoff]
    auth_f = auth[auth["ts"] < cutoff]
    recent_from = cutoff - pd.Timedelta(days=3)

    users = pd.DataFrame(index=pd.Index(sorted(
        set(listen_f.userId) | set(pages_f.userId) | set(auth_f.userId)), name="userId"))

    g = listen_f.groupby("userId")
    users["listens_total"] = g.size()
    users["listens_last3d"] = listen_f[listen_f.ts >= recent_from].groupby("userId").size()
    users["distinct_songs"] = g["song"].nunique()
    users["distinct_artists"] = g["artist"].nunique()
    users["avg_listen_duration"] = g["duration"].mean()
    users["active_days"] = listen_f.assign(d=listen_f.ts.dt.date).groupby("userId")["d"].nunique()

    page_counts = pages_f.groupby(["userId", "page"]).size().unstack(fill_value=0)
    for col, page in [("thumbs_up", "Thumbs Up"), ("thumbs_down", "Thumbs Down"),
                      ("help_visits", "Help"), ("downgrade_visits", "Downgrade"),
                      ("error_pages", "Error")]:
        users[col] = page_counts[page] if page in page_counts else 0

    if len(auth_f):
        users["auth_fail_rate"] = 1 - auth_f.groupby("userId")["success"].mean()

    last_level = pages_f.sort_values("ts").groupby("userId")["level"].last()
    users["is_paid"] = (last_level == "paid").astype(int)
    first_reg = pd.to_datetime(pages_f.groupby("userId")["registration"].min(), unit="ms")
    users["tenure_days"] = (cutoff - first_reg).dt.days

    last_event = pd.concat([listen_f[["userId", "ts"]], pages_f[["userId", "ts"]]]) \
        .groupby("userId")["ts"].max()
    users["days_since_last_event"] = (cutoff - last_event).dt.total_seconds() / 86400

    users = users.fillna(0)
    users["thumbs_ratio"] = users["thumbs_down"] / (users["thumbs_up"] + users["thumbs_down"] + 1)
    # share of listening in the last 3 days vs overall (declining engagement => low)
    users["listen_trend"] = users["listens_last3d"] / (users["listens_total"] + 1)

    # label: cancelled in the label window (events at/after cutoff)
    cancelled = pages[(pages.ts >= cutoff) & (pages.page == "Cancellation Confirmation")].userId
    users["churned"] = users.index.isin(set(cancelled)).astype(int)

    return users[FEATURE_COLUMNS + ["churned"]], cutoff
