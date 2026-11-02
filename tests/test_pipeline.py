import pandas as pd

from ml.features import FEATURE_COLUMNS, build_features
from scripts import generate_fake_events as gen

NOW = 1_800_000_000_000


def _frames(seed=1, users=80):
    t = gen.build(seed=seed, n_users=users, days=14, now_ms=NOW)
    out = {}
    for name, rows in t.items():
        df = pd.DataFrame(rows)
        df["ts"] = pd.to_datetime(df["ts"], unit="ms")
        out[name] = df
    return out["listen_events"], out["page_view_events"], out["auth_events"]


def test_generator_is_deterministic():
    a = gen.build(seed=7, n_users=20, now_ms=NOW)
    b = gen.build(seed=7, n_users=20, now_ms=NOW)
    assert a == b


def test_generator_escapes_non_ascii_like_eventsim():
    assert gen.esc("Björk") == '"Bj\\xc3\\xb6rk"'


def test_features_shape_and_no_nans():
    feats, cutoff = build_features(*_frames())
    assert list(feats.columns) == FEATURE_COLUMNS + ["churned"]
    assert not feats.isna().any().any()
    assert feats.churned.isin([0, 1]).all() and feats.churned.sum() > 0


def test_features_use_only_pre_cutoff_events():
    listen, pages, auth = _frames()
    feats, cutoff = build_features(listen, pages, auth)
    expected = listen[listen.ts < cutoff].groupby("userId").size()
    got = feats["listens_total"][expected.index]
    assert (got == expected).all()  # post-cutoff listens never counted


def test_churners_have_lower_engagement_on_average():
    feats, _ = build_features(*_frames(users=200))
    g = feats.groupby("churned")["listens_total"].mean()
    assert g[1] < g[0]
