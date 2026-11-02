"""Train + evaluate the churn model and write scores for the dashboard.

Usage:  python -m ml.train <data_dir> [out_dir]
Outputs in out_dir (default ml/artifacts): model.joblib, metrics.json,
features.parquet, churn_scores.parquet
"""
import json
import os
import sys

import joblib
import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.model_selection import StratifiedKFold, cross_val_predict, train_test_split
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from ml.features import (AUTH, FEATURE_COLUMNS, LISTEN, PAGES, build_features,
                         load_topic)


def run(data_dir, out_dir="ml/artifacts", seed=42):
    os.makedirs(out_dir, exist_ok=True)
    feats, cutoff = build_features(*[load_topic(data_dir, t) for t in (LISTEN, PAGES, AUTH)])
    X, y = feats[FEATURE_COLUMNS], feats["churned"]
    if y.nunique() < 2:
        raise SystemExit("Only one class in the label - generate more data.")

    X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.25, stratify=y, random_state=seed)
    models = {
        "logistic_regression": make_pipeline(StandardScaler(), LogisticRegression(max_iter=1000)),
        "gradient_boosting": GradientBoostingClassifier(random_state=seed),
    }
    metrics = {"n_users": int(len(feats)), "churn_rate": float(y.mean()),
               "cutoff": str(cutoff), "models": {}}
    best_name, best_auc = None, -1
    for name, model in models.items():
        model.fit(X_tr, y_tr)
        p = model.predict_proba(X_te)[:, 1]
        auc = roc_auc_score(y_te, p)
        metrics["models"][name] = {"roc_auc": float(auc),
                                   "pr_auc": float(average_precision_score(y_te, p))}
        if auc > best_auc:
            best_name, best_auc = name, auc
    metrics["best_model"] = best_name

    best = models[best_name]
    # honest scores for EVERY user: out-of-fold predictions (no user is scored by a
    # model that saw its own label)
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=seed)
    oof = cross_val_predict(best, X, y, cv=cv, method="predict_proba")[:, 1]
    metrics["models"][best_name]["cv_roc_auc"] = float(roc_auc_score(y, oof))
    best.fit(X, y)  # final model on all data for future scoring

    clf = best[-1] if hasattr(best, "steps") else best
    if hasattr(clf, "feature_importances_"):
        imp = dict(zip(FEATURE_COLUMNS, clf.feature_importances_.round(4).tolist()))
        metrics["feature_importance"] = dict(sorted(imp.items(), key=lambda kv: -kv[1]))

    joblib.dump({"model": best, "features": FEATURE_COLUMNS}, f"{out_dir}/model.joblib")
    feats.to_parquet(f"{out_dir}/features.parquet")
    scores = feats[["churned"]].assign(churn_risk=oof)
    scores.to_parquet(f"{out_dir}/churn_scores.parquet")
    with open(f"{out_dir}/metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)
    return metrics


if __name__ == "__main__":
    m = run(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else "ml/artifacts")
    print(json.dumps(m, indent=2))
