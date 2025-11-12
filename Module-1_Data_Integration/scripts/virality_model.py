# virality_model.py
import os
import pandas as pd

def train_from_csv(path="data/virality_train.csv", model_out="models/virality_model.txt"):
    if not os.path.exists(path):
        print(f"[virality_model] No training CSV found at '{path}'. Skipping training.")
        print("If you want to train, put a CSV with a 'target' column at that path or change the path argument.")
        return None

    df = pd.read_csv(path)
    print("[virality_model] Loaded train rows:", len(df))
    if "target" not in df.columns:
        raise ValueError("Training CSV must contain 'target' column (viral score).")

    try:
        import lightgbm as lgb
    except Exception:
        raise RuntimeError("lightgbm not installed. Install with: pip install lightgbm")

    X = df.drop(columns=["target"])
    y = df["target"]
    # keep numeric features only
    X = X.select_dtypes(include=["number"]).fillna(0)

    if X.shape[1] == 0:
        raise ValueError("No numeric features found in training CSV. Please provide numeric features.")

    dtrain = lgb.Dataset(X, label=y)
    params = {"objective": "regression", "metric": "rmse", "verbosity": -1}
    bst = lgb.train(params, dtrain, num_boost_round=50)
    os.makedirs(os.path.dirname(model_out), exist_ok=True)
    bst.save_model(model_out)
    print(f"[virality_model] Saved model to {model_out}")
    return model_out

if __name__ == "__main__":
    train_from_csv()


    
'''# virality_model.py
import os
from pathlib import Path

import pandas as pd
import joblib

MODEL_DIR = Path("models")
MODEL_DIR.mkdir(parents=True, exist_ok=True)

def train_from_csv(path="data/virality_train.csv", save=True):
    if not os.path.exists(path):
        print(f"[virality_model] No training CSV found at '{path}'. Skipping training.")
        print("If you want to train, place CSV at that path or pass path argument.")
        return None

    df = pd.read_csv(path)
    print("[virality_model] Loaded train rows:", len(df))

    if "target" not in df.columns:
        raise ValueError("Training CSV must contain 'target' column (viral score).")

    X = df.drop(columns=["target"])
    y = df["target"]

    # require numeric features; if none present, raise
    X_num = X.select_dtypes(include=["number"])
    if X_num.shape[1] == 0:
        raise ValueError("Training CSV must contain at least one numeric feature column.")

    X_num = X_num.fillna(0)

    # try LightGBM first; fallback to RandomForest
    try:
        import lightgbm as lgb
        dtrain = lgb.Dataset(X_num, label=y)
        params = {"objective": "regression", "metric": "rmse", "verbosity": -1}
        bst = lgb.train(params, dtrain, num_boost_round=100)
        model_path = MODEL_DIR / "virality_model.txt"
        bst.save_model(str(model_path))
        print(f"[virality_model] Saved LightGBM model to {model_path}")
        return str(model_path)
    except Exception as e:
        print("[virality_model] LightGBM not available or failed:", e)
        print("[virality_model] Falling back to sklearn RandomForest.")

    # sklearn fallback
    try:
        from sklearn.ensemble import RandomForestRegressor
        clf = RandomForestRegressor(n_estimators=100, random_state=42)
        clf.fit(X_num, y)
        model_path = MODEL_DIR / "virality_model.pkl"
        joblib.dump(clf, model_path)
        print(f"[virality_model] Saved sklearn model to {model_path}")
        return str(model_path)
    except Exception as e:
        print("[virality_model] sklearn training failed:", e)
        raise

def load_model(path=None):
    if path:
        if not os.path.exists(path):
            raise FileNotFoundError(f"No such model file: {path}")
        ext = Path(path).suffix
        if ext == ".txt":
            import lightgbm as lgb
            return lgb.Booster(model_file=path)
        else:
            return joblib.load(path)

    # try default locations
    if (MODEL_DIR / "virality_model.pkl").exists():
        return joblib.load(MODEL_DIR / "virality_model.pkl")
    if (MODEL_DIR / "virality_model.txt").exists():
        import lightgbm as lgb
        return lgb.Booster(model_file=str(MODEL_DIR / "virality_model.txt"))
    raise FileNotFoundError("No saved virality model found.")

if __name__ == "__main__":
    train_from_csv()
'''