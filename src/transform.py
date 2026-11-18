import logging
from pathlib import Path

import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

log = logging.getLogger(__name__)

RENAME = {
    "TransactionID": "transaction_id", "AccountID": "account_id",
    "TransactionAmount": "amount", "TransactionDate": "transaction_date",
    "TransactionType": "transaction_type", "Channel": "channel", "Location": "location",
    "CustomerAge": "customer_age", "CustomerOccupation": "customer_occupation",
    "LoginAttempts": "login_attempts", "AccountBalance": "account_balance",
    "DeviceID": "device_id", "MerchantID": "merchant_id",
    "IP Address": "ip_address", "TransactionDuration": "transaction_duration",
}
NUM = ["amount", "customer_age", "login_attempts", "account_balance", "transaction_duration"]
CAT = ["transaction_type", "channel", "customer_occupation", "location"]

def run(path: str) -> dict:
    staging = Path(path).parent
    df = pd.read_csv(path, encoding="utf-8-sig")
    empty = {"rows": 0, "dlq_rows": 0, "dates": [], "fact": None, "dims": {}, "dlq": None, "features": None}
    if df.empty:
        log.info("Transform: новых данных нет")
        return empty

    # 1) Профилирование
    log.info("Профилирование: %d строк, пропуски: %s", len(df), df.isna().sum().to_dict())

    # 2) Приведение типов
    df["TransactionDate"] = pd.to_datetime(df["TransactionDate"], errors="coerce", format="mixed")
    for c in ["TransactionAmount", "AccountBalance", "CustomerAge", "LoginAttempts", "TransactionDuration"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # 3) Дедупликация по TransactionID
    dups = int(df.duplicated(subset="TransactionID", keep="first").sum())
    df = df.drop_duplicates(subset="TransactionID", keep="first")

    # 4) Невалидные строки (сумма <= 0, битые даты/ключи) -> DLQ
    bad = (df["TransactionAmount"].isna() | (df["TransactionAmount"] <= 0)
           | df["TransactionDate"].isna() | df["TransactionID"].isna())
    dlq = df[bad]
    dlq.to_csv(staging / "dlq.csv", index=False)
    clean = df[~bad].rename(columns=RENAME).reset_index(drop=True)

    if clean.empty:
        log.warning("Transform: все %d строк отбракованы в DLQ", len(dlq))
        return {**empty, "dlq_rows": len(dlq), "dlq": str(staging / "dlq.csv")}

    # 5) Пропуски -> "Unknown"
    for c in ["location", "ip_address", "customer_occupation", "device_id", "merchant_id"]:
        clean[c] = clean[c].fillna("Unknown")

    # 6) Нормализация: измерения + факт
    dims = {
        "dim_customers": clean[["account_id", "customer_age", "customer_occupation"]].drop_duplicates("account_id"),
        "dim_channels":  clean[["channel"]].drop_duplicates(),
        "dim_devices":   clean[["device_id"]].drop_duplicates(),
        "dim_merchants": clean[["merchant_id"]].drop_duplicates(),
        "dim_locations": clean[["location"]].drop_duplicates(),
    }
    fact_columns = [
        "transaction_id", "account_id", "amount", "transaction_date",
        "transaction_type", "channel", "location", "login_attempts",
        "account_balance", "transaction_duration", "ip_address",
        "device_id", "merchant_id"
    ]
    fact = clean[fact_columns]
    fact.to_csv(staging / "fact.csv", index=False)
    for name, d in dims.items():
        d.to_csv(staging / f"{name}.csv", index=False)

    # 7) Агрегации и вычисления: статистика по дням и каналам
    stats = (fact.assign(day=fact["transaction_date"].dt.date)
             .groupby(["day", "channel"])
             .agg(tx_count=("amount", "size"),
                  total_amount=("amount", "sum"),
                  avg_amount=("amount", "mean"))
             .round(2)
             .reset_index())
    stats.to_csv(staging / "daily_stats.csv", index=False)

    # 8) Feature-слой: Pipeline + ColumnTransformer
    pre = ColumnTransformer([
        ("num", Pipeline([("imputer", SimpleImputer(strategy="median")),
                          ("scaler", StandardScaler())]), NUM),
        ("cat", Pipeline([("imputer", SimpleImputer(strategy="constant", fill_value="Unknown")),
                          ("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=False))]), CAT),
    ])
    pipe = Pipeline([("preprocessor", pre)])
    X = pipe.fit_transform(clean[NUM + CAT])
    features = pd.DataFrame(X, columns=pipe.named_steps["preprocessor"].get_feature_names_out())
    features.insert(0, "transaction_id", clean["transaction_id"])
    features.to_csv(staging / "features.csv", index=False)

    log.info("Transform: чистых=%d, дубликатов удалено=%d, DLQ=%d", len(fact), dups, len(dlq))
    return {"rows": len(fact), "dlq_rows": len(dlq),
            "dates": sorted(fact["transaction_date"].dt.strftime("%Y-%m-%d").unique().tolist()),
            "fact": str(staging / "fact.csv"),
            "dims": {n: str(staging / f"{n}.csv") for n in dims},
            "dlq": str(staging / "dlq.csv"),
            "features": str(staging / "features.csv")}