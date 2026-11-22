import time
from pathlib import Path
import numpy as np
import pandas as pd
import pytest

from src.extract import run as extract_run
from src.transform import run as transform_run
from src.load import run as load_run
pytestmark = pytest.mark.usefixtures("clean_dwh")
def generate_large_csv(path: Path, n: int, seed: int = 42):
    """Генерирует синтетический CSV заданного размера."""
    rng = np.random.default_rng(seed)
    data = {
        "TransactionID": [f"TX{i:08d}" for i in range(n)],
        "AccountID": [f"AC{rng.integers(1, 1000):05d}" for _ in range(n)],
        "TransactionAmount": np.round(rng.uniform(1, 2000, n), 2),
        "TransactionDate": pd.date_range("2023-01-01", periods=n, freq="min").strftime("%m/%d/%Y %H:%M"),
        "TransactionType": rng.choice(["Debit", "Credit"], n),
        "Channel": rng.choice(["ATM", "Online", "Branch", "Mobile App"], n),
        "Location": rng.choice(["Moscow", "SPb", "Kazan", "Novosibirsk"], n),
        "CustomerAge": rng.integers(18, 80, n),
        "CustomerOccupation": rng.choice(["Doctor", "Engineer", "Teacher", "Student"], n),
        "LoginAttempts": rng.integers(1, 5, n),
        "AccountBalance": np.round(rng.uniform(100, 100000, n), 2),
        "DeviceID": [f"D{rng.integers(1, 500):06d}" for _ in range(n)],
        "MerchantID": [f"M{rng.integers(1, 100):03d}" for _ in range(n)],
        "IP Address": [".".join(map(str, rng.integers(1, 255, 4))) for _ in range(n)],
        "TransactionDuration": rng.integers(5, 300, n),
    }
    df = pd.DataFrame(data)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    return path

@pytest.mark.parametrize("n_records, max_seconds", [
    (100_000, 60),   # 100K < 1 минута
    (500_000, 180),  # 500K < 3 минуты
    (1_000_000, 300),  # 1M < 5 минут (NFR-2)
])
def test_performance(set_data_dir, tmp_path, n_records, max_seconds):
    """Системный тест: пайплайн укладывается в лимит по времени."""
    csv_path = generate_large_csv(tmp_path / "raw" / "bank_transactions.csv", n_records)

    start = time.time()

    # Extract
    ex_result = extract_run()
    t_extract = time.time() - start

    # Transform
    tr_result = transform_run(ex_result["path"])
    t_transform = time.time() - start - t_extract

    # Load
    load_run(tr_result, ds="2026-11-18")
    t_total = time.time() - start

    print(f"\n[PERF] Records: {n_records}")
    print(f"  Extract:   {t_extract:.2f}s")
    print(f"  Transform: {t_transform:.2f}s")
    print(f"  Total:     {t_total:.2f}s (limit: {max_seconds}s)")

    assert t_total < max_seconds, (
        f"Пайплайн не уложился в {max_seconds}с для {n_records} записей"
    )
    assert ex_result["rows"] == n_records, "extract отдал не все строки — прогон не тот, что заявлен"
    assert tr_result["rows"] > 0