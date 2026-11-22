import pandas as pd
import pytest

from src.transform import run


def test_transform_deduplication(sample_csv, staging_dir):
    """FR-2.4: дубликаты по TransactionID удаляются."""
    extracted = pd.read_csv(sample_csv)
    extracted.to_csv(staging_dir / "extracted.csv", index=False)

    result = run(str(staging_dir / "extracted.csv"))
    fact = pd.read_csv(result["fact"])

    assert fact["transaction_id"].is_unique
    assert len(fact) == 2


def test_transform_dlq_contains_bad_rows(sample_csv, staging_dir):
    """FR-2.1: строки с отрицательной суммой попадают в DLQ."""
    extracted = pd.read_csv(sample_csv)
    extracted.to_csv(staging_dir / "extracted.csv", index=False)

    result = run(str(staging_dir / "extracted.csv"))
    dlq = pd.read_csv(result["dlq"])

    assert len(dlq) == 1
    assert dlq.iloc[0]["TransactionAmount"] <= 0


def test_transform_fillna_unknown(sample_csv, staging_dir):
    """FR-2.2: пропуски в категориальных полях заменяются на 'Unknown'."""
    extracted = pd.read_csv(sample_csv)
    extracted.to_csv(staging_dir / "extracted.csv", index=False)

    result = run(str(staging_dir / "extracted.csv"))
    fact = pd.read_csv(result["fact"])
    dim_customers = pd.read_csv(result["dims"]["dim_customers"])

    # Во второй строке (T2) location был None -> должен стать Unknown в факте
    t2_fact = fact[fact["transaction_id"] == "T2"].iloc[0]
    assert t2_fact["location"] == "Unknown"

    # CustomerOccupation для A2 был None -> должен стать Unknown в измерении
    t2_dim = dim_customers[dim_customers["account_id"] == "A2"].iloc[0]
    assert t2_dim["customer_occupation"] == "Unknown"


def test_transform_normalizes_to_star_schema(sample_csv, staging_dir):
    """FR-2.2: данные нормализуются в схему «звезда»."""
    extracted = pd.read_csv(sample_csv)
    extracted.to_csv(staging_dir / "extracted.csv", index=False)

    result = run(str(staging_dir / "extracted.csv"))

    dims = result["dims"]
    assert "dim_customers" in dims
    assert "dim_channels" in dims
    assert "dim_devices" in dims
    assert "dim_merchants" in dims
    assert "dim_locations" in dims

    customers = pd.read_csv(dims["dim_customers"])
    assert "account_id" in customers.columns
    assert customers["account_id"].is_unique


def test_transform_empty_input(staging_dir):
    """Transform корректно обрабатывает пустой батч."""
    empty_path = staging_dir / "extracted.csv"
    pd.DataFrame(columns=[
        "TransactionID", "AccountID", "TransactionAmount", "TransactionDate",
        "TransactionType", "Channel", "Location", "CustomerAge",
        "CustomerOccupation", "LoginAttempts", "AccountBalance",
        "DeviceID", "MerchantID", "IP Address", "TransactionDuration"
    ]).to_csv(empty_path, index=False)

    result = run(str(empty_path))
    assert result["rows"] == 0