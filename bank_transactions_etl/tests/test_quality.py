import pandas as pd
import pytest
from src.quality import validate


def _df(**over):
    row = dict(transaction_id="T1", account_id="A1",
               transaction_date=pd.Timestamp("2023-04-11 16:29"),
               amount=100.0, transaction_type="Debit", channel="ATM")
    row.update(over)
    return pd.DataFrame([row])


def test_quality_accepts_good_data():
    validate(_df())


def test_quality_blocks_zero_amount():
    with pytest.raises(ValueError, match="качества"):
        validate(_df(amount=0.0))

def test_quality_skips_empty_df():
    from src.quality import validate
    validate(pd.DataFrame())