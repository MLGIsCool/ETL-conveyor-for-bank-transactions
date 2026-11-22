import pandas as pd
import pytest
from unittest.mock import patch

from src.load import _upsert

class FakeCursor:
    """Фейковый курсор для проверки, что SQL был сгенерирован верно."""
    def __init__(self):
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append(sql)

def test_upsert_empty_df_does_nothing():
    """Пустой DataFrame не вызывает SQL."""
    cur = FakeCursor()
    df = pd.DataFrame()
    _upsert(cur, "fact_transactions", df, pk=["transaction_id"])
    assert len(cur.executed) == 0

@patch('src.load.execute_values')
def test_upsert_builds_insert_sql(mock_execute_values):
    """_upsert корректно формирует и вызывает INSERT ... ON CONFLICT."""
    cur = FakeCursor()
    df = pd.DataFrame({
        "transaction_id": ["T1"],
        "amount": [100.0],
        "channel": ["ATM"],
    })
    
    _upsert(cur, "fact_transactions", df, pk=["transaction_id"])
    
    # Проверяем, что функция execute_values была вызвана
    assert mock_execute_values.called
    
    # Достаем SQL-запрос из аргументов вызова (второй аргумент в call_args)
    call_args = mock_execute_values.call_args
    sql = call_args[0][1]  
    
    assert "INSERT INTO dwh.fact_transactions" in sql
    assert "ON CONFLICT (transaction_id)" in sql
    assert "DO UPDATE SET" in sql