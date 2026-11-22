import os
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock

from src.extract import run


def _setup_empty_db_mock(mock_connect):
    # Вспомогательная функция: настраивает мок БД так, будто таблица пуста (возвращает None)
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    # psycopg2 возвращает кортеж, поэтому используем (None,), а не [None]
    mock_cur.fetchone.return_value = (None,)  
    mock_conn.cursor.return_value = mock_cur
    mock_connect.return_value = mock_conn


@patch('src.extract.psycopg2.connect')
def test_extract_returns_dict_with_path(mock_connect, set_data_dir, sample_csv):
    """FR-1.1: Extract возвращает словарь с путём и количеством строк."""
    _setup_empty_db_mock(mock_connect)
    
    result = run()
    assert isinstance(result, dict)
    assert "path" in result
    assert "rows" in result
    assert os.path.exists(result["path"])


@patch('src.extract.psycopg2.connect')
def test_extract_reads_csv(mock_connect, set_data_dir, sample_csv):
    """Extract корректно читает CSV."""
    _setup_empty_db_mock(mock_connect)
    
    result = run()
    df = pd.read_csv(result["path"])
    assert len(df) > 0
    assert "TransactionID" in df.columns


@patch('src.extract.psycopg2.connect')
def test_extract_full_mode_on_empty_dwh(mock_connect, set_data_dir, sample_csv):
    """FR-1.2: при пустой DWH — полная загрузка (все строки)."""
    _setup_empty_db_mock(mock_connect)
    
    result = run()
    assert result["rows"] == 4


def test_extract_missing_file_raises(set_data_dir, tmp_path):
    """При отсутствии файла — понятное исключение."""
    with pytest.raises(FileNotFoundError):
        run()