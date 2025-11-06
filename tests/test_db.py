import pandas as pd
import numpy as np
import pytest
from unittest.mock import MagicMock, patch
from utils.db import PostgreSQLConnector

@pytest.fixture(autouse=True)
def clean_database(db_con):
    table_name = "dim_company"

    with db_con.get_connection() as conn:
        with conn.cursor() as cur:


def test_get_all_company_as_df(mocker):
    db_con = PostgreSQLConnector()
    mock_get_companies = mocker.patch.object(
        db_con,
        "get_all_companie"
        cur.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;")
            conn.commit()
    )

    data = [
        (1, "Global X DAX Germany ETF", "DAX", "Germany", "Index"),
        (16, "Tron Inc.", "Tron", "United States", "Leisure"),
        (9, "Nikkei225", "Nikkei225", "Japan", "Index")
    ]
    mock_get_companies.return_value = data
    
    columns = ["company_id", "name", "symbol", "country", "industry"]
    excepted_df = pd.DataFrame(data, columns=columns)

    result_df = db_con.get_dim_company_as_df()

    pd.testing.assert_frame_equal(excepted_df, result_df)
    mock_get_companies.assert_called_once()

def test_insert_company():
    mock_cursor = MagicMock()
    mock_con = MagicMock()

    # Wir patchen self.get_connection() so, dass es unsere mock_connection zurückgibt
    # "__enter__.return_value" stellt sicher, dass das Objekt nach dem "with conn:"-Block zurückgegeben wird
    mock_con.cursor.return_value.__enter__.return_value = mock_cursor

    db_con = PostgreSQLConnector()
    comp_dic = {
        "name" : "Global X DAX Germany ETF",
        "symbol" : "DAX",
        "country" : "Germany",
        "industry" : "Index"
    }
    db_con.insert_company(comp_dic)
    expected_sql = "INSERT INTO dim_company " \
    "(name, symbol, country, industry) VALUES " \
    "('Global X DAX Germany ETF', 'DAX', 'Germany', 'Index') " \
    "RETURNING company_id;"

    mock_con.execute.assert_called_once_with(expected_sql)

    mock_con.commit.assert_called_once()



