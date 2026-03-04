import pandas as pd
import numpy as np
import pytest
from unittest.mock import MagicMock, patch, ANY
from psycopg2 import sql
from utils.db import PostgreSQLConnector
from utils.config import yaml_read
import os

@pytest.fixture
def db_con():
    conn_params = {
      "host": "database",
      "port": 5432,
      "user": os.getenv("DB_USER"),
      "password": os.getenv("DB_PASSWORD"),
      "database": os.getenv("DB_NAME")
   }
    return PostgreSQLConnector(conn_params)

def test_get_all_company_as_df(mocker, db_con):
    mock_get_companies = mocker.patch.object(
        db_con,
        "get_all_companies"
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

def test_insert_company(mocker, db_con):
    #Stunt double for cur and con
    mock_cursor = MagicMock()
    mock_con = MagicMock()

    # Config for the -----with BLOCK-----
    #con.cursoer() returns mock_cursor
    mock_con.cursor.return_value = mock_cursor
    #con.cursor() as cur 
    mock_cursor.__enter__.return_value = mock_cursor
    # for with conn
    mock_con.__enter__.return_value = mock_con

    mocker.patch.object(
        db_con,
        "get_connection",
        return_value = mock_con
    )

    comp_dic = {
        "name" : "Global X DAX Germany ETF",
        "symbol" : "DAX",
        "country" : "Germany",
        "industry" : "Index"
    }

    db_con.insert_company(comp_dic)

    schema = yaml_read("schema.yaml")
    table_name = schema["tables"]["dim_company"]["name"]
    expected_sql = sql.SQL("""
                               INSERT INTO {table} (name, symbol, country, industry)
                               VALUES (%s, %s, %s, %s) 
                               RETURNING company_id;
        """).format(table=sql.Identifier(table_name))
    
    expected_values = (
        comp_dic["name"],
        comp_dic["symbol"],
        comp_dic["country"],
        comp_dic["industry"]
    )
    
    mock_cursor.execute.assert_called_once_with(expected_sql, expected_values)

def test_upsert_fact_prices(mocker, db_con):
    # 1. FIX: Psycopg2 C-Funktion patchen, damit MagicMock akzeptiert wird
    # KEINE AHNUNG 
    mocker.patch("psycopg2.extensions.quote_ident", side_effect=lambda s, c: f'"{s}"')
    
    mock_con = MagicMock()
    mock_cursor = MagicMock()

    # Setup for server connection with "with"
    mock_con.cursor.return_value = mock_cursor
    mock_con.__enter__.return_value = mock_con
    mock_cursor.__enter__.return_value = mock_cursor

    mocker.patch.object(
            db_con, 
            "get_connection", 
            return_value=mock_con
        ) 

    mock_exec_values = mocker.patch("utils.db.execute_values")

    example_df = pd.DataFrame ({
        'date_id': [9432],
        'company_id': [9],
        'close_price': [50512.32031250],
        'high_price': [50549.60156250],
        'low_price': [49838.98046875],
        'open_price': [49905.88078125],
        'volume': [122100000]
    })

    # call functionen to test
    db_con.upsert_fact_prices(example_df)

    #(cursor, query, argslist)
    args_called = mock_exec_values.call_args[0]

    called_cursor = args_called[0]
    called_query = args_called[1]
    called_data = args_called[2]

    assert called_cursor == mock_cursor

    query_string = called_query.as_string(mock_con)
    assert "INSERT INTO" in query_string
    assert "fact_prices" in query_string
    assert "ON CONFLICT (date_id, company_id)" in query_string

    expected_data = list(example_df.itertuples(index=False, name=None))
    assert called_data == expected_data

    #expected_values = list(example_df.itertuples(index=False, name=None))
    
   
"""
    db_con.upsert_fact_prices(example_df)

    # Arguments with them executed value is called (cursor, query, args)
    #args_called = mock_exec_values.call_args[0]
    #cursor_args = args_called[0]
    #query_obj = args_called[1] #sql.Compose Object
    #data_args = args_called[2] # List with the data

    # Convert the sql Object to String
    #query_string = query_obj.as_string(mock_cursor)

    #assert "INSERT INTO" in query_string
    #assert '"fact_prices"' in query_string
    #assert "ON CONFLICT (date_id, company_id)" in query_string
    mock_cursor.assert_called_once_with(excepted_query, expected_values)
    #assert data_args == expected_values


"""




