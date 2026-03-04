import os
import pytest
from testcontainers.postgres import PostgresContainer
from utils.helpers import BASE_DIR
import psycopg2
import time
from utils.db import PostgreSQLConnector
import pandas as pd
from psycopg2 import sql

postgres = PostgresContainer("postgres:16-alpine")

@pytest.fixture(scope="module")
def postgre_db(request):
    # 1. Start Container
    with PostgresContainer("postgres:15") as postgres:
        time.sleep(3)
        conn_params = {
            "host": postgres.get_container_host_ip(),
            "port": postgres.get_exposed_port(5432),
            "user": postgres.username,
            "password": postgres.password,
            "database": postgres.dbname,
        }
        if conn_params["host"] == "127.0.0.1" or conn_params["host"] == "localhost":
            conn_params["host"] = "host.docker.internal"

        # 2. Inizialize Schema
        with psycopg2.connect(**conn_params) as conn:
            with conn.cursor() as cur:
                with open(BASE_DIR / "init.sql", "r") as f:
                    cur.execute(f.read())
                conn.commit()

        # Connection parameters
        yield conn_params

def test_table_exists(postgre_db):
    # 3. test
    with psycopg2.connect(**postgre_db) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'fact_prices';")
            result = cur.fetchone()
            assert result[0] == 1

def test_get_connection(postgre_db):
    db = PostgreSQLConnector(postgre_db)
    con = db.get_connection()
    assert con.info.host ==  "host.docker.internal"


def test_upsert_fact_prices(postgre_db):
    db = PostgreSQLConnector(postgre_db)
    con = db.get_connection()

    data = [
        {
            "price_id": 1,
            "date_id": 1, 
            "company_id": 1, 
            "close_price": 22, 
            "high_price": 23, 
            "low_price": 24, 
            "open_price": 125, 
            "volume": 26
        },
        {
            "price_id": 2,
            "date_id": 1, 
            "company_id": 2, 
            "close_price": 12.50, 
            "high_price": 13.0, 
            "low_price": 12.0, 
            "open_price": 12.1, 
            "volume": 1000
        }
    ]
    data = pd.DataFrame.from_dict(data)

    # PREPARATION
    sql_insert_query_fact = "INSERT INTO fact_prices (price_id, date_id, company_id, close_price, high_price, low_price, open_price, volume) VALUES (1, 1, 1, 12, 13,14,15,16);"
    sql_insert_query_date = "INSERT INTO dim_date (date_id, full_date, day, month, year) VALUES (1, '12-01-1994','12', '01', '1994');"
    sql_insert_query_comp = "INSERT INTO dim_company (company_id, name, symbol, country, industry) VALUES (1, 'Eine Firma','EF', 'Ger', 'zuhause');"
    sql_insert_query_comp2 = "INSERT INTO dim_company (company_id, name, symbol, country, industry) VALUES (2, 'DieFama','DEF', 'Hus', 'nix');"

    with con.cursor() as cur:
        cur.execute(sql_insert_query_comp)
        cur.execute(sql_insert_query_comp2)
        cur.execute(sql_insert_query_date)
        cur.execute(sql_insert_query_fact)
    con.commit()

    # TEST
    db.upsert_fact_prices(data)

    sql_select_query_test_update = sql.SQL("SELECT close_price FROM fact_prices WHERE price_id = 1;")
    sql_select_query_test_insert = sql.SQL("SELECT * FROM fact_prices WHERE price_id = 2;")

    with db.get_connection() as conn:
        update_value = pd.read_sql_query(sql_select_query_test_update.as_string(conn), conn)
        insert_value = pd.read_sql_query(sql_select_query_test_insert.as_string(conn), conn)

    actual_price = update_value.iloc[0,0]

    assert actual_price == 22, f"Erwartet 22, aber bekam {actual_price}"
    assert not insert_value.empty, "Der Datensatz mit price_id 2 wurde nicht gefunden!"


def test_get_all_companies():
    db = PostgreSQLConnector(postgre_db)
    con = db.get_connection()

    sql_insert_comps_query1 = sql.SQL("INSERT INTO 'dim_company' (company_id, name, symbol, country, industry) VALUES (1, 'Firma1', 'F1', 'Ger', 'Holz');")
    sql_insert_comps_query2 = sql.SQL("INSERT INTO 'dim_company' (company_id, name, symbol, country, industry) VALUES (2, 'Firma2', 'F2', 'Ge', 'Metal');")
    sql_insert_comps_query3 = sql.SQL("INSERT INTO 'dim_company' (company_id, name, symbol, country, industry) VALUES (3, 'Firma3', 'F3', 'Fr', 'Luft');")

    with con.cursor() as cur: 
        cur.execute(sql_insert_comps_query1)
        cur.execute(sql_insert_comps_query2)
        cur.execute(sql_insert_comps_query3)
    con.commit()

    sql_select_query = sql.SQL("SELECT company_id, name FROM dim_company WHERE company_id = 3;")

    with db.get_connection() as con:
        selected_company = pd.read_sql_query(sql_select_query.as_string(con), con)

    assert selected_company.iloc[0,0] == 1
    assert selected_company.iloc[0,1] == "Firma3"

def test_get_all_dates():
    pass

def test_get_date_id():
    pass

def test_get_all_companies():
    pass

def test_insert_company():
    pass

def test_insert_to_staging():
    pass

def test_get_staging_prices():
    pass


def test_truncate_table():
    pass