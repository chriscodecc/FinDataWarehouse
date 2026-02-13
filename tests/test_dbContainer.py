import os
import pytest
from testcontainers.postgres import PostgresContainer
from utils.helpers import BASE_DIR
import psycopg2
import time

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
