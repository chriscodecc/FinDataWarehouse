from utils.db import PostgreSQLConnector

def test_get_connection():
    db_con = PostgreSQLConnector()
    conn = db_con.get_connection()

    assert conn is not None

def test_get_company_id():
    db_con = PostgreSQLConnector()
    assert db_con.get_company_id("DAX") == 49