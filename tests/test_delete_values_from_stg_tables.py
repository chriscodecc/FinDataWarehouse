from utils.db import PostgreSQLConnector

postCon = PostgreSQLConnector()

postCon.delete_all_values_from_table("stg_prices")