from utils.db import PostgreSQLConnector

db_con = PostgreSQLConnector()

print(db_con.get_dim_date_as_df())