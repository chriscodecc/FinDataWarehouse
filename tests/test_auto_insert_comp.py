from main import handle_missing_companies
from utils.db import PostgreSQLConnector
from extract.api_client import YahooFinanceClient


db_con = PostgreSQLConnector()
yf_Client = YahooFinanceClient()

dim_comp_df = db_con.get_dim_company_as_df()
stg_df = db_con.get_stg_prices()

handle_missing_companies(stg_df,dim_comp_df, db_con, yf_Client)

