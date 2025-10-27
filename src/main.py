from extract.api_client import YahooFinanceClient
from utils.db import PostgreSQLConnector as PostSQLCon
import csv
import os
from transform.csv_processor import CSVProcessor
import pandas as pd
from utils.config import yaml_read
from transform.stg_table_prozessor import StgProzessor
from datetime import datetime, timedelta
from utils.logger import get_logger

from utils.paths import BASE_DIR, CONFIG_DIR

def handle_missing_companies(stg_df: pd.DataFrame, dim_comp_df: pd.DataFrame, dbCon, yFinanceClient):
    stg_assets= set(stg_df["asset"])
    dim_comp_symbols = set(dim_comp_df["symbol"])
    missing_comps = stg_assets - dim_comp_symbols
    
    if missing_comps is not None:
       for symbol in missing_comps:
          company = yFinanceClient.fetch_company_info(symbol)
          dbCon.insert_company(company)
    
    if missing_comps: 
       return True
    return None


logger = get_logger(__name__)

def main():
   today = datetime.now()
   yesterdays = today - timedelta(days=1)
   date_to_fetch = yesterdays.strftime("%Y-%m-%d")

   logger.info(f"Start ETL-Pipline for: {date_to_fetch}")
   yf_client = YahooFinanceClient()
   stg_prozessor = StgProzessor()
   db_con = PostSQLCon()

   ticker = yaml_read("tickers.yaml")
   symboles = ticker["tickers"]
   start_date = str(ticker["start_date"])
   end_date = str(ticker["end_date"])

   logger.info("SCHRIT 1")
   stg_data_list = []
   for symbol in symboles:
      df = yf_client.fetch_finance_data(symbol, date_to_fetch)
      if df is not None and not df.empty:
         df = stg_prozessor.normalize_df_for_stg_prices(df, symbol)
         stg_data_list.append(df)
      else:
         logger.warning(f"WARNING: Couldn´t download data for {symbol}")

   logger.info("SCHRIT 2") 
   stg_normalized_df = pd.concat(stg_data_list, ignore_index=True)
   db_con.insert_to_staging(stg_normalized_df)

   logger.info("SCHRIT 3") 

   stg_df = db_con.get_stg_prices()

   logger.info("SCHRIT 4")
   if stg_df is not None and not stg_df.empty:
      dim_comp_df = db_con.get_dim_company_as_df()
      dim_date_df = db_con.get_dim_date_as_df()

   logger.info("SCHRIT 4")
   # Check for missing companys in dim_comp -> insert
   new_comps_insert = handle_missing_companies(stg_df, dim_comp_df, db_con, yf_client)

   logger.info("SCHRIT 5")
   if new_comps_insert:
      dim_comp_df = db_con.get_dim_company_as_df()

   logger.info("SCHRIT 6")
   enriched_df = stg_prozessor.get_enriched_df(stg_df, dim_comp_df, dim_date_df)

   logger.info("SCHRIT 7")
   facet_prices_df = stg_prozessor.stg_normalize_for_fact_prices(enriched_df)

   logger.info("SCHRIT 8")
   db_con.upsert_fact_prices(facet_prices_df)


if __name__=="__main__":
   try:
      main()
   except Exception as e:
      logger.error(f"ERROR: {e}")