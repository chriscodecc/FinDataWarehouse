from datetime import datetime, timedelta
import pandas as pd

from extract.api_client import YahooFinanceClient
from utils.db import PostgreSQLConnector as PostSQLCon
from utils.config import yaml_read
from transform.csv_processor import CSVProcessor
from transform.stg_processor import StgProcessor
from utils.logger import get_logger

logger = get_logger(__name__)

#from utils.paths import BASE_DIR, CONFIG_DIR
#import csv
#import os
def handle_missing_companies(stg_df: pd.DataFrame, dim_comp_df: pd.DataFrame, dbCon, yFinanceClient) -> bool:
    """
    Identifies companies present in staging but missing in dimension table.
    Fetches and inserts missing companies.
    """
    if stg_df is None or stg_df.empty:
       return None
    
    stg_assets= set(stg_df["asset"])
    dim_comp_symbols = set(dim_comp_df["symbol"])
    missing_companies = stg_assets - dim_comp_symbols
    
    if missing_companies:
        logger.info(f"Found {len(missing_companies)} missing companies. Fetching info...")
        for symbol in missing_companies:
            company_info = yFinanceClient.fetch_company_info(symbol)
            if company_info:
                dbCon.insert_company(company_info)
        return True

    return False


logger = get_logger(__name__)

def main():
   # 1. Setup Dates
   today = datetime.now()
   yesterdays = today - timedelta(days=1)
   date_to_fetch = yesterdays.strftime("%Y-%m-%d")

   logger.info(f"=== Start ETL-Pipline for: {date_to_fetch} ===")

   # 2. Initialize Components
   yf_client = YahooFinanceClient()
   stg_processor = StgProcessor()
   db_con = PostSQLCon()

   if not db_con.get_all_dates() or len(db_con.get_all_dates()) <= 0:
      logger.debug("dim_date is empty. \n Start filling...")
      db_con.create_dim_date()

   # 3. Load Config
   ticker_config = yaml_read("tickers.yaml")
   symbols = ticker_config["tickers"]
   
   # Used a set start and end date if there is one
   # else use yesterday
   if ticker_config["period"]["use_start_end_date"]:
      start_date = str(ticker_config["start_date"])
      end_date = str(ticker_config["end_date"])
   else:
      start_date = str(date_to_fetch)

   # 4. EXTRACT & TRANSFORM (Staging)
   stg_data_list = []
   for symbol in symbols:
      df = yf_client.fetch_finance_data(symbol, date_to_fetch)
      if df is not None and not df.empty:
         df_norm = stg_processor.normalize_df_for_stg_prices(df, symbol)
         if df_norm is not None:
            stg_data_list.append(df_norm)
      else:
         logger.warning(f"Skipping {symbol}: No data found.")

   if not stg_data_list:
       logger.warning("No data extracted for any symbol. Exiting.")
       return

   # 5. LOAD STAGING
   stg_normalized_df = pd.concat(stg_data_list, ignore_index=True)
   db_con.insert_to_staging(stg_normalized_df)

   # 6. ENRICH & LOAD FACT
   # Read back from DB (Staging Pattern)
   stg_df = db_con.get_stg_prices()

   if stg_df is not None:
      dim_comp_df = db_con.get_dim_company_as_df()
      dim_date_df = db_con.get_dim_date_as_df()

      # Handle Missing Dimensions
      if handle_missing_companies(stg_df, dim_comp_df, db_con, yf_client):
         # Reload dimensions if new companie were added
         dim_comp_df = db_con.get_dim_company_as_df()

      # Join Dimensions
      enriched_df = stg_processor.get_enriched_df(stg_df, dim_comp_df, dim_date_df)

      # Prepare for Fact Table
      facet_prices_df = stg_processor.stg_normalize_for_fact_prices(enriched_df)

      # Upsert
      db_con.upsert_fact_prices(facet_prices_df)
      logger.info("=== Pipeline finished successfully ===")
   else:
      logger.error("Staging table is empty after insert. Something is wrong.")
      

if __name__=="__main__":
   try:
      main()
   except Exception as e:
      logger.error(f"Critical Pipeline Error: {e}", exc_info=True)