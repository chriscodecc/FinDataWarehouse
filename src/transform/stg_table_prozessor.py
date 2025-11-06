import pandas as pd
from utils.logger import get_logger
from utils.paths import DATA_DIR, CONFIG_DIR
from utils.config import yaml_read
from utils.db import PostgreSQLConnector
import os
from utils.helpers import normalize_symbol

class StgProzessor():

    def __init__(self):
        pass

    def normalize_df_for_stg_prices(self, df_old: pd.DataFrame, asset_ticker: str):
        if not df_old.empty:
            # Load and Create the table schema
            schema = yaml_read("schema.yaml")
            cols = [col for col in schema["tables"]["stg_prices"]["columns"]]
            df_normalized = pd.DataFrame(columns=cols)

            # Merge duplicates and removes NaN values axis 0 = rows 
            df_old = df_old.T.groupby(level=0).first().T

            # Removes empty columns axis 1 = columns 
            # any / all
            df_old = df_old.dropna(axis=1, how="all")

            # Fill the normalized DF
            df_normalized["full_date"] = df_old["Date"]
            df_normalized["open_price"] = df_old["Open"]
            df_normalized["high_price"] = df_old["High"]
            df_normalized["low_price"] = df_old["Low"]
            df_normalized["close_price"] = df_old["Close"]
            df_normalized["volume"] = df_old["Volume"]
            df_normalized["asset"] = normalize_symbol(asset_ticker)

            return df_normalized
        return None

    def stg_normalize_for_fact_prices(self, enriched_df: pd.DataFrame):
        df_facet_prices =  enriched_df[[
            "date_id",
            "company_id",
            "close_price",
            "high_price",
            "low_price",
            "open_price",
            "volume"
        ]]

        if df_facet_prices["company_id"].isnull().any() or df_facet_prices["date_id"].isnull().any():
            print("WARNING: Some Datasets have no key")

        df_facet_prices.dropna(subset=["company_id", "date_id"], inplace=True)

        return df_facet_prices
    
    def get_enriched_df(self, stg_data: pd.DataFrame, dim_comp_df: pd.DataFrame, dim_date_df: pd.DataFrame):
        # Make sure asset/symbol have the same structure
        stg_data["asset"] = stg_data["asset"].str.strip().str.upper()
        dim_comp_df["symbol"] = dim_comp_df["symbol"].str.strip().str.upper()

        # Make sure the date formate is the same
        stg_data["full_date"] =  pd.to_datetime(stg_data["full_date"]).dt.strftime("%Y-%m-%d")
        dim_date_df["full_date"] =  pd.to_datetime(dim_date_df["full_date"]).dt.strftime("%Y-%m-%d")

        enriched_df = pd.merge(
            stg_data,
            dim_comp_df,
            left_on="asset",
            right_on="symbol",
            how="left"
       )
        enriched_df = pd.merge(
        enriched_df,
        dim_date_df,
        on="full_date",
        how="left"
        )
        print(enriched_df)
        return enriched_df
        
   