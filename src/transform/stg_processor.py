import pandas as pd
from utils.logger import get_logger
from utils.paths import DATA_DIR, CONFIG_DIR
from utils.config import yaml_read
from utils.db import PostgreSQLConnector
import os
from utils.helpers import normalize_symbol

class StgProcessor():
    """
    Handles transformation logic for Staging and Fact preparation.
    """
    def __init__(self):
        self.logger = get_logger(__name__)
        self.schema = yaml_read("schema.yaml")

    def normalize_df_for_stg_prices(self, df_old: pd.DataFrame, asset_ticker: str) -> pd.DataFrame | None:
        """Normalizes raw API dataframe to staging schema."""
        if df_old.empty:
            return None
  
        cols = [col for col in self.schema["tables"]["stg_prices"]["columns"]]
        df_normalized = pd.DataFrame(columns=cols)

        try:
            if isinstance(df_old.columns, pd.MultiIndex):
                df_old.columns = df_old.columns.get_level_values(0)     # Removes empty columns axis 1 = columns 
        except Exception as e:
            self.logger.warning(f"Index flatting warning: {e}")

        # Fill the normalized DF
        # Mapping yFinace columns to DB columns
        df_normalized["full_date"] = df_old["Date"]
        df_normalized["open_price"] = df_old["Open"]
        df_normalized["high_price"] = df_old["High"]
        df_normalized["low_price"] = df_old["Low"]
        df_normalized["close_price"] = df_old["Close"]
        df_normalized["volume"] = df_old["Volume"]
        df_normalized["asset"] = normalize_symbol(asset_ticker)

        return df_normalized

    def stg_normalize_for_fact_prices(self, enriched_df: pd.DataFrame):
        """Prepares enriched dataframe for fact_prices insertion."""
        df_facet_prices =  enriched_df[[
            "date_id",
            "company_id",
            "close_price",
            "high_price",
            "low_price",
            "open_price",
            "volume"
        ]].copy()

        # Data Quality Check
        if df_facet_prices["company_id"].isnull().any() or df_facet_prices["date_id"].isnull().any():
            self.logger.warning("Dropping rows with missing CompanyID or DateID.")

        df_facet_prices.dropna(subset=["company_id", "date_id"], inplace=True)
        return df_facet_prices
    
    def get_enriched_df(self, stg_data: pd.DataFrame, dim_comp_df: pd.DataFrame, dim_date_df: pd.DataFrame) -> pd.DataFrame:
        """Joins Staging data with Dimensions."""

        # Normalize join keys
        stg_data["asset"] = stg_data["asset"].str.strip().str.upper()
        dim_comp_df["symbol"] = dim_comp_df["symbol"].str.strip().str.upper()

        # Normalize Date formats
        stg_data["full_date"] =  pd.to_datetime(stg_data["full_date"]).dt.strftime("%Y-%m-%d")
        dim_date_df["full_date"] =  pd.to_datetime(dim_date_df["full_date"]).dt.strftime("%Y-%m-%d")

        # Join Company
        enriched_df = pd.merge(
            stg_data,
            dim_comp_df,
            left_on="asset",
            right_on="symbol",
            how="left"
       )
        # Join Date
        enriched_df = pd.merge(
        enriched_df,
        dim_date_df,
        on="full_date",
        how="left"
        )

        return enriched_df
        
   