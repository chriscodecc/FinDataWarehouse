import yfinance as yf
import csv
import requests
import pandas as pd
import os
from utils.logger import get_logger
from datetime import datetime, timedelta
from utils.paths import DATA_DIR, CONFIG_DIR
from utils.config import yaml_read
import re



class YahooFinanceClient:
    """
    Client for interacting with the Yahoo Finance API via yfinance.

    Responsibilities:
    - Download historical financial data.
    - Save financial data as CSV files.
    - Normalize ticker symbols.
    - Extract and return company metadata.
    - Generate standardized CSV file names.
    """

    def __init__(self):
        """
        Initialize the YahooFinanceClient.

        - Sets up a logger.
        - Initializes yfinance_data as None.
        """

        self.logger = get_logger(__name__)
        self.yfinance_data = None
        self.config = yaml_read("config.yaml")
        self.index_mapping = yaml_read("index_mapping.yaml")

    def save_to_csv(self, data : pd.DataFrame, ticker_name, startDate: str, endDate: str = ""):
        """
        Save a pandas DataFrame to a CSV file.

        Args:
            data (pd.DataFrame): The DataFrame containing financial data.
            ticker_name (str): The ticker symbol of the asset.
            startDate (str): Start date of the dataset in "YYYY-MM-DD" format.
            endDate (str, optional): End date of the dataset in "YYYY-MM-DD" format. Defaults to "".

        Notes:
            - Flattens MultiIndex columns if present.
            - Resets index to ensure "Date" is a column.
            - Adds an "Asset" column with the normalized ticker symbol.
            - Reorders columns so "Date" and "Asset" come first.
            - Saves the DataFrame as a CSV in the configured DATA_DIR.
        """
        filename = self.csv_file_name_generator(ticker_name, startDate, endDate)
        file_path = DATA_DIR / filename

        if not data.empty:
            # Resolve multi-index columns 
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = data.columns.get_level_values(0)

            data = data.reset_index()   #["Date"] = [startDate] * len(data)
            data["Asset"] = self.normalize_symbol(ticker_name)

            #Change column order -> set "Date" forward
            cols = ["Date", "Asset"] + [col for col in data.columns if col not in ["Date", "Asset"]]
            data = data[cols]
    
            data.to_csv(file_path, index=False)
            self.logger.debug(file_path / " is safed as csv")
     
    def normalize_symbol(self, symbole: str) -> str:
        """
        Normalize a ticker symbol by removing whitespace.

        Args:
            symbole (str): The ticker symbol.

        Returns:
            str: The normalized symbol. Returns an empty string if None.
        """
        if symbole is None:
            return ""
        return re.sub(r"\s+", "", symbole)

    def extract_meta(self, info: dict, symbole: str) -> dict:
        """
        Extract company metadata from a yfinance Ticker.info dictionary.

        Args:
            info (dict): Metadata dictionary returned by yfinance.
            symbole (str): The ticker symbol.

        Returns:
            dict: Dictionary containing:
                - symbol (str)
                - name (str)
                - country (str)
                - industry (str)
        """
        return {
            "symbol": symbole,
            "name":  info.get("longName") or info.get("shortName"),
            "country": (
                info.get("country") or
                info.get("region") or
                "Global"
            ),
            "industry": (
                info.get("industry") or
                info.get("sector") or
                info.get("quoteType") or
                "Unknown"
            )
        }    

    def  fetch_finance_data(self, symbol, date="2025-09-12", endDate = ""):
        """
        Fetch historical finance data for a ticker symbol.

        Args:
            ticker_symbol (str): The ticker symbol of the asset.
            date (str): Start date in "YYYY-MM-DD" format.
            endDate (str, optional): End date in "YYYY-MM-DD" format. Defaults to "".

        Notes:
            - If endDate is empty, fetches one day of data.
            - Handles connection timeouts, DNS errors, HTTP errors, and request exceptions.
            - Saves fetched data to CSV via save_to_csv.
            - Logs an error if no data is returned.
        """
        mapped_symbol = self.get_mapping_for_company(symbol)
        date_obj = datetime.strptime(date, "%Y-%m-%d")
        symbol
        try: 
            if endDate != "":
                self.yfinance_data = yf.download(mapped_symbol, start=date, end= endDate, timeout=5)
            else:
                self.yfinance_data = yf.download(mapped_symbol, start=date, end= date_obj + timedelta(days=1), timeout=5)
            #<symbol>_<YYYY-MM-DD>.csv
            if self.yfinance_data.empty:
                # https://query1.finance.yahoo.com
                self.logger.error(f"{symbol} yFinace data are empty ")
            else:
                if self.config["pipline"]["save_csv"]:
                    self.save_to_csv(self.yfinance_data, symbol, date, endDate)
                    return None
                else:
                   return self.stg_normalize_data(self.yfinance_data.reset_index(), symbol)
        

        # Handles slow or unreachable servers
        except requests.exceptions.Timeout:
           self.logger.error("Request timed out.")

        # Catches DNS failures
        except requests.exceptions.ConnectionError:
            self.logger.error("Failed to connect to the API.")

        # Cates HTTP errors like 404, 500...
        except requests.exceptions.HTTPError as err:
            self.logger.error(f"HTTP error occurred: {err}")

        # A Catch for everything else
        except requests.exceptions.RequestException as err:
            self.logger.error(f"Unexpected error: {err}")

    def fetch_company_info(self, symbole: str):
        """
        Fetch company metadata for a given ticker symbol.

        Args:
            symbole (str): The ticker symbol.

        Returns:
            dict | None: Dictionary with company metadata, or None if not found.

        Notes:
            - Uses index_mapping.yaml to resolve the ticker mapping.
            - Logs an error if the symbol is unknown.
        """
        
        # TODO: NICHT GETESTETE ÄNDERUNG SIEHE GET_MAPPING_FOR_COM IN VERBINDUNG MIT DIESER FKT
        symbole = self.get_mapping_for_company(symbole)
        try:
            company = yf.Ticker(symbole)
            return self.extract_meta(company.info, symbole)
        except:
            self.logger.error(f"Unknown Key, cant insert {symbole}")
        return None
    
    def get_mapping_for_company(self, symbole:str):
        symbole = self.normalize_symbol(symbole)
        return self.index_mapping["indices"][symbole]
        
    def csv_file_name_generator(self, symbole: str, startDate: str, endDate: str = ""):
        """
        Generate a standardized CSV filename for storing financial data.

        Args:
            symbole (str): The ticker symbol.
            startDate (str): Start date in "YYYY-MM-DD" format.
            endDate (str, optional): End date in "YYYY-MM-DD" format. Defaults to "".

        Returns:
            str: Filename in the format:
                 - "<SYMBOL>_<STARTDATE>.csv" if endDate is empty.
                 - "<SYMBOL>_<STARTDATE>_to_<ENDDATE>.csv" otherwise.
        """
        if endDate == "":
            return symbole + "_" + startDate + ".csv"
        else:
            return symbole + "_" + startDate + "_to_" + endDate + ".csv"
            
    def stg_normalize_data(self, df_old: pd.DataFrame, symbole):
        # Load and Create the table schema
        schema = yaml_read("schema.yaml")
        cols = [col for col in schema["tables"]["stg_prices"]["columns"]]
        df_normalized = pd.DataFrame(columns=cols)
        
        # Merge duplicates and removes NaN values axis 0 = rows 
        df_old = df_old.groupby(level=0, axis=1).first()

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
        df_normalized["asset"] = symbole

        return df_normalized