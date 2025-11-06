import yfinance as yf
import csv
import requests
import pandas as pd
import os
from utils.logger import get_logger
from datetime import datetime, timedelta
from utils.paths import DATA_DIR, CONFIG_DIR
from utils.config import yaml_read
from utils.helpers import normalize_symbol
from transform.csv_processor import CSVProcessor
import time



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
            "name":  info.get("longName") or info.get("shortName") or symbole,
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

    def fetch_finance_data(self, symbol, date="2025-09-12", endDate = ""):
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
                    csv_prozessort = CSVProcessor()
                    csv_prozessort.save_to_csv(self.yfinance_data, symbol, date, endDate)
                    return None
                else:
                   print (self.yfinance_data)
                   return self.yfinance_data.reset_index()

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

        return None

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
        max_reties = 10
        delay_seconds = 2
        attempts = 0
        
        while attempts < max_reties:
            attempts += 1
            self.logger.info(f"Versuch {attempts}/{max_reties} für {symbole} (API-Symbol: {symbole})")
            try:
                company = yf.Ticker(symbole)
                if company.info:
                    return self.extract_meta(company.info, symbole)
            except:
                self.logger.error(f"Unknown Key, cant insert {symbole}")
            return None

            if attempts < max_reties:
                time.sleep(delay_seconds)

    
    def get_mapping_for_company(self, symbole:str):
        symbole = normalize_symbol(symbole)
        return self.index_mapping["indices"][symbole]
        
    
    
