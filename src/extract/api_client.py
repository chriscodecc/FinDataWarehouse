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
    """

    def __init__(self):
        self.logger = get_logger(__name__)
        self.yfinance_data = None
        self.config = yaml_read("config.yaml")
        self.index_mapping = yaml_read("index_mapping.yaml")

    def extract_meta(self, info: dict, symbole: str) -> dict:
        """Extract company metadata from yfinance info."""
        return {
            "symbol": symbole,
            "name":  info.get("longName") or info.get("shortName") or symbole,
            "country": info.get("country") or info.get("region") or "Global",
            "industry": info.get("industry") or info.get("sector") or info.get("quoteType") or "Unknown"
        }    

    def fetch_finance_data(self, symbol, date="2025-09-12", endDate = "") -> pd.DataFrame | None:
        """
        Fetch historical fincace data.
        Returns DataFrame if successful, None otherwise.
        """
        mapped_symbol = self.get_mapping_for_company(symbol)
        date_obj = datetime.strptime(date, "%Y-%m-%d")

        try: 
            if endDate:
                self.yfinance_data = yf.download(mapped_symbol, start=date, end= endDate, timeout=5)
            else:
                self.yfinance_data = yf.download(mapped_symbol, start=date, end= date_obj + timedelta(days=1), timeout=5)
            #<symbol>_<YYYY-MM-DD>.csv
            if self.yfinance_data.empty:
                # https://query1.finance.yahoo.com
                self.logger.error(f"{symbol} yFinace data are empty ")
                return None

            # Save to CSV file config
            if self.config.get("pipline", {}).get("save_csv", False):
                csv_prozessort = CSVProcessor()
                csv_prozessort.save_to_csv(self.yfinance_data, symbol, date, endDate)
                
            return self.yfinance_data.reset_index()
        
        except requests.exceptions.Timeout:
           self.logger.error("Request timed out.")
        except requests.exceptions.ConnectionError:
            self.logger.error("Failed to connect to the API.")
        except requests.exceptions.RequestException as err:
            self.logger.error(f"Unexpected error: {err}")

        return None

    def fetch_company_info(self, symbol: str) -> dict:
        """Fetch company metadata with retries."""
        max_reties = 3
        delay_seconds = 2
        attempts = 0
        
        while attempts < max_reties:
            attempts += 1
            try:
                company = yf.Ticker(symbol)
                if company.info:
                    return self.extract_meta(company.info, symbol)
            except Exception as e:
                self.logger.warning(f"Attempt {attempts} failed for {symbol}: {e}")

            if attempts < max_reties:
                time.sleep(delay_seconds)

        self.logger.error(f"Could not fetch info for {symbol} after retries.")
        return None
        
    def get_mapping_for_company(self, symbol:str) -> str:
        """Resolve internal symbol to Yahoo Finance ticker."""
        symbol = normalize_symbol(symbol)
        return self.index_mapping["indices"].get(symbol, symbol)
        
    
    
