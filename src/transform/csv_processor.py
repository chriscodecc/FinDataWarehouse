from pandas import DataFrame, read_csv
from utils.logger import get_logger
from utils.paths import DATA_DIR, CONFIG_DIR
from utils.config import yaml_read
from utils.db import PostgreSQLConnector
import os

class CSVProcessor():
    """
    Processor class for handling CSV files with financial data.

    Responsibilities:
    - Read CSV files into pandas DataFrames.
    - Extract company codes and dates from DataFrames.
    - Transform raw CSV records into fact_prices-compatible DataFrames.
    """

    def __init__(self):
        """
        Initialize the CSVProcessor.

        - Sets up a logger instance for this processor.
        """
        self.logger = get_logger(__name__)
        
    def get_comp_code(df: DataFrame, y: int = 0):
        """
        Get the company code (symbol) from a DataFrame row.

        Args:
            df (DataFrame): DataFrame containing financial records.
            y (int): Row index to extract from. Defaults to 0.

        Returns:
            Any: The value at column index 1 of the specified row.
        """
        return df.iloc[y][1]

    def get_date(df: DataFrame, y: int = 0):
        """
        Get the date value from a DataFrame row.

        Args:
            df (DataFrame): DataFrame containing financial records.
            y (int): Row index to extract from. Defaults to 0.

        Returns:
            Any: The value at column index 0 of the specified row.
        """
        return df.iloc[y][0]   

    def convert_csv_to_DataFrame(self, csv_filename):
        """
        Read a CSV file from the data directory into a pandas DataFrame.

        Args:
            csv_filename (str): The CSV filename located inside DATA_DIR.

        Returns:
            DataFrame: A pandas DataFrame with file contents.
                       Returns an empty DataFrame if the file is not found.

        Notes:
            - Logs an error if the file does not exist.
        """
        # Reads the given file in data/filename
        try:
            df = read_csv(DATA_DIR / csv_filename)
            return df
        except FileNotFoundError as exerr: 
            self.logger.error("FileExistError." +  str(exerr))
            return DataFrame()

    def transform_csv_records(self, df_old: DataFrame, dbCon: PostgreSQLConnector):
        """
        Transform raw CSV records into a schema-aligned DataFrame
        for insertion into the fact_prices table.

        Args:
            df_old (DataFrame): Raw DataFrame loaded from CSV, containing:
                - Date
                - Asset
                - Open
                - High
                - Low
                - Close
                - Volume
            dbCon (PostgreSQLConnector): Database connector for lookups.

        Returns:
            DataFrame: A new DataFrame with the schema:
                - company_id
                - date_id
                - close_price
                - high_price
                - low_price
                - open_price
                - volume

        Notes:
            - company_id is resolved via dbCon.get_company_id.
            - date_id is resolved via dbCon.get_date_id.
            - Schema is dynamically loaded from schema.yaml.
        """
        #Price id Company_id Close_ Hight Low Open Volumen 
        #dbCon = PostgreSQLConnector()
        schema = yaml_read("schema.yaml")
        cols = [col for col in schema["tables"]["fact_prices"]["columns"]]
        df_new = DataFrame(columns=cols)

        df_new["company_id"] = df_old.apply(lambda row: dbCon.get_company_id(row["Asset"]), axis=1)
        df_new["date_id"] = df_old.apply(lambda row: dbCon.get_date_id(row["Date"]), axis=1)
        
        
        df_new["close_price"] = df_old["Close"]
        df_new["high_price"] = df_old["High"]
        df_new["low_price"] = df_old["Low"]
        df_new["open_price"] = df_old["Open"]
        df_new["volume"] = df_old["Volume"]

        return df_new
