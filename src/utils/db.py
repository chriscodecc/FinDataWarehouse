import psycopg2 as psy
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os
import yaml
from utils.logger import get_logger
from utils.paths import DATA_DIR, BASE_DIR, CONFIG_DIR
import pandas as pd
from utils.config import yaml_read
import re
from sqlalchemy import create_engine


class PostgreSQLConnector():
    """
    Connector class for PostgreSQL databases.

    Responsibilities:
    - Establish and manage database connections.
    - Access dimension tables (dim_company, dim_date).
    - Perform upsert operations on fact_prices.
    - Insert new companies into dim_company.
    - Generate dim_date entries from a Pandas DataFrame.
    """
    
    def __init__(self):
        """
        Initialize the PostgreSQLConnector.

        - Loads environment variables for sensitive data (user, password).
        - Reads non-sensitive configuration values from "config.yaml".
        - Loads schema definitions from schema.yaml.
        - Prepares database connection parameters.
        """

        self.logger = get_logger(__name__)
        self.connection = None
        #data_dir = os.path.join(os.path.dirname(__file__), "..","..", "config.yaml")
        config_dir = CONFIG_DIR / "config.yaml"
        # Load environment variables from .env file (contains sensitive data)
        load_dotenv()
         # Gets tabelen configuration from schema.yaml
        self.schema = yaml_read("schema.yaml")

        # Load non-sensitive configuration values from config.yaml
        with open(config_dir, "r") as config_yaml:
            config = yaml.safe_load(config_yaml)
            self.logger.debug(config_dir / " is loaded")

        # Retrieve sensitive database credentials from environment variables
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")

        # Retrieve non-sensitive database connection settings from config.yaml
        self.host = config["database"]["host"]
        self.port = config["database"]["port"]
        self.dbname = config["database"]["name"]

    def _connect(self):
        """
        Establish a new PostgreSQL connection.

        Returns:
            psycopg2 connection object if successful, None otherwise.

        Raises:
            psycopg2.OperationalError: If connection cannot be established.
            Exception: For any unknown errors.
        """

        try:
           return psy.connect(dbname=self.dbname, user=self.user, password=self.password, host=self.host, port=self.port)       
        
        except psy.OperationalError as e:
            self.logger.critical("Could not connect to DB", e)
        except Exception as e:
            self.logger.critical("Unknown Error:", e)

    def _get_curser(self):
        """
        Retrieve a database cursor.

        Ensures that a connection is established before returning a cursor.

        Returns:
            psycopg2 cursor object.
        """

        if self.connection is None or self.connection.closed != 0:
            self.connection = self._connect()
        return self.connection.cursor()

    def _close(self, cur ):
        """
        Close the active database connection.

        Notes:
            - Safe to call multiple times.
            - Will raise if no connection was ever established.
        """
        cur.close
        self.connection.close()
       
    def get_company_id(self, comp_code: str):
        """
        Look up a company_id in the company dimension table.

        Args:
            comp_code (str): The company symbol (ticker code).

        Returns:
            int | None: The company_id if it exists, otherwise None.

        Notes:
            - The table name is read dynamically from "schema.yaml".
            - Whitespace is removed from the symbol before comparison.
        """
        cur = self._get_curser()

        table_company = self.schema["tables"]["dim_company"]["name"]

        cur.execute(f"SELECT company_id, symbol FROM {table_company} WHERE symbol = %s;", (comp_code,))

        # Fetch all rows from the query as a list of tuples, e.g. [('DAX',), ('SAP',), ...]
        rows = cur.fetchall()
        self.connection.close()
        # Check if the provided company code exists among the known symbols
        for id, symbole in rows:
            if comp_code == re.sub(r"\s+", "", symbole):
                return id
        return None

    def get_date_id(self, date ="2000-01-09"):
        """
        Look up a date_id in the date dimension table.

        Args:
            date (str): Date in "YYYY-MM-DD" format.

        Returns:
            int | None: The date_id if it exists, otherwise None.

        Notes:
            - The table name is read dynamically from "schema.yaml".
        """    
        cur = self._get_curser()
        table_date = self.schema["tables"]["dim_date"]["name"]

        cur.execute(f"SELECT date_id FROM {table_date} WHERE full_date='{date}';")
        date_id = cur.fetchone()[0]
        
        return date_id

    def upsert_fact_prices(self, df):
        """
        Insert or update daily price records in fact_prices.

        Args:
            df (pd.DataFrame): DataFrame containing the columns:
                - date_id (int)
                - company_id (int)
                - close_price (float)
                - high_price (float)
                - low_price (float)
                - open_price (float)
                - volume (int)

        Notes:
            - Uses PostgreSQL "ON CONFLICT (date_id, company_id) DO UPDATE".
            - Requires a UNIQUE constraint on (date_id, company_id).
            - Drops "price_id" from the DataFrame if present.
        """

        df = df.drop("price_id", axis=1)
        cur = self._get_curser()
        # Converts df to Tuple List
        records = df.to_records(index=False).tolist()
        tabele_prices = self.schema["tables"]["fact_prices"]["name"]
        insert_query = f"""INSERT INTO {tabele_prices} 
                        (date_id, company_id, close_price, high_price, low_price, open_price, volume) VALUES %s 
                        ON CONFLICT (date_id, company_id) DO UPDATE SET
                        close_price = EXCLUDED.close_price,
                        high_price = EXCLUDED.high_price,
                        low_price = EXCLUDED.low_price,
                        open_price = EXCLUDED.open_price,
                        volume = EXCLUDED.volume
                        RETURNING price_id;"""

        # Bulk Insert
        execute_values(cur, insert_query, records)
        self.logger.debug(insert_query + "succsesfull")
        self.connection.commit()
        cur.close()
        self.connection.close

    def insert_company(self, comp_info: dict):
        """
        Insert a new company into the company dimension table.

        Args:
            comp_info (dict): Dictionary with the following keys:
                - name (str)
                - symbol (str)
                - country (str)
                - industry (str)

        Returns:
            int: The company_id of the inserted row.

        Notes:
            - The table name is read dynamically from "schema.yaml".
        """
        cur = self._get_curser()

        tabele_company = self.schema["tables"]["dim_company"]["name"]
        insert_query = f""" INSERT INTO {tabele_company} 
                            (name, symbol, country, industry) VALUES (%s, %s, %s, %s) RETURNING company_id;"""

        cur.execute(insert_query, ( 
            comp_info["name"],
            comp_info["symbol"],
            comp_info["country"],
            comp_info["industry"]))
        self.connection.commit()
        self.connection.close()

    def create_dim_date(self, start = "2000-01-01", end="2030-12-31"):
        """
        Generate and populate the date dimension table with daily granularity.

        Args:
            start (str): Start date in "YYYY-MM-DD" format.
            end (str): End date in "YYYY-MM-DD" format.

        Notes:
            - Creates the columns: full_date, day, month, year.
            - Uses Pandas "date_range" and SQLAlchemy "to_sql".
            - Appends data to the dim_date table.
        """

        dates = pd.date_range (start=start, end=end, freq="D")
        df = pd.DataFrame ({
            "full_date" : dates,
            "day" : dates.day,
            "month" : dates.month,
            "year" : dates.year
        })
        
        engine = create_engine(f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}")
        df.to_sql("dim_date", engine, if_exists="append", index=False)

        #["company_id", "name", "symbol", "country", "industry"]

    def insert_staging(self, df: pd.DataFrame):
        cur = self._get_curser()
        print(df)
        df = df.drop("load_timestamp", axis=1)
        df["full_date"] = df["full_date"].astype(str)
        records = df.to_records(index=False).tolist()
        table_stg_prices = self.schema["tables"]["stg_prices"]["name"]
        insert_query = f""" INSERT INTO {table_stg_prices}
                        (asset, full_date, open_price, high_price, low_price,close_price, volume ) 
                        VALUES %s ;"""
        print(insert_query, records)
        execute_values(cur,insert_query,records)
        self.logger.debug(f"{insert_query} succesfull")#
        self.connection.commit()
        self._close(cur)
        
