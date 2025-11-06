import psycopg2 as psy
from psycopg2.extras import execute_values
from psycopg2 import sql, errors
from dotenv import load_dotenv
import os
import yaml
from utils.logger import get_logger
from utils.paths import DATA_DIR, BASE_DIR, CONFIG_DIR
import pandas as pd
from utils.config import yaml_read
from utils.helpers import normalize_symbol
from sqlalchemy import create_engine
import re


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
        config_dir = CONFIG_DIR / "config.yaml"
        # Load environment variables from .env file (contains sensitive data)
        dotenv_path = BASE_DIR / ".env"
        load_dotenv(dotenv_path)

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

    def get_connection(self):
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

        """
        Close the active database connection.

        Notes:
            - Safe to call multiple times.
            - Will raise if no connection was ever established.
        """
        #self.connection.close
       
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
    
        table_company = self.schema["tables"]["dim_company"]["name"]

        # Fetch all rows from the query as a list of tuples, e.g. [('DAX',), ('SAP',), ...]
        rows = self.get_all_companie()
        # Check if the provided company code exists among the known symbols
        for id, symbole in rows:
            if comp_code == normalize_symbol(symbole):
                return id
        return None
    
    def get_all_companie(self):
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM dim_company;")
                return cur.fetchall()
            
    def get_all_date(self):
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM dim_date;")
                return cur.fetchall()     

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
        
        table_date = self.schema["tables"]["dim_date"]["name"]
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT date_id, symbol FROM {table_date} WHERE full_date = %s;", date)
                result = cur.fetchone()        
                return result[0] if result else None

    def upsert_fact_prices(self, df: pd.DataFrame):
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
        df = df.drop(columns=["price_id"], errors="ignore")
        
        conn = self.get_connection()
        # Converts df to Tuple List
        df.drop_duplicates(subset=["date_id","company_id"], keep="last", inplace=True)
        records = df.to_records(index=False).tolist()

       # TODO: normalize for auto stg insert for date and comp get id
      
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
        execute_values(conn.cursor(), insert_query, records)
        self.logger.debug("Upsert succsesfull")
        conn.commit()

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
        conn = self.get_connection()
        cur = conn.cursor()

        tabele_company = self.schema["tables"]["dim_company"]["name"]
        insert_query = f""" INSERT INTO {tabele_company} 
                            (name, symbol, country, industry) VALUES (%s, %s, %s, %s) RETURNING company_id;"""
        
        cur.execute(insert_query, ( 
            comp_info["name"],
            comp_info["symbol"],
            comp_info["country"],
            comp_info["industry"]))
        conn.commit()

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
        
        #engine = create_engine(f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}")
        #df.to_sql("dim_date", engine, if_exists="append", index=False)
        conn = self.get_connection()
        cur = conn.cursor()
        df['full_date'] = pd.to_datetime(df['full_date'], unit='ms').dt.date
        records = df.to_records(index=False).tolist()
        table_date = table_stg_prices = self.schema["tables"]["dim_date"]["name"]
        
        insert_query = f"""INSERT INTO {table_date} 
                        (full_date, day, month, year) VALUES %s;"""
        
        execute_values(cur, insert_query, records)
        self.logger.info("Date´s inserted succesfull.")
        conn.commit()

    def insert_to_staging(self, df: pd.DataFrame):

        conn = self.get_connection()
        df = df.drop("load_timestamp", axis=1)
        df["full_date"] = df["full_date"].astype(str)
        records = df.to_records(index=False).tolist()
        table_stg_prices = self.schema["tables"]["stg_prices"]["name"]

        insert_query = f""" INSERT INTO {table_stg_prices}
                        (asset, full_date, open_price, high_price, low_price,close_price, volume ) 
                        VALUES %s ;"""
        
        execute_values(conn.cursor(),insert_query,records)
        self.logger.debug(f"{insert_query} succesfull")#
        conn.commit()
        
    def get_stg_prices(self):
        """
        SELECT all Data from stg_prices

         Returns:
            pandas DataFrame if successful, None otherwise.
        """
        con = self.get_connection() 
        table_stg_prices = self.schema["tables"]["stg_prices"]["name"]

        query = f"SELECT * FROM {table_stg_prices};"
        df = pd.read_sql_query(query,con)
        
        if not df.empty:
            self.delete_all_values_from_table(table_stg_prices)
            return df
        else:
            return None
        
    def delete_all_values_from_table(self, tableName: str):
        query = sql.SQL("TRUNCATE TABLE {table}").format(table=sql.Identifier(tableName)) 

        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SET LOCAL statement_timeout = '5s';")
                    cur.execute(query)
                    
            self.logger.info(f"Table '{tableName}' truncated successfully.")    
        except errors.LockNotAvailable:
            self.logger.error(f"Timeout. The table {tableName} is block by another prozess!")

        except Exception as e:
            self.logger.error("A random Ork appears. You can't finish the TRUNCATE!")
    
    def get_dim_company_as_df(self):
        conn = self.get_connection() 
        schema = yaml_read("schema.yaml")
        cols = [col for col in schema["tables"]["dim_company"]["columns"]]
        data = self.get_all_companie()

        return pd.DataFrame(data,columns=cols)
 
    def get_dim_date_as_df(self):
        conn = self.get_connection() 
        schema = yaml_read("schema.yaml")
        cols = [col for col in schema["tables"]["dim_date"]["columns"]]
        dim_date= pd.DataFrame(columns=cols)
        data = self.get_all_date() # sie obenaaaaaaaaaaaaaaaaaaaaaaaaaaaa

        return pd.DataFrame(data, columns=cols)