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
    Handles connections, queries, and bulk operations safely using psycopg2.sql.
    """
    
    def __init__(self):
        self.logger = get_logger(__name__)
        #self.conn = None
        self.config_path = CONFIG_DIR / "config.yaml"
        
        # Load environment variables 
        load_dotenv(BASE_DIR / ".env")

         # Load schema and config
        self.schema = yaml_read("schema.yaml")
        with open(self.config_path, "r") as config_yaml:
            self.app_config = yaml.safe_load(config_yaml)

        # Database Credentials
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")
        self.host = self.app_config["database"]["host"]
        self.port = self.app_config["database"]["port"]
        self.dbname = self.app_config["database"]["name"]

    def get_connection(self):
        """Establich a new database connection."""

        try:
           return psy.connect(
               dbname=self.dbname, 
               user=self.user, 
               password=self.password, 
               host=self.host, 
               port=self.port
            )          
        except psy.OperationalError as e:
            self.logger.critical("Could not connect to DB: {e}")
            raise
        except Exception as e:
            self.logger.critical("Unknown DB Error: {e}")
            raise
       
    def get_company_id(self, comp_code: str) -> int | None:
        """Fetch company_id by symbole."""
        rows = self.get_all_companies()
        for comp_id, symbol in rows:
            if normalize_symbol(comp_code) == normalize_symbol(symbol):
                return comp_id
        return None
    
    def get_all_companies(self) -> list:
        """Fetch all rows from dim_company."""
        table_name = self.schema["tables"]["dim_company"]["name"]
        query = sql.SQL("SELECT * FROM {table};").format(table=sql.Identifier(table_name))

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                return cur.fetchall()
            
    def get_all_dates(self) -> list:
        """Fetch all rows from dim_date."""
        table_name = self.schema["tables"]["dim_date"]["name"]
        query = sql.SQL("SELECT * FROM {table};").format(table=sql.Identifier(table_name))

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                return cur.fetchall()     

    def get_date_id(self, full_date : str) -> int | None:
        """Fetch date_id by date string."""
        table_name = self.schema["tables"]["dim_date"]["name"]
        query = sql.SQL("SELECT date_id FROME {table} WHERE full_date= %s").format(table=sql.Identifier(table_name))

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (full_date,))
                result = cur.fetchone()        
                return result[0] if result else None

    def upsert_fact_prices(self, df: pd.DataFrame) -> None:
        """Upsert daily price records into the fact_prices."""
        if df.empty:
            self.logger.info("No Data to insert into fact_prices.")
            return
        
        # df to table structure 
        cols_to_insert = ["date_id", "company_id", "close_price", "high_price", "low_price", "open_price", "volume"]
        df_final = df[cols_to_insert].copy()

        table_name = self.schema["tables"]["fact_prices"]["name"]
        records = df_final.to_records(index=False).tolist()

        # build SQL-Query 
        columns_sql = sql.SQL(", ").join(map(sql.Identifier, cols_to_insert))
        query = sql.SQL("INSERT INTO {table} ({cols}) VALUES %s").format(
            table=sql.Identifier(table_name),
            cols=columns_sql
        )

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # 3. Massen-Insert
                execute_values(cur, query, records)
                conn.commit() 
                self.logger.info(f"DONE! {len(df_final)} Datasets saved in fact_prices.")
 

    def insert_company(self, comp_info: dict) -> None:
        """Insert a new company into dim_company."""
        table_name = self.schema["tables"]["dim_company"]["name"]
        insert_query = sql.SQL("""
                               INSERT INTO {table} (name, symbol, country, industry)
                               VALUES (%s, %s, %s, %s) 
                               RETURNING company_id;
        """).format(table=sql.Identifier(table_name))

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(insert_query, ( 
                    comp_info.get("name"),
                    comp_info.get("symbol"),
                    comp_info.get("country"),
                    comp_info.get("industry")
                ))
                conn.commit()
                self.logger.info(f"Inserted new company: {comp_info.get('symbol')}")   

    def create_dim_date(self, start = "2000-01-01", end="2030-12-31") -> None:
        """Populate dim_date."""
        dates = pd.date_range (start=start, end=end, freq="D")
        df = pd.DataFrame ({
            "date_id" : dates.strftime("%Y%m%d").astype(int),
            "full_date" : dates,
            "day" : dates.day,
            "month" : dates.month,
            "year" : dates.year
        })
        df['full_date'] = df['full_date'].dt.date
        records = df.to_records(index=False).tolist()

        table_date = self.schema["tables"]["dim_date"]["name"]
        insert_query = sql.SQL("""INSERT INTO {table} (date_id, full_date, day, month, year) VALUES %s;""").format(table=sql.Identifier(table_date))
        
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                execute_values(cur, insert_query, records)
                conn.commit()
                self.logger.info("Date dimension populated successfully.")

    def insert_to_staging(self, df: pd.DataFrame) -> None:
        """Bulk insert into staging table."""
        if "load_timestamp" in df.columns:
            df = df.drop("load_timestamp", axis=1)

        df["full_date"] = df["full_date"].astype(str)
        records = df.to_records(index=False).tolist()

        table_name = self.schema["tables"]["stg_prices"]["name"]

        # Truncate first to ensure staging is clean for the new batch
        self.truncate_table(table_name)

        insert_query = sql.SQL("""
                INSERT INTO {table} (asset, full_date, open_price, high_price, low_price,close_price, volume ) 
                        VALUES %s ;
        """).format(table=sql.Identifier(table_name))
        
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                execute_values(cur, insert_query, records)
                conn.commit()
                self.logger.debug(f"Inserted {len(records)} rows into staging.")

    def get_stg_prices(self) -> pd.DataFrame | None:
        """Fetch data from staging."""
        table_name = self.schema["tables"]["stg_prices"]["name"]
        query = sql.SQL("SELECT * FROM {table} ;").format(table=sql.Identifier(table_name))

        with self.get_connection() as conn:
            df = pd.read_sql_query(query.as_string(conn), conn)
        
        if not df.empty:
            return df
        return None
        
    def truncate_table(self, table_Name: str) -> None:
        """Safe truncate methode."""
        query = sql.SQL("TRUNCATE TABLE {table}").format(table=sql.Identifier(table_Name)) 
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SET LOCAL statement_timeout = '5s';")
                    cur.execute(query)
                    conn.commit()        
            self.logger.info(f"Table '{table_Name}' truncated successfully.")    
        except errors.LockNotAvailable:
            self.logger.error(f"Timeout. The table {table_Name} is block by another prozess!")
        except Exception as e:
            self.logger.error(f"Error truncating {table_Name}: {e}")
        
        
    
    def get_dim_company_as_df(self) -> pd.DataFrame:
        schema = yaml_read("schema.yaml")
        cols = [col for col in schema["tables"]["dim_company"]["columns"]]
        data = self.get_all_companies()
        return pd.DataFrame(data, columns=cols)
 
    def get_dim_date_as_df(self):
        schema = yaml_read("schema.yaml")
        cols = [col for col in schema["tables"]["dim_date"]["columns"]]
        dim_date= pd.DataFrame(columns=cols)
        data = self.get_all_dates() 
        return pd.DataFrame(data, columns=cols)
    
   