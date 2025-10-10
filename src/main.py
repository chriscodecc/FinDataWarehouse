from extract.api_client import YahooFinanceClient
from utils.db import PostgreSQLConnector as PostSQLCon
import csv
import os
from transform.csv_processor import CSVProcessor
import pandas as pd
from utils.config import yaml_read


def get_comp_id(df: pd.DataFrame, y: int = 0):
    if not df.empty:
        return df.iloc[y][1]
    return None

def get_date(df: pd.DataFrame, y: int = 0):
    if not df.empty:
        return df.iloc[y][0]
    return None



def insert_csv_into_DB(dbCon: PostSQLCon, df: pd.DataFrame, yFinaceClient: YahooFinanceClient, csvProzessor: CSVProcessor):
    # If the Comp for the value exist e.g DAX in dim_comp table 
    # the csv data will be insertet 
    company_code = get_comp_id(df)

    # Check if the comp id doesnt exists
    if dbCon.get_company_id(company_code) is None:
        # Insert the missing Company
        dbCon.insert_company(yFinaceClient.fetch_company_info(company_code))
    # Inserts the df in the table fact_prieces 

    print(df)
    df = csvProzessor.transform_csv_records(df, dbCon)
    dbCon.upsert_fact_prices(df)

def stg_auto_download_and_insert(yahooFinClient: YahooFinanceClient, dbCon: PostSQLCon):
    ticker_conig = yaml_read("tickers.yaml")
    start_date = str(ticker_conig["start_date"])
    end_date = str(ticker_conig["end_date"])
    tickers = ticker_conig["tickers"]

    df_list = []
    for symbol in tickers:
        df = yahooFinClient.fetch_finance_data(symbol,start_date, end_date)
        if df is not None:     
            df_list.append(df)

    for item in df_list:
        dbCon.insert_staging(item)
        print(item)
        

def main():
    # {"DAX" : "^GDAXI", "S&P500" : "^GSPC", "Dow Jones" : "^DJI", "Bitcoin" : "BTC-USD"}
    yahooFinaceClient = YahooFinanceClient()
    #yahooFinaceClient.fetch_finance_data("Dow Jones", "2025-09-17")
    #yahooFinaceClient.fetch_finance_data("DAX", "2025-09-17")
    #yahooFinaceClient.fetch_finance_data("S&P500", "2025-09-17")
    #yahooFinaceClient.fetch_finance_data("Bitcoin", "2025-09-17")
    
    dbCon = PostSQLCon()
    csvProzessor = CSVProcessor()
    stg_auto_download_and_insert(yahooFinaceClient, dbCon)


if __name__=="__main__":
    main()