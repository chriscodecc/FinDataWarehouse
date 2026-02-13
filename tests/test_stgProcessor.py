import pandas as pd
import numpy as np
from utils.helpers import yaml_read
from transform.stg_processor import StgProcessor
import pytest

@pytest.fixture
def stg_Processor():
    return StgProcessor()

def test_normalize_df_for_stg_prices(stg_Processor):
    columns = pd.MultiIndex.from_tuples([
        ('Price', 'Date'),
        ('Price', 'Close'),
        ('Price', 'High'),
        ('Price', 'Low'),
        ('Price', 'Open'),
        ('Price', 'Volume')
    ], names=['', 'Ticker'])

    columns = pd.MultiIndex.from_tuples([
        ('Date', ''),
        ('Close', '^N225'),
        ('High', '^N225'),
        ('Low', '^N225'),
        ('Open', '^N225'),
        ('Volume', '^N225')
    ])

    data = [
        # Datum, Close, High, Low, Open, Volume
        ['2025-10-29', 51307.648438, 51412.96875, 50365.621094, 50453.640625, 129700000]
    ]
    df_test = pd.DataFrame(data, columns=columns)
    df_test = stg_Processor.normalize_df_for_stg_prices(df_test, "^N225")
    df_raw = pd.DataFrame(data=None, columns=columns)
    

    schema = yaml_read("schema.yaml")
    cols = [col for col in schema["tables"]["stg_prices"]["columns"]]
    df_normalized = pd.DataFrame(columns=cols)

    pd.testing.assert_index_equal(df_test.columns, df_normalized.columns) 
    assert stg_Processor.normalize_df_for_stg_prices(df_raw, "^N225") is None

def test_get_enriched_df(stg_Processor):
    dim_date = pd.DataFrame({
        "date_id" : [55],
        "full_date" : ["2025-10-27"],
        "day" : [27],
        "month" : [10],
        "year" : [2025]
    })

    dim_comp = pd.DataFrame({
        "company_id" : [1],
        "name" : ["Global X DAX Germany ETF"],
        "symbol" : ["DAX"],
        "country" : ["Germany"],
        "industry" : ["ETF"],
    })

    stg_data_df = pd.DataFrame ({
        "load_timestamp" : ["2025-11-01 11:37:32.224135 +00:00"],
        "asset" : ["DAX"], 
        "full_date" : ["2025-10-27"], 
        "open_price" : [49905.80078125], 
        "high_price" : [50549.60156250], 
        "low_price" : [49838.98046875], 
        "close_price" : [50512.32031250], 
        "volume" : [122100000], 
        })
    
    expected_df = pd.DataFrame({
        "load_timestamp" : ["2025-11-01 11:37:32.224135 +00:00"],
         "asset" : ["DAX"], 
         "full_date" : ["2025-10-27"],
         "open_price" : [49905.80078125],
         "high_price" : [50549.60156250], 
         "low_price" : [49838.98046875], 
        "close_price" : [50512.32031250], 
        "volume" : [122100000], 
        "company_id" : [1],
        "name" : ["Global X DAX Germany ETF"],
         "symbol" : ["DAX"],
        "country" : ["Germany"],
        "industry" : ["ETF"],
         "date_id" : [55],
        "day" : [27],
        "month" : [10],
        "year" : [2025]
    })
    
    result_df = stg_Processor.get_enriched_df(stg_data_df, dim_comp, dim_date)

    pd.testing.assert_frame_equal(result_df, expected_df)


def test_stg_normalize_for_fact_prices(stg_Processor):
    enriched_df = pd.DataFrame({
        "load_timestamp" : ["2025-11-01 11:37:32.224135 +00:00"],
         "asset" : ["DAX"], 
         "full_date" : ["2025-10-27"],
         "open_price" : [49905.80078125],
         "high_price" : [50549.60156250], 
         "low_price" : [49838.98046875], 
        "close_price" : [50512.32031250], 
        "volume" : [122100000], 
        "company_id" : [1],
        "name" : ["Global X DAX Germany ETF"],
         "symbol" : ["DAX"],
        "country" : ["Germany"],
        "industry" : ["ETF"],
         "date_id" : [55],
        "day" : [27],
        "month" : [10],
        "year" : [2025]
    })

    excepted_df = pd.DataFrame({
        "date_id" : [55],
        "company_id" : [1],
        "close_price" : [50512.32031250],
        "high_price" : [50549.60156250],
        "low_price" : [49838.98046875], 
        "open_price" : [49905.80078125],
        "volume" : [122100000]
    })

    result_df = stg_Processor.stg_normalize_for_fact_prices(excepted_df)

    pd.testing.assert_frame_equal(result_df, excepted_df)
    
    
    #result_df = stgProzessor.get_enriched_df(stg_data_df, dim_comp, dim_date)

    #pd.testing.assert_frame_equal(result_df, expected_df)











# 77 53 191    2429