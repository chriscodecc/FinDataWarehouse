import pandas as pd
import numpy as np
from utils.helpers import yaml_read
from extract.api_client import YahooFinanceClient

def test_extract_meta():
    api_client = YahooFinanceClient()
    symbole = "DAX"
    info_dic = {
        "longName" : "Global X DAX Germany ETF",
        "shortName" : "DAX",
        "country" : "Germany",
        "region" : "Europe",
        "industry" : "Index",
        "sector" : "Financial services",
        "quoteType" : "Price Index"
    }

    result_dic = api_client.extract_meta(info_dic, symbole)
    assert result_dic["name"] == "Global X DAX Germany ETF"
    assert result_dic["country"] == "Germany"
    assert result_dic["industry"] == "Index" 
    assert result_dic["symbol"] == "DAX" 

def test_extract_meta_fallback():
    api_client = YahooFinanceClient()
    symbole = "DAX"
    info_dic = {
        "shortName" : "DAX",
        "country" : "Germany",
        "region" : "Europe",
        "industry" : "Index",
        "sector" : "Financial services",
        "quoteType" : "Price Index"
    }

    result_dic = api_client.extract_meta(info_dic, symbole)
    assert result_dic["name"] == "DAX"

def test_extract_meta_ticker_fallback():
    api_client = YahooFinanceClient()
    symbole = "DAX"
    info_dic = {
        "country" : "Germany",
        "region" : "Europe",
        "industry" : "Index",
        "sector" : "Financial services",
        "quoteType" : "Price Index"
    }

    result_dic = api_client.extract_meta(info_dic, symbole)
    assert result_dic["name"] == symbole

def test_fetch_finace_data_is_None(mocker):
    api_client = YahooFinanceClient()
    mock_get_data = mocker.patch.object(
        api_client,
        "fetch_finance_data"
    )
    mock_get_data.return_value = None
    result = api_client.fetch_finance_data("some rnd ticker")
    
    assert result is None
    mock_get_data.assert_called_once_with("some rnd ticker")
