from extract.api_client import YahooFinanceClient

y_Client = YahooFinanceClient()

data_for_dax = y_Client.fetch_finance_data(symbol="DAX", date="2025-10-13")
print(data_for_dax)