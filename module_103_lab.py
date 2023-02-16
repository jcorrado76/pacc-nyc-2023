import httpx
from prefect import flow, task
from prefect.blocks.system import Secret
import pandas as pd

VANTAGE_API_KEY = Secret.load("vantage-api-key")

# Access the stored secret
VANTAGE_API_KEY.get()

VANTAGE_URL = 'https://www.alphavantage.co/query'


@task
def fetch_stock_data(ticker: str = "IBM"):
    stock_data_response = httpx.get(
        VANTAGE_URL,
        params=dict(
            function="TIME_SERIES_INTRADAY",
            symbol=ticker,
            interval="5min",
            apikey=VANTAGE_API_KEY
        ),
    )
    return stock_data_response.json()


@task
def extract_stock_time_series(vantage_api_response: dict):
    time_series_open_values = []
    for timestamp, open_close_dict in vantage_api_response['Time Series (5min)'].items():
        time_series_open_values.append({
            "ts": timestamp,
            "open": float(open_close_dict["1. open"])
        })
    return pd.DataFrame.from_dict(time_series_open_values).set_index("ts").sort_index()


@task
def compute_average_opening_price(open_values_df: pd.DataFrame):
    print(open_values_df['open'].mean())


@flow(name="Transform Stock Data")
def transform_stock_data():
    stock_data_response = fetch_stock_data(ticker="IBM")
    stock_data_df: pd.DataFrame = extract_stock_time_series(stock_data_response)
    compute_average_opening_price(stock_data_df)


if __name__ == "__main__":
    transform_stock_data()
