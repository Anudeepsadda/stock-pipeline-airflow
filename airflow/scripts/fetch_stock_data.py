import os
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

# Get environment variables (from .env file)
API_KEY = os.getenv("STOCK_API_KEY", "demo")
DB_USER = os.getenv("POSTGRES_USER", "airflow")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "airflow")
DB_NAME = os.getenv("POSTGRES_DB", "stocks")
DB_HOST = "postgres"   # container name in docker-compose
DB_PORT = os.getenv("POSTGRES_PORT", "5432")

# Function to fetch stock data from Alpha Vantage
def fetch_stock_data(symbol="AAPL"):
    url = f"https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "apikey": API_KEY
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        if "Time Series (Daily)" not in data:
            raise ValueError("Invalid response from API")

        time_series = data["Time Series (Daily)"]
        rows = []

        for day, values in time_series.items():
            rows.append((
                symbol,
                datetime.strptime(day, "%Y-%m-%d").date(),
                float(values["1. open"]),
                float(values["2. high"]),
                float(values["3. low"]),
                float(values["4. close"]),
                int(values["5. volume"])
            ))

        return rows

    except Exception as e:
        print(f"Error fetching stock data: {e}")
        return []

# Function to store data into PostgreSQL
def store_stock_data(rows):
    if not rows:
        print("No data to store")
        return

    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            port=DB_PORT
        )
        cur = conn.cursor()

        insert_query = """
        INSERT INTO stock_prices (symbol, trading_day, open, high, low, close, volume)
        VALUES %s
        ON CONFLICT (symbol, trading_day)
        DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume;
        """

        execute_values(cur, insert_query, rows)
        conn.commit()
        cur.close()
        conn.close()

        print(f"Stored {len(rows)} rows successfully.")

    except Exception as e:
        print(f"Error storing stock data: {e}")

# Main script
if __name__ == "__main__":
    stock_symbol = "AAPL"  # you can change to MSFT, TSLA, etc.
    rows = fetch_stock_data(stock_symbol)
    store_stock_data(rows)