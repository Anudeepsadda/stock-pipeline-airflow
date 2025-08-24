# Stock Data Pipeline with Airflow and Docker

## Overview
This project builds a Dockerized pipeline using Apache Airflow to fetch daily stock market data from Alpha Vantage API and store it in a PostgreSQL database.

## How It Works
1. Airflow DAG runs daily.
2. Calls `fetch_stock_data.py` script.
3. Script fetches data from Alpha Vantage API.
4. Data is inserted/updated into `stock_prices` table.

## Setup Instructions
1. Clone the repo
2. Create a `.env` file with:
   - POSTGRES_USER=airflow
   - POSTGRES_PASSWORD=airflow
   - POSTGRES_DB=stocks
   - POSTGRES_PORT=5432
   - STOCK_API_KEY=your_api_key_here
   - AIRFLOW__CORE__FERNET_KEY=nvW64XP7LfWKO3r6sJTHy4AJm7rCdv5u_GZcdnBee7Y=
   - AIRFLOW__WEBSERVER__SECRET_KEY=myverysecretkey123
3. Run:
   ```bash
   docker-compose up --build