# FinDataWarehouse

**Building a Local Data Warehouse for Financial Market Data**

---

## Project Overview

FinDataWarehouse is a Python-based data warehouse project that collects, transforms, and stores financial market data (e.g., stock prices, indices, cryptocurrencies) in a PostgreSQL database. The goal is to provide a clean and scalable data foundation for analysis and visualization.

---

## Features

- Extract financial data via APIs such as Yahoo Finance, Alpha Vantage, or CoinGecko
- Transform data: normalization, time series structure, star-schema modeling
- Load data into PostgreSQL (staging and fact/dimension tables)
- Automated pipeline for multiple stocks and time periods
- Jupyter notebooks for exploration, analysis, and visualization
- Designed for future scaling with Docker, Airflow, Spark, and Kafka

---

## Project Structure

FinDataWarehouse/
├── data/ # Raw data (CSV etc.)
├── notebooks/ # Jupyter notebooks for exploration & analysis
├── src/ # Python code
│ ├── extract/ # API calls
│ ├── transform/ # Data cleaning and transformation
│ ├── load/ # Loading into PostgreSQL
│ ├── utils/ # Helper functions, logging, config
│ └── main.py # Entry point for pipeline
├── tests/ # pytest unit tests
├── requirements.txt # Python dependencies
├── config.yaml # Configuration (DB, API keys)
├── .env # Local secrets
└── README.md # Project description

---

## Installation

1. Clone the repository:

git clone https://github.com/ChrisCodeCC/FinDataWarehouse.git
cd FinDataWarehouse

2. Create a virtual environment:

python -m venv .venv
source .venv/bin/activate  # Linux/Mac
.venv\Scripts\activate     # Windows

3. Install dependencies:

pip install -r requirements.txt

4. Security 

echo ".env" >> .gitignore
---

## Usage instruction

python src/main.py
jupyter notebook notebooks/




"# FinDataWarehouse" 

## How to git
git add .
git commit .m "Descreption" 

git push

git pull