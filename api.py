from fastapi import FastAPI, HTTPException
import pandas as pd
from sqlalchemy import create_engine, text
import os

app = FastAPI(title="FinData API")

DB_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@database:5432/{os.getenv('DB_NAME')}"
engine = create_engine(DB_URL)

from sqlalchemy import text # Wichtig: text importieren

@app.get("/prices/{ticker}")
def get_prices_by_ticker(ticker: str):
    # WICHTIG: Prüfe, ob die Tabellennamen in deiner DB 
    # wirklich exakt so geschrieben werden (Kleinschreibung!)
    query = text("""
        SELECT c.name, d.full_date, f.close_price
        FROM fact_prices f
        JOIN dim_company c ON f.company_id = c.company_id
        JOIN dim_date d ON f.date_id = d.date_id
        WHERE UPPER(c.symbol) = :t
        ORDER BY d.full_date DESC
        LIMIT 50
    """)
    
    try:
        # Wir nutzen 'begin()', um eine saubere Transaktion zu starten
        with engine.begin() as conn:
            df = pd.read_sql_query(query, conn, params={"t": ticker.upper()})
            
        if df.empty:
            return {"message": f"Keine Daten für {ticker} gefunden.", "data": []}
            
        # Datum in String umwandeln für JSON-Kompatibilität
        df['full_date'] = df['full_date'].astype(str)
        return df.to_dict(orient="records")

    except Exception as e:
        # Das schreibt den ECHTEN Fehler direkt in dein Browser-Fenster!
        raise HTTPException(status_code=500, detail=f"DB-Error: {str(e)}")