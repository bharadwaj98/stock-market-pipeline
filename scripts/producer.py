import yfinance as yf
import time
import json
from kafka import KafkaProducer
from datetime import datetime
import pandas as pd

producer = KafkaProducer(
    bootstrap_servers=['host.docker.internal'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# EXPANDED DATASET (Tech, Finance, Retail, Auto)
TICKERS = [
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 
    'NVDA', 'JPM', 'V', 'WMT', 'PG'
]

def fetch_stock_data():
    print(f"Tracking {len(TICKERS)} symbols...")
    while True:
        try:
            # Download 1 minute interval data
            data = yf.download(TICKERS, period="1d", interval="1m", progress=False, group_by='ticker')
            
            current_time = datetime.now().isoformat()
            
            for ticker in TICKERS:
                try:
                    ticker_data = data[ticker].iloc[-1]
                    
                    # --- FIX: Replace NaN with None ---
                    # This ensures JSON serializes it as null, not NaN
                    record = ticker_data.to_dict()
                    for key, value in record.items():
                        if pd.isna(value):
                            record[key] = None
                    
                    # Add Metadata
                    record['ticker'] = ticker
                    record['event_time'] = str(ticker_data.name)
                    record['processing_time'] = current_time
                    
                    producer.send('stock_prices', value=record)
                    print(f"Sent {ticker}")
                except Exception as err:
                    pass
            
            producer.flush()
            time.sleep(10) 
            
        except Exception as e:
            print(f"Global Error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    fetch_stock_data()