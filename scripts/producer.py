import yfinance as yf
import time
import json
from kafka import KafkaProducer
from datetime import datetime

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

TICKERS = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']

def fetch_stock_data():
    while True:
        try:
            # Fetch real-time data
            data = yf.download(TICKERS, period="1d", interval="1m", progress=False)
            
            # Use the latest available row
            latest = data.iloc[-1]
            
            for ticker in TICKERS:
                # Structure the data
                # Note: yfinance format changes often, keeping it simple here
                stock_record = {
                    'ticker': ticker,
                    'price': float(latest['Close'][ticker]),
                    'timestamp': datetime.now().isoformat()
                }
                
                # Send to Kafka Topic 'stock_prices'
                producer.send('stock_prices', value=stock_record)
                print(f"Sent: {stock_record}")
            
            producer.flush()
            time.sleep(10) # Wait 10 seconds before next fetch
            
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    fetch_stock_data()