import yfinance as yf
import json
import os
import snowflake.connector
from datetime import datetime, timezone

# CONFIGURATION (Same as your consumer)
SNOWFLAKE_USER = 'BHARADWAJ'
SNOWFLAKE_PASSWORD = 'tempPassword@123'
SNOWFLAKE_ACCOUNT = 'OAMDBRI-OXC72527' 
WAREHOUSE = 'COMPUTE_WH'
DATABASE = 'STOCK_DB'
SCHEMA = 'RAW'

# Initialize Snowflake Connection
conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse=WAREHOUSE,
    database=DATABASE,
    schema=SCHEMA
)

TICKERS = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']

def fetch_and_load_metadata():
    print("Fetching company metadata...")
    data_list = []
    
    for ticker in TICKERS:
        try:
            # Fetch info from yfinance
            t = yf.Ticker(ticker)
            info = t.info
            
            # Map to the exact columns in your Snowflake Table
            # Keys must match Table Column names (Case Insensitive)
            record = {
                'TICKER': ticker,
                'NAME': info.get('longName'),
                'SECTOR': info.get('sector'),
                'INDUSTRY': info.get('industry'),
                'MARKET_CAP': info.get('marketCap')
            }
            record['INGESTION_TIME'] = datetime.now(timezone.utc).isoformat()
            data_list.append(record)
            print(f"Fetched info for {ticker}")
        except Exception as e:
            print(f"Error fetching {ticker}: {e}")

    # Generate filename
    file_name = "data/company_meta_batch.json"

    # Write to local JSON
    with open(file_name, 'w') as f:
        for record in data_list:
            f.write(json.dumps(record) + "\n")
            
    print(f"Uploading {file_name} to Snowflake...")
    
    cursor = conn.cursor()
    try:
        # 1. PUT file to Stage
        # Using abspath to ensure Windows paths work correctly
        local_file_path = os.path.abspath(file_name).replace('\\', '/')
        put_cmd = f"PUT file://{local_file_path} @RAW.LOCAL_STAGE AUTO_COMPRESS=TRUE"
        cursor.execute(put_cmd)
        
        # 2. COPY INTO Table
        # Note: Using MATCH_BY_COLUMN_NAME just like you did for prices
        copy_cmd = """
        COPY INTO RAW.COMPANY_INFO_JSON 
        FROM @RAW.LOCAL_STAGE 
        FILE_FORMAT = (TYPE = 'JSON')
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        PURGE = TRUE
        """
        cursor.execute(copy_cmd)
        print("Batch Upload success.")
        
    except Exception as e:
        print(f"Snowflake Error: {e}")
    finally:
        cursor.close()
        # Optional: Clean up local file
        if os.path.exists(file_name):
            os.remove(file_name)

if __name__ == "__main__":
    fetch_and_load_metadata()