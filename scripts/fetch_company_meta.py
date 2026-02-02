import yfinance as yf
import json
import os
import snowflake.connector
from datetime import datetime, timezone

# CONFIGURATION
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

    # --- PATH FIX FOR DOCKER vs LOCAL ---
    # In Docker, we mounted data to /opt/airflow/data
    if os.path.exists('/opt/airflow/data'):
        file_path = '/opt/airflow/data/company_meta_batch.json'
    else:
        # Local Windows fallback
        file_path = os.path.join(os.getcwd(), 'data', 'company_meta_batch.json')

    print(f"Writing to: {file_path}")

    # Write to local JSON
    with open(file_path, 'w') as f:
        for record in data_list:
            f.write(json.dumps(record) + "\n")
            
    print(f"Uploading {file_path} to Snowflake...")
    
    cursor = conn.cursor()
    try:
        # 1. PUT file to Stage
        # Use simple forward slashes for Snowflake PUT command
        # (Snowflake doesn't like Windows backslashes in PUT paths)
        snowflake_path = file_path.replace('\\', '/')
        
        put_cmd = f"PUT file://{snowflake_path} @RAW.LOCAL_STAGE AUTO_COMPRESS=TRUE"
        cursor.execute(put_cmd)
        
        # 2. COPY INTO Table
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
        # Optional: Clean up
        if os.path.exists(file_path):
            os.remove(file_path)

if __name__ == "__main__":
    fetch_and_load_metadata()