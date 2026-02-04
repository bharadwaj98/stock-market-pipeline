import json
import os
from kafka import KafkaConsumer
from dotenv import load_dotenv
import snowflake.connector
from datetime import datetime, timezone

load_dotenv() 

# CONFIGURATION
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')

# Initialize Snowflake Connection
conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse=WAREHOUSE,
    database=DATABASE,
    schema=SCHEMA
)

# Get Bootstrap server from Env Var (set in docker-compose) or default to localhost (for local testing)
BOOTSTRAP_SERVER = os.getenv('BOOTSTRAP_SERVER', 'localhost:9092')

consumer = KafkaConsumer(
    'stock_prices',
    bootstrap_servers=[BOOTSTRAP_SERVER],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='snowflake-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

BATCH_SIZE = 10  # Process 10 records at a time (keep small for testing)
buffer = []

def upload_to_snowflake(data_buffer):
    file_name = f"data/stock_batch_{len(data_buffer)}.json"
    
    # Ensure the data directory exists to prevent FileNotFoundError
    os.makedirs(os.path.dirname(file_name), exist_ok=True)
    
    # 1. Write buffer to local JSON file
    with open(file_name, 'w') as f:
        # Create a list of JSON objects
        for record in data_buffer:
            # Add current time to the record before writing
            record['INGESTION_TIME'] = datetime.now(timezone.utc).isoformat()
            
            f.write(json.dumps(record) + "\n")
            
    print(f"Uploading {file_name}...")
    
    # 2. PUT file to Snowflake Internal Stage
    cursor = conn.cursor()
    try:
        # Upload
        local_file_path = os.path.abspath(file_name).replace('\\', '/')
        
        put_cmd = f"PUT file://{local_file_path} @RAW.LOCAL_STAGE AUTO_COMPRESS=TRUE"
        cursor.execute(put_cmd)
        
        # Load into Table
        copy_cmd = """
        COPY INTO RAW.STOCK_PRICES_JSON (RECORD_CONTENT)
        FROM @RAW.LOCAL_STAGE 
        FILE_FORMAT = (TYPE = 'JSON')
        PURGE = TRUE
        """
        cursor.execute(copy_cmd)
        print("Upload success.")
    finally:
        cursor.close()
        
        # 3. Clean up local file
        if os.path.exists(file_name):
            os.remove(file_name)

# Main Loop
print("Listening for messages...")
for message in consumer:
    buffer.append(message.value)
    
    if len(buffer) >= BATCH_SIZE:
        upload_to_snowflake(buffer)
        buffer = [] # Clear buffer