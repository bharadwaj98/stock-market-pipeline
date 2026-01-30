import json
import os
from kafka import KafkaConsumer
import snowflake.connector
from datetime import datetime, timezone

# CONFIGURATION
SNOWFLAKE_USER = 'BHARADWAJ'
SNOWFLAKE_PASSWORD = 'tempPassword@123'
SNOWFLAKE_ACCOUNT = 'OAMDBRI-OXC72527' # e.g. xy12345.us-east-1
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

consumer = KafkaConsumer(
    'stock_prices',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='snowflake-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

BATCH_SIZE = 10  # Process 10 records at a time (keep small for testing)
buffer = []

def upload_to_snowflake(data_buffer):
    file_name = f"data/stock_batch_{len(data_buffer)}.json"
    
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
        COPY INTO RAW.STOCK_PRICES_JSON 
        FROM @RAW.LOCAL_STAGE 
        FILE_FORMAT = (TYPE = 'JSON')
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
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