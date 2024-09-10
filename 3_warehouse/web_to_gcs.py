import os
import pandas as pd
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pyarrow as pa
from google.cloud import storage
import logging
import requests
import hashlib
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


"""
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""
# services = ['fhv','green','yellow']
init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/olutu/Downloads/DEng/learn/wk1_basics/1_terrademo/keys/mycred.json"

# switch out the bucketname
BUCKET = os.environ.get("GCP_GCS_BUCKET", "gcp-de-datastore")

table_schema_green = pa.schema(
    [
        ('VendorID',pa.string()),
        ('lpep_pickup_datetime',pa.timestamp('s')),
        ('lpep_dropoff_datetime',pa.timestamp('s')),
        ('store_and_fwd_flag',pa.string()),
        ('RatecodeID',pa.int64()),
        ('PULocationID',pa.int64()),
        ('DOLocationID',pa.int64()),
        ('passenger_count',pa.int64()),
        ('trip_distance',pa.float64()),
        ('fare_amount',pa.float64()),
        ('extra',pa.float64()),
        ('mta_tax',pa.float64()),
        ('tip_amount',pa.float64()),
        ('tolls_amount',pa.float64()),
        ('ehail_fee',pa.float64()),
        ('improvement_surcharge',pa.float64()),
        ('total_amount',pa.float64()),
        ('payment_type',pa.int64()),
        ('trip_type',pa.int64()),
        ('congestion_surcharge',pa.float64()),
    ]
)

table_schema_yellow = pa.schema(
   [
        ('VendorID', pa.string()), 
        ('tpep_pickup_datetime', pa.timestamp('s')), 
        ('tpep_dropoff_datetime', pa.timestamp('s')), 
        ('passenger_count', pa.int64()), 
        ('trip_distance', pa.float64()), 
        ('RatecodeID', pa.string()), 
        ('store_and_fwd_flag', pa.string()), 
        ('PULocationID', pa.int64()), 
        ('DOLocationID', pa.int64()), 
        ('payment_type', pa.int64()), 
        ('fare_amount',pa.float64()), 
        ('extra',pa.float64()), 
        ('mta_tax', pa.float64()), 
        ('tip_amount', pa.float64()), 
        ('tolls_amount', pa.float64()), 
        ('improvement_surcharge', pa.float64()), 
        ('total_amount', pa.float64()), 
        ('congestion_surcharge', pa.float64())]

)

### do the same schema casting for the fhv data 

table_schema_fhv = pa.schema(
    [
        ('dispatching_base_num', pa.string()),
        ('pickup_datetime', pa.timestamp('s')),
        ('dropoff_datetime', pa.timestamp('s')),
        ('PULocationID', pa.int64()),
        ('DOLocationID', pa.int64()),
        ('SR_Flag', pa.int64()),
        ('Affiliated_base_number', pa.string())
    ]
)


def calculate_md5(file_name):
    """Calculate MD5 checksum for a file."""
    md5_hash = hashlib.md5()
    with open(file_name, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            md5_hash.update(chunk)
    return md5_hash.hexdigest()

def download_file_with_retries(url, file_name, retries=3):
    """Download a file with retries and check its integrity."""
    attempt = 0
    while attempt < retries:
        r = requests.get(url)
        open(file_name, 'wb').write(r.content)

        if os.path.getsize(file_name) > 0:
            print(f"Download successful: {file_name}")
            return True
        else:
            print(f"Retrying download ({attempt + 1}/{retries})...")
            attempt += 1
            time.sleep(2)  # Wait before retrying

    return False

def format_to_parquet(src_file, service):
    if not src_file.endswith('.csv.gz'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return

    # Read the CSV file
    table = pv.read_csv(src_file)

    # Convert all column names to lowercase
    table = table.rename_columns([col.lower() for col in table.column_names])

    # Choose the correct schema and standardize field names to lowercase
    if service == 'yellow':
        schema = table_schema_yellow
    elif service == 'green':
        schema = table_schema_green
    elif service == 'fhv':
        schema = table_schema_fhv
    else:
        logging.error(f"Unknown service type: {service}")
        return

    # Standardize schema field names to lowercase
    schema = pa.schema([
        pa.field(field.name.lower(), field.type)
        for field in schema
    ])

    # Cast the table to the standardized schema
    try:
        table = table.cast(schema)
    except pa.lib.ArrowInvalid as e:
        logging.error(f"Schema casting failed for {src_file}: {e}")
        return

    # Write the table to a Parquet file
    pq.write_table(table, src_file.replace('.csv.gz', '.parquet'))



def upload_to_gcs(bucket, object_name, local_file):
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

def process_file(year, month, service):
    file_name = f"{service}_tripdata_{year}-{month}.csv.gz"
    request_url = f"{init_url}{service}/{file_name}"

    success = download_file_with_retries(request_url, file_name)
    if not success:
        print(f"Failed to download {file_name} after multiple attempts.")
        return

    try:
        df = pd.read_csv(file_name, compression='gzip', low_memory=False)
    except EOFError:
        print(f"Error reading {file_name}: file is corrupted or incomplete. Skipping this file.")
        return

    try:
        format_to_parquet(file_name, service)
        parquet_file = file_name.replace('.csv.gz', '.parquet')
        print(f"Parquet: {parquet_file}")
    except Exception as e:
        print(f"Failed to convert {file_name} to Parquet: {e}")
        return

    try:
        upload_to_gcs(BUCKET, f"{service}/{parquet_file}", parquet_file)
        print(f"GCS: {service}/{parquet_file}")
    except Exception as e:
        print(f"Failed to upload {parquet_file} to GCS: {e}")

def web_to_gcs_parallel(year, service, max_workers=4):
    months = [f'{i:02d}' for i in range(1, 13)]

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(process_file, year, month, service)
            for month in months
        ]
        for future in as_completed(futures):
            try:
                future.result()  # Will raise exceptions if any occurred
            except Exception as e:
                print(f"An error occurred: {e}")

# web_to_gcs_parallel('2019', 'green', max_workers=4)
# web_to_gcs_parallel('2020', 'green', max_workers=4)
# web_to_gcs_parallel('2019', 'yellow', max_workers=4)
# web_to_gcs_parallel('2020', 'yellow', max_workers=4)
# web_to_gcs_parallel('2020', 'fhv', max_workers=4)
web_to_gcs_parallel('2019', 'fhv', max_workers=4)