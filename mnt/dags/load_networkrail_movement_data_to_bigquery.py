import logging
from datetime import datetime
from pytz import timezone as tz

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from airflow.utils import timezone

import google
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account


TIMEZONE_LONDON: tz = tz("Europe/London")
S3_CONN = "minio"
S3_BUCKET = "networkrail"
VARIABLE_BIGQUERY_CREDENTIAL_SECRET = "bigquery_credential_secret"
VARIABLE_BIGQUERY_PROJECT_ID = "bigquery_project_id"
BIGQUERY_TABLE_ID = "networkrail.movements"


def _list_files(**context) -> list:
    """List files from MinIO for the given date."""
    ds = context["ds"]
    
    # Parse date components
    year = ds[:4]
    month = ds[5:7]
    day = ds[8:10]
    
    # Create prefix path
    prefix = f"year={year}/month={month}/day={day}/"
    
    # Use direct boto3 for MinIO access
    import boto3
    s3 = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minio',
        aws_secret_access_key='minio123'
    )
    
    try:
        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
        files = [obj['Key'] for obj in response.get('Contents', [])]
        logging.info(f"Found {len(files)} files in {prefix}")
        if files:
            logging.info(f"Sample files: {files[:3]}")
        return files
    except Exception as e:
        logging.error(f"Error listing files: {e}")
        return []

def _download_files(**context) -> str:
    ds = context["ds"]
    files = context["ti"].xcom_pull(task_ids="list_files", key="return_value")

    # Use direct boto3 for MinIO access
    import boto3
    s3 = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minio',
        aws_secret_access_key='minio123'
    )

    events = []
    for each in files:
        try:
            # Download file content directly
            response = s3.get_object(Bucket=S3_BUCKET, Key=each)
            content = response['Body'].read().decode('utf-8')
            events.append(content)
        except Exception as e:
            logging.error(f"Error downloading {each}: {e}")

    output_file_name = f"/tmp/{ds}-events.json"
    logging.info(f"Creating output file: {output_file_name}")
    logging.info(f"Number of events to write: {len(events)}")
    
    with open(output_file_name, "w") as f:
        for i, event in enumerate(events):
            if i < 3:  # Log first 3 events for debugging
                logging.info(f"Event {i}: {event[:200]}...")  # First 200 chars
            # Clean the event and write as a single line
            cleaned_event = event.replace('\n', ' ').replace('\r', ' ').strip()
            f.write(cleaned_event + "\n")
    
    # Verify file was created
    import os
    if os.path.exists(output_file_name):
        file_size = os.path.getsize(output_file_name)
        logging.info(f"File created successfully: {output_file_name}, size: {file_size} bytes")
    else:
        logging.error(f"File was not created: {output_file_name}")

    return output_file_name


def remove_old_data(bq_client: bigquery.Client, table: str, ds: str) -> None:
    try:
        logging.info(f"Checking if table exists: {table}")
        
        # First check if table exists
        try:
            table_obj = bq_client.get_table(table)
            logging.info(f"Table exists with {table_obj.num_rows} rows")
        except google.api_core.exceptions.NotFound:
            logging.info("Table does not exist yet - will be created during load")
            return
        
        # Count rows to be deleted
        count_query = bq_client.query(
            f"""
            SELECT COUNT(*) as count FROM `{table}` WHERE DATE(actual_timestamp) = "{ds}"
            """
        )
        count_result = count_query.result()
        rows_to_delete = list(count_result)[0].count
        logging.info(f"Found {rows_to_delete} rows to delete for date {ds}")
        
        if rows_to_delete > 0:
            delete_query = bq_client.query(
                f"""
                DELETE FROM `{table}` WHERE DATE(actual_timestamp) = "{ds}"
                """
            )
            delete_query.result()
            logging.info(f"Successfully deleted {rows_to_delete} rows for date {ds}")
        else:
            logging.info(f"No rows found to delete for date {ds}")
            
    except Exception as e:
        logging.error(f"Error during data removal: {e}")
        raise


def _load_data_to_bigquery(**context) -> None:
    ds = context["ds"]
    file_name = context["ti"].xcom_pull(task_ids="download_files", key="return_value")
    
    logging.info(f"Attempting to load data from file: {file_name}")
    
    # Check if file exists
    import os
    if os.path.exists(file_name):
        file_size = os.path.getsize(file_name)
        logging.info(f"File exists: {file_name}, size: {file_size} bytes")
        
        # Read first few lines to debug
        with open(file_name, 'r') as f:
            first_lines = [f.readline().strip() for _ in range(3)]
        logging.info(f"First few lines: {first_lines}")
        
        # Count lines and check if it's valid line-delimited JSON
        with open(file_name, 'r') as f:
            all_lines = f.readlines()
        logging.info(f"Total lines in file: {len(all_lines)}")
        
        # Try to parse first few lines as JSON to validate
        import json
        for i, line in enumerate(all_lines[:3]):
            try:
                json.loads(line.strip())
                logging.info(f"Line {i+1} is valid JSON")
            except json.JSONDecodeError as e:
                logging.error(f"Line {i+1} is NOT valid JSON: {e}")
                logging.error(f"Line content: {line[:100]}...")
    else:
        logging.error(f"File does not exist: {file_name}")
        return
    
    df = pd.read_json(file_name, dtype=str, lines=True)
    
    # Debug: Show available columns
    logging.info(f"DataFrame columns: {list(df.columns)}")
    logging.info(f"DataFrame shape: {df.shape}")
    
    # The actual_timestamp is nested in raw_data, so we need to extract it
    if 'raw_data' in df.columns:
        # Parse the JSON in raw_data and extract fields
        import json
        
        def extract_field(raw_data_obj, field_name):
            try:
                if pd.isna(raw_data_obj):
                    return None
                
                # raw_data is already a dict from pandas read_json
                if isinstance(raw_data_obj, dict):
                    return raw_data_obj.get(field_name)
                
                # Fallback for any other type
                logging.error(f"Unexpected raw_data type: {type(raw_data_obj)}")
                return None
            except Exception as e:
                logging.error(f"Error extracting {field_name}: {e}")
                return None
        
        # Extract all the nested fields from raw_data to match our BigQuery schema
        df["actual_timestamp"] = df["raw_data"].apply(lambda x: extract_field(x, "actual_timestamp"))
        df["gbtt_timestamp"] = df["raw_data"].apply(lambda x: extract_field(x, "gbtt_timestamp"))
        df["loc_stanox"] = df["raw_data"].apply(lambda x: extract_field(x, "loc_stanox"))
        df["planned_timestamp"] = df["raw_data"].apply(lambda x: extract_field(x, "planned_timestamp"))
        df["timetable_variation"] = df["raw_data"].apply(lambda x: extract_field(x, "timetable_variation"))
        df["train_id"] = df["raw_data"].apply(lambda x: extract_field(x, "train_id"))
        df["delay_monitoring_point"] = df["raw_data"].apply(lambda x: extract_field(x, "delay_monitoring_point"))
        df["next_report_run_time"] = df["raw_data"].apply(lambda x: extract_field(x, "next_report_run_time"))
        df["reporting_stanox"] = df["raw_data"].apply(lambda x: extract_field(x, "reporting_stanox"))
        df["correction_ind"] = df["raw_data"].apply(lambda x: extract_field(x, "correction_ind"))
        df["event_source"] = df["raw_data"].apply(lambda x: extract_field(x, "event_source"))
        df["train_file_address"] = df["raw_data"].apply(lambda x: extract_field(x, "train_file_address"))
        df["division_code"] = df["raw_data"].apply(lambda x: extract_field(x, "division_code"))
        df["train_terminated"] = df["raw_data"].apply(lambda x: extract_field(x, "train_terminated"))
        df["offroute_ind"] = df["raw_data"].apply(lambda x: extract_field(x, "offroute_ind"))
        df["train_service_code"] = df["raw_data"].apply(lambda x: extract_field(x, "train_service_code"))
        df["auto_expected"] = df["raw_data"].apply(lambda x: extract_field(x, "auto_expected"))
        df["route"] = df["raw_data"].apply(lambda x: extract_field(x, "route"))
        df["planned_event_type"] = df["raw_data"].apply(lambda x: extract_field(x, "planned_event_type"))
        df["next_report_stanox"] = df["raw_data"].apply(lambda x: extract_field(x, "next_report_stanox"))
        
        # Also extract some top-level fields
        df["event_type"] = df["event_type"]
        df["toc_id"] = df["toc_id"]
        df["variation_status"] = df["variation_status"]
        
        logging.info(f"Extracted columns: {list(df.columns)}")
        logging.info(f"Sample actual_timestamp values: {df['actual_timestamp'].head()}")
        logging.info(f"Sample train_id values: {df['train_id'].head()}")
        
        # Debug: Check what raw_data actually contains
        logging.info(f"Raw data column type: {type(df['raw_data'].iloc[0])}")
        logging.info(f"First raw_data keys: {list(df['raw_data'].iloc[0].keys()) if isinstance(df['raw_data'].iloc[0], dict) else 'Not a dict'}")
        logging.info(f"Sample actual_timestamp from raw_data: {df['raw_data'].iloc[0].get('actual_timestamp') if isinstance(df['raw_data'].iloc[0], dict) else 'N/A'}")
        logging.info(f"Sample train_id from raw_data: {df['raw_data'].iloc[0].get('train_id') if isinstance(df['raw_data'].iloc[0], dict) else 'N/A'}")
    else:
        logging.error("raw_data column not found in DataFrame")
        logging.error(f"Available columns: {list(df.columns)}")
        return
    
    # Convert timestamp to datetime - only for non-None values
    if df["actual_timestamp"].notna().any():
        # Filter out None values before conversion
        valid_timestamps = df["actual_timestamp"].notna()
        df.loc[valid_timestamps, "actual_timestamp"] = df.loc[valid_timestamps, "actual_timestamp"].astype(int) / 1000
        df.loc[valid_timestamps, "actual_timestamp"] = df.loc[valid_timestamps, "actual_timestamp"] \
            .map(datetime.utcfromtimestamp) \
            .map(TIMEZONE_LONDON.fromutc)
        logging.info(f"Converted {valid_timestamps.sum()} timestamps successfully")
    else:
        logging.error("No valid timestamps found!")
        return
    
    # Final DataFrame info before BigQuery load
    logging.info(f"Final DataFrame shape: {df.shape}")
    logging.info(f"Final DataFrame columns: {list(df.columns)}")
    
    # Ensure we have all required columns for BigQuery
    required_columns = [field.name for field in bigquery_schema]
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        logging.error(f"Missing required columns: {missing_columns}")
        logging.error(f"Available columns: {list(df.columns)}")
        return
    else:
        logging.info("All required columns are present")
    
    # Sample data
    logging.info(f"Sample data:")
    logging.info(df.head(2).to_string())
    
    # Final validation - check for empty DataFrame
    if df.empty:
        logging.error("DataFrame is empty! Cannot load to BigQuery.")
        return
    
    # Check for any remaining None values in critical fields
    critical_fields = ['actual_timestamp', 'train_id', 'event_type']
    for field in critical_fields:
        if field in df.columns:
            null_count = df[field].isna().sum()
            logging.info(f"Field '{field}' has {null_count} null values out of {len(df)} rows")
        else:
            logging.error(f"Critical field '{field}' is missing!")
            return

    # Create schema that matches our extracted columns
    bigquery_schema = [
        bigquery.SchemaField("event_type", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("toc_id", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("variation_status", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("actual_timestamp", bigquery.enums.SqlTypeNames.TIMESTAMP),
        bigquery.SchemaField("gbtt_timestamp", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("loc_stanox", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("planned_timestamp", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("timetable_variation", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("train_id", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("delay_monitoring_point", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("next_report_run_time", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("reporting_stanox", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("correction_ind", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("event_source", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("train_file_address", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("division_code", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("train_terminated", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("offroute_ind", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("train_service_code", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("auto_expected", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("route", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("planned_event_type", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("next_report_stanox", bigquery.enums.SqlTypeNames.STRING),
    ]
    job_config = bigquery.LoadJobConfig(schema=bigquery_schema)

    bq_credential_secret = Variable.get(VARIABLE_BIGQUERY_CREDENTIAL_SECRET, deserialize_json=True)
    bq_client = bigquery.Client(
        credentials=service_account.Credentials.from_service_account_info(bq_credential_secret),
        project=Variable.get(VARIABLE_BIGQUERY_PROJECT_ID),
    )

    # Remove old data for this date
    logging.info(f"Removing old data for date: {ds}")
    remove_old_data(bq_client, BIGQUERY_TABLE_ID, ds)
    logging.info("Old data removal completed")

    # Load data to BigQuery
    logging.info(f"Starting BigQuery load for {len(df)} rows to table: {BIGQUERY_TABLE_ID}")
    logging.info(f"BigQuery project: {Variable.get(VARIABLE_BIGQUERY_PROJECT_ID)}")
    
    try:
        job = bq_client.load_table_from_dataframe(df, BIGQUERY_TABLE_ID, job_config=job_config)
        logging.info(f"BigQuery job started: {job.job_id}")
        
        # Wait for job completion
        job.result()
        logging.info("BigQuery job completed successfully!")
        
        # Get job statistics
        logging.info(f"Job statistics:")
        logging.info(f"  - Rows processed: {job.output_rows}")
        logging.info(f"  - Bytes processed: {job.bytes_processed}")
        logging.info(f"  - Errors: {job.errors}")
        
        # Verify data was loaded
        table = bq_client.get_table(BIGQUERY_TABLE_ID)
        logging.info(f"Table verification:")
        logging.info(f"  - Table ID: {table.table_id}")
        logging.info(f"  - Row count: {table.num_rows}")
        logging.info(f"  - Created: {table.created}")
        logging.info(f"  - Modified: {table.modified}")
        
        logging.info("🎉 SUCCESS: Data successfully loaded to BigQuery!")
        
        # Final summary
        logging.info("=" * 60)
        logging.info("📊 FINAL SUMMARY:")
        logging.info(f"  ✅ Rows processed: {len(df)}")
        logging.info(f"  ✅ BigQuery table: {BIGQUERY_TABLE_ID}")
        logging.info(f"  ✅ Project: {Variable.get(VARIABLE_BIGQUERY_PROJECT_ID)}")
        logging.info(f"  ✅ Date: {ds}")
        logging.info("=" * 60)
        
    except Exception as e:
        logging.error(f"❌ BigQuery load failed: {e}")
        raise


default_args = {
    "owner": "Brenda",
}
with DAG(
    "load_networkrail_movement_data_to_bigquery",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=timezone.datetime(2022, 7, 13),
    catchup=False,
) as dag:

    start = EmptyOperator(task_id="start")

    list_files = PythonOperator(
        task_id="list_files",
        python_callable=_list_files,
    )

    download_files = PythonOperator(
        task_id="download_files",
        python_callable=_download_files,
    )

    load_data_to_bigquery = PythonOperator(
        task_id="load_data_to_bigquery",
        python_callable=_load_data_to_bigquery,
    )

    end = EmptyOperator(task_id="end")

    start >> list_files >> download_files >> load_data_to_bigquery >> end
