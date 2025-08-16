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
    import json
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
        valid_events = 0
        for i, event in enumerate(events):
            if i < 3:  # Log first 3 events for debugging
                logging.info(f"Event {i}: {event[:200]}...")  # First 200 chars
            
            # Parse the event to ensure it's valid JSON, then write it back
            try:
                parsed_event = json.loads(event)
                # Write the properly formatted JSON
                f.write(json.dumps(parsed_event) + "\n")
                valid_events += 1
            except json.JSONDecodeError as e:
                logging.error(f"Invalid JSON in event {i}: {e}")
                # Skip invalid events
                continue
        
        logging.info(f"Successfully wrote {valid_events} valid events to file")
    
    # Verify file was created and has content
    import os
    if os.path.exists(output_file_name):
        file_size = os.path.getsize(output_file_name)
        logging.info(f"File created successfully: {output_file_name}, size: {file_size} bytes")
        
        # Check if file has content
        if file_size == 0:
            logging.error("File is empty! No valid events were written.")
            return None
        elif valid_events == 0:
            logging.error("No valid events were processed!")
            return None
        else:
            logging.info(f"File contains {valid_events} valid events, ready for processing")
    else:
        logging.error(f"File was not created: {output_file_name}")
        return None

    return output_file_name


def remove_old_data(bq_client: bigquery.Client, table: str, ds: str) -> None:
    try:
        logging.info(f"Checking if table exists: {table}")
        
        # First check if table exists
        try:
            table_obj = bq_client.get_table(table)
            logging.info(f"Table exists with {table_obj.num_rows} rows")
            
            # Check if schema is compatible
            raw_data_field = None
            for field in table_obj.schema:
                if field.name == 'raw_data':
                    raw_data_field = field
                    break
            
            if raw_data_field and raw_data_field.field_type == 'STRING':
                logging.warning("Existing table has raw_data as STRING, but new data has it as RECORD")
                logging.info("Dropping and recreating table to match new schema...")
                
                # Drop the existing table
                bq_client.delete_table(table, not_found_ok=True)
                logging.info(f"Deleted existing table: {table}")
                return
                
        except google.api_core.exceptions.NotFound:
            logging.info("Table does not exist yet - will be created during load")
            return
        
        # Count rows to be deleted - use a safer approach
        try:
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
            logging.warning(f"Could not remove old data (table may be empty or have different schema): {e}")
            logging.info("Continuing with data load...")
            
    except Exception as e:
        logging.error(f"Error during data removal: {e}")
        raise


def _load_data_to_bigquery(**context) -> None:
    ds = context["ds"]
    file_name = context["ti"].xcom_pull(task_ids="download_files", key="return_value")
    
    # Define BigQuery schema for the extracted data
    bigquery_schema = [
        bigquery.SchemaField("event_type", "STRING"),
        bigquery.SchemaField("toc_id", "STRING"),
        bigquery.SchemaField("variation_status", "STRING"),
        bigquery.SchemaField("uk_datetime", "STRING"),
        bigquery.SchemaField("timestamp", "FLOAT"),
        bigquery.SchemaField("processed_at", "STRING"),
        bigquery.SchemaField("actual_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("gbtt_timestamp", "STRING"),
        bigquery.SchemaField("loc_stanox", "STRING"),
        bigquery.SchemaField("planned_timestamp", "STRING"),
        bigquery.SchemaField("timetable_variation", "STRING"),
        bigquery.SchemaField("train_id", "STRING"),
        bigquery.SchemaField("delay_monitoring_point", "STRING"),
        bigquery.SchemaField("next_report_run_time", "STRING"),
        bigquery.SchemaField("reporting_stanox", "STRING"),
        bigquery.SchemaField("correction_ind", "STRING"),
        bigquery.SchemaField("event_source", "STRING"),
        bigquery.SchemaField("train_file_address", "STRING"),
        bigquery.SchemaField("division_code", "STRING"),
        bigquery.SchemaField("train_terminated", "STRING"),
        bigquery.SchemaField("offroute_ind", "STRING"),
        bigquery.SchemaField("train_service_code", "STRING"),
        bigquery.SchemaField("auto_expected", "STRING"),
        bigquery.SchemaField("route", "STRING"),
        bigquery.SchemaField("planned_event_type", "STRING"),
        bigquery.SchemaField("next_report_stanox", "STRING")
    ]
    
    logging.info(f"Attempting to load data from file: {file_name}")
    
    # Check if file_name is None (download failed)
    if file_name is None:
        logging.error("Download files task failed - no file to process")
        return
    
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
    
    # Read the file line by line and parse JSON manually to ensure proper parsing
    import json
    parsed_data = []
    
    with open(file_name, 'r') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if line:
                try:
                    data = json.loads(line)
                    parsed_data.append(data)
                except json.JSONDecodeError as e:
                    logging.error(f"Failed to parse line {line_num}: {e}")
                    logging.error(f"Line content: {line[:100]}...")
                    continue
    
    if not parsed_data:
        logging.error("No valid JSON data found in file!")
        return
    
    logging.info(f"Successfully parsed {len(parsed_data)} JSON lines")
    
    # Convert to DataFrame
    df = pd.DataFrame(parsed_data)
    
    # Debug: Show available columns
    logging.info(f"DataFrame columns: {list(df.columns)}")
    logging.info(f"DataFrame shape: {df.shape}")
    
    # The actual_timestamp is nested in raw_data, so we need to extract it
    if 'raw_data' in df.columns:
        # raw_data should now be a dictionary, not a string
        logging.info(f"Raw data column type: {type(df['raw_data'].iloc[0])}")
        logging.info(f"First raw_data sample: {str(df['raw_data'].iloc[0])[:100]}...")
        
        def extract_field(raw_data_dict, field_name):
            try:
                if pd.isna(raw_data_dict):
                    return None
                
                # raw_data should now be a dictionary after manual JSON parsing
                if isinstance(raw_data_dict, dict):
                    value = raw_data_dict.get(field_name)
                    if value is not None:
                        return str(value)  # Ensure all values are strings for BigQuery
                    return None
                else:
                    logging.error(f"Unexpected raw_data type: {type(raw_data_dict)}")
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
        logging.info(f"First raw_data sample: {str(df['raw_data'].iloc[0])[:100]}...")
        
        # Test extraction on first row
        first_raw_data = df['raw_data'].iloc[0]
        if isinstance(first_raw_data, str):
            try:
                parsed = json.loads(first_raw_data)
                logging.info(f"Successfully parsed first raw_data")
                logging.info(f"Sample actual_timestamp: {parsed.get('actual_timestamp')}")
                logging.info(f"Sample train_id: {parsed.get('train_id')}")
            except Exception as e:
                logging.error(f"Failed to parse first raw_data: {e}")
        else:
            logging.info(f"First raw_data is not a string: {type(first_raw_data)}")
    else:
        logging.error("raw_data column not found in DataFrame")
        logging.error(f"Available columns: {list(df.columns)}")
        return
    
    # Convert timestamp to datetime - only for non-None values
    if df["actual_timestamp"].notna().any():
        # Filter out None values before conversion
        valid_timestamps = df["actual_timestamp"].notna()
        valid_count = valid_timestamps.sum()
        logging.info(f"Found {valid_count} valid timestamps out of {len(df)} total rows")
        
        if valid_count > 0:
            try:
                df.loc[valid_timestamps, "actual_timestamp"] = df.loc[valid_timestamps, "actual_timestamp"].astype(int) / 1000
                df.loc[valid_timestamps, "actual_timestamp"] = df.loc[valid_timestamps, "actual_timestamp"] \
                    .map(datetime.utcfromtimestamp) \
                    .map(TIMEZONE_LONDON.fromutc)
                logging.info(f"Converted {valid_count} timestamps successfully")
            except Exception as e:
                logging.error(f"Error converting timestamps: {e}")
                # Continue without timestamp conversion
        else:
            logging.warning("No valid timestamps found, continuing without timestamp conversion")
    else:
        logging.warning("No valid timestamps found, continuing without timestamp conversion")
    
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
    
    # Check if we have any data to load
    if len(df) == 0:
        logging.error("DataFrame is empty, nothing to load to BigQuery")
        return
    
    # Check if we have any non-None values in key columns
    key_columns = ['train_id', 'actual_timestamp', 'toc_id']
    non_null_counts = {col: df[col].notna().sum() for col in key_columns if col in df.columns}
    logging.info(f"Non-null counts in key columns: {non_null_counts}")
    
    if all(count == 0 for count in non_null_counts.values()):
        logging.error("All key columns are null, data extraction failed")
        return
    
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
    
    # Ensure we have at least some valid data
    total_rows = len(df)
    valid_rows = df[['train_id', 'actual_timestamp', 'toc_id']].notna().all(axis=1).sum()
    logging.info(f"Data quality check: {valid_rows} valid rows out of {total_rows} total rows")
    
    if valid_rows == 0:
        logging.error("No valid rows found! Cannot proceed with BigQuery load.")
        return
    
    # Filter to only valid rows for BigQuery load
    df_clean = df[df[['train_id', 'actual_timestamp', 'toc_id']].notna().all(axis=1)].copy()
    logging.info(f"Proceeding with {len(df_clean)} clean rows for BigQuery load")
    
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
    logging.info(f"Starting BigQuery load for {len(df_clean)} rows to table: {BIGQUERY_TABLE_ID}")
    logging.info(f"BigQuery project: {Variable.get(VARIABLE_BIGQUERY_PROJECT_ID)}")
    
    try:
        job = bq_client.load_table_from_dataframe(df_clean, BIGQUERY_TABLE_ID, job_config=job_config)
        logging.info(f"BigQuery job started: {job.job_id}")
        
        # Wait for job completion
    job.result()
        logging.info("BigQuery job completed successfully!")
        
        # Get job statistics
        logging.info(f"Job statistics:")
        logging.info(f"  - Rows processed: {job.output_rows}")
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
        logging.info(f"  ✅ Rows processed: {len(df_clean)}")
        logging.info(f"  ✅ BigQuery table: {BIGQUERY_TABLE_ID}")
        logging.info(f"  ✅ Project: {Variable.get(VARIABLE_BIGQUERY_PROJECT_ID)}")
        logging.info(f"  ✅ Date: {ds}")
        logging.info(f"  ✅ Data quality: {valid_rows}/{total_rows} rows passed validation")
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
