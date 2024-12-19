import os
import pandas as pd
from googleapiclient.discovery import build
from google.oauth2 import service_account
from google.cloud import storage
from google.cloud import bigquery

# Functions for extraction, transformation, and loading
def extract_data(sheet_id, range_name,destination_file):
    credentials = service_account.Credentials.from_service_account_file(
        r"C:\Users\junai\Desktop\technical_challenge\service account key\shaped-clarity-445113-s1-b141ace262f2.json",
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
    )
    service = build('sheets', 'v4', credentials=credentials)
    sheet = service.spreadsheets()
    # print("Sheet value from line no 14:",sheet)
    result = sheet.values().get(spreadsheetId=sheet_id, range=range_name).execute()
    rows = result.get('values', [])
    # print("printing rows:",rows)
    # Check if any data was returned
    if not rows:
        print('No data found.')
        return

    # Convert the data to a pandas DataFrame (skip header row and use it as column names)
    df = pd.DataFrame(rows[1:], columns=rows[0])
    print("df value:",df)
    # Save the DataFrame to a CSV file
    df.to_csv(destination_file, index=False)
    
    print(f"Google Sheet data downloaded to {destination_file}")
    return df

def transform_data(df):
    # Check if df is None or empty
    if df is None:
        print("DataFrame is None!")
        return None
    elif df.empty:
        print("DataFrame is empty!")
        return df
    print("DataFrame before transformation:", df)

    # Transform 'cancel_time' column to the correct timestamp format
    if 'cancel_time' in df.columns:
        df['cancel_time'] = pd.to_datetime(df['cancel_time'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')

    # Transform other timestamp columns if needed
    if 'subscription_started' in df.columns:
        df['subscription_started'] = pd.to_datetime(df['subscription_started'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')

    print("DataFrame after transformation:", df)
    return df


def upload_to_gcs(bucket_name, file_name, destination):
    print("entering into upload_to_gcs:")
    print(storage.__version__)
    credentials = service_account.Credentials.from_service_account_file(
        r"C:\Users\junai\Desktop\technical_challenge\service account key\gcp_storage_shaped-clarity-445113-s1-7b970c5df827.json"
    )
    client = storage.Client(credentials=credentials)
    print("client = ",client)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination)
    print("blob",blob)
    blob.upload_from_filename(file_name)
    print(f"Uploaded {file_name} to GCS bucket {bucket_name} as {destination}")

def load_to_bigquery(table_id, file_path):
    credentials = service_account.Credentials.from_service_account_file(
        r"C:\Users\junai\Desktop\technical_challenge\service account key\gcp_storage_shaped-clarity-445113-s1-7b970c5df827.json"
    )
    client = bigquery.Client(credentials=credentials)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
    )
    with open(file_path, "rb") as file:
        job = client.load_table_from_file(file, table_id, job_config=job_config)
    job.result()
    print(f"Data from {file_path} loaded into BigQuery table {table_id}")

# Main execution flow
if __name__ == "__main__":
    df = extract_data(sheet_id="1qqgq98j2Qjkk7oLh2nTlvOciFUUUhr3KTFdzeLmlJ28", range_name="Sheet1!A1:C50050",destination_file="download_csv/downloaded_sheet.csv")
    df = transform_data(df)

    # Transform data
    transformed_df = transform_data(df)
    transformed_file = "cohort_transformed/cohort_transformed_data.csv"
    if transformed_df is not None:
        os.makedirs(os.path.dirname(transformed_file), exist_ok=True)  # Ensure the directory exists
        transformed_df.to_csv(transformed_file, index=False)

        # Upload to GCS
        upload_to_gcs('cohort_data_bucket', transformed_file, 'cohort_transformed/cohort_transformed_data.csv')

    
    # Load to BigQuery
        table_id = "shaped-clarity-445113-s1.cohort_dataset.transformed_data"
        load_to_bigquery(table_id, transformed_file)