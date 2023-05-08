from google.cloud import bigquery
from dotenv import load_dotenv
from google.oauth2 import service_account
from google.cloud import storage
import os


load_dotenv()

keypath = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
bucket_name = os.getenv("GOLD_GCS_BUCKET")
bigquery_dataset = os.getenv("BIGQUERY_DATASET")

credentials = service_account.Credentials.from_service_account_file(keypath)

# Create a BigQuery client with the credentials
bq_client = bigquery.Client(credentials=credentials)

storage_client = storage.Client.from_service_account_json(keypath)


def create_bigquery_tables():
    # Get a list of the CSV files in the GCS bucket
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs()
    csv_files = [blob for blob in blobs if blob.name.endswith(".csv")]

    # Loop through the CSV files and load each into a separate BigQuery table
    for csv_file in csv_files:
        # Set up the BigQuery dataset and table
        dataset_name = bigquery_dataset
        table_name = csv_file.name.replace(".csv", "")
        dataset_ref = bq_client.dataset(dataset_name)
        table_ref = dataset_ref.table(table_name)

        # Set up the BigQuery job configuration
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.skip_leading_rows = 1
        job_config.autodetect = True

        # Load the data from the CSV file into the BigQuery table
        with csv_file.open("rb") as f:
            job = bq_client.load_table_from_file(f, table_ref, job_config=job_config)

        # Wait for the job to complete
        job.result()

        print(f"Loaded {csv_file.name} into {dataset_name}.{table_name}")
