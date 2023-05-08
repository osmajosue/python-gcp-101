import os
from extract import Extract
from load import Load
from dotenv import load_dotenv
from google.cloud import storage
import os

load_dotenv()

keypath = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
raw_gcs_bucket = os.getenv("RAW_GCS_BUCKET")
mysql_conn_string = os.getenv("SQLDB_CONNECTION_STRING")
mongo_conn_string = os.getenv("MONGO_CONNECTION_STRING")
db_name = os.getenv("DB_NAME")

storage_client = storage.Client.from_service_account_json(keypath)

extract = Extract()
load = Load()


def ingest_mysql():
    df_list = []

    try:
        df_list = extract.read_all_mysql_tables(db_name)
        if bool(df_list):
            print(f"All tables are loaded into dataframes and saved in a List!!")
    except Exception as e:
        print(f"Error trying to read the mysql data...", {e})

    load.bulk_load_to_gcs(df_list, storage_client, raw_gcs_bucket)


def ingest_mongo():
    df_collections = []

    try:
        df_collections = extract.read_all_mongo_collections(db_name)
        if bool(df_collections):
            print(f"All tables are loaded into dataframes and saved in a List!!")
    except Exception as e:
        print(f"Error trying to read the mysql data...", {e})

    load.bulk_load_to_gcs(df_collections, storage_client, raw_gcs_bucket)
