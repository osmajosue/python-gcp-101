from extract import Extract
from load import Load
from transform import Transform
from dotenv import load_dotenv
from google.cloud import storage
import os

load_dotenv()

key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
raw_gcs_bucket = os.getenv("RAW_GCS_BUCKET")
gold_bucket = os.getenv("GOLD_GCS_BUCKET")
mysql_conn_string = os.getenv("SQLDB_CONNECTION_STRING")
mongo_conn_string = os.getenv("MONGO_CONNECTION_STRING")
db_name = os.getenv("DB_NAME")


storage_client = storage.Client.from_service_account_json(key_path)

extract = Extract()
load = Load()
transform = Transform()


# df_list = extract.read_from_gcs(storage_client, raw_gcs_bucket)
def etl():
    categories_df = extract.read_one_from_gcs(
        storage_client, raw_gcs_bucket, "categories"
    )
    order_items_df = extract.read_one_from_gcs(
        storage_client, raw_gcs_bucket, "order_items"
    )
    orders_df = extract.read_one_from_gcs(storage_client, raw_gcs_bucket, "orders")
    products_df = extract.read_one_from_gcs(storage_client, raw_gcs_bucket, "products")
    customers_df = extract.read_one_from_gcs(
        storage_client, raw_gcs_bucket, "customers"
    )

    top_sellers = transform.top_sellers(
        order_items_df, products_df, categories_df, "top_sellers"
    )
    orders_not_completed_by_state = transform.orders_not_completed_by_state(
        orders_df, customers_df, "orders_not_completed_by_state"
    )
    dates_with_more_sales = transform.dates_with_more_sales(
        orders_df, order_items_df, "dates_with_more_sales"
    )

    transformed_dataframes = []
    transformed_dataframes.extend(
        [top_sellers, orders_not_completed_by_state, dates_with_more_sales]
    )

    load.bulk_load_to_gcs(transformed_dataframes, storage_client, gold_bucket)
