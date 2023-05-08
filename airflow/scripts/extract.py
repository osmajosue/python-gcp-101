import sqlalchemy as db
from sqlalchemy import text, MetaData
import pandas as pd
from pymongo import MongoClient
import io
import os


class Extract:
    def read_from_mysql(self, db_name, mysql_conn_string, table_name):
        engine = db.create_engine(f"{mysql_conn_string}/{db_name}")
        con = engine.connect()
        df = pd.read_sql_query(text(f"SELECT * FROM {table_name}"), con=con)
        return df

    def read_from_mongodb(self, db_n, collection_n, mongo_conn_string):
        client = MongoClient(mongo_conn_string)

        db_name = client[db_n]
        collection_name = db_name[collection_n]

        collection = collection_name.find({})
        collection_df = pd.DataFrame(collection)
        return collection_df

    def read_all_mysql_tables(self, db_name, mysql_conn_string):
        engine = db.create_engine(f"{mysql_conn_string}/{db_name}")
        con = engine.connect()

        metadata = MetaData()
        metadata.reflect(bind=engine)
        table_names = metadata.tables.keys()

        # Extract each table into a dataframe
        dataframes = []
        for table_name in table_names:
            df = pd.read_sql_table(table_name, con)
            dataframes.append((df, table_name))

        return dataframes

    def read_all_mongo_collections(self, db_name, mongo_conn_string):
        client = MongoClient(mongo_conn_string)

        # Get the database
        db = client[db_name]

        # Get the list of collections
        collections = [
            c for c in db.list_collection_names() if not c.startswith("system.")
        ]

        # Loop over the collections and retrieve the data as dataframes
        dataframes = []
        for collection_name in collections:
            collection = db[collection_name]
            cursor = collection.find()
            df = pd.DataFrame(list(cursor))
            dataframes.append((df, collection_name))

        return dataframes

    def read_from_gcs(self, storage_client, bucket_name):
        try:
            bucket = storage_client.get_bucket(bucket_name)

            print(f"Bucket {bucket} retrieved!!")

            # Get a list of all the CSV files in the bucket
            blobs = bucket.list_blobs()
            csv_files = [blob for blob in blobs if blob.name.endswith(".csv")]

            # Loop through the CSV blobs and read each one into a pandas dataframe
            dataframes = []
            for blob in csv_files:
                file_name = blob.name
                content = blob.download_as_string()
                df = pd.read_csv(io.BytesIO(content))
                dataframes.append((df, file_name))
        except Exception as e:
            print(f"There was an error while extracting data from {bucket}: {e}")

        return dataframes

    def read_one_from_gcs(self, storage_client, bucket_name, file_name):
        df = None
        bucket = storage_client.get_bucket(bucket_name)
        print(f"Bucket {bucket} retrieved!!")

        # Get a list of all the CSV files in the bucket
        blob = bucket.blob(f"{file_name}.csv")
        try:
            data = blob.download_as_string()
            df = pd.read_csv(io.BytesIO(data))
        except Exception as e:
            print(f"There was an error while extracting data from {bucket}: {e}")

        if df is not None:
            return df

    def extract_dataframe_from_csv(self, path: str, headers: dict):
        dataframes_list = []
        try:
            files = [f for f in os.listdir(path) if not f.startswith(".")]
            for file in files:
                df = pd.read_csv(path + "/" + file, names=headers[file], sep="|")
                dataframes_list.append((df, file))
        except Exception as e:
            print(f"Dataframe conversion error: {e}")
        return dataframes_list
