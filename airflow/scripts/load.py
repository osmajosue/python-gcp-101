class Load:
    def load_to_gcs(self, file_name, df, storage_client, bucket_name):
        try:
            csv_data = df.to_csv(encoding="utf-8", index=False)

            file_name = f"{file_name}.csv"
            bucket = storage_client.bucket(bucket_name)

            blob = bucket.blob(file_name)

            blob.upload_from_string(csv_data)
            print(f"{file_name} was loaded into {bucket_name} succesfully!")
        except Exception as e:
            print(f"Error while loading the {file_name} data to gcs: {e}")

    def bulk_load_to_gcs(self, df_list, storage_client, bucket_name):
        try:
            for df in df_list:
                csv_data = df[0].to_csv(encoding="utf-8", index=False)
                file_name = f"{df[1]}.csv"
                bucket = storage_client.bucket(bucket_name)

                blob = bucket.blob(file_name)
                blob.upload_from_string(csv_data)
                print(f"{file_name} was loaded into {bucket_name} succesfully!")

        except Exception as e:
            print(f"Error while loading the {file_name} data to gcs: {e}")
