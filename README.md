# python-gcp-101
This is a small data engineering project using python, mysql, mongodb, airflow, docker and gcp services.

# Requirements and Instructions
All the modules and requirements are inside the Dockerfile and docker-compose files.

In order to run this project you need to have installed Docker Desktop (Windows), a mongo atlas account, a gcp account and at least 8 GB of RAM.
- You will need to have two GCS buckets one for the raw and one for the gold layer. You will need to have a bigquery dataset.
- You will need to fill the values in the .ENV file with your own in order for this to work. Also, need to add a service account from GCP in the CREDS folder.

You can run this project navigating into the root folder and opening a terminal.
- Run ```docker compose build``` to make sure everything is up to date.
- Run ```docker compose up``` or ```docker compose up -d ``` to start the containers.
- Go to localhost:8080 to see the airflow web server instance.
- You can verify that the 6 csv files have been moved to your raw layer bucket and that 3 transformed csvs have been moved to the gold layer and tables in bigquery.
