FROM apache/airflow:2.5.0-python3.8

USER root

# Install any OS dependencies here
RUN apt-get update


# Switch back to the airflow user
USER airflow

# Install Python dependencies here
RUN mkdir ./scripts
RUN mkdir ./creds

COPY scripts ./scripts
ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/scripts"

COPY creds ./creds
COPY .env ./.env
COPY requirements.txt .

RUN pip install --upgrade pip
RUN pip install -r requirements.txt
