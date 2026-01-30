FROM apache/airflow:2.9.0

# Switch to root to install git (needed for dbt deps sometimes)
USER root
RUN apt-get update && \
    apt-get install -y git && \
    apt-get clean

# Switch back to airflow user to install Python packages
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir \
    dbt-snowflake \
    snowflake-connector-python \
    pandas \
    yfinance \
    kafka-python