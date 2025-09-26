from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess

default_args = {
    'start_date': datetime(2025, 9, 26),
}

def extract():
    import requests
    url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/latest/owid-covid-latest.csv"
    r = requests.get(url)
    with open("/tmp/covid.csv", "wb") as f:
        f.write(r.content)

def ingest():
    subprocess.run(["gsutil", "cp", "/tmp/covid.csv", "gs://data-cloud-wassim/raw/covid.csv"], check=True)

def transform():
    import pandas as pd
    df = pd.read_csv("/tmp/covid.csv")
    df2 = df[["location", "new_cases"]]
    df2.to_csv("/tmp/covid_clean.csv", index=False)

def load():
    subprocess.run([
        "bq", "load", "--autodetect", "--source_format=CSV",
        "data.covid_clean", "gs://data-cloud-wassim/data/covid_clean.csv"
    ], check=True)

with DAG("covid_pipeline", schedule_interval="@daily", default_args=default_args, catchup=False) as dag:
    t1 = PythonOperator(task_id="extract", python_callable=extract)
    t2 = PythonOperator(task_id="ingest", python_callable=ingest)
    t3 = PythonOperator(task_id="transform", python_callable=transform)
    t4 = PythonOperator(task_id="load", python_callable=load)

    t1 >> t2 >> t3 >> t4
