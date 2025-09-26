Lab 9 – Mini-projet d’orchestration ELT
🎯 Objectifs

Construire un pipeline complet avec Airflow pour orchestrer un processus ELT sur GCP :

Extraction d’un fichier CSV depuis une API.

Ingestion dans Cloud Storage.

Transformation avec Pandas.

Chargement dans BigQuery.

🔧 Prérequis

Airflow fonctionnel (Lab 6).

Bucket GCP existant (gs://data-cloud-wassim).

Dataset BigQuery créé (data).

Service account avec les rôles nécessaires pour BigQuery et Cloud Storage.

📝 Étapes
1️⃣ Extraction depuis une API

Créer un script Python pour télécharger le CSV :

# /tmp/extract_covid.py
import requests

url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/latest/owid-covid-latest.csv"
r = requests.get(url)
with open("/tmp/covid.csv", "wb") as f:
    f.write(r.content)

print("Fichier CSV téléchargé : /tmp/covid.csv")


Exécution :

python3 /tmp/extract_covid.py



2️⃣ Ingestion dans Cloud Storage

Uploader le CSV vers le bucket GCP :

gsutil cp /tmp/covid.csv gs://data-cloud-wassim/raw/covid.csv




3️⃣ Transformation avec Pandas

Nettoyer le CSV pour ne garder que location et new_cases :

# /tmp/transform_covid.py
import pandas as pd

df = pd.read_csv("/tmp/covid.csv")
df_clean = df[["location", "new_cases"]]
df_clean.to_csv("/tmp/covid_clean.csv", index=False)

print("CSV transformé créé : /tmp/covid_clean.csv")


Exécution :

python3 /tmp/transform_covid.py
cat /tmp/covid_clean.csv | head



4️⃣ Chargement dans BigQuery

Charger le CSV transformé dans BigQuery :

bq --location=EU load \
--autodetect \
--source_format=CSV \
data.covid_clean \
gs://data-cloud-wassim/raw/covid_clean.csv


Capture d’écran : table covid_clean créée dans BigQuery.

5️⃣ DAG Airflow

Assembler les tâches dans un DAG Airflow /home/imadb/airflow/dags/covid_pipeline.py :

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
        "data.covid_clean", "gs://data-cloud-wassim/raw/covid_clean.csv"
    ], check=True)

with DAG("covid_pipeline", schedule_interval="@daily", default_args=default_args, catchup=False) as dag:
    t1 = PythonOperator(task_id="extract", python_callable=extract)
    t2 = PythonOperator(task_id="ingest", python_callable=ingest)
    t3 = PythonOperator(task_id="transform", python_callable=transform)
    t4 = PythonOperator(task_id="load", python_callable=load)

    t1 >> t2 >> t3 >> t4




6️⃣ Vérification dans BigQuery

Exécuter la requête SQL pour vérifier les données :

SELECT location, SUM(new_cases) as total_cases
FROM `data-cloud-mydigitalschool.data.covid_clean`
GROUP BY location
ORDER BY total_cases DESC;




📁 Organisation des fichiers
/tmp/extract_covid.py
/tmp/transform_covid.py
/home/imadb/airflow/dags/covid_pipeline.py



Toutes les commandes gsutil et bq doivent être exécutées avec un compte de service ayant les bons scopes (cloud-platform).

Le DAG Airflow orchestre l’ETL complet : extraction → ingestion → transformation → chargement.

Les captures d’écran doivent montrer :

Téléchargement CSV

Bucket GCS avec fichiers

CSV transformé

Table BigQuery

DAG Airflow

Résultats de la requête SQL
