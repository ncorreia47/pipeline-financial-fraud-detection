from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
import sys

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.ingest.create_kaggle_key import CreateKaggleKey
from src.ingest.get_files_from_kaggle import GetCsvFileFromKaggle

def download_kaggle():
    
    CreateKaggleKey.create_kaggle_key()
    downloader = GetCsvFileFromKaggle()
    downloader.get_csv()
    os.makedirs('/data/events', exist_ok=True)
    with open('/data/events/dataset_ready.done', 'w') as f:
        f.write('dataset ready\n')
    print("Download e evento gerado.")

with DAG('raw_kaggle_financial_dataset', 
         start_date=days_ago(1), 
         schedule_interval=None, 
         catchup=False) as dag:
    
    PythonOperator(task_id='raw_kaggle_financial_dataset', 
                   python_callable=download_kaggle)
