from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.ingest.create_kaggle_key import CreateKaggleKey
from src.ingest.get_files_from_kaggle import GetCsvFileFromKaggle
from src.utils.custom_logger import Printer, BrightYellowPrint

def download_kaggle():
    CreateKaggleKey.create_kaggle_key()
    printer = Printer(BrightYellowPrint())
    downloader = GetCsvFileFromKaggle(printer)
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
