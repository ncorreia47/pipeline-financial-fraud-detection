import os
import logging
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.python import PythonOperator
from kaggle.api.kaggle_api_extended import KaggleApi


class GetCsvFileFromKaggle:

    def __init__(self):
        self.api = KaggleApi()
        load_dotenv()

    def get_csv(self) -> pd.DataFrame:
        """
        Método responsável por realizar o download do arquivo csv do kaggle, realizar a descompactação do arquivo (.zip),
        renomear o arquivo e retornar o dataframe pandas.

        return: pd.DataFrame
        """

        self._authenticate()
        self._load_env_vars()
        self._download_and_extract()
        self._rename_file()

        return self._load_dataframe()


    def _authenticate(self):

        logging.info('Realizando autenticação da API...')
        self.api.authenticate()
        logging.info('Autenticação realizada!')


    def _load_env_vars(self):

        logging.info('Carregando variáveis de ambiente...')
        self.dataset_path = os.getenv("DATASET_PATH")
        self.dataset_name = os.getenv("DATASET_NAME")
        self.dataset_local_path = os.getenv("DATASET_LOCAL_PATH")
        self.file_name = os.getenv("FILE_NAME")
        self.local_file_name = os.path.join(self.dataset_local_path, self.file_name)
        logging.info('Variáveis carregadas!')


    def _download_and_extract(self):

        logging.info('Descompactando o arquivo...')
        self.api.dataset_download_files(self.dataset_path, path=self.dataset_local_path, unzip=True)
        logging.info('Arquivo descompactado!')


    def _rename_file(self):

        logging.info('Renomeando arquivo...')
        if os.path.exists(self.local_file_name):
            os.remove(self.local_file_name)
        os.rename(
            os.path.join(self.dataset_local_path, self.dataset_name),
            self.local_file_name
        )
        logging.info('Arquivo renomeado!')


    def _load_dataframe(self) -> pd.DataFrame:

        logging.info('Criando o dataframe...')
        df = pd.read_csv(self.local_file_name, encoding='utf-8', sep=',', on_bad_lines='skip', engine='python')
        logging.info('Dataframe criado!')
        logging.info(df.head())

        return df


def download_csv():
    downloader = GetCsvFileFromKaggle()
    downloader.get_csv()


def check_file_exists():
    
    load_dotenv()
    logging.info('Validando a criação do arquivo...')
    dataset_local_path = os.getenv('DATASET_LOCAL_PATH')
    file_name = os.getenv('FILE_NAME')

    target_file = os.path.join(dataset_local_path, file_name)
    if not os.path.isfile(target_file):
        raise FileNotFoundError(f"Arquivo esperado não encontrado: {target_file}")

    logging.info(f"Arquivo {target_file} encontrado com sucesso!")


default_args = {
    'owner': 'data engineer',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG(
    dag_id="raw_kaggle_financial_dataset",
    description="DAG para baixar e preparar dataset do Kaggle",
    schedule_interval=None,
    catchup=False,
    tags=["raw", "kaggle", "financial"],
    
) as dag:

    download_dataset = PythonOperator(
        task_id="download_dataset",
        python_callable=download_csv,
    )

    check_if_file_exists = PythonOperator(
        task_id="check_file",
        python_callable=check_file_exists,
    )

    download_dataset >> check_if_file_exists
