import os
import pandas as pd
from dotenv import load_dotenv
from airflow.src.utils.custom_logger import Printer, BrightYellowPrint
from airflow.src.ingest.create_kaggle_key import CreateKaggleKey
from airflow.src.ingest.get_files_from_kaggle import GetCsvFileFromKaggle
from airflow.src.sandbox.sandbox_postgres import PostgresConnection
from airflow.src.sandbox.sandbox_aws_s3 import AWSS3Connection
from airflow.src.utils.custom_functions import create_dt_processamento_column


def main():
    
    # Inicia o pipeline de extração
    CreateKaggleKey.create_kaggle_key()
    printer = Printer(BrightYellowPrint())
    downloader = GetCsvFileFromKaggle(printer)
    downloader.get_csv()

    # Carrega o dataset para a sandbox no Postgres (on-premisse)
    load_dotenv()
    dataset_local_path = os.getenv("DATASET_LOCAL_PATH")
    file_name = os.getenv("FILE_NAME")
    table_name = os.getenv("POSTGRES_TABLE")
    control_table = os.getenv('POSTGRES_CONTROL_TABLE')
    local_file_name = os.path.join(dataset_local_path, file_name)
    dataframe = pd.read_csv(local_file_name)

    # Realiza os tratamentos mínimos no dataframe
    dataframe['dt_processamento'] = create_dt_processamento_column()
    
    # Sobe o arquivo no S3
    s3_connection = AWSS3Connection(printer)
    s3_connection.upload_to_s3()


    # Inicia a conexão Posgres (on-premisse)
    pg = PostgresConnection(printer)
    engine, metadata = pg.create_pg_connection()
    
    # Cria a tabela para armazenar o dataframe e uma tabela de controle
    printer.set_strategy(BrightYellowPrint())
    printer.display(f'Criando a tabela de controle {control_table}')
    pg.create_table_if_not_exist(engine, metadata, is_control_table=True)

    printer.set_strategy(BrightYellowPrint())
    printer.display(f'Criando a tabela {table_name}')
    pg.create_table_if_not_exist(engine, metadata, dataframe)

    # Carrega o dataset para a sandbox no Postgres (on-premisse)
    pg.insert_pg_sandbox(engine, dataframe)


if __name__ == "__main__":
    main()

