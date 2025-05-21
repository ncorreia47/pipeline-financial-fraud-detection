import os
from kaggle.api.kaggle_api_extended import KaggleApi
from dotenv import load_dotenv
from colorama import init, Fore, Style

# Customização de saídas nos prompts de terminais
init(autoreset=True)


api = KaggleApi()
api.authenticate()

 # Carrega as variáveis do arquivo .env
load_dotenv()
dataset_path = os.getenv("DATASET_PATH")
dataset_name = os.getenv("DATASET_NAME")
dataset_local_path = os.getenv("DATASET_LOCAL_PATH")

# Ou baixa arquivo específico (sem descompactar)
api.dataset_download_file(f'{dataset_path}', file_name=f'{dataset_name}', path=f'{dataset_local_path}')
os.rename(f'{dataset_local_path}/{dataset_name}', f'{dataset_local_path}/financial_fraud_detection.csv')
