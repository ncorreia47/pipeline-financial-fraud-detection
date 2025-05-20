import os
from kaggle.api.kaggle_api_extended import KaggleApi

# TO-DO: criar classe para autenticacao e download do dataframe

# Inicializa a API
api = KaggleApi()
api.authenticate()

# Nome do dataset (exemplo: 'zynicide/wine-reviews')
dataset = 'zynicide/wine-reviews'

# Pasta destino
dest_path = 'dados_kaggle'
os.makedirs(dest_path, exist_ok=True)

# Faz o download
api.dataset_download_files(dataset, path=dest_path, unzip=True)

print("Download finalizado com sucesso!")
