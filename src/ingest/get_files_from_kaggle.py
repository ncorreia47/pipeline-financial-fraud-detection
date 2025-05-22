import os
import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi
from dotenv import load_dotenv
from colorama import init, Fore, Style

# Customização de saídas nos prompts de terminais
init(autoreset=True)

class GetCsvFileFromKaggle:

    @staticmethod
    def get_csv() -> pd.DataFrame:
        """
        Método responsável por realizar o download do arquivo csv do kaggle, realizar a descompactação do arquivo (.zip),
        renomear o arquivo e retornar o dataframe pandas.

        return: pd.DataFrame
        """

        # TO-DO: aplicar strategy methods

        print(Style.BRIGHT + Fore.YELLOW + 'Realizando autenticação da API...')
        api = KaggleApi()
        api.authenticate()
        print(Style.BRIGHT + Fore.GREEN + 'Autenticação relizada!')

        print(Style.BRIGHT + Fore.YELLOW + 'Carregando variáveis de ambiente...')
        load_dotenv()
        dataset_path = os.getenv("DATASET_PATH")
        dataset_name = os.getenv("DATASET_NAME")
        dataset_local_path = os.getenv("DATASET_LOCAL_PATH")
        file_name = os.getenv("FILE_NAME")
        local_file_name = os.path.join(dataset_local_path, file_name)
        print(Style.BRIGHT + Fore.GREEN + 'Todas as variáveis carregadas!')

        print(Style.BRIGHT + Fore.YELLOW + 'Descompactando o arquivo...')
        api.dataset_download_files(f'{dataset_path}', path=f'{dataset_local_path}', unzip=True)
        print(Style.BRIGHT + Fore.GREEN + 'Arquivo descompactado!')

        if os.path.exists(local_file_name):
            os.remove(local_file_name)
        
        os.rename(f'{dataset_local_path}/{dataset_name}', f'{local_file_name}')
        
        print(Style.BRIGHT + Fore.YELLOW + 'Criando o dataframe...')
        df =  pd.read_csv(f'{local_file_name}', encoding='utf-8', sep=',', on_bad_lines='skip')
        print(Style.BRIGHT + Fore.GREEN + 'Datframe criado!\nAmostra dos dados:\n')
        print(df.head())

        return df


if __name__ == '__main__':
    GetCsvFileFromKaggle.get_csv()