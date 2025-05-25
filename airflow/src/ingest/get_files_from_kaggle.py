import os
import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi
from dotenv import load_dotenv

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

        print('Realizando autenticação da API...')
        self.api.authenticate()
        print('Autenticação realizada!')


    def _load_env_vars(self):

        print('Carregando variáveis de ambiente...')
        self.dataset_path = os.getenv("DATASET_PATH")
        self.dataset_name = os.getenv("DATASET_NAME")
        self.dataset_local_path = os.getenv("DATASET_LOCAL_PATH")
        self.file_name = os.getenv("FILE_NAME")
        self.local_file_name = os.path.join(self.dataset_local_path, self.file_name)
        print('Variáveis carregadas!')


    def _download_and_extract(self):

        print('Descompactando o arquivo...')
        self.api.dataset_download_files(self.dataset_path, path=self.dataset_local_path, unzip=True)
        print('Arquivo descompactado!')


    def _rename_file(self):

        if os.path.exists(self.local_file_name):
            os.remove(self.local_file_name)
        os.rename(
            os.path.join(self.dataset_local_path, self.dataset_name),
            self.local_file_name
        )


    def _load_dataframe(self) -> pd.DataFrame:

        print('Criando o dataframe...')
        df = pd.read_csv(self.local_file_name, encoding='utf-8', sep=',', on_bad_lines='skip')
        print('Dataframe criado!')
        print(df.head())

        return df