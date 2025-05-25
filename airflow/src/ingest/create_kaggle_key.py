import os
import json
from dotenv import load_dotenv

class CreateKaggleKey:
    """
    Classe responsável por criar o arquivo de configurações do kaggle
    """

    @staticmethod
    def create_kaggle_key() -> None:
        
        print("Carregando variáveis de ambiente")

        load_dotenv()
        kaggle_user = os.getenv("KAGGLE_USER")
        kaggle_key = os.getenv("KAGGLE_KEY")
        kaggle_url = os.getenv("KAGGLE_URL")
        kaggle_file = os.getenv("KAGGLE_FILE")
        kaggle_api_token = {"username": kaggle_user,"key": kaggle_key}
        kaggle_path = os.path.expanduser(f'{kaggle_url}')
        kaggle_file_path = os.path.join(kaggle_path, kaggle_file)

        print("Variáveis carregadas com sucesso!")

        print("Criando o arquivo")
        try:
            # Cria a pasta ~/.kaggle se não existir
            os.makedirs(kaggle_path, exist_ok=True)

            # Salva o arquivo
            with open(kaggle_file_path, "w") as f:
                json.dump(kaggle_api_token, f)
            
            print("Arquivo criado com sucesso!")

            try:

                print("Concedendo permissões necessárias")
                os.chmod(kaggle_file_path, 0o600)
                print("Permissões concedidas!")

            except Exception as permission_error:

                print("Erro ao conceder permissões!")
                raise permission_error

        except Exception as create_file_error:
            
            print("Erro ao criar o arquivo!")
            raise create_file_error
