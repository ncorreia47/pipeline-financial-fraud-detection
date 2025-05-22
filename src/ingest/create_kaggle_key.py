import os
import json
from dotenv import load_dotenv
from colorama import init, Fore, Style

# Customização de saídas nos prompts de terminais
init(autoreset=True)

class CreateKaggleKey:
    """
    Classe responsável por criar o arquivo de configurações do kaggle
    """

    @staticmethod
    def create_kaggle_key() -> None:
        
        # Carrega as variáveis do arquivo .env
        load_dotenv()
        kaggle_user = os.getenv("KAGGLE_USER")
        kaggle_key = os.getenv("KAGGLE_KEY")
        kaggle_url = os.getenv("KAGGLE_URL")
        kaggle_file = os.getenv("KAGGLE_FILE")
        kaggle_api_token = {"username": kaggle_user,"key": kaggle_key}
        kaggle_path = os.path.expanduser(f'{kaggle_url}')
        kaggle_file_path = os.path.join(kaggle_path, kaggle_file)

        try:
            # Cria a pasta ~/.kaggle se não existir
            os.makedirs(kaggle_path, exist_ok=True)

            # Salva o arquivo
            with open(kaggle_file_path, "w") as f:
                json.dump(kaggle_api_token, f)
            
            print(Style.BRIGHT + Fore.GREEN + 'Arquivo criado com sucesso!')

            try:
                # Garante as permissões corretas (necessário em alguns sistemas)
                os.chmod(kaggle_file_path, 0o600)

            except Exception as permission_error:
                print(Style.BRIGHT + Fore.RED + 'Erro ao atualizar permissões!')
                raise permission_error

        except Exception as create_file_error:
            print(Style.BRIGHT + Fore.RED + 'Erro ao criar o arquivo!')
            raise create_file_error

        
if __name__ == '__main__':
    CreateKaggleKey.create_kaggle_key()
