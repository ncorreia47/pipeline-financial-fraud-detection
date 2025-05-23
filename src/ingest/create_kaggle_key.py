import os
import json
from dotenv import load_dotenv
from src.utils.custom_logger import BrightYellowPrint, RedBoldPrint, GreenNormalPrint, Printer

class CreateKaggleKey:
    """
    Classe responsável por criar o arquivo de configurações do kaggle
    """

    @staticmethod
    def create_kaggle_key() -> None:
        
        printer = Printer(BrightYellowPrint())
        printer.display("Carregando variáveis de ambiente")

        load_dotenv()
        kaggle_user = os.getenv("KAGGLE_USER")
        kaggle_key = os.getenv("KAGGLE_KEY")
        kaggle_url = os.getenv("KAGGLE_URL")
        kaggle_file = os.getenv("KAGGLE_FILE")
        kaggle_api_token = {"username": kaggle_user,"key": kaggle_key}
        kaggle_path = os.path.expanduser(f'{kaggle_url}')
        kaggle_file_path = os.path.join(kaggle_path, kaggle_file)

        printer.set_strategy(GreenNormalPrint())
        printer.display("Variáveis carregadas com sucesso!")


        printer.set_strategy(BrightYellowPrint())
        printer.display("Criando o arquivo")
        try:
            # Cria a pasta ~/.kaggle se não existir
            os.makedirs(kaggle_path, exist_ok=True)

            # Salva o arquivo
            with open(kaggle_file_path, "w") as f:
                json.dump(kaggle_api_token, f)
            
            printer.set_strategy(GreenNormalPrint())
            printer.display("Arquivo criado com sucesso!")

            try:
                printer.set_strategy(BrightYellowPrint())
                printer.display("Concedendo permissões necessárias")
                
                os.chmod(kaggle_file_path, 0o600)

                printer.set_strategy(GreenNormalPrint())
                printer.display("Permissões concedidas!")

            except Exception as permission_error:
                printer.set_strategy(RedBoldPrint())
                printer.display("Erro ao conceder permissões!")
                raise permission_error

        except Exception as create_file_error:
            printer.set_strategy(RedBoldPrint())
            printer.display("Erro ao criar o arquivo!")
            raise create_file_error
