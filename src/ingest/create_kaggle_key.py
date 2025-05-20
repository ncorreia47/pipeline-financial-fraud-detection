import os
import json
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env
load_dotenv()

# Caminho para onde você salvou o kaggle.json
kaggle_user = os.getenv("KAGGLE_USER")
kaggle_key = os.getenv("KAGGLE_KEY")
kaggle_api_token = {"username": kaggle_user,"key":kaggle_key}

# Cria a pasta ~/.kaggle se não existir
os.makedirs(os.path.expanduser("~/.kaggle"), exist_ok=True)

# Salva o arquivo
with open(os.path.expanduser("~/.kaggle/kaggle.json"), "w") as f:
    json.dump(kaggle_api_token, f)

# Garante as permissões corretas (necessário em alguns sistemas)
os.chmod(os.path.expanduser("~/.kaggle/kaggle.json"), 0o600)
