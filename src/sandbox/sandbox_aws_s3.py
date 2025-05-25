import boto3
import os
from dotenv import load_dotenv

class AWSS3Connection:
    
    def create_s3_connection(self):
        
        load_dotenv()
        aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_access_key = load_dotenv("AWS_SECRET_ACCESS_KEY")
        s3_client = boto3.client('s3', aws_access_key_id, aws_secret_access_key)

        return s3_client
    

    def upload_to_s3(self):
        """
        Faz upload de um arquivo para um bucket S3.
        """
        # Inicializa o cliente S3
        s3_client = self.create_s3_connection()

        # Carrega as variáveis de ambiente
        load_dotenv()
        bucket_name = os.getenv("AWS_BUCKET_NAME")
        dataset_local_path = os.getenv("DATASET_LOCAL_PATH")
        file_name = os.getenv("FILE_NAME")
        file_path = os.path.join(dataset_local_path, file_name)
        
        try:
            s3_client.upload_file(file_path, bucket_name, file_name)
            print(f"Arquivo {file_path} enviado com sucesso para s3://{bucket_name}/{file_name}")
        except Exception as e:
            print(f"Erro ao enviar o arquivo: {e}")
