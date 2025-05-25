import boto3
import os
from dotenv import load_dotenv

class AWSS3Connection:

    def __init__(self):
        load_dotenv()


    def create_s3_connection(self):
        
        aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

        print('Criando a conexão com S3')
        try:
            s3_client = boto3.client(
                's3', 
                aws_access_key_id=aws_access_key_id, 
                aws_secret_access_key=aws_secret_access_key
                )
        
            print('Conexão com S3 criada com sucesso!')

            return s3_client
        
        except Exception as create_s3_connection_error:

            print('Problemas ao criar a conexão com S3')
            raise create_s3_connection_error 
    

    def upload_to_s3(self):
        """
        Faz upload de um arquivo para um bucket S3.
        """
        # Inicializa o cliente S3
        s3_client = self.create_s3_connection()
        
        # Carrega as variáveis de ambiente
        bucket_name = os.getenv("AWS_BUCKET_NAME")
        dataset_local_path = os.getenv("DATASET_LOCAL_PATH")
        file_name = os.getenv("FILE_NAME")
        file_path = os.path.join(dataset_local_path, file_name)
        
        print('Iniciando o processo de upload do arquivo para o S3. Isso pode demorar um pouco')
        try:

            s3_client.upload_file(file_path, bucket_name, file_name)
            print(f'Arquivo {file_name} enviado com sucesso para s3://{bucket_name}')

        except Exception as upload_file_error:

            print(f'Arquivo {file_name} não foi enviado para s3://{bucket_name}')
            raise upload_file_error
