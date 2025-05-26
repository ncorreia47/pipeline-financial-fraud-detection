import os
import pandas as pd
import uuid
import logging
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, Table, Column, MetaData, String, Integer, Float, DateTime, inspect
from sqlalchemy.dialects.postgresql import TIMESTAMP
from airflow import DAG
from airflow.operators.python import PythonOperator


class PostgresConnection:
    """
    Classe responsável pela criação da conexão com o banco Postgres. A ideia é simular um ambiente on-premisse.
    """

    def __init__(self):
        load_dotenv()


    def create_pg_connection(self):
        
        logging.info('Carregando variáveis')
        self.host = os.getenv("POSTGRES_HOST")
        self.port = os.getenv("POSTGRES_PORT")
        self.dbname = os.getenv("POSTGRES_DBNAME")
        self.user = os.getenv("POSTGRES_USERNAME")
        self.password = os.getenv("POSTGRES_PASSWORD")
        self.schema = os.getenv("POSTGRES_SCHEMA")
        self.table = os.getenv("POSTGRES_TABLE")
        self.control_table = os.getenv("POSTGRES_CONTROL_TABLE")

        logging.info('Iniciando criação da conexão')

        try:

            engine = create_engine(f'postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}',  connect_args={"sslmode": "disable"})
            metadata = MetaData()
            logging.info('Conexão criada com sucesso!')
            return engine, metadata

        except Exception as create_connection_error:

            logging.info('Erro ao criar a conexão no banco Posgres')
            raise create_connection_error
        
    
    def remap_datatype(self, value):
        """
        Método responsável por converter os valores do dataframe para um tipo aceitável no Postgres
        """
        
        if pd.api.types.is_integer_dtype(value):
            return Integer
        elif pd.api.types.is_float_dtype(value):
            return Float
        elif pd.api.types.is_bool_dtype(value):
            return String
        elif pd.api.types.is_datetime64_any_dtype(value):
            return TIMESTAMP(timezone=True)
        else:
            return String(255)
    

    def create_table_if_not_exist(self, engine, metadata, dataframe=None, is_control_table=False):
        
        logging.info(f'Iniciando o processo de criação de tabelas')
        if is_control_table:

            new_table = Table(
                self.control_table, metadata,
                Column('run_id', String(36), primary_key=False),
                Column('dt_processamento', DateTime(timezone=True)),
                Column('total_registros', Integer),
                Column('ds_status', String(20)),
                Column('ds_column', String(255)),
                Column('vl_percent_nulo', Float),
                schema=self.schema
                )
        else: 

            if not engine.dialect.has_table(engine.connect(), self.table):
                columns = []

                for column_name in dataframe.columns:
                    column_type = self.remap_datatype(dataframe[column_name])
                    is_nullable = dataframe[column_name].isnull().any()
                    columns.append(Column(column_name, column_type, nullable=is_nullable))

                new_table = Table(self.table, metadata, *columns, schema=self.schema)
        
        inspector = inspect(engine)
        tables = inspector.get_table_names(schema=self.schema)

        # Cria a tabela definida em new_table caso ela não exista:
        if new_table.name in tables:
            logging.info(f'Tabela {new_table.name} já existe.')
            pass

        else:
            new_table.create(engine, checkfirst=True)
            logging.info(f'Tabela {new_table.name} criada com sucesso!')

    
    def insert_pg_sandbox(self, engine, dataframe):

        logging.info(f'Iniciando o processo de inserção dos dados. Isso pode demorar um pouco')
        dataframe = dataframe.where(pd.notna(dataframe), None)
        total_registros = len(dataframe)

        try:

            dataframe.to_sql(self.table, engine, schema=self.schema, index=False, if_exists='append')
            logging.info(f'{total_registros} registros inseridos com sucesso!')

            self.insert_pg_control_table(engine, dataframe, status='SUCESS')

        except Exception as insert_pg_error:

            logging.info(f'Erro ao inserir os dados na tabela {self.table}')
            self.insert_pg_control_table(engine, dataframe, status='ERROR')

            raise insert_pg_error


    def insert_pg_control_table(self, engine, dataframe, status):
        
        logging.info(f'Criando o processo de log na tabela de controle')

        run_id = str(uuid.uuid4())
        total_registros = len(dataframe)
        metrics = []

        for column in dataframe.columns:
            nullable = dataframe[column].isnull().sum()
            percent_nullable = (nullable / total_registros) * 100
            metrics.append({'column': column, 'vl_percent_nulo': percent_nullable})
        
        df_controle = pd.DataFrame([{
            'run_id': run_id,
            'dt_processamento': datetime.now(),
            'total_registros': total_registros,
            'ds_status': status,
            'ds_column': m['column'],
            'vl_percent_nulo': m['vl_percent_nulo']
            } for m in metrics])
        
        df_controle.to_sql(self.control_table, engine, schema=self.schema, index=False, if_exists='append')
        print(f'Registro {run_id} inserido com sucesso na tabela de controle')


def create_postgres_table(is_control_table:bool):

    load_dotenv()

    # Cria a conexão
    pg = PostgresConnection()
    engine, metadata = pg.create_pg_connection()

    # Carrega as variáveis de ambiente
    dataset_local_path = os.getenv('DATASET_LOCAL_PATH')
    file_name = os.getenv('FILE_NAME')
    path_file = os.path.join(dataset_local_path, file_name)

    # Carrega o dataframe
    dataframe = pd.read_csv(path_file, encoding='utf-8', sep=',', on_bad_lines='skip', engine='python')

    # Cria a tabela de controle
    pg.create_table_if_not_exist(engine, metadata, dataframe, is_control_table)


default_args = {
    'owner': 'data engineer',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG(
    dag_id="raw_postgres_financial_dataset",
    description="DAG persistir os dados do CSV no banco Postgres on-premisse",
    schedule_interval=None,
    catchup=False,
    tags=["raw", "kaggle", "financial", "postgres"],
    
) as dag:

    create_control_table = PythonOperator(
        task_id="create_control_table",
        python_callable=create_postgres_table,
        op_kwargs={'is_control_table': True}
    )

    create_financial_table = PythonOperator(
        task_id="create_financial_table",
        python_callable=create_postgres_table,
        op_kwargs={'is_control_table': False}
    )


    [create_control_table] >> create_financial_table
