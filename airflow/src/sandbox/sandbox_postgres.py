import os
import pandas as pd
import uuid
from dotenv import load_dotenv
from sqlalchemy import create_engine, Table, Column, MetaData, String, Integer, Float, DateTime
from sqlalchemy.dialects.postgresql import TIMESTAMP
from src.utils.custom_functions import create_dt_processamento_column

class PostgresConnection:
    """
    Classe responsável pela criação da conexão com o banco Postgres. A ideia é simular um ambiente on-premisse.
    """

    def __init__(self):
        load_dotenv()


    def create_pg_connection(self):
        
        print('Carregando variáveis')
        self.host = os.getenv("POSTGRES_HOST")
        self.port = os.getenv("POSTGRES_PORT")
        self.dbname = os.getenv("POSTGRES_DBNAME")
        self.user = os.getenv("POSTGRES_USERNAME")
        self.password = os.getenv("POSTGRES_PASSWORD")
        self.schema = os.getenv("POSTGRES_SCHEMA")
        self.table = os.getenv("POSTGRES_TABLE")
        self.control_table = os.getenv("POSTGRES_CONTROL_TABLE")

        print('Iniciando criação da conexão')

        try:

            engine = create_engine(f'postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}',  connect_args={"sslmode": "disable"})
            metadata = MetaData()
            print('Conexão criada com sucesso!')
            return engine, metadata

        except Exception as create_connection_error:

            print('Erro ao criar a conexão no banco Posgres')
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
        
        # Cria a tabela definida em new_table:
        new_table.create(engine, checkfirst=True)
        
        print(f'Tabela {new_table.name} criada com sucesso!')

    
    def insert_pg_sandbox(self, engine, dataframe):

        print(f'Iniciando o processo de inserção dos dados. Isso pode demorar um pouco')
        dataframe = dataframe.where(pd.notna(dataframe), None)
        total_registros = len(dataframe)

        try:

            dataframe.to_sql(self.table, engine, schema=self.schema, index=False, if_exists='append')
            print(f'{total_registros} registros inseridos com sucesso!')

            self.insert_pg_control_table(engine, dataframe, status='SUCESS')

        except Exception as insert_pg_error:

            print(f'Erro ao inserir os dados na tabela {self.table}')
            self.insert_pg_control_table(engine, dataframe, status='ERROR')

            raise insert_pg_error


    def insert_pg_control_table(self, engine, dataframe, status):
        
        print(f'Criando o processo de log na tabela de controle')

        run_id = str(uuid.uuid4())
        total_registros = len(dataframe)
        metrics = []

        for column in dataframe.columns:
            nullable = dataframe[column].isnull().sum()
            percent_nullable = (nullable / total_registros) * 100
            metrics.append({'column': column, 'vl_percent_nulo': percent_nullable})
        
        df_controle = pd.DataFrame([{
            'run_id': run_id,
            'dt_processamento': create_dt_processamento_column(),
            'total_registros': total_registros,
            'ds_status': status,
            'ds_column': m['column'],
            'vl_percent_nulo': m['vl_percent_nulo']
            } for m in metrics])
        
        df_controle.to_sql(self.control_table, engine, schema=self.schema, index=False, if_exists='append')
        print(f'Registro {run_id} inserido com sucesso na tabela de controle')