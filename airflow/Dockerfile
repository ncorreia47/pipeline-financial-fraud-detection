# Usa a imagem oficial do Airflow como base
FROM apache/airflow:2.9.3

# Troca para root para instalar pacotes de sistema
USER root

# Atualiza repositórios e instala dependências para compilar psycopg2 e outras libs nativas
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    gcc \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Volta para o usuário airflow
USER airflow

# Copia o arquivo requirements.txt para a imagem
COPY --chown=airflow:root requirements.txt .

# Instala as dependências Python especificadas no requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Define o diretório de trabalho padrão
WORKDIR /opt/airflow

COPY ./dags /opt/airflow/dags

# Expõe a porta padrão do Airflow (opcional)
EXPOSE 8080

# Comando padrão herdado da imagem oficial, pode ser omitido
CMD ["webserver"]

