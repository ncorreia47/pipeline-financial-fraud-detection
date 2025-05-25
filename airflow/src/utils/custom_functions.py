import pytz
import pandas as pd
from datetime import datetime


def create_dt_processamento_column() -> pd.Timestamp:

        tz = pytz.timezone('America/Sao_Paulo')
        dt_processamento = pd.Timestamp(datetime.now(tz))

        return dt_processamento