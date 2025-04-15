from sqlalchemy import create_engine
from configs import *
from psycopg2 import connect

def get_db_connection():
    connection = connect(
        dbname=PG_DATABASE,
        host=PG_HOST,
        user=PG_USER,
        password=PG_PASSWORD
    )
    return connection

def get_engine():
    engine = create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}")
    return engine

def is_number(value):
    if isinstance(value, bool):
        return False    
    if isinstance(value, int):
        value = int(value)
        return value
    if isinstance(value, float):
        value = float(value)
        return value
    try:
        float(value)
        return True
    except ValueError:
        return False