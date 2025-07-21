import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine 

load_dotenv()

def getuso_destino():
    user     = os.getenv("user_bigdata")
    password = os.getenv("password_bigdata")
    host     = os.getenv("host_bigdata_lectura")
    schema   = os.getenv("schema_bigdata")
    
    engine         = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{schema}')
    dataprecuso    = pd.read_sql_query(f"SELECT * FROM  {schema}.bogota_catastro_precuso" , engine)
    dataprecdestin = pd.read_sql_query(f"SELECT * FROM  {schema}.bogota_catastro_precdestin" , engine)
    engine.dispose()
    return dataprecuso,dataprecdestin

def usobydestino():
    user     = os.getenv("user_bigdata")
    password = os.getenv("password_bigdata")
    host     = os.getenv("host_bigdata_lectura")
    schema   = os.getenv("schema_bigdata")
    
    engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{schema}')
    data   = pd.read_sql_query(f"SELECT * FROM  {schema}.data_bogota_destinouso" , engine)
    engine.dispose()
    return data

def usosuelo_class():
    user     = os.getenv("user_bigdata")
    password = os.getenv("password_bigdata")
    host     = os.getenv("host_bigdata_lectura")
    schema   = os.getenv("schema_bigdata")
    
    engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{schema}')
    data   = pd.read_sql_query(f"SELECT * FROM  {schema}.data_bogota_usosuelo" , engine)
    engine.dispose()
    return data
