import pandas as pd
import json
import os
import uuid
from datetime import datetime
from sqlalchemy import create_engine 
from dotenv import load_dotenv

from functions.general_functions import  get_multiple_data_bucket, generar_codigo
from functions.clean_json import clean_json

load_dotenv()

user     = os.getenv("user")
password = os.getenv("password")
host     = os.getenv("host")
schema   = os.getenv("schema")
port     = os.getenv("port")

def main(inputvar={}):
    
    # ——————————————————————————————————————————————————————————————————————— #
    # Variables inputvar: 
    # ——————————————————————————————————————————————————————————————————————— #
    barmanpre = inputvar['barmanpre'] if 'barmanpre' in inputvar and isinstance(inputvar['barmanpre'],str) else None

    # ——————————————————————————————————————————————————————————————————————— #
    # Estructura de datos
    # ——————————————————————————————————————————————————————————————————————— #
    data                    = pd.DataFrame()
    data_propietarios       = pd.DataFrame()
    data_prediales_actuales = pd.DataFrame()
    data_caracteristicas    = pd.DataFrame()
    if isinstance(barmanpre,str) and barmanpre!='':
        lista   = [x.strip() for x in barmanpre.split('|')]
        formato = []
        for items in lista:
            manzcodigo = items[0:9]
            filename   = generar_codigo(manzcodigo)
            formato   += [
                {'file': f"_caracteristicas/_bogota_caracteristicas_building_manzana/{filename}.parquet", "name": "caracteristicas", "barmanpre": items, "data": pd.DataFrame(), "run": True},
                {'file': f"_prediales/_bogota_prediales_actuales_manzana/{filename}.parquet", "name": "prediales_actuales", "barmanpre": items, "data": pd.DataFrame(), "run": True},
            ]

        resultado = get_multiple_data_bucket(formato, barmanpre=None, max_workers=10)
        
        data_caracteristicas  = selectdata(resultado,"caracteristicas")
        data_caracteristicas  = data_caracteristicas[data_caracteristicas['barmanpre'].isin(lista)]
        data_caracteristicas  = data_caracteristicas.drop_duplicates(subset='barmanpre',keep='first')
        
        data_prediales_actuales = selectdata(resultado,"prediales_actuales")
        data_prediales_actuales = data_prediales_actuales[data_prediales_actuales['barmanpre'].isin(lista)] if not data_prediales_actuales.empty else data_prediales_actuales
    
    # ——————————————————————————————————————————————————————————————————————— #
    # Estructura de la API
    # ——————————————————————————————————————————————————————————————————————— #
    if not data_prediales_actuales.empty:
        df                      = data_prediales_actuales.groupby(['chip'])['year'].max().reset_index()
        df.columns              = ['chip','maxyear']
        data_prediales_actuales = data_prediales_actuales.merge(df,on='chip',how='left',validate='m:1')
        data_prediales_actuales = data_prediales_actuales[data_prediales_actuales['year']==data_prediales_actuales['maxyear']]
    
        df                   = data_prediales_actuales.groupby('barmanpre')['identificacion'].nunique().reset_index()
        df.columns           = ['barmanpre','propietarios']
        data_caracteristicas = data_caracteristicas.merge(df,on='barmanpre',how='left',validate='m:1')
    
        df         = data_prediales_actuales.groupby('chip').agg({'avaluo_catastral':'max','impuesto_predial':'max','barmanpre':'first'}).reset_index()
        df.columns = ['chip','avaluo_catastral','impuesto_predial','barmanpre']
        df         = df.groupby('barmanpre').agg({'avaluo_catastral':'sum','impuesto_predial':'sum'}).reset_index()
        df.columns = ['barmanpre','avaluo_catastral','impuesto_predial']
        data_caracteristicas.drop(columns=['avaluo_catastral','impuesto_predial'],errors='ignore',inplace=True)
        data_caracteristicas = data_caracteristicas.merge(df,on='barmanpre',how='left',validate='m:1')
        data_propietarios    = data_prediales_actuales[['barmanpre','tipo','identificacion']]
        
    # poligonos
    data = get_lotes_radio(barmanpre=barmanpre)
    if not data_caracteristicas.empty:
        data = data.merge(data_caracteristicas,on='barmanpre',how='left',validate='m:1')
    
    caracteristicas = []
    if not data.empty:
        caracteristicas = json.loads(data.to_json(orient='records'))
        
    propietarios = []
    if not data_propietarios.empty:
        propietarios = json.loads(data_propietarios.to_json(orient='records'))
        
    meta = {
        "timestamp": datetime.now().isoformat(),
        "requestId": str(uuid.uuid4()),
        "filtersApplied": {
            "barmanpre": inputvar.get("barmanpre"),
        }
    } 
    
    output = {
        "meta":meta,
        'dataCaracteristicas':caracteristicas, 
        "dataPropietarios": propietarios,
        }
    
    output = clean_json(output)
    
    return output

def selectdata(resultado,file):
    result = pd.DataFrame()
    for item in resultado:
        if file==item['name']:
            datapaso = item['data']
            result   = pd.concat([result,datapaso])
    return result

def get_lotes_radio(barmanpre=None):
    data = pd.DataFrame()
    if isinstance(barmanpre,str) and barmanpre!='':
        barmanpre = barmanpre.split('|')
    if isinstance(barmanpre,list) and barmanpre!=[]:
        barmanpre = list(map(str, barmanpre))
        lista     = "','".join(barmanpre)
        query     = f" barmanpre IN ('{lista}')"
        engine    = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{schema}')
        data      = pd.read_sql_query(f"SELECT barmanpre,ST_AsText(geometry) as wkt  FROM  bigdata.bogota_data_lotes WHERE {query}" , engine)
        engine.dispose()
    return data