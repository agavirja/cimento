import pandas as pd
import json
import os
import geopandas as gpd
import uuid
from datetime import datetime
from area import area as areapolygon
from sqlalchemy import create_engine 
from shapely.ops import unary_union
from dotenv import load_dotenv

load_dotenv()

from functions.general_functions import get_data_bucket
from functions.getuso_destino import usosuelo_class
from functions.clean_json import clean_json


user     = os.getenv("user")
password = os.getenv("password")
host     = os.getenv("host")
schema   = os.getenv("schema")
port     = os.getenv("port")

def main(inputvar={}):
    
    # Data diccionario combinacion de lotes 
    data_formato = get_data_bucket(file_key='_caracteristicas/_bogota_lotes_combinacion_diccionario.parquet', columns=['variable', 'indice', 'input'])

    # Diccionario | Decodificar variables
    inputvar_mapeado = inputvar.copy()
    for key, value in inputvar.items():
        if key in data_formato['variable'].values:
            filtro = (data_formato['variable'] == key) & (data_formato['input'] == str(value))
            if filtro.any():
                indice = data_formato.loc[filtro, 'indice'].values[0]
                inputvar_mapeado[key] = int(indice)
            
    polygon               = inputvar_mapeado['polygon'] if 'polygon' in inputvar_mapeado and isinstance(inputvar_mapeado['polygon'], str) else None
    areaminlote           = inputvar_mapeado['areaminlote'] if 'areaminlote' in inputvar_mapeado else 0
    areamaxlote           = inputvar_mapeado['areamaxlote'] if 'areamaxlote' in inputvar_mapeado else 0
    estratomin            = inputvar_mapeado['estratomin'] if 'estratomin' in inputvar_mapeado else 0
    estratomax            = inputvar_mapeado['estratomax'] if 'estratomax' in inputvar_mapeado else 0
    precuso               = inputvar_mapeado['precuso'] if 'precuso' in inputvar_mapeado else []

    pisos                 = inputvar_mapeado['pisos'] if 'pisos' in inputvar_mapeado else 0
    altura_min_pot        = inputvar_mapeado['altura_min_pot'] if 'altura_min_pot' in inputvar_mapeado else 0
    tratamiento           = inputvar_mapeado['tratamiento'] if 'tratamiento' in inputvar_mapeado else 0
    actuacion_estrategica = inputvar_mapeado['actuacion_estrategica'] if 'actuacion_estrategica' in inputvar_mapeado else 0
    area_de_actividad     = inputvar_mapeado['area_de_actividad'] if 'area_de_actividad' in inputvar_mapeado else 0
    via_principal         = inputvar_mapeado['via_principal'] if 'via_principal' in inputvar_mapeado else 0
    numero_propietarios   = inputvar_mapeado['numero_propietarios'] if 'numero_propietarios' in inputvar_mapeado else 0

    #-------------------------------------------------------------------------#
    # 1. Lista de lotes:
    #-------------------------------------------------------------------------#
    engine       = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{schema}')
    query        = f" tratamiento={tratamiento} AND actuacion_estrategica={actuacion_estrategica} AND area_de_actividad={area_de_actividad} AND pisos={pisos} AND numero_propietarios={numero_propietarios} AND altura_min_pot={altura_min_pot} AND via_principal={via_principal}"
    data         = pd.read_sql_query(f"SELECT lista FROM  bigdata.bogota_lotes_normativa WHERE {query} LIMIT 1" , engine)
    datageometry = pd.DataFrame()

    #-------------------------------------------------------------------------#
    # 2. Filtro por poligono:
    #-------------------------------------------------------------------------#
    query = ""
    if isinstance(polygon,str) and polygon!='' and 'none' not in polygon.lower():
        query        = f" ST_CONTAINS(ST_GEOMFROMTEXT('{polygon}',4326),geometry)"
        datageometry = pd.read_sql_query(f"SELECT barmanpre FROM  bigdata.bogota_data_lotes_fastsearch WHERE {query}" , engine)
        
        if not datageometry.empty:
            lista        = data['lista'].iloc[0].split('|')
            idd          = datageometry['barmanpre'].isin(lista)
            datageometry = datageometry[idd]
            
        if not datageometry.empty:
            lista = list(datageometry['barmanpre'].unique())
            lista = "','".join(lista)
            query = f" barmanpre IN ('{lista}')"
            datageometry = pd.read_sql_query(f"SELECT barmanpre, manzcodigo, ST_AsText(geometry) as wkt FROM  bigdata.bogota_data_lotes WHERE {query}" , engine)
            
    if not datageometry.empty:
        datageometry['geometry']    = gpd.GeoSeries.from_wkt(datageometry['wkt'])
        datageometry                = gpd.GeoDataFrame(datageometry, geometry='geometry')
        datageometry['areapolygon'] = datageometry['geometry'].apply( lambda x: areapolygon(x.__geo_interface__))

    #-------------------------------------------------------------------------#
    # 3. Filtro por area maxima [remover los lotes que por si solos tienen mas de "areamax"]
    #-------------------------------------------------------------------------#
    if not datageometry.empty and 'areapolygon' in datageometry and areamaxlote>0:
        datageometry = datageometry[datageometry['areapolygon']<=areamaxlote]
        
    #-------------------------------------------------------------------------#
    # 4. Filtros adicionales: 
    #-------------------------------------------------------------------------#
    if not datageometry.empty and 'barmanpre' in datageometry:
        lista = list(datageometry['barmanpre'].unique())
        lista = "','".join(lista)
        query = f" barmanpre IN ('{lista}')"
        datamerge = pd.read_sql_query(f"SELECT barmanpre, preaconst, estrato, predios, lista_precuso as precuso, lista_precdestin as precdestin FROM  bigdata.bogota_data_caracteristicas WHERE {query}" , engine)

        #-----------------------------------------------------------------#
        # Filtros
        if not datamerge.empty and 'estrato' in datamerge and estratomin>0:
            datamerge = datamerge[datamerge['estrato']>=estratomin]

        if not datamerge.empty and 'estrato' in datamerge and estratomax>0:
            datamerge = datamerge[datamerge['estrato']<=estratomax]
            
        if isinstance(precuso,list) and precuso!=[]:
            if not any([x for x in precuso if isinstance(x,str) and 'todo' in x.lower()]):
                df            = datamerge.copy()
                df['precuso'] = df['precuso'].apply(lambda x: x.split('|'))
                df            = df.explode('precuso')
                
                datauso = usosuelo_class()
                lista   = list(datauso[datauso['clasificacion'].isin(precuso)]['precuso'].unique())
                if isinstance(lista,list) and lista!=[] and 'precuso' in datamerge:
                    df        = df[df['precuso'].isin(lista)]
                    datamerge = datamerge[datamerge['barmanpre'].isin(df['barmanpre'])]
     
        #-----------------------------------------------------------------#
        # Filtro de vias publicas / parques publicos
        if not datamerge.empty and 'precdestin' in datamerge:
            df               = datamerge.copy()
            df['precdestin'] = df['precdestin'].apply(lambda x: x.split('|'))
            df               = df.explode('precdestin')
            df               = df[df['precdestin'].isin(['65','66'])]
            idd              = datamerge['barmanpre'].isin(df['barmanpre'])
            datamerge        = datamerge[~idd]

        if not datamerge.empty:
            idd          = datageometry['barmanpre'].isin(datamerge['barmanpre'])
            datageometry = datageometry[idd]
                
    #-------------------------------------------------------------------------#
    # 5. Unir lotes
    #-------------------------------------------------------------------------#
    if not datageometry.empty and 'geometry' in datageometry:
        datagroup                = datageometry.groupby('manzcodigo').apply(lambda x: unary_union(x.geometry)).reset_index()
        datagroup.columns        = ['manzcodigo','geometry']
        
        datamerge                = datageometry.groupby('manzcodigo').agg({'barmanpre': lambda x: '|'.join(x),'areapolygon':'sum'}).reset_index()
        datamerge.columns        = ['manzcodigo','barmanpre','areamaxcombinacion']
        datageometry             = datagroup.merge(datamerge,on='manzcodigo',how='left',validate='1:1')
        datageometry             = gpd.GeoDataFrame(datageometry, geometry='geometry')
        datageometry             = datageometry.explode(index_parts=False) 

    #-------------------------------------------------------------------------#
    # 6. Filtro por area minima [remover los lotes que combinados no tienen mas de "areamin"]
    #-------------------------------------------------------------------------#
    if not datageometry.empty and 'areamaxcombinacion' in datageometry and areaminlote>0:
        datageometry = datageometry[datageometry['areamaxcombinacion']>=areaminlote]

    #-------------------------------------------------------------------------#
    # 7. Ajustes del geometry: 
    #-------------------------------------------------------------------------#
    resultado = []
    if not datageometry.empty:
        datageometry['geometry'] = datageometry['geometry'].apply(lambda x: x.wkt)
        datageometry = pd.DataFrame(datageometry)
        resultado    = json.loads(datageometry.to_json(orient='records'))
        
    engine.dispose()
    
    
    meta = {
        "timestamp": datetime.now().isoformat(),
        "requestId": str(uuid.uuid4()),
        "filtersApplied": {
            "barmanpre": inputvar.get("barmanpre"),
            "chip": inputvar.get("chip"),
            "matriculaInmobiliaria": inputvar.get("matriculainmobiliaria"),
        }
    } 
    
    output = {
        "meta":meta,
        "data":resultado
        }
    
    output = clean_json(output)
    
    return output