import pandas as pd
import json
import os
import uuid
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine

from functions.getuso_destino import usosuelo_class
from functions.clean_json import clean_json

load_dotenv()

user     = os.getenv("user")
password = os.getenv("password")
host     = os.getenv("host")
schema   = os.getenv("schema")
port     = os.getenv("port")
    
def main(inputvar={}):
    
    #inputvar = {'tipoinmueble': ['Todos'], 'areamin': 0, 'areamax': 0, 'antiguedadmin': 0, 'antiguedadmax': 2025, 'estratomin': 0, 'estratomax': 0, 'polygon': 'POLYGON ((-74.052315 4.690699, -74.052422 4.689929, -74.051349 4.689779, -74.051285 4.690399, -74.052315 4.690699))'}
    engine   = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{schema}')
    data     = pd.DataFrame()
    
    polygon       = inputvar.get('polygon',None)
    areamin       = inputvar.get('areamin', 0)
    areamax       = inputvar.get('areamax', 0)
    tipoinmueble  = inputvar.get('tipoinmueble', [])
    antiguedadmin = inputvar.get('antiguedadmin', 0)
    antiguedadmax = inputvar.get('antiguedadmax', 0)
    estratomin    = inputvar.get('estratomin', 0)
    estratomax    = inputvar.get('estratomax', 0)
    precuso       = inputvar.get('precuso', [])

    datatotal   = pd.DataFrame()
    datapredios = pd.DataFrame(columns=['barmanpre', 'preaconst', 'preaterre', 'prevetustzmin', 'prevetustzmax', 'estrato', 'predios', 'connpisos', 'connsotano', 'contsemis', 'conelevaci', 'formato_direccion', 'nombre_conjunto', 'prenbarrio', 'precbarrio', 'locnombre', 'preusoph', 'manzcodigo', 'wkt'])

    if isinstance(polygon, str) and polygon!='' and not 'none' in polygon.lower():

        query = f" ST_CONTAINS(ST_GEOMFROMTEXT('{polygon}',4326),geometry)"  #f" ST_WITHIN(geometry,ST_GEOMFROMTEXT('{polygon}'))"
        data  = pd.read_sql_query(f"SELECT barmanpre FROM  bigdata.bogota_data_lotes_fastsearch WHERE {query}" , engine)

        if not data.empty:
            lista       = "','".join(data['barmanpre'].astype(str).unique())
            query       = f" barmanpre IN ('{lista}')"
            datatotal   = pd.read_sql_query(f"SELECT * FROM  bigdata.bogota_data_caracteristicas WHERE {query}" , engine)
            datapredios = pd.read_sql_query(f"SELECT barmanpre, preaconst, preaterre, prevetustz, precuso, precdestin FROM  bigdata.bogota_data_predios WHERE {query}" , engine)

        #-----------------------------------------------------------------#
        # Filtros
        if not datapredios.empty and 'precdestin' in datapredios:
            idd         = datapredios['precdestin'].isin(['65','66'])
            datapredios = datapredios[~idd]

        if not datapredios.empty and 'preaconst' in datapredios and areamin>0:
            datapredios = datapredios[datapredios['preaconst']>=areamin]
            
        if not datapredios.empty and 'preaconst' in datapredios and areamax>0:
            datapredios = datapredios[datapredios['preaconst']<=areamax]
          
        if not datapredios.empty and 'prevetustz' in  datapredios and antiguedadmin>0:
            datapredios = datapredios[datapredios['prevetustz']>=antiguedadmin]
            
        if not datapredios.empty and 'prevetustz' in  datapredios and antiguedadmax>0:
            datapredios = datapredios[datapredios['prevetustz']<=antiguedadmax]
            
        if isinstance(tipoinmueble,list) and tipoinmueble!=[]:
            if not any([x for x in tipoinmueble if isinstance(x,str) and 'todo' in x.lower()]):
                datauso = usosuelo_class()
                lista   = list(datauso[datauso['clasificacion'].isin(tipoinmueble)]['precuso'].unique())
                if isinstance(lista,list) and lista!=[] and 'precuso' in datapredios:
                    datapredios = datapredios[datapredios['precuso'].isin(lista)]

        if isinstance(precuso,list) and precuso!=[]:
            datapredios = datapredios[datapredios['precuso'].isin(precuso)]

        if not datapredios.empty and not datatotal.empty:
            datatotal = datatotal[datatotal['barmanpre'].isin(datapredios['barmanpre'])]

        if not datatotal.empty and 'estrato' in datatotal and estratomin>0:
            datatotal = datatotal[datatotal['estrato']>=estratomin]
            
        if not datatotal.empty and 'estrato' in datatotal and estratomax>0:
            datatotal = datatotal[datatotal['estrato']<=estratomax]
    
        if not datatotal.empty:
            lista     = "','".join(datatotal['barmanpre'].astype(str).unique())
            query     = f" barmanpre IN ('{lista}')"
            data      = pd.read_sql_query(f"SELECT barmanpre,ST_AsText(geometry) as wkt FROM  bigdata.bogota_data_lotes WHERE {query}" , engine)
            datatotal = datatotal.merge(data,on='barmanpre',how='outer')
   
    engine.dispose()
    
    data = []
    if not datatotal.empty:
        data = json.loads(datatotal.to_json(orient='records', date_format='iso'))
        
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
        "data":data
        }
    
    output = clean_json(output)

    return output