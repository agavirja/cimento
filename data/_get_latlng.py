import pandas as pd
import os
import re
import uuid
import requests
from unidecode import unidecode
from urllib.parse import quote_plus
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine

from functions.coddir import coddir
from functions.clean_json import clean_json

load_dotenv()

def main(inputvar={}):
    
    direccion = inputvar.get('direccion',None)
    ciudad    = inputvar.get('ciudad',None)
    chip      = inputvar.get('chip',None)
    matricula = inputvar.get('matricula',None)
    
    if direccion is None and isinstance(chip,str) and chip!='':
        direccion = chip2direccion(chip=chip)
    
    if direccion is None and isinstance(matricula,str) and matricula!='':
        direccion = matricula2direccion(matricula)
    
    direccion_output = direccion
    
    latitud,longitud,barmanpre = [None]*3
    if isinstance(direccion,str) and direccion!='' and isinstance(ciudad,str) and ciudad!='':
        ciudad = unidecode(ciudad).lower()
        ciudad = re.sub(r'\s+', ' ', ciudad)
        
    if isinstance(ciudad,str) and 'bogota' in ciudad:
        latitud,longitud,barmanpre = getlatlngfromcatastro(direccion)
        
    if latitud is None or longitud is None and isinstance(direccion,str) and direccion!='':
        
        if isinstance(ciudad,str) and ciudad!='':
            ciudad = unidecode(ciudad).lower()
            ciudad = re.sub(r'\s+', ' ', ciudad)
            direccion = f'{direccion},{ciudad}'
            
        if isinstance(direccion,str) and direccion!='':
            direccion = f'{direccion},colombia'
            
        api_key  = os.getenv("API_KEY")
        direccion_codificada = quote_plus(direccion)
        url      = f"https://maps.googleapis.com/maps/api/geocode/json?address={direccion_codificada}&key={api_key}"
        response = requests.get(url)
        data     = response.json()
    
        if data['status'] == 'OK':
            latitud  = data['results'][0]['geometry']['location']['lat']
            longitud = data['results'][0]['geometry']['location']['lng']
                 
    output = {
        'meta':{
            "timestamp": datetime.now().isoformat(),
            "requestId": str(uuid.uuid4()),
            "filtersApplied": {
                "direccion": direccion
            }
        },
        'location':{'latitud':latitud,'longitud':longitud,'barmanpre':barmanpre,'direccion':direccion_output},
        }
 
    output = clean_json(output)
    
    return output


def chip2direccion(chip=None):
    direccion = None
    if isinstance(chip,str) and chip!="":
        user     = os.getenv("user_bigdata")
        password = os.getenv("password_bigdata")
        host     = os.getenv("host_bigdata_lectura")
        schema   = os.getenv("schema_bigdata")
        engine   = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{schema}')
        query    = f"prechip='{chip}'"
        data     = pd.read_sql_query(f"SELECT predirecc FROM  bigdata.data_bogota_catastro WHERE {query} AND ( precdestin<>'65' AND precdestin<>'66') LIMIT 1" , engine)
        if not data.empty:
            direccion = data['predirecc'].iloc[0]
        engine.dispose()
    return direccion


def matricula2direccion(matricula):
    direccion = None
    #matricula = '050N20616596'
    if matricula is not None and matricula!="" and isinstance(matricula,str):
        user     = os.getenv("user_bigdata")
        password = os.getenv("password_bigdata")
        host     = os.getenv("host_bigdata_lectura")
        schema   = os.getenv("schema_bigdata")
        engine   = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{schema}')
    
        try:
            matricula    = re.sub(r"[^0-9A-Za-z]",'',matricula).upper()
            coincidencia = re.search(r"[A-Za-z]", matricula)
            parte1       = matricula[:coincidencia.start()]
            parte2       = matricula[coincidencia.start()+1:]
            letra        = re.sub('[^a-zA-Z]','',matricula)
            
            matriculas = [matricula, 
                          f'{parte1}{letra}{parte2}',
                          f'{parte1.lstrip("0")}{letra}{parte2}',
                          f'{parte1}{letra}0{parte2}',
                          f'{parte1.lstrip("0")}{letra}0{parte2}',
                          f'{parte1.lstrip("0")}{letra}{parte2.lstrip("0")}',
                          f'0{parte1.lstrip("0")}{letra}{parte2}',
                          f'0{parte1.lstrip("0")}{letra}{parte2.lstrip("0")}',
                          f'0{parte1.lstrip("0")}{letra}0{parte2.lstrip("0")}',
                          f'{parte1}{letra}0{parte2.lstrip("0")}',   
                          f'{parte1.lstrip("0")}{letra}0{parte2.lstrip("0")}',  
                          f'{parte1}{letra}{parte2.lstrip("0")}',
                          f'0{parte1.lstrip("0")}{letra}00{parte2.lstrip("0")}',
                          f'0{parte1.lstrip("0")}{letra}{parte2.lstrip("0")}',
                          f'{parte1}{letra}00{parte2.lstrip("0")}',
                          f'{parte1.lstrip("0")}{letra}00{parte2.lstrip("0")}',
                          ]
            lista = "','".join(list(set(matriculas)))
            query = f" matriculainmobiliaria IN ('{lista}')"
            data  = pd.read_sql_query(f"SELECT direccion FROM  bigdata.data_bogota_shd_2025_objeto_predial WHERE {query} " , engine)
            if not data.empty:
                data = pd.read_sql_query(f"SELECT direccion FROM  bigdata.data_bogota_shd_objetocontrato WHERE {query} " , engine)
            if not data.empty:
                direccion = data['direccion'].iloc[0]
        except: pass
        engine.dispose()
    return direccion

def getlatlngfromcatastro(direccion):
    latitud,longitud,barmanpre = [None]*3
    if isinstance(direccion, str):
        user     = os.getenv("user_bigdata")
        password = os.getenv("password_bigdata")
        host     = os.getenv("host_bigdata_lectura")
        schema   = os.getenv("schema_bigdata")
        engine   = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{schema}')
            
        fcoddir = coddir(direccion)
        data    = pd.read_sql_query(f"SELECT barmanpre, latitud,longitud FROM bigdata.data_bogota_catastro WHERE coddir='{fcoddir}'" , engine)
        engine.dispose()
        
        if not data.empty and 'latitud' in data and 'longitud' in data:
            latitud  = data['latitud'].median()
            longitud = data['longitud'].median()
        if not data.empty and 'barmanpre' in data:
            barmanpre = list(data['barmanpre'].unique())
    return latitud,longitud,barmanpre