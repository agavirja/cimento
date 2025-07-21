import os
import json
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine 

load_dotenv()

user     = os.getenv("user")
password = os.getenv("password")
host     = os.getenv("host")
schema   = os.getenv("schema")
port     = os.getenv("port")

# ——————————————————————————————————————————————————————————————————————————— #
# Buscar todos los listings de acuerdo a los filtros
def byinput(inputvar={}):
    
    engine   = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{schema}')
    data     = pd.DataFrame(columns=['code', 'tipoinmueble', 'tiponegocio', 'direccion', 'areaconstruida', 'habitaciones', 'banos', 'garajes', 'valor', 'fecha_inicial', 'inmobiliaria', 'url_img', 'url', 'latitud', 'longitud', 'code'])

    polygon         = inputvar['polygon'] if 'polygon' in inputvar and isinstance(inputvar['polygon'], str) else None
    code            = inputvar['code'] if 'code' in inputvar and isinstance(inputvar['code'], str) else None
    areamin         = inputvar['areamin'] if 'areamin' in inputvar else 0
    areamax         = inputvar['areamax'] if 'areamax' in inputvar else 0
    tipoinmueble    = inputvar['tipoinmueble']  if 'tipoinmueble'  in inputvar else None
    tiponegocio     = inputvar['tiponegocio']  if 'tiponegocio'  in inputvar else None
    antiguedadmin   = inputvar['antiguedadmin'] if 'antiguedadmin' in inputvar else 0
    antiguedadmax   = inputvar['antiguedadmax'] if 'antiguedadmax' in inputvar else 0
    estratomin      = inputvar['estratomin'] if 'estratomin' in inputvar else 0
    estratomax      = inputvar['estratomax'] if 'estratomax' in inputvar else 0
    habitacionesmin = inputvar['habitacionesmin'] if 'habitacionesmin' in inputvar else 0
    habitacionesmax = inputvar['habitacionesmax'] if 'habitacionesmax' in inputvar else 0
    banosmin        = inputvar['banosmin'] if 'banosmin' in inputvar else 0
    banosmax        = inputvar['banosmax'] if 'banosmax' in inputvar else 0
    garajesmin      = inputvar['garajesmin'] if 'garajesmin' in inputvar else 0
    garajesmax      = inputvar['garajesmax'] if 'garajesmax' in inputvar else 0
    
    conds = []
    if isinstance(tipoinmueble, str) and tipoinmueble.strip() != "":
        conds.append(f"tipoinmueble = '{tipoinmueble}'")
    
    if isinstance(tiponegocio, str) and tiponegocio.strip() != "":
        conds.append(f"tiponegocio = '{tiponegocio}'")
    
    if areamin > 0:
        conds.append(f"areaconstruida >= {areamin}")
    if areamax > 0:
        conds.append(f"areaconstruida <= {areamax}")

    if estratomin > 0:
        conds.append(f"estrato >= {estratomin}")
    if estratomax > 0:
        conds.append(f"estrato <= {estratomax}")
    
    if habitacionesmin > 0:
        conds.append(f"habitaciones >= {habitacionesmin}")
    if habitacionesmax > 0:
        conds.append(f"habitaciones <= {habitacionesmax}")
    
    if banosmin > 0:
        conds.append(f"banos >= {banosmin}")
    if banosmax > 0:
        conds.append(f"banos <= {banosmax}")
    
    if garajesmin > 0:
        conds.append(f"garajes >= {garajesmin}")
    if garajesmax > 0:
        conds.append(f"garajes <= {garajesmax}")

    conds = " AND ".join(conds) if isinstance(conds,list) and conds!=[] else ''
    
    if isinstance(polygon, str) and polygon!='' and not 'none' in polygon.lower():
        conds += f" AND ST_CONTAINS(ST_GEOMFROMTEXT('{polygon}'),Point(`longitud`,`latitud`))"  #f" ST_WITHIN(geometry,ST_GEOMFROMTEXT('{polygon}'))"
        
    if isinstance(conds,str) and conds!='':
        conds = conds.strip().strip('AND').strip()
        data  = pd.read_sql_query(f"SELECT code,tipoinmueble,tiponegocio,direccion,areaconstruida,habitaciones,banos,garajes,valor,fecha_inicial,inmobiliaria,url_img,latitud,longitud,url FROM  bigdata.data_listings_activos WHERE {conds}" , engine)

    if data.empty and isinstance(code,str) and code!='':
        code = code.split('|')
    if data.empty and isinstance(code,list) and code!=[]:
        code  = [x.strip() for x in code if isinstance(x,str) and x!='']
        query = "','".join(code)
        query = f" `code` IN ('{query}')"
        data  = pd.read_sql_query(f"SELECT code,tipoinmueble,tiponegocio,direccion,areaconstruida,habitaciones,banos,garajes,valor,fecha_inicial,inmobiliaria,url_img,latitud,longitud,url FROM  bigdata.data_listings_activos WHERE {query}" , engine)
        
    engine.dispose()
    return json.loads(data.to_json(orient='records', date_format='iso'))


# ——————————————————————————————————————————————————————————————————————————— #
# Cararcteristicas de un inmueble por codigo
def bycode(code=None):
    
    engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{schema}')
    data   = pd.DataFrame()
    if isinstance(code,str) and code!='':
        code = code.split('|')
    elif isinstance(code,(float,int)):
        code = [f'{code}']
    if isinstance(code,list) and code!=[]:
        code   = list(map(str, code))
        lista  = "','".join(code)
        query  = f" code IN ('{lista}')"
        engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{schema}')
        data  = pd.read_sql_query(f"SELECT * FROM  bigdata.data_listings_activos WHERE {query}" , engine)
        engine.dispose()

    return json.loads(data.to_json(orient='records', date_format='iso'))