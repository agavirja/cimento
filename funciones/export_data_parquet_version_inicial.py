import pandas as pd
import json
import re
import os
import shutil
import geopandas as gpd
import matplotlib.colors as mcolors
import matplotlib.cm as cm
from sqlalchemy import create_engine 
from datetime import datetime
from shapely import wkt
from shapely.geometry import Point

from dotenv import load_dotenv

load_dotenv()

from funciones.general_functions import  uploadparquet


def getdata():
    
    #-------------------------------------------------------------------------#
    # Lista de placas
    #-------------------------------------------------------------------------#
    dataplacas = pd.read_excel(r'D:\Dropbox\Empresa\Urbex\_APP_placas\data\PlacasFontanar.xlsx')
    dataplacas.columns = [x.lower() for x in list(dataplacas)]
    dataplacas.rename(columns={'placas':'placa'},inplace=True)

    #-------------------------------------------------------------------------#
    # Data vehiculos
    #-------------------------------------------------------------------------#

    lista  = list(dataplacas['placa'].unique())
    lista  = "','".join(lista)
    query  = f" placa IN ('{lista}')"
    
    user     = os.getenv("user_bigdata")
    password = os.getenv("password_bigdata")
    host     = os.getenv("host_bigdata_lectura")
    schema   = os.getenv("schema_bigdata")
    engine   = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{schema}')
    data_vehiculos         = pd.read_sql_query(f"SELECT * FROM bigdata.vehiculos WHERE {query}" , engine)
    data_vehiculos_shd_pdf = pd.read_sql_query(f"SELECT * FROM bigdata.vehiculos_shd_pdf WHERE {query}" , engine)
    engine.dispose()
    
    datamerge = data_vehiculos_shd_pdf.sort_values(by=['ano_gravable'],ascending=False).drop_duplicates(subset='placa',keep='first')
    variables = [x for x in ['placa', 'clase_vehiculo', 'modelo', 'uso', 'nombres_apellidos', 'identificacion', 'avaluo_comercial', 'marca', 'linea', 'ano_gravable', 'impuesto_a_cargo', 'cilindraje', 'url','direccion_notificacion'] if x in datamerge]
    datamerge = datamerge[variables]
    
    idd = datamerge['marca'].astype(str).str.contains('SIN|MARCA')
    if sum(idd)>0: datamerge.loc[idd,'marca'] = None
    for i in ['clase_vehiculo', 'uso', 'nombres_apellidos', 'identificacion', 'marca', 'linea','cilindraje']:
        if i in datamerge:
            datamerge[i] = datamerge[i].apply(lambda x: x.strip() if isinstance(x,str) and x!='' else None)
            
    
    datamerge.rename(columns={'clase_vehiculo': 'clase_new', 'modelo': 'modelo_new', 'uso': 'tipoServicio_new', 'nombres_apellidos': 'nombre_new', 'identificacion': 'numID_new', 'avaluo_comercial': 'avaluo_new', 'marca': 'marca_new', 'linea': 'linea_new'},inplace=True)
    data = data_vehiculos.merge(datamerge,on='placa',how='left',validate='m:1')
    for i in ['clase', 'tipoServicio', 'nombre', 'numID', 'marca', 'linea']:
        if i in data:
            data[i] = data[i].apply(lambda x: x.strip() if isinstance(x,str) and x.strip()!='' else None)
            
    for i in ['avaluo','avaluo_new','modelo','modelo_new','impuesto_a_cargo']:
        if i in data:
            data[i] = pd.to_numeric(data[i],errors='coerce')
            idd     = data[i]>0
            data.loc[~idd,i] = None
            
    for i in ['clase', 'modelo', 'tipoServicio', 'nombre', 'numID', 'avaluo', 'marca', 'linea']:
        if i in data and f'{i}_new' in data: 
            idd = (data[i].isnull()) & (data[f'{i}_new'].notnull())
            if sum(idd)>0:
                data.loc[idd,i] = data.loc[idd,f'{i}_new']
            del data[f'{i}_new']
            
    data['carroceria_new'] = data['carroceria'].copy()
    data['carroceria_new'] = data['carroceria_new'].replace(["SEDAN","HATCHBACK","PICKUP","WAGON"],["AUTOMOVIL","AUTOMOVIL","CAMIONETA","CAMPERO"])
    idd = data['carroceria_new'].isin(["AUTOMOVIL","CAMIONETA","CAMPERO"])
    data.loc[~idd,'carroceria_new'] = None
    idd = (data['clase'].isnull()) & (data['carroceria_new'].notnull())
    if sum(idd)>0:
        data.loc[idd,'clase'] = data.loc[idd,'carroceria_new']
    del data['carroceria_new']
    
    
    idd = data['clase'].isin(['AUTOMOVIL', 'CAMPERO', 'CAMIONETA', 'MICROBUS', 'CUADRICICLO'])
    data.loc[~idd,'clase'] = None
    
    #-------------------------------------------------------------------------#
    # Data propietarios
    #-------------------------------------------------------------------------#
    
    lista  = list(data['numID'].unique())
    lista  = "','".join(lista)
    query  = f" nroIdentificacion IN ('{lista}')"

    user     = os.getenv("user_bigdata")
    password = os.getenv("password_bigdata")
    host     = os.getenv("host_bigdata_lectura")
    schema   = os.getenv("schema_bigdata")
    engine   = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{schema}')
    datapropietarios = pd.read_sql_query(f"SELECT * FROM bigdata.data_bogota_catastro_propietario WHERE {query}" , engine)
    engine.dispose()
    
    for i in [1,2,3,4,5]:
        datapropietarios[f'telefono{i}'] = datapropietarios['telefonos'].apply(lambda x: getparam(x,'numero',i-1))
    for i in [1,2,3]:
        datapropietarios[f'email{i}'] = datapropietarios['email'].apply(lambda x: getparam(x,'direccion',i-1))
    datapropietarios.drop(columns=['telefonos','email'],inplace=True)

    vartel = [x for x in list(datapropietarios) if 'telefono' in x]
    if isinstance(vartel,list) and vartel!=[]:
        datapropietarios['telefonos'] = datapropietarios[vartel].apply(lambda row: ' | '.join(pd.Series([str(num).split('.0')[0] for num in row if not pd.isna(num)]).unique()), axis=1)
    else: datapropietarios['telefonos'] = None
    
    varmail = [x for x in list(datapropietarios) if 'email' in x]
    if isinstance(varmail,list) and varmail!=[]:
        datapropietarios['email'] = datapropietarios[varmail].apply(lambda row: ' | '.join(pd.Series([str(num).lower() for num in row if pd.notnull(num)]).unique()) , axis=1)
    else: datapropietarios['email'] = None

    varname = [x for x in ['primerNombre','segundoNombre','primerApellido','segundoApellido'] if x in datapropietarios]
    if isinstance(varname,list) and varname!=[]:
        datapropietarios['nombre'] = datapropietarios[varname].apply(lambda row: ' '.join([str(num) for num in row if not pd.isna(num)]), axis=1)
    else: datapropietarios['nombre'] = None
    
    datapropietarios['fechaDocumento'] = pd.to_datetime(datapropietarios['fechaDocumento'], errors='coerce')
    hoy = datetime.now()
    datapropietarios['edad'] = datapropietarios.apply(lambda x: ((hoy - x['fechaDocumento']).days // 365 + 18), axis=1)
    
    datapropietarios['tipoDocumento'] = datapropietarios['tipoDocumento'].apply(lambda x: re.sub('[^a-zA-Z]','',x))
    datapropietarios['tipoDocumento'] = datapropietarios['tipoDocumento'].replace(['PASAPORTE'],['PA'])
    
    
    datapropietarios.rename(columns={'tipoDocumento':'tipoID','nroIdentificacion':'numID','nombre':'propietario'},inplace=True)
    datapropietarios = datapropietarios[['tipoID','numID','telefonos','email','propietario','edad']]
    datamerge        = datapropietarios.drop_duplicates(subset=['tipoID','numID'],keep='first')
    data             = data.merge(datamerge,on=['tipoID','numID'],how='left',validate='m:1')
  
    #-------------------------------------------------------------------------#
    # Data propietarios
    #-------------------------------------------------------------------------#

    lista  = list(data['numID'].unique())
    lista  = "','".join(lista)
    query  = f" numero IN ('{lista}')"

    user     = os.getenv("user_write_urbex")
    password = os.getenv("password_write_urbex")
    host     = os.getenv("host_write_urbex")
    schema   = os.getenv("schema_write_urbex")
    engine   = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{schema}')
    dataid   = pd.read_sql_query(f"SELECT * FROM bigdata.general_propietarios WHERE {query}" , engine)
    engine.dispose()
    
    df1       = pd.DataFrame()
    df2       = pd.DataFrame()
    variable  = 'propiedades_shd'
    if not dataid.empty:
        dataid.index      = range(len(dataid))
        dataid['idmerge'] = range(len(dataid))
        df           = dataid[dataid[variable].notnull()]
        df[variable] = df[variable].apply(lambda x: json.loads(x) if isinstance(x, str) else None)
        df           = df[df[variable].notnull()]
        df           = df.explode(variable)
        df           = df.apply(lambda x: {**(x[variable]), 'numero': x['numero'], 'idmerge':x['idmerge']}, axis=1)
        df           = pd.DataFrame(df)
        df.columns   = ['formato']
        df           = pd.json_normalize(df['formato'])
        df1          = df.copy()
        
    variable = 'transacciones_snr'
    if not dataid.empty:
        dataid.index      = range(len(dataid))
        dataid['idmerge'] = range(len(dataid))
        df           = dataid[dataid[variable].notnull()]
        df[variable] = df[variable].apply(lambda x: json.loads(x) if isinstance(x, str) else None)
        df           = df[df[variable].notnull()]
        df           = df.explode(variable)
        df           = df.apply(lambda x: {**(x[variable]), 'numero': x['numero'], 'idmerge':x['idmerge']}, axis=1)
        df           = pd.DataFrame(df)
        df.columns   = ['formato']
        df           = pd.json_normalize(df['formato'])
        df2          = df.copy()
        
    datachip = pd.concat([df1[['chip']],df2[['chip']]])

    lista  = list(datachip[datachip['chip'].notnull()]['chip'].unique())
    lista  = "','".join(lista)
    query  = f" prechip IN ('{lista}')"
    query2 = f" chip IN ('{lista}')"
    
    user     = os.getenv("user_bigdata")
    password = os.getenv("password_bigdata")
    host     = os.getenv("host_bigdata_lectura")
    schema   = os.getenv("schema_bigdata")
    engine   = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{schema}')
    datacatastro     = pd.read_sql_query(f"SELECT prechip as chip, latitud, longitud, estrato, prenbarrio, precbarrio as scacodigo, precuso FROM bigdata.data_bogota_catastro WHERE {query}" , engine)
    dataobjeto2024   = pd.read_sql_query(f"SELECT chip, avaluo_catastral as avaluocatastral_new FROM bigdata.data_bogota_shd_2024 WHERE {query2}" , engine)
    dataobjeto2025   = pd.read_sql_query(f"SELECT chip, avaluocatastral FROM bigdata.data_bogota_shd_2025_avaluo_catastral WHERE {query2}" , engine)
    engine.dispose()

    datamerge    = dataobjeto2025.sort_values(by=['chip','avaluocatastral'],ascending=False).drop_duplicates(subset=['chip'],keep='first')
    datacatastro = datacatastro.merge(datamerge,on='chip',how='left',validate='m:1')
    datamerge    = dataobjeto2024.sort_values(by=['chip','avaluocatastral_new'],ascending=False).drop_duplicates(subset=['chip'],keep='first')
    datacatastro = datacatastro.merge(datamerge,on='chip',how='left',validate='m:1')
    idd          = (datacatastro['avaluocatastral'].isnull()) &  (datacatastro['avaluocatastral_new'].notnull())
    if sum(idd)>0:
        datacatastro.loc[idd,'avaluocatastral'] = datacatastro.loc[idd,'avaluocatastral_new']
        
    
    def getNumProperty(x):
        try: 
            dd = pd.DataFrame(json.loads(x))
            if not dd.empty:
                dd = dd.drop_duplicates(subset='chip',keep='first')
                return len(dd)
            else:
                return 0
            #return len(json.loads(x)) if len(json.loads(x))>0 else 1
        except: return 0

    df            = dataid.copy()
    df['numprop'] = df['propiedades_shd'].apply(lambda x: getNumProperty(x))
    df            = df.groupby('numero')['numprop'].max().reset_index()
    df.columns    = ['numID','numprop']
    data          = data.merge(df,on='numID',how='left',validate='m:1')
    
    #-------------------------------------------------------------------------#
    # Variables geoespaciales
    #-------------------------------------------------------------------------#
    
    user     = os.getenv("user_bigdata")
    password = os.getenv("password_bigdata")
    host     = os.getenv("host_bigdata_lectura")
    schema   = os.getenv("schema_bigdata")
    engine   = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{schema}')
    datalocalidad = pd.read_sql_query("SELECT locnombre, ST_AsText(geometry) as wkt FROM  bigdata.data_bogota_localidades" , engine)
    databarrios   = pd.read_sql_query("SELECT scacodigo,scanombre, ST_AsText(geometry) as wkt FROM  bigdata.data_bogota_barriocatastro" , engine)
    engine.dispose()
    
    datalocalidad['geometry'] = gpd.GeoSeries.from_wkt(datalocalidad['wkt'])
    datalocalidad             = gpd.GeoDataFrame(datalocalidad, geometry="geometry", crs="EPSG:4326")
    
    databarrios['geometry']   = gpd.GeoSeries.from_wkt(databarrios['wkt'])
    databarrios               = gpd.GeoDataFrame(databarrios, geometry="geometry", crs="EPSG:4326")
    
    df          = datacatastro.drop_duplicates(subset='chip',keep='first')
    df          = df.groupby('scacodigo')['chip'].count().reset_index()
    df.columns  = ['scacodigo','conteo']
    databarrios = databarrios.merge(df,on='scacodigo',how='left',validate='1:1')
    databarrios = asignar_colores(databarrios)
    
    datacatastro["geometry"] = datacatastro.apply(lambda x: Point(x["longitud"], x["latitud"]), axis=1)
    datacatastro             = gpd.GeoDataFrame(datacatastro, geometry="geometry", crs="EPSG:4326") 
    datacatastro             = gpd.sjoin(datacatastro, datalocalidad, how="left", predicate="within")
    
    df            = datacatastro.drop_duplicates(subset='chip',keep='first')
    df            = df.groupby('locnombre')['chip'].count().reset_index()
    df.columns    = ['locnombre','conteo']
    datalocalidad = datalocalidad.merge(df,on='locnombre',how='left',validate='1:1')
    datalocalidad = asignar_colores(datalocalidad)

    del datacatastro["geometry"]
    datacatastro = pd.DataFrame(datacatastro)

    del datalocalidad["geometry"]
    datalocalidad = pd.DataFrame(datalocalidad)
    
    #-----------------------------------------------------------------------------#
    #*
    # Guardar data completa Digital Ocean
    #*
    #-----------------------------------------------------------------------------#
    user_path = os.path.expanduser("~")
    folder    = '_vehiculos_placas/_placas_fontanar_test'
    subfolder = folder.split('/')[0].strip()
    ruta      = os.path.join(user_path, "Downloads",subfolder)
    if os.path.exists(ruta):
        shutil.rmtree(ruta)
    os.makedirs(ruta)
    
    uploadparquet(data,'_vehiculos_placas','_placas_fontanar_test')
    uploadparquet(databarrios,'_vehiculos_placas','_barrios')
    uploadparquet(datalocalidad,'_vehiculos_placas','_localidad')
    
  
def getparam(x,tipo,pos):
    try: return json.loads(x)[pos][tipo]
    except: return None
    
def asignar_colores(df):
    
    # Reemplazar NaN con el valor mínimo para no afectar la escala
    min_val = df["conteo"].min()
    min_val = 0
    df["conteo"].fillna(min_val, inplace=True)
    
    # Normalizar los valores de conteo para mapearlos en la escala de colores
    norm = mcolors.Normalize(vmin=df["conteo"].min(), vmax=df["conteo"].max())
    cmap = cm.get_cmap("viridis")  # Usamos la escala de colores viridis
    
    df["color"] = df["conteo"].apply(lambda x: mcolors.to_hex(cmap(norm(x))))
    
    return df

    