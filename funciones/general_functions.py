import re
import os
import json
import orjson
import pandas as pd
import numpy as np
import boto3
import fsspec
import geopandas as gpd
import hashlib
from io import BytesIO
from tqdm import tqdm
from joblib import Parallel, delayed
from unidecode import unidecode
from shapely.geometry import Point,Polygon
from concurrent.futures import ThreadPoolExecutor, as_completed

tqdm.pandas()

#-----------------------------------------------------------------------------#
# Guardar datos en DO como parquet
#-----------------------------------------------------------------------------#
def uploadparquet(data,folder,nombre):
    ACCESS_KEY  = 'DO80193KAL72B4UN27HG'
    SECRET_KEY  = 'x9XqJcjm8HYmL0WKfWmR9QR2oUP84fZz2QZeluxDTtk'
    REGION      = 'nyc3'
    BUCKET_NAME = 'etl-urbex'
    
    # Guardar parquet
    user_path = os.path.expanduser("~")
    subfolder = folder.split('/')[0].strip()
    filepath  = os.path.join(user_path, "Downloads",subfolder,f"{nombre}.parquet")
    data.to_parquet(filepath, engine="pyarrow", compression="snappy")
    filename  = f"{folder}/{nombre}.parquet"

    session = boto3.session.Session()
    client  = session.client('s3', region_name=REGION, endpoint_url='https://nyc3.digitaloceanspaces.com',
                            aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

    with open(filepath, 'rb') as f:
        client.upload_fileobj(f, BUCKET_NAME, filename, ExtraArgs={'ContentType': 'application/x-parquet', 'ACL': 'private'})
    result = {'filename':filename,'url': f'https://{BUCKET_NAME}.{REGION}.digitaloceanspaces.com/{filename}'}

    if os.path.exists(filepath):
        os.remove(filepath)
        
    return result

#-----------------------------------------------------------------------------#
# read data digital ocean
#-----------------------------------------------------------------------------#
def get_data_bucket(file_key,columns=None):
    
    ACCESS_KEY = 'DO801VBERAAVW9LN9QHN'
    SECRET_KEY = 'buwqwpQcXFCaVco02m+o3sIfqZTKcgMw0Vkeb0nvyO4'
    SPACE_NAME = "etl-urbex"
    REGION     = "nyc3"
    
    session = boto3.session.Session()
    client  = session.client('s3',
                            region_name=REGION,
                            endpoint_url=f'https://{REGION}.digitaloceanspaces.com',
                            aws_access_key_id=ACCESS_KEY,
                            aws_secret_access_key=SECRET_KEY)
    
    #file_key = "_snr/snr_oficina2mpio.parquet"  # Ruta dentro del bucket
    response = client.get_object(Bucket=SPACE_NAME, Key=file_key)
    if isinstance(columns,list) and columns!=[]:
        data = pd.read_parquet(BytesIO(response['Body'].read()), engine="pyarrow", columns=columns)
    else:
        data = pd.read_parquet(BytesIO(response['Body'].read()), engine="pyarrow")

    return data

#-----------------------------------------------------------------------------#
# read data digital ocean multiples files en paralelo
#-----------------------------------------------------------------------------#
def get_multiple_data_bucket(file_keys, columns=None, max_workers=5):
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(get_data_bucket, key, columns): key for key in file_keys}
        for future in as_completed(futures):
            try:
                data = future.result()
                results.append(data)
            except: pass
    if results:
        combined_data = pd.concat(results, ignore_index=True)
    else:
        combined_data = pd.DataFrame()
    return combined_data

#-----------------------------------------------------------------------------#
# get files digital ocean
#-----------------------------------------------------------------------------#
def get_files_bucket(BUCKET_NAME,PREFIX=None):
    ACCESS_KEY  = 'DO00JMM7DR78LD2JAAEA'
    SECRET_KEY  = 'BxBRkj7i5D9TaWkV+CHeji7RKcCNz8myLsndmRadOyQ'
    REGION      = 'nyc3'
    session     = boto3.session.Session()
    client      = session.client('s3',
                              region_name=REGION,
                              endpoint_url=f'https://{REGION}.digitaloceanspaces.com',
                              aws_access_key_id=ACCESS_KEY,
                              aws_secret_access_key=SECRET_KEY)
    
    paginator = client.get_paginator('list_objects_v2')
    archivos = []
    
    if isinstance(PREFIX,str) and PREFIX!='':
        for page in tqdm(paginator.paginate(Bucket=BUCKET_NAME, Prefix=PREFIX)):
            if "Contents" in page:
                archivos.extend([obj["Key"] for obj in page["Contents"]])
    else:
        for page in tqdm(paginator.paginate(Bucket=BUCKET_NAME)):
            if "Contents" in page:
                archivos.extend([obj["Key"] for obj in page["Contents"]])
    
    files = pd.DataFrame(archivos, columns=['nombre'])
    files['nombre'] = files['nombre'].apply(lambda x: x.split('.')[0].strip())
    return files

#-----------------------------------------------------------------------------#
# Guardar datos fraccionados o en batch para luego leerlos y agruparlos
#-----------------------------------------------------------------------------#
def process_batch(batch,datamerge):
    user_path      = os.path.expanduser("~")
    downloads_path = os.path.join(user_path, "Downloads")
    ruta           = os.path.join(downloads_path, "_group")

    df = gpd.sjoin(batch, datamerge, how='left', predicate='intersects')
    if not df.empty and 'isin' in df:
        df        = df[df['isin'] == 1]
        variables = [x for x in ['geometry', 'index_right', 'index_left', 'isin'] if x in df]
        df        = df.drop(columns=variables)
        df        = pd.DataFrame(df)
    
        variables_fecha = detectar_columnas_fecha(df)
        if isinstance(variables_fecha, list) and variables_fecha:
            for col in variables_fecha:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime("%Y-%m-%d").fillna('')
        
        grouped_data = df.groupby('barmanpre').agg(list)
        grouped_data = grouped_data.map(lambda x: [safe_convert(v) for v in x])
        output_file  = os.path.join(ruta, f"_{batch['barmanpre'].iloc[0]}.parquet")
        grouped_data = grouped_data.apply(lambda x: orjson.dumps(x.to_dict()), axis=1).reset_index(name="group")
        grouped_data.to_parquet(output_file, engine="pyarrow", compression="snappy")
            
def safe_convert(value):
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="ignore")  # Convertir bytes a string
    elif isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")  # Convertir fechas
    elif pd.isna(value) or value is pd.NaT:
        return ""
    return value

#-----------------------------------------------------------------------------#
# Group by barmanpre
#-----------------------------------------------------------------------------#
def group_by_barmanpre(data,folder,filename,colname, batch_size= 100_000, n_jobs=4):
    
    data['barmanpre'] = data['barmanpre'].apply(lambda x: x if isinstance(x,str) and x!='' else None)
    data              = data[data['barmanpre'].notnull()]
    
    variables_fecha = detectar_columnas_fecha(data)
    if isinstance(variables_fecha, list) and variables_fecha:
        for col in variables_fecha:
            if col in data.columns:
                data[col] = pd.to_datetime(data[col], errors='coerce').dt.strftime("%Y-%m-%d").fillna('')
        
    batch_size    = batch_size  # Ajusta según la memoria disponible
    n_jobs        = n_jobs        # Número de núcleos de CPU a usar en paralelo
    grouped       = data.groupby("barmanpre")
    grouped_dfs   = [group for _, group in grouped]
    batches       = []
    current_batch = []
    current_size  = 0
    
    for df in grouped_dfs:
        current_size += len(df)
        current_batch.append(df)
    
        if current_size >= batch_size:
            batches.append(pd.concat(current_batch)) 
            current_batch = [] 
            current_size = 0
    
    if current_batch:
        batches.append(pd.concat(current_batch))
    
    def process_batch_group(chunk):
        return (
            chunk.groupby("barmanpre")
            .agg(list)
            .apply(lambda x: orjson.dumps(convert_bytes(x.to_dict())), axis=1)
            .reset_index(name=colname)
        )
    
    results = Parallel(n_jobs=n_jobs)(
        delayed(process_batch_group)(batch) for batch in tqdm(batches, desc="Procesando batches", unit="batch")
    )
    
    df = pd.concat(results, ignore_index=True)
    uploadparquet(df,folder,filename)
    
def detectar_columnas_fecha(df):
    columnas_fecha = []
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):  
            columnas_fecha.append(col)
    return columnas_fecha

def convert_bytes(obj):
    if isinstance(obj, bytes):
        return obj.decode("utf-8")
    elif isinstance(obj, list):
        return [convert_bytes(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: convert_bytes(value) for key, value in obj.items()}
    elif isinstance(obj, pd.Timestamp):
        return obj.strftime("%Y-%m-%d")
    elif pd.isna(obj) or obj is pd.NaT:
        return ""
    return obj

#-----------------------------------------------------------------------------#
# Procesar formato de fila
#-----------------------------------------------------------------------------#
def procesar_fila(row):
    valores = []
    for valor in row:
        if pd.notnull(valor):
            val = str(valor).strip()
            val = re.sub(r"\s+", " ", val) 
            if val.lower() in ['none', '']:
                continue
            if "|" in val:
                subvalores = [re.sub(r"\s+", " ", sub.strip()) for sub in val.split("|") if sub.strip().lower() not in ['none', '']]
                valores.extend(subvalores)
            else:
                valores.append(val)
    valores_unicos = []
    for v in valores:
        if v not in valores_unicos:
            valores_unicos.append(v)
    return " | ".join(valores_unicos)

#-----------------------------------------------------------------------------#
# Poligono circular - Radio al rededor de un punto
#-----------------------------------------------------------------------------#
def circle_polygon(metros,lat,lng):
    grados   = np.arange(-180, 190, 10)
    Clat     = ((metros/1000.0)/6371.0)*180/np.pi
    Clng     = Clat/np.cos(lat*np.pi/180.0)
    theta    = np.pi*grados/180.0
    longitud = lng + Clng*np.cos(theta)
    latitud  = lat + Clat*np.sin(theta)
    return Polygon([[x, y] for x,y in zip(longitud,latitud)])

#-----------------------------------------------------------------------------#
# Funciones para SNR
#-----------------------------------------------------------------------------#
def getEXACTfecha(x):
    result = None
    try:
        x = json.loads(x)
        continuar = 0
        for i in ['fecha','fecha:','fecha expedicion','fecha expedicion:','fecha recaudo','fecha recaudo:']:
            for j in x:
                if i==re.sub('\s+',' ',unidecode(j['value'].lower())):
                    posicion = x.index(j)
                    result   = x[posicion+1]['value']
                    continuar = 1
                    break
            if continuar==1:
                break
    except: result = None
    if result is None:
        result = getINfecha(x)
    return result
    
def getINfecha(x):
    result = None
    try:
        x = json.loads(x)
        continuar = 0
        for i in ['fecha','fecha:','fecha expedicion','fecha expedicion:','fecha recaudo','fecha recaudo:']:
            for j in x:
                if i in re.sub('\s+',' ',unidecode(j['value'].lower())):
                    posicion = x.index(j)
                    result   = x[posicion+1]['value']
                    continuar = 1
                    break
            if continuar==1:
                break
    except: result = None
    return result

def getvalue(x,pos):
    try: return x[pos]['value']
    except: return None
def getname(x,pos):
    try: return x[pos]['variable']
    except: return None  
    
    
#-----------------------------------------------------------------------------#
# Generar codigo para archivos
#-----------------------------------------------------------------------------#
def generar_codigo(x):
    hash_sha256 = hashlib.sha256(x.encode()).hexdigest()
    codigo      = hash_sha256[:16]
    return codigo