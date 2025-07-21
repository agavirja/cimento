import pandas as pd
import hashlib
import tempfile
import os
import boto3
import uuid
import datetime
import json
from dotenv import load_dotenv
from io import BytesIO
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()
tqdm.pandas()

# ——————————————————————————————————————————————————————————————————————————— #
# read data digital ocean multiples files en paralelo
# ——————————————————————————————————————————————————————————————————————————— #
def get_multiple_data_bucket(file_objects, barmanpre=None, max_workers=5):

    def process_file(obj):
        try:
            if 'run' in obj and obj['run']:
                df = get_data_bucket(obj['file'])
                if isinstance(barmanpre, str) and barmanpre != '' and 'barmanpre' in df.columns:
                    df = df[df['barmanpre'] == barmanpre]
                else:
                    if 'barmanpre' in obj and isinstance(obj['barmanpre'],str) and obj['barmanpre']!='':
                        df = df[df['barmanpre'] == obj['barmanpre']]
                    
                obj['data'] = df
        except: pass
        return obj

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_file, obj): obj for obj in file_objects}
        for future in as_completed(futures):
            results.append(future.result())
    
    return results

# ——————————————————————————————————————————————————————————————————————————— #
# read data digital ocean
# ——————————————————————————————————————————————————————————————————————————— #
def get_data_bucket(file_key,columns=None):
    
    ACCESS_KEY = os.getenv("ACCESS_KEY_etlurbex")
    SECRET_KEY = os.getenv("SECRET_KEY_etlurbex")
    SPACE_NAME = os.getenv("SPACE_NAME_etlurbex")
    REGION     = os.getenv("REGION_etlurbex")
    
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

# ——————————————————————————————————————————————————————————————————————————— #
# Generar codigo para archivos
# ——————————————————————————————————————————————————————————————————————————— #
def generar_codigo(x):
    hash_sha256 = hashlib.sha256(x.encode()).hexdigest()
    codigo      = hash_sha256[:16]
    return codigo

# ——————————————————————————————————————————————————————————————————————————— #
# Guardar archivo de excel en storage
# ——————————————————————————————————————————————————————————————————————————— #
def upload_excel_to_spaces(data, folder):

    ACCESS_KEY  = os.getenv("ACCESS_KEY_iconos")
    SECRET_KEY  = os.getenv("SECRET_KEY_iconos")
    REGION      = os.getenv("REGION_iconos")
    BUCKET_NAME = os.getenv("BUCKET_NAME_iconos")

    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as temp_excel:
        temp_excel_path = temp_excel.name
        data.to_excel(temp_excel_path, index=False)
    
    session = boto3.session.Session()
    client  = session.client('s3',
                              region_name=REGION,
                              endpoint_url=f'https://{REGION}.digitaloceanspaces.com',
                              aws_access_key_id=ACCESS_KEY,
                              aws_secret_access_key=SECRET_KEY)
    
    random_filename = f"{uuid.uuid4()}.xlsx"
    filename        = f"{folder}/{random_filename}"
    expiration_date = datetime.datetime.utcnow() + datetime.timedelta(days=2)
    with open(temp_excel_path, 'rb') as f:
        client.upload_fileobj(
            f,
            BUCKET_NAME,
            filename,
            ExtraArgs={
                'ContentType': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                'ACL': 'public-read',
                'Expires': expiration_date
            }
        )
    
    if os.path.exists(temp_excel_path):
        os.remove(temp_excel_path)

    return  f'https://{BUCKET_NAME}.{REGION}.digitaloceanspaces.com/{filename}'

# ——————————————————————————————————————————————————————————————————————————— #
# Guardar archivo de JSON en storage con expiracion
# ——————————————————————————————————————————————————————————————————————————— #
def upload_json_to_spaces(data):

    ACCESS_KEY  = os.getenv("ACCESS_KEY_iconos")
    SECRET_KEY  = os.getenv("SECRET_KEY_iconos")
    REGION      = os.getenv("REGION_iconos")
    BUCKET_NAME = os.getenv("BUCKET_NAME_iconos")

    with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as tmp:
        json.dump(data, tmp, ensure_ascii=False)
        tmp_path = tmp.name

    folder  = '_temp_files'
    session = boto3.session.Session()
    client  = session.client(
        's3',
        region_name=REGION,
        endpoint_url=f'https://{REGION}.digitaloceanspaces.com',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )

    random_filename = f"{uuid.uuid4()}.json"
    object_key      = f"{folder}/{random_filename}"
    expires_at      = datetime.datetime.utcnow() + datetime.timedelta(days=1)

    with open(tmp_path, 'rb') as f:
        client.upload_fileobj(
            f,
            BUCKET_NAME,
            object_key,
            ExtraArgs={
                'ContentType': 'application/json',
                'ACL': 'public-read',          # o 'private' según tu caso
                'Expires': expires_at          # header HTTP
            })

    try:
        os.remove(tmp_path)
    except OSError:
        pass

    return object_key

# ——————————————————————————————————————————————————————————————————————————— #
# Guardar archivo dataframe en parquet
# ——————————————————————————————————————————————————————————————————————————— #
def uploadparquet(data):
    ACCESS_KEY  = os.getenv("ACCESS_KEY_iconos")
    SECRET_KEY  = os.getenv("SECRET_KEY_iconos")
    REGION      = os.getenv("REGION_iconos")
    BUCKET_NAME = os.getenv("BUCKET_NAME_iconos")
    
    # Crear archivo temporal con extensión .parquet y en modo binario
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name
    
    # Escribir el DataFrame al archivo Parquet (fuera del bloque 'with')
    data.to_parquet(tmp_path, engine="pyarrow", compression="snappy")
        
    folder  = '_temp_files'
    session = boto3.session.Session()
    client  = session.client(
        's3',
        region_name=REGION,
        endpoint_url=f'https://{REGION}.digitaloceanspaces.com',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )
        
    random_filename = f"{uuid.uuid4()}.parquet"
    object_key      = f"{folder}/{random_filename}"
    expires_at      = datetime.datetime.utcnow() + datetime.timedelta(days=1)
    
    with open(tmp_path, 'rb') as f:
        client.upload_fileobj(
            f,
            BUCKET_NAME,
            object_key,
            ExtraArgs={
                'ContentType': 'application/x-parquet',
                'ACL': 'public-read',          # o 'private' según tu caso
                'Expires': expires_at          # header HTTP
            })
    try:
        os.remove(tmp_path)
    except OSError:
        pass
    return object_key

# ——————————————————————————————————————————————————————————————————————————— #
# Get Predial Link
# ——————————————————————————————————————————————————————————————————————————— #
def getPredialLink(year,chip):
    
    def generar_codigo_predial(x):
        hash_sha256 = hashlib.sha256(x.encode()).hexdigest()
        codigo      = hash_sha256[:12]
        return codigo
    
    year     = int(year)
    file_key = generar_codigo_predial(f'{year}{chip}{year}').upper()
    file_key = f"{file_key}.pdf"
    
    ACCESS_KEY  = os.getenv("ACCESS_KEY_iconos")
    SECRET_KEY  = os.getenv("SECRET_KEY_iconos")
    REGION      = os.getenv("REGION_iconos")
    SPACE_NAME  = "prediales"
    
    session = boto3.session.Session()
    client  = session.client('s3',
                            region_name=REGION,
                            endpoint_url=f'https://{REGION}.digitaloceanspaces.com',
                            aws_access_key_id=ACCESS_KEY,
                            aws_secret_access_key=SECRET_KEY)
    try:
        client.get_object(Bucket=SPACE_NAME, Key=file_key)
        link     = f'https://{SPACE_NAME}.{REGION}.digitaloceanspaces.com/{file_key}'
    except: link = None
    return link

# ——————————————————————————————————————————————————————————————————————————— #
# Anonimizacion
# ——————————————————————————————————————————————————————————————————————————— #
def hash_email(email):
    if pd.isna(email) or email == "":
        return ""
    email = email.strip().lower()  # Estandarizar (el hashing es sensible a mayúsculas/minúsculas)
    return hashlib.sha256(email.encode()).hexdigest()

# Función para anonimizar nombres y documentos
def anonymize_text(text):
    if pd.isna(text) or text == "":
        return ""
    words = text.split()
    masked_words = []
    for word in words:
        if len(word) > 3:
            masked_words.append(word[:3] + "*" * (len(word) - 3))
        else:
            masked_words.append(word)  # Si la palabra tiene 3 caracteres o menos, se deja igual
    return " ".join(masked_words)

# Función para anonimizar números de teléfono
def anonymize_phone(phone):
    if pd.isna(phone) or phone == "":
        return ""
    phones = phone.split(" | ")
    masked_phones = [p[:3] + "***" if len(p) > 3 else p for p in phones]
    return " | ".join(masked_phones)

# ——————————————————————————————————————————————————————————————————————————— #
# Funcion auxiliar para compilar la data de storage | parquet
# ——————————————————————————————————————————————————————————————————————————— #
def selectdata(resultado,file,barmanpre=None):
    result = pd.DataFrame()
    for item in resultado:
        if file==item['name']:
            datapaso = item['data']
            result   = pd.concat([result,datapaso])
            
    if not result.empty:
        result  = result.drop_duplicates()
        
    return result