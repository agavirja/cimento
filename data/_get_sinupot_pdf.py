import requests
import pandas as pd
import os
import urllib.parse
import re
import nest_asyncio
import aiohttp
import asyncio
import ssl
import tempfile
import uuid
import datetime
import boto3
from numbers import Number
from dotenv import load_dotenv
from pypdf import PdfReader, PdfWriter
from sqlalchemy import create_engine 

load_dotenv()

def main(inputvar={}):
    
    # ——————————————————————————————————————————————————————————————————————— #
    # Variables inputvar: 
    # ——————————————————————————————————————————————————————————————————————— #
    chip        = inputvar['chip'] if 'chip' in inputvar and isinstance(inputvar['chip'],str) else None
    arealote    = inputvar['arealote'] if 'arealote' in inputvar and isinstance(inputvar['arealote'],Number) and inputvar['arealote']>0 else 1000
    
    # ——————————————————————————————————————————————————————————————————————— #
    # Estructura de la API
    # ——————————————————————————————————————————————————————————————————————— #

    urlist      = []
    pdf_url     = None
    codigo_lote = None
    
    if isinstance(chip,str) and chip!='':
        chip = chip.split('|')
    if isinstance(chip,list) and chip!=[]:
        codigo_lote = chip2codigolote_sinupot(chip)
    
    # Licencias
    if isinstance(codigo_lote,list) and codigo_lote!=[]:
        for codigo in codigo_lote:
            # Licencias
            params = {
                "f": "json",
                "where": f"CODIGO_LOTE = '{codigo}'",
                "returnGeometry": "false",
                "spatialRel": "esriSpatialRelIntersects",
                "outFields": "NEW_ID_EXPEDIENTE",
                "returnDistinctValues": "true"
            }
            url = "https://sinu.sdp.gov.co/serverp/rest/services/Consultas/MapServer/15/query?" + urllib.parse.urlencode(params)
            try:    r = requests.get(url,verify=False).json()
            except: r = None

            if r is not None:
                if 'features' in r:
                    for j in r['features']:
                        if 'attributes' in j and 'NEW_ID_EXPEDIENTE' in j['attributes']:
                            codigo = j['attributes']['NEW_ID_EXPEDIENTE']
                            urlist.append({'tipo':'expendiente','numero':codigo,'url':f'https://sinu.sdp.gov.co/serverp/rest/services/Reportes/GeneracionReportes/GPServer/GeneracionReportes/execute?f=json&tipo=LIC&codigo={codigo}&generar=true','pdf':None})
    # Reporte [chip]
    if isinstance(chip,list) and chip!=[]:
        for ichip in chip:
            url = f'https://sinu.sdp.gov.co/serverp/rest/services/Reportes/GeneracionReportes/GPServer/GeneracionReportes/execute?f=json&tipo=CONS&codigo={ichip}&generar=true'
            urlist.append({'tipo':'reporte','url':url,'pdf':None})
        
    # Plusvalia:
    if isinstance(chip,list) and chip!=[]:
        for ichip in chip:
            url = f'https://sinu.sdp.gov.co/serverp/rest/services/Reportes/GeneracionReportes/GPServer/GeneracionReportes/execute?f=json&tipo=PLUS&codigo={ichip}&generar=true'
            urlist.append({'tipo':'plusvalia','url':url,'pdf':None})
    
    # Estacion telecomunicacion
    if isinstance(chip,list) and chip!=[]:
        for ichip in chip:
            url = f'https://sinu.sdp.gov.co/serverp/rest/services/Reportes/GeneracionReportes/GPServer/GeneracionReportes/execute?f=json&tipo=ESTE&codigo={ichip}&generar=true'
            urlist.append({'tipo':'telecomunicacion','url':url,'pdf':None})
        
    # Estratificacion
    if isinstance(chip,list) and chip!=[]:
        for ichip in chip:
            codigo_json = {"chip": ichip, "generado": ""}
            codigo_encoded = urllib.parse.quote(str(codigo_json).replace("'", '"'))
            url = f'https://sinu.sdp.gov.co/serverp/rest/services/Reportes/GeneracionReportes/GPServer/GeneracionReportes/execute?f=json&tipo=EXT&codigo={codigo_encoded}&generar=true'
            urlist.append({'tipo':'estratificacion','url':url,'pdf':None})
    
    # Pot 555
    tipoarea = clasificar_area_lote(arealote)
    if isinstance(chip,list) and chip!=[] and isinstance(tipoarea,str) and tipoarea!='':
        formato    = {'UNIFAMILIAR-BIFAMILIAR': '101', 'MULTIFAMILIAR-COLECTIVA': '102', 'HABITACIONALES CON SERVICIOS': '103', 'COMERCIOS Y SERVICIOS BASICOS': '201', 'SERVICIOS DE OFICINAS Y SERVICIOS DE HOSPEDAJE': '202', 'SERVICIOS AL AUTOMOVIL': '203', 'SERVICIOS ESPECIALES': '204', 'SERVICIOS LOGISTICOS': '205', 'PRODUCCION ARTESANAL': '301', 'INDUSTRIA LIVIANA': '302', 'INDUSTRIA MEDIANA': '303', 'INDUSTRIA PESADA': '304', 'DOTACIONAL': '401'}
        for key, value in formato.items():
            for ichip in chip:
                codigo_json    = {"chip": ichip,"tipocodigo": "subuso","codigo": value,"tipo": tipoarea}
                codigo_encoded = urllib.parse.quote(str(codigo_json).replace("'", '"'))
                url            = f'https://sinu.sdp.gov.co/serverp/rest/services/Reportes/GeneracionReportes/GPServer/GeneracionReportes/execute?f=json&tipo=RSPU&codigo={codigo_encoded}&generar=true'
                urlist.append({'tipo':'usosueloPOT555','url':url,'pdf':None})
    
    # POT 190
    if isinstance(chip,list) and chip!=[]:
        for ichip in chip:
            url = f'https://sinu.sdp.gov.co/serverp/rest/services/Reportes/GeneracionReportes/GPServer/GeneracionReportes/execute?f=json&tipo=NURB&codigo={ichip}&generar=true'
            urlist.append({'tipo':'pot190','url':url,'pdf':None})
    
            # reserva vial POT 190
            url = f'https://sinu.sdp.gov.co/serverp/rest/services/Reportes/GeneracionReportes/GPServer/GeneracionReportes/execute?f=json&tipo=REVI&codigo={ichip}&generar=true'
            urlist.append({'tipo':'reservavialpot190','url':url,'pdf':None})
    
            # reserva vial POT 190
            url = f'https://sinu.sdp.gov.co/serverp/rest/services/Reportes/GeneracionReportes/GPServer/GeneracionReportes/execute?f=json&tipo=AME&codigo={ichip}&generar=true'
            urlist.append({'tipo':'zonasamenazapot190','url':url,'pdf':None})

    if isinstance(urlist,list) and urlist!=[]:
        pdf_url = upload_pdf_to_spaces(urlist)

    return {'link':pdf_url}

def upload_pdf_to_spaces(urlist):

    ACCESS_KEY  = os.getenv("ACCESS_KEY_iconos")
    SECRET_KEY  = os.getenv("SECRET_KEY_iconos")
    REGION      = os.getenv("REGION_iconos")
    BUCKET_NAME = os.getenv("BUCKET_NAME_iconos")
    
    # Crear archivo temporal para el PDF
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as temp_pdf:
        temp_pdf_path = temp_pdf.name
    
    try:
        # Generar el PDF combinado
        pdf_generated = generate_combined_pdf(urlist, temp_pdf_path)
        
        if not pdf_generated:
            return None
        
        # Configurar cliente de S3 (DigitalOcean Spaces)
        session = boto3.session.Session()
        client = session.client('s3',
                               region_name=REGION,
                               endpoint_url=f'https://{REGION}.digitaloceanspaces.com',
                               aws_access_key_id=ACCESS_KEY,
                               aws_secret_access_key=SECRET_KEY)
        
        # Generar nombre único y ruta del archivo
        random_filename = f"{uuid.uuid4()}.pdf"
        filename = f"_temp/{random_filename}"
        
        # Fecha de expiración: 1 día
        expiration_date = datetime.datetime.utcnow() + datetime.timedelta(days=1)
        
        # Subir archivo
        with open(temp_pdf_path, 'rb') as f:
            client.upload_fileobj(
                f,
                BUCKET_NAME,
                filename,
                ExtraArgs={
                    'ContentType': 'application/pdf',
                    'ACL': 'public-read',
                    'Expires': expiration_date
                }
            )
        
        # Retornar URL del archivo
        return f'https://{BUCKET_NAME}.{REGION}.digitaloceanspaces.com/{filename}'
        
    except Exception as e:
        print(f"Error al subir PDF: {e}")
        return None
    finally:
        # Limpiar archivo temporal
        if os.path.exists(temp_pdf_path):
            os.remove(temp_pdf_path)

def generate_combined_pdf(urlist, output_path):

    try:
        # Requests paralelo y asíncrono para obtener URLs de PDFs
        nest_asyncio.apply()

        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        async def fetch_pdf(session, url_info):
            try:
                async with session.get(url_info['url'], ssl=ssl_context) as response:
                    r = await response.json()
                    pdf_url = r['results'][0]['value']['url'] if r.get('results') else None
                    url_info['pdf'] = pdf_url
            except:
                url_info['pdf'] = None
            return url_info
        
        async def fetch_all_pdfs(urls):
            async with aiohttp.ClientSession() as session:
                tasks = [fetch_pdf(session, url_info) for url_info in urls]
                return await asyncio.gather(*tasks)
        
        async def main_async():
            await fetch_all_pdfs(urlist)
        
        asyncio.run(main_async())
        
        # Consolidar todos los PDFs en uno solo
        pdf_urls = []
        for i in urlist:
            if 'pdf' in i and isinstance(i['pdf'], str) and i['pdf'] != '':
                pdf_urls.append(i['pdf'])

        if not pdf_urls:
            return False
            
        # Crear PDF combinado
        writer = PdfWriter()
        
        for idx, url in enumerate(pdf_urls):
            try:
                response = requests.get(url, verify=False)
                response.raise_for_status()
                
                with tempfile.NamedTemporaryFile(suffix=f'_temp_{idx}.pdf', delete=False) as temp_individual:
                    temp_individual_path = temp_individual.name
                    temp_individual.write(response.content)
                
                try:
                    reader = PdfReader(temp_individual_path)
                    for page in range(len(reader.pages)):
                        writer.add_page(reader.pages[page])
                except Exception as e:
                    print(f"Error procesando PDF {idx}: {e}")
                finally:
                    if os.path.exists(temp_individual_path):
                        os.remove(temp_individual_path)
            except Exception as e:
                print(f"Error descargando PDF de URL {url}: {e}")
        
        # Guardar PDF final
        with open(output_path, 'wb') as final_pdf:
            writer.write(final_pdf)
            
        return True
        
    except Exception as e:
        print(f"Error generando PDF combinado: {e}")
        return False

def extract_year(titulo):
    match = re.search(r'\b(19|20)\d{2}\b', titulo)
    return int(match.group()) if match else None

def chip2codigolote_sinupot(chip):
    user       = os.getenv("user_bigdata")
    password   = os.getenv("password_bigdata")
    host       = os.getenv("host_bigdata_lectura")
    schema     = os.getenv("schema_bigdata")
    
    engine     = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{schema}')
    codigolote = None
    if isinstance(chip, str) and chip!='':
        datacatastro = pd.read_sql_query(f"SELECT precbarrio,precmanz,precpredio FROM  bigdata.data_bogota_catastro WHERE prechip='{chip}'" , engine)
        if not datacatastro.empty:
            codigolote = f"{datacatastro['precbarrio'].iloc[0]}{datacatastro['precmanz'].iloc[0]}{datacatastro['precpredio'].iloc[0]}"

    if isinstance(chip, list) and chip!=[]:
        lista        = "','".join(chip)
        query        = f" prechip IN ('{lista}')"
        datacatastro = pd.read_sql_query(f"SELECT precbarrio,precmanz,precpredio FROM  bigdata.data_bogota_catastro WHERE {query}" , engine)
        if not datacatastro.empty:
            datacatastro['codigo'] = datacatastro.apply(lambda x: f"{x['precbarrio']}{x['precmanz']}{x['precpredio']}",axis=1)
            codigolote             = list(datacatastro['codigo'].unique())
    engine.dispose()
    return codigolote

def clasificar_area_lote(area):
    if area is None:
        return None
    else:
        if area < 500:
            return 'TIPO 1' # 'Menor a 500 m2'
        elif 500 <= area <= 4000:
            return 'TIPO 2' # 'Entre 500 y 4.000 m2'
        else:
            return 'TIPO 3' # 'Mayor a 4.000 m2'