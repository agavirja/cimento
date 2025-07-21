import pandas as pd
import requests
import uuid
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

from functions.clean_json import clean_json

def main(inputvar={}):

    # ——————————————————————————————————————————————————————————————————————— #
    # Variables inputvar: 
    # ——————————————————————————————————————————————————————————————————————— #
    identificacion = inputvar['identificacion'] if 'identificacion' in inputvar and isinstance(inputvar['identificacion'],str) else None

    if isinstance(identificacion,str) and identificacion!='':
        identificacion = identificacion.split('|')

    def fetch_data(identificacion):
        response_info = {}
        # Iterar sobre los posibles códigos de documento
        for coddoc in [4, 3, 5, 6, 1, 2, 7]:
            url = "https://oficinavirtual.shd.gov.co/ServiciosRITDQ/resources/contribuyente"
            data = {
                "codTId": coddoc,
                "nroId": f"{identificacion}"
            }
            try:
                response_info = requests.post(url, json=data, verify=False).json()
                if 'codigoError' in response_info and isinstance(response_info['codigoError'], str) and '0' in response_info['codigoError']:
                    break
            except:
                pass

        # Vehículos
        dataresult = pd.DataFrame()
        try:
            url = "https://oficinavirtual.shd.gov.co/ServiciosRITDQ/resources/consultas/vehiculos"
            data = {"idSujeto": response_info['contribuyente']['idSujeto']}
            response_vehiculos = requests.post(url, json=data, verify=False).json()
            dataresult = pd.DataFrame(response_vehiculos['vehiculos'])
            if not dataresult.empty:
                for j in ['carroceria', 'clase', 'linea', 'marca', 'tipoServicio']:
                    dataresult[j] = dataresult[j].apply(lambda x: getitem(x))
                dataresult['identificacion'] = identificacion  # Agregar identificación al DataFrame
        except:
            pass

        return dataresult
    
    with ThreadPoolExecutor() as executor:
        resultados = list(executor.map(fetch_data, identificacion))

    datos_totales = pd.concat(resultados, ignore_index=True)
    
    meta = {
        "timestamp": datetime.now().isoformat(),
        "requestId": str(uuid.uuid4()),
        "filtersApplied": {
            "identificacion": inputvar.get("identificacion"),
        }
    } 
    
    output = {
        "meta": meta,
        "data": datos_totales.to_dict(orient='records')
        
    }
    output = clean_json(output)
    
    return output

def getitem(x):
    try:
        return x['nombre'].strip()
    except:
        return x