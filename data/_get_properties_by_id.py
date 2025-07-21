import pandas as pd
import uuid
from datetime import datetime

from functions.general_functions import  get_multiple_data_bucket, generar_codigo, selectdata
from functions.clean_json import clean_json

def main(inputvar={}):

    # ——————————————————————————————————————————————————————————————————————— #
    # Variables inputvar: 
    # ——————————————————————————————————————————————————————————————————————— #
    identificacion = inputvar['identificacion'] if 'identificacion' in inputvar and isinstance(inputvar['identificacion'],str) else None
    
    # ——————————————————————————————————————————————————————————————————————— #
    # Estructura de datos
    # ——————————————————————————————————————————————————————————————————————— #
    data_info    = pd.DataFrame()
    data_predial = pd.DataFrame()
    data_snr     = pd.DataFrame()

    if isinstance(identificacion,str) and identificacion!='':
        file_configs = [
            ("_propietarios/_grupo_propietarios_informacion", "propietarios_info"),
            ("_propietarios/_bogota_propietarios_prediales_grupos", "propietarios_prediales"),
            ("_propietarios/_propietarios_snr_grupos", "propietarios_snr"),
        ]
        lista   = [x.strip() for x in identificacion.split('|') if isinstance(x, str)]
        formato = []
        for items in lista:
            codid    = (int(items) // 10000) * 10
            filename = generar_codigo(str(codid))
            for ruta, name in file_configs:
                formato.append({
                    "file":   f"{ruta}/{filename}.parquet",
                    "name":   name,
                    "barmanpre": None,
                    "data":   pd.DataFrame(),
                    "run": True,
                })
                
        resultado    = get_multiple_data_bucket(formato, barmanpre=None, max_workers=10)
        data_info    = selectdata(resultado,"propietarios_info", barmanpre=None)
        data_predial = selectdata(resultado,"propietarios_prediales", barmanpre=None)
        data_snr     = selectdata(resultado,"propietarios_snr", barmanpre=None)
        
    
    # ——————————————————————————————————————————————————————————————————————— #
    # Estructura de la API
    # ——————————————————————————————————————————————————————————————————————— #
    if not data_info.empty: 
        data_info = data_info[data_info['nroIdentificacion'].isin(lista)]
    
    if not data_predial.empty: 
        data_predial = data_predial[data_predial['identificacion'].isin(lista)]
        
    if not data_snr.empty: 
        data_snr = data_snr[data_snr['nrodocumento'].isin(lista)]
          
        
    meta = {
        "timestamp": datetime.now().isoformat(),
        "requestId": str(uuid.uuid4()),
        "filtersApplied": {
            "identificacion": inputvar.get("identificacion"),
        }
    } 
    
    output = {
        "meta": meta,
        "dataInfo": data_info.to_dict(orient='records'),
        "dataPredial": data_predial.to_dict(orient='records'),
        "dataSNR": data_snr.to_dict(orient='records'),
        
    }
    output = clean_json(output)


    return output