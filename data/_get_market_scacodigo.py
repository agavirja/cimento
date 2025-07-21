import pandas as pd
import uuid
from datetime import datetime

from functions.general_functions import  get_multiple_data_bucket, generar_codigo, selectdata
from functions.clean_json import clean_json

def main(inputvar={}):

    # ——————————————————————————————————————————————————————————————————————— #
    # Variables inputvar: 
    # ——————————————————————————————————————————————————————————————————————— #
    barmanpre = inputvar['barmanpre'] if 'barmanpre' in inputvar and isinstance(inputvar['barmanpre'],str) else None

    # ——————————————————————————————————————————————————————————————————————— #
    # Estructura de datos
    # ——————————————————————————————————————————————————————————————————————— #
    data_scacodigo_market = pd.DataFrame()
    if isinstance(barmanpre,str) and barmanpre!='':
        lista   = [x.strip()[:6] for x in barmanpre.split('|') if isinstance(x, str)]
        lista   = list(set(lista))
        formato = []
        for items in lista:
            filename = generar_codigo(items)
            ruta     = "_heat_map/_bogota_scacodigo_market"
            formato.append({
                "file":   f"{ruta}/{filename}.parquet",
                "name":   "scacodigo_market",
                "barmanpre": None,
                "data":   pd.DataFrame(),
                "run": True,
            })
        
        resultado             = get_multiple_data_bucket(formato, barmanpre=None, max_workers=10)
        data_scacodigo_market = selectdata(resultado,"scacodigo_market", barmanpre=lista)

    # ——————————————————————————————————————————————————————————————————————— #
    # Estructura de la API
    # ——————————————————————————————————————————————————————————————————————— #
    # Definir el tipo de inmueble de referencia
    tipoinmueble_referencia = None
    if 'tipoinmueble' in inputvar: 
        if isinstance(inputvar['tipoinmueble'],str) and inputvar['tipoinmueble']!='':
            tipoinmueble_referencia = inputvar['tipoinmueble'].split('|')
        elif  isinstance(inputvar['tipoinmueble'],list) and inputvar['tipoinmueble']!=[]:
            tipoinmueble_referencia = inputvar['tipoinmueble']
            
    if tipoinmueble_referencia is None:
        tipoinmueble_referencia = ['Apartamento']  # Valor por defecto
    
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
        "venta": {
            "precio": None,
            "valorizacion": None
        },
        "arriendo": {
            "precio": None,
            "valorizacion": None
        },
        "lista": []
    }
    
    # Lista
    if not data_scacodigo_market.empty: 
        variables       = [x for x in ['tipoinmueble', 'tiponegocio', 'valor_mt2', 'transacciones', 'scanombre'] if x in data_scacodigo_market]
        output['lista'] = data_scacodigo_market[variables].to_dict(orient='records')

    # Datos de venta
    df = data_scacodigo_market[(data_scacodigo_market['tipoinmueble'].isin(tipoinmueble_referencia)) & 
                              (data_scacodigo_market['tiponegocio'] == 'Venta')]
    
    if not df.empty:
        output["venta"]["precio"] = float(df['valor_mt2'].iloc[0]) if 'valor_mt2' in df else None
        output["venta"]["valorizacion"] = df['valorizacion'].iloc[0] if 'valorizacion' in df else None
    
    # Datos de arriendo
    df = data_scacodigo_market[(data_scacodigo_market['tipoinmueble'].isin(tipoinmueble_referencia)) & 
                              (data_scacodigo_market['tiponegocio'] == 'Arriendo')]
    
    if not df.empty:
        output["arriendo"]["precio"] = float(df['valor_mt2'].iloc[0]) if 'valor_mt2' in df else None
        output["arriendo"]["valorizacion"] = df['valorizacion'].iloc[0] if 'valorizacion' in df else None


    output = clean_json(output)
        
    return output