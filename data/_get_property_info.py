import pandas as pd
import uuid
import json
from datetime import datetime

from data._get_listings import bycode, byinput

from functions.clean_json import clean_json

def main(inputvar={}):

    # ——————————————————————————————————————————————————————————————————————— #
    # Variables inputvar: 
    # ——————————————————————————————————————————————————————————————————————— #
    code = inputvar['code'] if 'code' in inputvar and isinstance(inputvar['code'],str) else None

    # ——————————————————————————————————————————————————————————————————————— #
    # Estructura de datos
    # ——————————————————————————————————————————————————————————————————————— #
    data = []
    if isinstance(code,str) and code!='':
        try:    data = bycode(code=code)
        except: 
            try: data = byinput(inputvar=inputvar)
            except: pass
    else:
        try: data = byinput(inputvar=inputvar)
        except: pass
    
    # ——————————————————————————————————————————————————————————————————————— #
    # Estructura de la API
    # ——————————————————————————————————————————————————————————————————————— #    
    
   
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