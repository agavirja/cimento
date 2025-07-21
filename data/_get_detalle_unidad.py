import uuid
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

from data._get_predial_general import main as get_predial_general
from data._get_snr_general import main as get_snr_general
from data._get_propietarios import main as get_propietarios
from data._get_ctl_licencias import ctl as get_ctl

from functions.clean_json import clean_json

def main(inputvar={}):

    apis_config = [
        {'name': 'prediales', 'func': get_predial_general, 'title': 'Información predial'},
        {'name': 'transacciones', 'func': get_snr_general, 'title': 'Transacciones inmobiliarias'},
        {'name': 'propietarios', 'func': get_propietarios, 'title': 'Datos de propietarios'},
        {'name': 'ctl', 'func': get_ctl, 'title': 'CTL, Catastro, Topografía y Límites'},
    ]
    
    max_workers   = inputvar.get('max_workers', 3) 
    results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures_fase1 = {api['name']: executor.submit(api['func'], inputvar) for api in apis_config}
        
        for key, future in futures_fase1.items():
            try:
                results[key] = future.result(timeout=60)
            except Exception as e:
                results[key] = {"error": str(e)}
                
    output = {
        "meta": {
            "timestamp": datetime.now().isoformat(),
            "requestId": str(uuid.uuid4()),
        },
        "data": {
            **results, 
        }
    }
    
    output = clean_json(output)

    return output