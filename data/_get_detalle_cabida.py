import uuid
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

from data._get_lotes_caracteristicas import main as get_lotes_caracteristicas
from data._get_lotes_construcciones import main as get_lotes_construcciones
from data._get_pot import main as get_pot
from data._get_predial_general import main as get_predial_general
from data._get_propietarios import main as get_propietarios
from data._get_snr_polygon import main as get_snr_polygon
from data._get_galeria import main as get_galeria

from functions.clean_json import clean_json

def main(inputvar={}):

    apis_config = [
        {'name': 'lotes_caracteristicas', 'func': get_lotes_caracteristicas, 'title': 'Características del lote'},
        {'name': 'lotes_construcciones', 'func': get_lotes_construcciones, 'title': 'Construcciones del lote'},
        {'name': 'pot_bogota', 'func': get_pot, 'title': 'Plan de Ordenamiento Territorial'},
        {'name': 'prediales', 'func': get_predial_general, 'title': 'Información predial'},
        {'name': 'propietarios', 'func': get_propietarios, 'title': 'Datos de propietarios'},
        {'name': 'transacciones_polygon', 'func': get_snr_polygon, 'title': 'Estadísticas de transacciones'},
        {'name': 'market_proyectos', 'func': get_galeria, 'title': 'Proyectos nuevos en el área'}
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