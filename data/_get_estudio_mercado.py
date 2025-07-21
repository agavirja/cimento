import uuid
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

from data._get_lotes_polygon import main as get_lotes_polygon
from data._get_snr_polygon import main as get_snr_polygon
from data._get_predial_polygon import main as get_predial_polygon
from data._get_market_analysis import main as get_market_analysis
from data._get_lotes_optimos_polygon import main as get_lotes_optimos_polygon

from functions.clean_json import clean_json

def main(inputvar={}):

    segmentacion              = inputvar.get('segmentacion', None)
    necesita_barmanpre_optimo = isinstance(segmentacion, str) and 'poligono' in segmentacion.lower()

    max_workers = inputvar.get('max_workers', 3)
    results     = {} 

    apis_fase1_config = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        apis_fase1_config['lotes_polygon'] = executor.submit(get_lotes_polygon, inputvar.copy())
        if necesita_barmanpre_optimo:
            apis_fase1_config['lista_optima_barmanpre'] = executor.submit(get_lotes_optimos_polygon, inputvar.copy())

        for key, future in apis_fase1_config.items():
            try:
                results[key] = future.result(timeout=60)
            except Exception as e:
                results[key] = {"error": str(e)}

    if necesita_barmanpre_optimo:
        data_barmanpre_optimo = results.get('lista_optima_barmanpre', {})
        try:
            barmanpre_optimo = data_barmanpre_optimo.get('barmanpre', None) if isinstance(data_barmanpre_optimo, dict) else None
            if isinstance(barmanpre_optimo, list) and barmanpre_optimo:
                inputvar['barmanpre'] = '|'.join(barmanpre_optimo)
            elif isinstance(barmanpre_optimo, str) and barmanpre_optimo:
                inputvar['barmanpre'] = barmanpre_optimo
            else:
                inputvar['barmanpre'] = inputvar.get('barmanpre', None)
        except:
            inputvar['barmanpre'] = inputvar.get('barmanpre', None)

    else:
        inputvar['barmanpre'] = inputvar.get('barmanpre', None)


    apis_fase2_config = [
        {'name': 'transacciones_polygon', 'func': get_snr_polygon, 'title': 'Transacciones en área geográfica'},
        {'name': 'prediales_polygon', 'func': get_predial_polygon, 'title': 'Información predial en área geográfica'},
        {'name': 'market_analysis', 'func': get_market_analysis, 'title': 'Análisis de mercado'}
    ]

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures_fase2 = {api['name']: executor.submit(api['func'], inputvar.copy()) for api in apis_fase2_config}
        
        for key, future in futures_fase2.items():
            try:
                results[key] = future.result(timeout=60)
            except Exception as e:
                results[key] = {"error": str(e)}

    output = {
        "meta": {
            "timestamp": datetime.now().isoformat(),
            "requestId": str(uuid.uuid4()),
        },
        "data": results
    }
    
    output = clean_json(output)

    return output