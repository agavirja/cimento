import uuid
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

from data._get_lotes_caracteristicas import main as get_lotes_caracteristicas
from data._get_lotes_construcciones import main as get_lotes_construcciones
from data._get_predial_general import main as get_predial_general
from data._get_snr_general import main as get_snr_general
from data._get_propietarios import main as get_propietarios
from data._get_pot import main as get_pot
from data._get_dane import main as get_dane
from data._get_ctl_licencias import ctl as get_ctl, licencias_construccion as get_licencias
from data._get_market_analysis import main as get_market_analysis
from data._get_market_scacodigo import main as get_market_scacodigo

from functions.getuso_destino import usosuelo_class
from functions.clean_json import clean_json

def main(inputvar={}):

    apis_fase1_config = [
        {'name': 'lotes_caracteristicas', 'func': get_lotes_caracteristicas, 'title': 'Características del lote'},
        {'name': 'lotes_construcciones', 'func': get_lotes_construcciones, 'title': 'Construcciones del lote'},
        {'name': 'prediales', 'func': get_predial_general, 'title': 'Información predial'},
        {'name': 'transacciones', 'func': get_snr_general, 'title': 'Transacciones inmobiliarias'},
        {'name': 'propietarios', 'func': get_propietarios, 'title': 'Datos de propietarios'},
        {'name': 'pot_bogota', 'func': get_pot, 'title': 'Plan de Ordenamiento Territorial'},
        {'name': 'dane', 'func': get_dane, 'title': 'Información demográfica DANE'},
        {'name': 'ctl', 'func': get_ctl, 'title': 'CTL, Catastro, Topografía y Límites'},
        {'name': 'licencias_construccion', 'func': get_licencias, 'title': 'Licencias de construcción'}
    ]
    
    apis_fase2_config = [
        {'name': 'market_analysis', 'func': get_market_analysis, 'title': 'Análisis de mercado'},
        {'name': 'market_statistics', 'func': get_market_scacodigo, 'title': 'Estadísticas del mercado'}
    ]

    max_workers = inputvar.get('max_workers', 3) 
    
    fase1_results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures_fase1 = {api['name']: executor.submit(api['func'], inputvar) for api in apis_fase1_config}
        
        for key, future in futures_fase1.items():
            try:
                fase1_results[key] = future.result(timeout=60)
            except Exception as e:
                fase1_results[key] = {"error": str(e)}

    data_caracteristicas    = fase1_results.get('lotes_caracteristicas', {})
    data_prediales_actuales = fase1_results.get('prediales', {})
    data_transacciones      = fase1_results.get('transacciones', {})

    direccion = None
    sources   = [
        (inputvar, 'direccion'),
        (data_caracteristicas, 'formato_direccion'),
        (data_prediales_actuales, 'predios', 0, 'predirecc'),
        (data_transacciones, 'tabla_transacciones', 0, 'predirecc')
    ]
    
    for source in sources:
        if direccion:
            break
        try:
            if len(source) == 2:
                data, key = source
                if key in data and isinstance(data[key], str) and data[key] != '':
                    direccion = data[key]
            elif len(source) == 4:
                data, key1, idx, key2 = source
                if (key1 in data and isinstance(data[key1], list) and 
                    len(data[key1]) > idx and key2 in data[key1][idx] and
                    isinstance(data[key1][idx][key2], str) and 
                    data[key1][idx][key2] != ''):
                    direccion = data[key1][idx][key2]
        except (KeyError, IndexError, TypeError):
            continue

    lista_precuso = []
    if 'lista_precuso' in data_caracteristicas:
        precuso_raw = data_caracteristicas['lista_precuso']
        if isinstance(precuso_raw, str) and precuso_raw != '':
            lista_precuso = [x.strip() for x in precuso_raw.split('|') if x.strip()]
            
    inputvar.update({'direccion': direccion, 'precuso': lista_precuso})
    tipoinmueble = []
    try:
        datauso = usosuelo_class() # Instancia tu clase de uso de suelo
        if not datauso.empty and lista_precuso:
            # Asegúrate de que 'clasificacion' sea una serie de pandas para usar .replace
            datauso.df['clasificacion'] = datauso.df['clasificacion'].replace(['Comercio', 'Bodegas', 'Industria', 'Oficinas', 'Restaurante'], ['Local', 'Bodega', 'Bodega', 'Oficina', 'Local'])
            
            casa_codes = datauso.df['precuso'].isin(['001', '037'])
            datauso.df.loc[casa_codes, 'clasificacion'] = 'Casa'
            
            apt_codes = datauso.df['precuso'].isin(['002', '038'])
            datauso.df.loc[apt_codes, 'clasificacion'] = 'Apartamento'
            
            datauso_filtered = datauso.df[datauso.df['precuso'].isin(lista_precuso)]
            if not datauso_filtered.empty:
                tipoinmueble = list(datauso_filtered['clasificacion'].unique())
    except:
        pass
    inputvar['tipoinmueble'] = tipoinmueble

    fase2_results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures_fase2 = {api['name']: executor.submit(api['func'], inputvar) for api in apis_fase2_config}
        
        for key, future in futures_fase2.items():
            try:
                fase2_results[key] = future.result(timeout=60)
            except Exception as e:
                fase2_results[key] = {"error": str(e)}

    output = {
        "meta": {
            "timestamp": datetime.now().isoformat(),
            "requestId": str(uuid.uuid4()),
        },
        "data": {
            **fase1_results, 
            **fase2_results
        }
    }
    
    output = clean_json(output)

    return output