import pandas as pd
import json
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
    data_scacodigo_dane = pd.DataFrame()
    if isinstance(barmanpre,str) and barmanpre!='':

        lista   = [x.strip()[:6] for x in barmanpre.split('|') if isinstance(x, str)]
        lista   = list(set(lista))
        ruta    = "_dane/_bogota_scacodigo_dane"
        formato = []
        for items in lista:
            filename = generar_codigo(items)
            formato.append({
                "file":   f"{ruta}/{filename}.parquet",
                "name":   "dane",
                "barmanpre": None,
                "data":   pd.DataFrame(),
                "run": True,
            })
            
        resultado           = get_multiple_data_bucket(formato, barmanpre=None, max_workers=10)
        data_scacodigo_dane = selectdata(resultado,"dane", barmanpre=lista)

    # ——————————————————————————————————————————————————————————————————————— #
    # Estructura de la API
    # ——————————————————————————————————————————————————————————————————————— #
    
    output = {}
    
    # Iterar por cada fila del dataframe
    for idx, row in data_scacodigo_dane.iterrows():
        if pd.notna(row['dane']):
            try:
                parsed = json.loads(row['dane'])[0]  # Convertir string JSON a dict
                for key, value in parsed.items():
                    number = to_number(value)
                    if number is not None:
                        if key in output:
                            output[key] += number
                        else:
                            output[key] = number
            except:pass
            
    output = transform(output.copy())
    if isinstance(output,dict):
        meta = {
            "timestamp": datetime.now().isoformat(),
            "requestId": str(uuid.uuid4()),
            "filtersApplied": {
                "barmanpre": inputvar.get("barmanpre"),
                "chip": inputvar.get("chip"),
                "matriculaInmobiliaria": inputvar.get("matriculainmobiliaria"),
            }
        } 
        output.update(meta)
        
    
    output = clean_json(output)
    
    return output

def to_number(value):
    try:
        return float(value)
    except:
        return None
    
    

def transform(flat_dict):
    
   
    CATEGORIES = {
        "viviendas": {
            "total": "Total viviendas",
            "porTipo": {
                "casa": "Casa",
                "apartamento": "Apartamento",
                "tipoCuarto": "Tipo cuarto"
            },
            "sinConstruccion": "Lote (Unidad sin construcción)"
        },
        "usoNoResidencial": {
            "unidadNoResidencial": "Unidad no residencial",
            "lugarEspecialAlojamiento": "Lugar especial de alojamiento - LEA",
            "industria": "Industria (uso no residencial)",
            "comercio": "Comercio (uso no residencial)",
            "servicios": "Servicios (uso no residencial)",
            "institucional": "Institucional (uso no residencial)",
            "parqueZonaVerde": "Parque/ Zona Verde (uso no residencial)",
            "enConstruccion": "En Construcción (uso no residencial)",
            "sinInformacion": "Sin información (uso no residencial)"
        },
        "estadoOcupacional": {
            "ocupadaPersonasPresentes": "Ocupada con personas presentes",
            "ocupadaPersonasAusentes": "Ocupada con todas las personas ausentes",
            "temporal": "Vivienda temporal (para vacaciones, trabajo, etc.)",
            "desocupada": "Desocupada"
        },
        "hogares": {
            "total": "Hogares"
        },
        "estratos": {
            "1": "Estrato 1",
            "2": "Estrato 2",
            "3": "Estrato 3",
            "4": "Estrato 4",
            "5": "Estrato 5",
            "6": "Estrato 6",
            "noEspecificado": "No sabe o no tiene estrato"
        },
        "clasificacionLetras": {
            **{letter: letter for letter in list("ABCFDEGHJKL MNOPQ") if letter.strip()}
        },
        "poblacion": {
            "total": "Total personas",
            "hombres": "Hombres",
            "mujeres": "Mujeres"
        },
        "edades": {
            "0_9": "0 a 9 años",
            "10_19": "10 a 19 años",
            "20_29": "20 a 29 años",
            "30_39": "30 a 39 años",
            "40_49": "40 a 49 años",
            "50_59": "50 a 59 años",
            "60_69": "60 a 69 años",
            "70_79": "70 a 79 años",
            "80_mas": "80 años o más"
        },
        "educacion": {
            "ninguno": "Ninguno (Educacion)",
            "sinInformacion": "Sin Información (Educacion)",
            "preescolarPrimaria": "Preescolar - Prejardin, Básica primaria 1 (Educacion)",
            "secundariaMedia": "Básica secundaria 6, Media tecnica 10, Normalista 10 (Educacion)",
            "tecnicaTecnologicaUniversitario": "Técnica profesional 1 año, Tecnológica 1 año, Universitario 1 año (Educacion)",
            "posgrado": "Especialización 1 año, Maestria 1 año, Doctorado 1 año (Educacion)"
        },
        "usoMixto": {
            "total": "Uso mixto",
            "industria": "Industria (uso mixto)",
            "comercio": "Comercio (uso mixto)",
            "servicios": "Servicios (uso mixto)",
            "sinInformacion": "Sin información (uso mixto)"
        }
    }
    
    nested = {}
    for category, mapping in CATEGORIES.items():
        if category not in nested:
            nested[category] = {}
        for key, value in mapping.items():
            if isinstance(value, dict):
                nested[category][key] = {}
                for sub_key, flat_key in value.items():
                    nested[category][key][sub_key] = flat_dict.get(flat_key)
            else:
                nested[category][key] = flat_dict.get(value)
    return nested
