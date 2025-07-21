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
    datapot = pd.DataFrame()
    if isinstance(barmanpre,str) and barmanpre!='':

        lista   = [x.strip() for x in barmanpre.split('|') if isinstance(x, str)]
        ruta    = "_pot/_bogota_pot_manzana"
        formato = []
        for items in lista:
            manzcodigo = items[:9]
            filename   = generar_codigo(manzcodigo)
            formato.append({
                "file":   f"{ruta}/{filename}.parquet",
                "name":   "pot",
                "barmanpre": items,
                "data":   pd.DataFrame(),
                "run": True,
            })
            
        resultado = get_multiple_data_bucket(formato, barmanpre=None, max_workers=10)
        datapot   = selectdata(resultado,"pot", barmanpre=lista)

    alldata = []
    for idx,items in datapot.iterrows():
        output_paso = {}
        for i in list(datapot):
            try: 
                datapaso          = pd.DataFrame.from_dict(json.loads(items[i]))
                output_paso[i] = datapaso.to_dict(orient='records')
            except: pass
        if isinstance(output_paso,dict) and output_paso!={}:
            alldata.append(output_paso)
            
    datacompilada = []
    if isinstance(alldata,list) and alldata!=[]: 
        datacompilada = compile_data(alldata)
             
   
    principal = {
        "tratamiento_urbanistico": {
            "tipo_tratamiento": "No aplica",
            "tipologia": "No aplica",
            "altura_max": "No aplica",
            "acto_admin": "No aplica"
        },
        "area_actividad": {
            "nombre": "No aplica"
        },
        "actuacion_estrategica": {
            "nombre": "No aplica",
            "priorizacion": "No aplica"
        },
        "antejardin": {
            "dimension": "No aplica",
            "descripcion": "No aplica"
        },
        "aeronautica": {
            "elevacion": "No aplica",
            "altura": "No aplica",
            "codigo_upi": "No aplica",
            "nombre_upi": "No aplica",
        }, 
        "upl": {
            "codigo_upl": "No aplica",
            "nombre_upl": "No aplica",
            }
    }
    
    if isinstance(datacompilada,dict):
        # Tratamiento urbanístico
        if 'pot_tratamiento_urbanistico' in datacompilada:
            try:
                if 'nombre_tra' in datacompilada['pot_tratamiento_urbanistico']:
                    principal["tratamiento_urbanistico"]["tipo_tratamiento"] = datacompilada['pot_tratamiento_urbanistico']['nombre_tra']
                
                if 'tipologia' in datacompilada['pot_tratamiento_urbanistico']:
                    principal["tratamiento_urbanistico"]["tipologia"] = datacompilada['pot_tratamiento_urbanistico']['tipologia']
                
                if 'altura_max' in datacompilada['pot_tratamiento_urbanistico']:
                    principal["tratamiento_urbanistico"]["altura_max"] = datacompilada['pot_tratamiento_urbanistico']['altura_max']
                
                if 'numero_act' in datacompilada['pot_tratamiento_urbanistico']:
                    principal["tratamiento_urbanistico"]["acto_admin"] = datacompilada['pot_tratamiento_urbanistico']['acto_admin']
            except: pass
        
        # Área actividad
        if 'pot_area_actividad_pot_bogota_d' in datacompilada:
            try:
                if 'nombre_are' in datacompilada['pot_area_actividad_pot_bogota_d']:
                    principal["area_actividad"]["nombre"] = datacompilada['pot_area_actividad_pot_bogota_d']['nombre_are']
            except: pass
        
        # Actuación estratégica
        if 'pot_actuacion_estrategica' in datacompilada:
            try:
                if 'nombre' in datacompilada['pot_actuacion_estrategica']:
                        principal["actuacion_estrategica"]["nombre"] = datacompilada['pot_actuacion_estrategica']['nombre']
                
                if 'priorizaci' in datacompilada['pot_actuacion_estrategica']:
                        principal["actuacion_estrategica"]["priorizacion"] = datacompilada['pot_actuacion_estrategica']['priorizaci']
            except: pass
        
        # Antejardín
        if 'pot_antejardin' in datacompilada:
            try:                
                if 'dimension' in datacompilada['pot_antejardin']:
                    principal["antejardin"]["dimension"] = datacompilada['pot_antejardin']['dimension']
                
                if 'observacio' in datacompilada['pot_antejardin']:
                    principal["antejardin"]["descripcion"] = datacompilada['pot_antejardin']['observacio']
            except: pass
        
        
        # Elevacion
        if 'pot_area_elevacion_maxima' in datacompilada:
            try:
                if 'elevacion_' in datacompilada['pot_area_elevacion_maxima']:
                    elevacion = datacompilada['pot_area_elevacion_maxima']['elevacion_'].split('|')
                    elevacion = [float(x) for x in elevacion if _is_number(x)] 
                    elevacion = int(max(elevacion)) if isinstance(elevacion,list) and elevacion!=[] else None
                    if elevacion is not None:
                        principal["aeronautica"]["elevacion"] = elevacion
    
    
                if 'elevacion' in datacompilada['pot_area_elevacion_maxima']:
                    elevacion = datacompilada['pot_area_elevacion_maxima']['elevacion'].split('|')
                    elevacion = [float(x) for x in elevacion if _is_number(x)] 
                    elevacion = int(max(elevacion)) if isinstance(elevacion,list) and elevacion!=[] else None
                    if elevacion is not None:
                        principal["aeronautica"]["elevacion"] = elevacion
                        
                
                if 'altura' in datacompilada['pot_area_elevacion_maxima']:
                    altura = datacompilada['pot_area_elevacion_maxima']['altura'].split('|')
                    altura = [float(x) for x in altura if _is_number(x)] 
                    altura = int(max(altura)) if isinstance(altura,list) and altura!=[] else None
                    if altura is not None:
                        principal["aeronautica"]["altura"] = altura

                if 'codigo_upi' in datacompilada['pot_area_elevacion_maxima']:
                    principal["aeronautica"]["codigo_upi"] = datacompilada['pot_area_elevacion_maxima']['codigo_upi']
                        
                if 'nombre_upi' in datacompilada['pot_area_elevacion_maxima']:
                    principal["aeronautica"]["nombre_upi"] = datacompilada['pot_area_elevacion_maxima']['nombre_upi']
            except: pass
        
        # UPL
        if 'pot_unidad_planeamiento_local' in datacompilada:
            try:
                datapaso = pd.DataFrame.from_dict(json.loads(datapot['pot_unidad_planeamiento_local'].iloc[0]))
                
                if 'codigo_upl' in datacompilada['pot_unidad_planeamiento_local']:
                    idd = datapaso['codigo_upl'].apply(lambda x: x if isinstance(x, str) and x != '' else None).notnull()
                    if sum(idd) > 0:
                        principal["upl"]["codigo_upl"] = ' | '.join(datapaso[idd]['codigo_upl'].unique())
                        
                if 'nombre' in datacompilada['pot_unidad_planeamiento_local']:
                    idd = datapaso['nombre'].apply(lambda x: x if isinstance(x, str) and x != '' else None).notnull()
                    if sum(idd) > 0:
                        principal["upl"]["nombre_upl"] = ' | '.join(datapaso[idd]['nombre'].unique())
    
            except: pass
        
    
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
        "allData":alldata,
        'dataCompilada':datacompilada, 
        "principalData": principal,
        
        }
    
    output = clean_json(output)
    
    return output

def compile_data(alldata):

    compilatedData = {}
    
    for entry in alldata:
        for top_level_key, value in entry.items():
            if top_level_key not in compilatedData:
                compilatedData[top_level_key] = {}
            # Caso 1: El valor es una lista de diccionarios
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        for field_name, field_value in item.items():
                            field_value_str = str(field_value) if field_value is not None else ""
                            if field_name not in compilatedData[top_level_key]:
                                compilatedData[top_level_key][field_name] = set()
                            if field_value_str:  # Solo agregar si no está vacío
                                compilatedData[top_level_key][field_name].add(field_value_str)
                    else:
                        item_str = str(item) if item is not None else ""
                        if "_atomic_values" not in compilatedData[top_level_key]:
                            compilatedData[top_level_key]["_atomic_values"] = set()
                        if item_str:
                            compilatedData[top_level_key]["_atomic_values"].add(item_str)
            # Caso 2: El valor es atómico (no es lista)
            else:
                value_str = str(value) if value is not None else ""
                if "_atomic_values" not in compilatedData[top_level_key]:
                    compilatedData[top_level_key]["_atomic_values"] = set()
                if value_str:
                    compilatedData[top_level_key]["_atomic_values"].add(value_str)
    
    for top_level_key in compilatedData:
        for field_name in compilatedData[top_level_key]:
            if isinstance(compilatedData[top_level_key][field_name], set):
                unique_values = sorted(list(compilatedData[top_level_key][field_name]))
                compilatedData[top_level_key][field_name] = "|".join(unique_values)
    
    return compilatedData

def _is_number(s: str) -> bool:
    try:
        float(s)
        return True
    except ValueError:
        return False