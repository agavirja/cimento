import pandas as pd
import uuid
from datetime import datetime

from functions.general_functions import  get_multiple_data_bucket, generar_codigo, selectdata
from functions.clean_json import clean_json

def main(inputvar={}):

    # ——————————————————————————————————————————————————————————————————————— #
    # Variables inputvar: 
    # ——————————————————————————————————————————————————————————————————————— #
    barmanpre             = inputvar['barmanpre'] if 'barmanpre' in inputvar and isinstance(inputvar['barmanpre'],str) else None
    chip                  = inputvar['chip'] if 'chip' in inputvar and isinstance(inputvar['chip'],str) else None
    matriculainmobiliaria = inputvar['matriculainmobiliaria'] if 'matriculainmobiliaria' in inputvar and isinstance(inputvar['matriculainmobiliaria'],str) else None
    tabla_export          = inputvar['get_tabla'] if 'get_tabla' in  inputvar else True

    # ——————————————————————————————————————————————————————————————————————— #
    # Estructura de datos
    # ——————————————————————————————————————————————————————————————————————— #
    data_propietarios = pd.DataFrame()
    if isinstance(barmanpre,str) and barmanpre!='':

        lista   = [x.strip() for x in barmanpre.split('|') if isinstance(x, str)]
        ruta    = "_propietarios/_bogota_propietarios_manzana"
        formato = []
        for items in lista:
            manzcodigo = items[:9]
            filename   = generar_codigo(manzcodigo)
            formato.append({
                "file":   f"{ruta}/{filename}.parquet",
                "name":   "propietarios",
                "barmanpre": items,
                "data":   pd.DataFrame(),
                "run": True,
            })
            
        resultado         = get_multiple_data_bucket(formato, barmanpre=None, max_workers=10)
        data_propietarios = selectdata(resultado,"propietarios", barmanpre=lista)

    # ——————————————————————————————————————————————————————————————————————— #
    # Estructura de la API
    # ——————————————————————————————————————————————————————————————————————— #
    
    tabla_propietarios  = []
    conteo_propietarios = None
    conteo_by_tipo      = []
    
    
        # Filtro por chip:
    if not data_propietarios.empty and isinstance(chip,str) and chip!='' and 'chip' in data_propietarios:
        data_propietarios = data_propietarios[data_propietarios['chip']==chip]

        # Filtro por matriculainmobiliaria:
    if not data_propietarios.empty and isinstance(matriculainmobiliaria,str) and matriculainmobiliaria!='' and 'matriculainmobiliaria' in data_propietarios:
        data_propietarios = data_propietarios[data_propietarios['matriculainmobiliaria']==matriculainmobiliaria]

    if not data_propietarios.empty:

        if tabla_export:
            df                 = data_propietarios.where(pd.notnull(data_propietarios), None)
            tabla_propietarios = df.fillna(value="").to_dict(orient='records')

        # Contar propietarios únicos
        df  = data_propietarios[data_propietarios['identificacion'].notnull()].sort_values(by=['chip', 'year', 'identificacion'], ascending=False).drop_duplicates(subset=['chip', 'identificacion'], keep='first')
        idd = df['precuso'].isin(['048','049','051','098'])
        df  = df[~idd]
        if not df.empty:
            df['isin'] = 1
            data_propietarios_building         = df.groupby(['isin'])['identificacion'].nunique().reset_index()
            data_propietarios_building.columns = ['isin', 'propietarios']
            conteo_propietarios                = int(data_propietarios_building['propietarios'].iloc[0]) if not data_propietarios_building.empty else None
            
            if 'tipoPropietario' in df.columns:
                df             = df.drop_duplicates(subset='identificacion')
                tipo_persona   = df.groupby('tipoPropietario').size().reset_index(name='count')
                conteo_by_tipo = tipo_persona.to_dict(orient='records')

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
        "meta": meta,
        "data": tabla_propietarios,
        "owners": {
            "count":conteo_propietarios,
            "byType":conteo_by_tipo
            } 
    }
    output = clean_json(output)


    return output