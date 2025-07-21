import pandas as pd
import json
from numbers import Number

from functions.general_functions import  get_multiple_data_bucket, generar_codigo, selectdata
from functions.getuso_destino import usosuelo_class
from functions.clean_json import clean_json

def main(inputvar={}):

    # ——————————————————————————————————————————————————————————————————————— #
    # Variables inputvar: 
    # ——————————————————————————————————————————————————————————————————————— #
    barmanpre = inputvar['barmanpre'] if 'barmanpre' in inputvar and isinstance(inputvar['barmanpre'],str) else None
        
        
    # ——————————————————————————————————————————————————————————————————————— #
    # Estructura de datos
    # ——————————————————————————————————————————————————————————————————————— #
    data_predios                    = pd.DataFrame()
    data_caracteristicas            = pd.DataFrame()
    data_tipologias_caracteristicas = pd.DataFrame()
    output                          = default_output(data_caracteristicas)

    if isinstance(barmanpre,str) and barmanpre!='':
        file_configs = [
            ("_caracteristicas/_bogota_caracteristicas_building_manzana", "caracteristicas"),
            ("_caracteristicas/_bogota_caracteristicas_predios_manzana", "predios"),
        ]
        lista   = [x.strip() for x in barmanpre.split('|') if isinstance(x, str)]
        formato = []
        for items in lista:
            manzcodigo = items[:9]
            filename   = generar_codigo(manzcodigo)
            for ruta, name in file_configs:
                formato.append({
                    "file":   f"{ruta}/{filename}.parquet",
                    "name":   name,
                    "barmanpre": items,
                    "data":   pd.DataFrame(),
                    "run": True,
                })
                
        resultado            = get_multiple_data_bucket(formato, barmanpre=None, max_workers=10)
        data_caracteristicas = selectdata(resultado,"caracteristicas", barmanpre=lista)
        data_predios         = selectdata(resultado,"predios", barmanpre=lista)
        
    # ——————————————————————————————————————————————————————————————————————— #
    # Estructura de la API
    # ——————————————————————————————————————————————————————————————————————— #
    if not data_caracteristicas.empty:
        variables = [x for x in ['predios_precuso','predios_precdestin'] if x in data_caracteristicas]
        data_tipologias_caracteristicas = data_caracteristicas[['predios_precuso','predios_precdestin']] if not data_caracteristicas.empty and variables!=[] else pd.DataFrame()
        
        if len(data_caracteristicas) > 1:
            vartipo = {'barmanpre': '|', 'preaconst': 'sum', 'preaterre': 'sum', 'prevetustzmin': 'min', 'prevetustzmax': 'max', 'estrato': 'max', 'predios': 'sum', 'connpisos': 'max', 'connsotano': 'max', 'contsemis': 'max', 'conelevaci': 'max', 'formato_direccion': '|', 'nombre_conjunto': '|', 'prenbarrio': '|', 'precbarrio': '|', 'locnombre': '|', 'preusoph': '|', 'esquinero': 'max', 'viaprincipal': 'max', 'lista_precuso': '|', 'lista_precdestin': '|', 'manzcodigo': '|'}
            data_caracteristicas = compilar_datacaracteristicas(data_caracteristicas, vartipo)

        output = default_output(data_caracteristicas)

    # Procesar tipologías
    tipologia_data = []
    if not data_tipologias_caracteristicas.empty:
        try:
            data_tipologia = pd.DataFrame()
            for _, items in data_tipologias_caracteristicas.iterrows():
                datapaso       = pd.DataFrame.from_dict(json.loads(items['predios_precuso']))
                data_tipologia = pd.concat([data_tipologia, datapaso])
            
            if not data_tipologia.empty:
                data_tipologia         = data_tipologia.groupby('precuso').agg({'predios_precuso': 'sum','preaconst_precuso': 'sum','preaterre_precuso': 'sum'}).reset_index()
                data_tipologia.columns = ['precuso', 'predios_precuso', 'preaconst_precuso', 'preaterre_precuso']
                data_tipologia['preaconst_precusoprop'] = data_tipologia['preaconst_precuso'] / data_tipologia['preaconst_precuso'].sum() * 100

                data_uso_suelo = usosuelo_class()
                if 'precuso' in data_tipologia and not data_uso_suelo.empty:
                    data_uso_suelo = data_uso_suelo.drop_duplicates(subset='precuso')
                    data_tipologia = data_tipologia.merge(data_uso_suelo[['precuso', 'usosuelo']], on='precuso', how='left', validate='m:1')
                else:
                    data_tipologia['usosuelo'] = None
                
                data_tipologia['usosuelo'] = data_tipologia['usosuelo'].apply(lambda x: x if isinstance(x, str) and x != '' else 'Sin información')
                tipologia_data = data_tipologia.to_dict(orient='records')
        except: pass
    output["tipologia"] = tipologia_data
    
    for i in ['lista_precuso','lista_precdestin','preusoph']:
        if i in output and isinstance(output[i],str):
            lista = list(set([x.strip() for x in output[i].split('|') if isinstance(x,str)]))
            output[i] = '|'.join([x.strip() for x in lista])
            
    lista_predios = []
    if not data_predios.empty: 
        lista_predios = data_predios.to_dict(orient='records')
    output['lista_predios'] = lista_predios

    output = clean_json(output)

    return output

def default_output(data_caracteristicas):
    output = {
        "barmanpre": data_caracteristicas['barmanpre'].iloc[0] if 'barmanpre' in data_caracteristicas and isinstance(data_caracteristicas['barmanpre'].iloc[0], str) else "",
        "formato_direccion": data_caracteristicas['formato_direccion'].iloc[0] if 'formato_direccion' in data_caracteristicas and isinstance(data_caracteristicas['formato_direccion'].iloc[0], str) else "",
        "locnombre": data_caracteristicas['locnombre'].iloc[0] if 'locnombre' in data_caracteristicas and isinstance(data_caracteristicas['locnombre'].iloc[0], str) else "",
        "prenbarrio": data_caracteristicas['prenbarrio'].iloc[0] if 'prenbarrio' in data_caracteristicas and isinstance(data_caracteristicas['prenbarrio'].iloc[0], str) else "",
        "precbarrio": data_caracteristicas['precbarrio'].iloc[0] if 'precbarrio' in data_caracteristicas and isinstance(data_caracteristicas['precbarrio'].iloc[0], str) else "",
        "coddir": data_caracteristicas['coddir'].iloc[0] if 'coddir' in data_caracteristicas and isinstance(data_caracteristicas['coddir'].iloc[0], str) else "",
        "manzcodigo": data_caracteristicas['manzcodigo'].iloc[0] if 'manzcodigo' in data_caracteristicas and isinstance(data_caracteristicas['manzcodigo'].iloc[0], str) else "",
        "estrato": int(data_caracteristicas['estrato'].iloc[0]) if 'estrato' in data_caracteristicas and isinstance(data_caracteristicas['estrato'].iloc[0], Number) else 0,
        "nombre_conjunto": data_caracteristicas['nombre_conjunto'].iloc[0] if 'nombre_conjunto' in data_caracteristicas and isinstance(data_caracteristicas['nombre_conjunto'].iloc[0], str) else "",
        "predios": int(data_caracteristicas['predios'].sum()) if 'predios' in data_caracteristicas and isinstance(data_caracteristicas['predios'].sum(), Number) else 0,
        "lista_precuso": data_caracteristicas['lista_precuso'].iloc[0] if 'lista_precuso' in data_caracteristicas and isinstance(data_caracteristicas['lista_precuso'].iloc[0], str) else "",
        "lista_precdestin": data_caracteristicas['lista_precdestin'].iloc[0] if 'lista_precdestin' in data_caracteristicas and isinstance(data_caracteristicas['lista_precdestin'].iloc[0], str) else "",
        "connpisos": int(data_caracteristicas['connpisos'].max()) if 'connpisos' in data_caracteristicas and _is_number(data_caracteristicas['connpisos'].max()) and pd.notna(data_caracteristicas['connpisos'].max()) and isinstance(data_caracteristicas['connpisos'].max(), Number) else 0,
        "connsotano": int(data_caracteristicas['connsotano'].max()) if 'connsotano' in data_caracteristicas and _is_number(data_caracteristicas['connsotano'].max()) and pd.notna(data_caracteristicas['connsotano'].max()) and isinstance(data_caracteristicas['connsotano'].max(), Number) else 0,
        "contsemis": int(data_caracteristicas['contsemis'].sum()) if 'contsemis' in data_caracteristicas and _is_number(data_caracteristicas['contsemis'].sum()) and pd.notna(data_caracteristicas['contsemis'].sum()) and  isinstance(data_caracteristicas['contsemis'].sum(), Number) else 0,
        "conelevaci": int(data_caracteristicas['conelevaci'].max()) if 'conelevaci' in data_caracteristicas and _is_number(data_caracteristicas['conelevaci'].max()) and pd.notna(data_caracteristicas['conelevaci'].max()) and  isinstance(data_caracteristicas['conelevaci'].max(), Number) else 0,
        "preaconst": float(data_caracteristicas['preaconst'].sum()) if 'preaconst' in data_caracteristicas and _is_number(data_caracteristicas['preaconst'].sum()) and pd.notna(data_caracteristicas['preaconst'].sum()) and  isinstance(data_caracteristicas['preaconst'].sum(), Number) else 0.0,
        "preaterre": float(data_caracteristicas['preaterre'].sum()) if 'preaterre' in data_caracteristicas and _is_number(data_caracteristicas['preaterre'].sum()) and pd.notna(data_caracteristicas['preaterre'].sum()) and  isinstance(data_caracteristicas['preaterre'].sum(), Number) else 0.0,
        "prevetustzmin": int(data_caracteristicas['prevetustzmin'].min()) if 'prevetustzmin' in data_caracteristicas and _is_number(data_caracteristicas['prevetustzmin'].min()) and pd.notna(data_caracteristicas['prevetustzmin'].min())  and isinstance(data_caracteristicas['prevetustzmin'].min(), Number) else 0,
        "prevetustzmax": int(data_caracteristicas['prevetustzmax'].max()) if 'prevetustzmax' in data_caracteristicas and _is_number(data_caracteristicas['prevetustzmax'].max()) and pd.notna(data_caracteristicas['prevetustzmax'].max()) and isinstance(data_caracteristicas['prevetustzmax'].max(), Number) else 0,
        "preusoph": data_caracteristicas['preusoph'].iloc[0].replace('S','Si').replace('N','No') if 'preusoph' in data_caracteristicas and isinstance(data_caracteristicas['preusoph'].iloc[0], str) else "No",
        "esquinero": "Si" if 'esquinero' in data_caracteristicas and data_caracteristicas['esquinero'].sum()>0 else "No",
        "viaprincipal": "Si" if 'viaprincipal' in data_caracteristicas and data_caracteristicas['viaprincipal'].sum()>0 else "No",
        "tipologia":[],
    }
    return output
    
def compilar_datacaracteristicas(data, vartipo):
    df             = pd.DataFrame([{'isin':1}])
    data['isin']   = 1
    listavariables = {}
    for key,values in vartipo.items():
        if values=='|':
            datapaso = data.groupby('isin')[key].apply(lambda x: " | ".join(w.strip() for w in x.unique() if isinstance(w, str) and w != "")).reset_index()
            df       = df.merge(datapaso,on='isin',how='left',validate='1:1')
        else: listavariables.update({key:values})
            
    if isinstance(listavariables,dict) and listavariables!={}:
        datapaso = data.groupby('isin').agg(listavariables).reset_index()
        df       = df.merge(datapaso,on='isin',how='left',validate='1:1')
        
    df.drop(columns=['isin'],inplace=True)
    return df

def _is_number(s: str) -> bool:
    try:
        float(s)
        return True
    except ValueError:
        return False