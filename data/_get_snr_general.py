import pandas as pd
import uuid
from datetime import datetime, timedelta

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
    data_transacciones_building = pd.DataFrame()
    if isinstance(barmanpre,str) and barmanpre!='':

        lista            = [x.strip() for x in barmanpre.split('|') if isinstance(x, str)]
        lista_manzcodigo = list(set([x[:9] for x in lista]))
        ruta             = "_snr/_bogota_snr_manzana"
        formato          = []
        for items in lista_manzcodigo:
            filename   = generar_codigo(items)
            formato.append({
                "file":   f"{ruta}/{filename}.parquet",
                "name":   "snr",
                "barmanpre": None,
                "data":   pd.DataFrame(),
                "run": True,
            })
            
        resultado                   = get_multiple_data_bucket(formato, barmanpre=None, max_workers=10)
        data_transacciones_building = selectdata(resultado,"snr", barmanpre=lista)
        if not data_transacciones_building.empty:
            data_transacciones_building = data_transacciones_building[data_transacciones_building['barmanpre'].isin(lista)]


    # ——————————————————————————————————————————————————————————————————————— #
    # Filtros a la data
    # ——————————————————————————————————————————————————————————————————————— #
    
        # Filtro por chip:
    if not data_transacciones_building.empty and isinstance(chip,str) and chip!='' and 'chip' in data_transacciones_building:
        data_transacciones_building = data_transacciones_building[data_transacciones_building['chip']==chip]

        # Filtro por matriculainmobiliaria:
    if not data_transacciones_building.empty and isinstance(matriculainmobiliaria,str) and matriculainmobiliaria!='' and 'matriculainmobiliaria' in data_transacciones_building:
        data_transacciones_building = data_transacciones_building[data_transacciones_building['matriculainmobiliaria']==matriculainmobiliaria]
        
    if not data_transacciones_building.empty:

        for j in ['cuantia', 'preaconst']:
            if j in data_transacciones_building:
                data_transacciones_building[j] = pd.to_numeric(data_transacciones_building[j], errors='coerce')

        if 'fecha_documento_publico' in data_transacciones_building:
            data_transacciones_building['fecha_documento_publico'] = pd.to_datetime(data_transacciones_building['fecha_documento_publico'])
        
        # Calcular valor por metro cuadrado
        df  = data_transacciones_building.copy()
        idd = df['precuso'].isin(['048', '049', '051', '098']) # Parqueaderos PH y Depósitos
        df  = df[~idd]
        if not df.empty:
            df              = df.groupby(['docid','chip']).agg({'cuantia': 'max','preaconst': 'max'}).reset_index()
            df.columns      = ['docid','chip','cuantia','preaconst']
            df              = df.groupby('docid').agg({'cuantia': 'max','preaconst': 'sum'}).reset_index()
            df.columns      = ['docid', 'cuantiapaso', 'areapaso']
            idd             = (df['cuantiapaso'] > 0) & (df['areapaso'] > 0)
            df['valor_mt2'] = None
            df.loc[idd, 'valor_mt2']    = df.loc[idd, 'cuantiapaso'] / df.loc[idd, 'areapaso']
            data_transacciones_building = data_transacciones_building.merge(df[['docid', 'valor_mt2']], on='docid', how='left', validate='m:1')
        else: 
            data_transacciones_building['valor_mt2'] = None
    
    
    if not data_transacciones_building.empty:
        areamin       = inputvar.get('areamin', 0)
        areamax       = inputvar.get('areamax', 0)
        prevetustzmin = inputvar.get('desde_antiguedad', 0)
        prevetustzmax = inputvar.get('hasta_antiguedad', 0)
        estratomin    = inputvar.get('estratomin', 0)
        estratomax    = inputvar.get('estratomax', 0)
        precuso       = inputvar.get('precuso', [])
    
        if areamin>0 and 'preaconst' in data_transacciones_building:
            data_transacciones_building = data_transacciones_building[data_transacciones_building['preaconst']>=areamin]
        if areamax>0 and 'preaconst' in data_transacciones_building:
            data_transacciones_building = data_transacciones_building[data_transacciones_building['preaconst']<=areamax]

        if prevetustzmin>0 and 'prevetustz' in data_transacciones_building:
            data_transacciones_building = data_transacciones_building[data_transacciones_building['prevetustz']>=prevetustzmin]
        if prevetustzmax>0 and 'prevetustz' in data_transacciones_building:
            data_transacciones_building = data_transacciones_building[data_transacciones_building['prevetustz']<=prevetustzmax]

        if estratomin>0 and 'estrato' in data_transacciones_building:
            data_transacciones_building = data_transacciones_building[data_transacciones_building['estrato']>=estratomin]
        if estratomax>0 and 'estrato' in data_transacciones_building:
            data_transacciones_building = data_transacciones_building[data_transacciones_building['estrato']<=estratomax]

        if isinstance(precuso,list) and precuso!=[] and 'precuso' in data_transacciones_building:
            data_transacciones_building = data_transacciones_building[data_transacciones_building['precuso'].isin(precuso)]
            
            
    # ——————————————————————————————————————————————————————————————————————— #
    # Estructura de la API
    # ——————————————————————————————————————————————————————————————————————— #
    
    tabla_transacciones = []
    summary             = {"ultimoAno": {"valor_promedio_mt2": None,"total_compraventas": None},"historico": {"valor_promedio_mt2": None,"total_compraventas": None}}
    annualData          = {"priceByYear": None,"countByYear": None}
    statistics          = {"min": None,"q1": None,"median": None,"q3": None,"max": None,"mean": None}
    yearlyBreakdown     = {"thisYear": [],"oneYear": [],"threeYears": [],"fiveYears": []}
    
    if not data_transacciones_building.empty:
        
        #---------------------------------------------------------------------#
        # Tabla
        tabla_transacciones = []
        if tabla_export:
            
            data_transacciones_building = data_transacciones_building.sort_values(by=['docid', 'fecha_documento_publico', 'preaconst'], ascending=False)
            if 'link' not in data_transacciones_building:
                data_transacciones_building['link'] = data_transacciones_building['docid'].apply(lambda x: f"https://radicacion.supernotariado.gov.co/app/static/ServletFilesViewer?docId={x}") if 'docid' in data_transacciones_building  else ''
            if 'link' not in data_transacciones_building:
                data_transacciones_building['link'] = ''
                
            for _, row in data_transacciones_building.iterrows():
                item = {
                    'link': row.get('link', ''),
                    "docid": row.get('docid', None),
                    "predirecc": row.get('predirecc', None),
                    "fecha_documento_publico": row.get('fecha_documento_publico').strftime('%Y-%m-%d') if pd.notnull(row.get('fecha_documento_publico', None)) else None,
                    "codigo": row.get('codigo', None),
                    "nombre": row.get('nombre', None),
                    "cuantia": float(row.get('cuantia', 0)) if pd.notnull(row.get('cuantia', None)) else None,
                    "entidad": row.get('entidad', None),
                    "numero_documento_publico": row.get('numero_documento_publico', None),
                    "oficina": row.get('oficina', None),
                    "preaconst": float(row.get('preaconst', 0)) if pd.notnull(row.get('preaconst', None)) else None,
                    "preaterre": float(row.get('preaterre', 0)) if pd.notnull(row.get('preaterre', None)) else None,
                    "chip": row.get('chip', None),
                    "usosuelo": row.get('usosuelo', None),
                    "tipo_documento_publico": row.get('tipo_documento_publico', None),
                    "titular": row.get('titular', None),
                    "telefonos": row.get('telefonos', None),
                    "email": row.get('email', None),
                    "tipo": row.get('tipo', None),
                    "nrodocumento": row.get('nrodocumento', None),
                    'tipoPropietario':row.get('tipoPropietario', None),
                    "valor_mt2": float(row.get('valor_mt2', 0)) if pd.notnull(row.get('valor_mt2', None)) else None
                }
                tabla_transacciones.append(item)
            
            
        # Filtrar transacciones relevantes y excluir parqueaderos/depósitos
        data_transacciones_building = data_transacciones_building[data_transacciones_building['codigo'].isin(['125', '126', '164', '168', '169', '0125', '0126', '0164', '0168', '0169'])]
        idd                         = data_transacciones_building['precuso'].isin(['048', '049', '051', '098'])  # Parqueaderos PH y Depósitos
        data_transacciones_building = data_transacciones_building[~idd]
        data_transacciones_building = data_transacciones_building.sort_values(by=['docid', 'fecha_documento_publico', 'valor_mt2'], ascending=False).drop_duplicates(subset='docid', keep='first')

        # Filtrar último año
        fecha_corte = datetime.now() - timedelta(days=365)
        data_transacciones_building_last_year = data_transacciones_building[data_transacciones_building['fecha_documento_publico'] >= fecha_corte]
        
        #---------------------------------------------------------------------#
        # Estadísticas último año
        summary = {
            "ultimoAno": {"valor_promedio_mt2": float(data_transacciones_building_last_year['valor_mt2'].median()) if not data_transacciones_building_last_year.empty and 'valor_mt2' in data_transacciones_building_last_year else None,"total_compraventas": int(len(data_transacciones_building_last_year)) if not data_transacciones_building_last_year.empty else 0},
            "historico": {"valor_promedio_mt2": float(data_transacciones_building['valor_mt2'].median()) if not data_transacciones_building.empty and 'valor_mt2' in data_transacciones_building else None,"total_compraventas": int(len(data_transacciones_building)) if not data_transacciones_building.empty else 0},
        }
        
        # Datos para gráfica por año
        if 'fecha_documento_publico' in data_transacciones_building:
            data_transacciones_building['year'] = data_transacciones_building['fecha_documento_publico'].dt.year
            years_data = data_transacciones_building[data_transacciones_building['year'] > 2020]

            if not years_data.empty:
                valor_anual  = years_data.groupby('year')['valor_mt2'].median().reset_index()
                conteo_anual = years_data.groupby('year').size().reset_index(name='count')
                annualData   = { "priceByYear": valor_anual.to_dict(orient='records'),"countByYear": conteo_anual.to_dict(orient='records')}
        
        # Estadísticas descriptivas para boxplot
        valores_mt2 = data_transacciones_building['valor_mt2'].dropna()
        if not valores_mt2.empty:
            statistics  = {
                "min": float(valores_mt2.min()),
                "q1": float(valores_mt2.quantile(0.25)),
                "median": float(valores_mt2.median()),
                "q3": float(valores_mt2.quantile(0.75)),
                "max": float(valores_mt2.max()),
                "mean": float(valores_mt2.mean())
            }
            
        # Transacciones por ano de construido del edificio [edificios recientes]
        yearlyBreakdown = {}
        if 'prevetustz' in data_transacciones_building:
            df          = data_transacciones_building[data_transacciones_building['prevetustz']>=datetime.now().year]
            df          = df.groupby(['precuso']).agg({'valor_mt2':['count','min','median','max']}).reset_index()
            df.columns  = ['precuso','obs','valormt2_min','valormt2_median','valormt2_max']
            output_paso = df.to_dict(orient='records')
            yearlyBreakdown['thisYear'] = output_paso
            
            df          = data_transacciones_building[data_transacciones_building['prevetustz']>=(datetime.now().year-1)]
            df          = df.groupby('precuso').agg({'valor_mt2':['count','min','median','max']}).reset_index()
            df.columns  = ['precuso','obs','valormt2_min','valormt2_median','valormt2_max']
            output_paso = df.to_dict(orient='records')
            yearlyBreakdown['oneYear'] = output_paso
            
            df          = data_transacciones_building[data_transacciones_building['prevetustz']>=(datetime.now().year-3)]
            df          = df.groupby(['precuso']).agg({'valor_mt2':['count','min','median','max']}).reset_index()
            df.columns  = ['precuso','obs','valormt2_min','valormt2_median','valormt2_max']
            output_paso = df.to_dict(orient='records')
            yearlyBreakdown['threeYears'] = output_paso
            
            df          = data_transacciones_building[data_transacciones_building['prevetustz']>=(datetime.now().year-5)]
            df          = df.groupby(['precuso']).agg({'valor_mt2':['count','min','median','max']}).reset_index()
            df.columns  = ['precuso','obs','valormt2_min','valormt2_median','valormt2_max']
            output_paso = df.to_dict(orient='records')
            yearlyBreakdown['fiveYears'] = output_paso
                
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
        "transactions": tabla_transacciones,
        "summary": summary,
        "annualData": annualData,
        "statistics": statistics,
        "yearlyBreakdown": yearlyBreakdown,
        "barmanpreMark": {
            "lastYearMark":[],
            "allMark":[],
            }
    }
    
    output = clean_json(output)

    return output