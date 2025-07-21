import pandas as pd
import uuid
import os
import geopandas as gpd
from shapely import wkt
from dotenv import load_dotenv
from sqlalchemy import create_engine
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
    poligono              = inputvar['polygon'] if 'polygon' in inputvar and isinstance(inputvar['polygon'],str) else None

    # ——————————————————————————————————————————————————————————————————————— #
    # Estructura de datos
    # ——————————————————————————————————————————————————————————————————————— #
    data_transacciones = pd.DataFrame()
    if isinstance(barmanpre,str) and barmanpre!='':

        lista            = [x.strip() for x in barmanpre.split('|') if isinstance(x, str)]
        lista_manzcodigo = list(set([x[:9] for x in lista]))
        ruta             = "_snr/_bogota_snr_estadisticas_manzana"
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
            
        resultado          = get_multiple_data_bucket(formato, barmanpre=None, max_workers=10)
        data_transacciones = selectdata(resultado,"snr", barmanpre=None)
       
        if not data_transacciones.empty:
            data_transacciones = data_transacciones.drop_duplicates()
    
    # ——————————————————————————————————————————————————————————————————————— #
    # Filtros a la data
    # ——————————————————————————————————————————————————————————————————————— #
    
        # Filtro por chip:
    if not data_transacciones.empty and isinstance(chip,str) and chip!='' and 'chip' in data_transacciones:
        data_transacciones = data_transacciones[data_transacciones['chip']==chip]

        # Filtro por matriculainmobiliaria:
    if not data_transacciones.empty and isinstance(matriculainmobiliaria,str) and matriculainmobiliaria!='' and 'matriculainmobiliaria' in data_transacciones:
        data_transacciones = data_transacciones[data_transacciones['matriculainmobiliaria']==matriculainmobiliaria]
        
        # Filtros
    if not data_transacciones.empty:
        areamin       = inputvar.get('areamin', 0)
        areamax       = inputvar.get('areamax', 0)
        prevetustzmin = inputvar.get('desde_antiguedad', 0)
        prevetustzmax = inputvar.get('hasta_antiguedad', 0)
        estratomin    = inputvar.get('estratomin', 0)
        estratomax    = inputvar.get('estratomax', 0)
        precuso       = inputvar.get('precuso', [])
    
        if areamin>0 and 'preaconst' in data_transacciones:
            data_transacciones = data_transacciones[data_transacciones['preaconst']>=areamin]
        if areamax>0 and 'preaconst' in data_transacciones:
            data_transacciones = data_transacciones[data_transacciones['preaconst']<=areamax]

        if prevetustzmin>0 and 'prevetustz' in data_transacciones:
            data_transacciones = data_transacciones[data_transacciones['prevetustz']>=prevetustzmin]
        if prevetustzmax>0 and 'prevetustz' in data_transacciones:
            data_transacciones = data_transacciones[data_transacciones['prevetustz']<=prevetustzmax]

        if estratomin>0 and 'estrato' in data_transacciones:
            data_transacciones = data_transacciones[data_transacciones['estrato']>=estratomin]
        if estratomax>0 and 'estrato' in data_transacciones:
            data_transacciones = data_transacciones[data_transacciones['estrato']<=estratomax]

        if isinstance(precuso,list) and precuso!=[] and 'precuso' in data_transacciones:
            data_transacciones = data_transacciones[data_transacciones['precuso'].isin(precuso)]
            
        # Filtro de poligono
    if poligono is not None:
        
        if 'latitud' in data_transacciones and 'longitud' in data_transacciones:
            from shapely.geometry import Point
            polygon_contains          = wkt.loads(poligono)
            data_transacciones['idx'] = data_transacciones.apply(lambda row: polygon_contains.contains(Point(row['longitud'], row['latitud'])), axis=1)
            data_transacciones        = data_transacciones[data_transacciones['idx']]
            data_transacciones.drop(columns=['idx'],inplace=True)
        else:
            data_transacciones = polygon_filter(data=data_transacciones, polygon=poligono)
    
    # ——————————————————————————————————————————————————————————————————————— #
    # Estructura de la API
    # ——————————————————————————————————————————————————————————————————————— #
    
    summary                  = {"ultimoAno": {"valor_promedio_mt2": None,"total_compraventas": None},"historico": {"valor_promedio_mt2": None,"total_compraventas": None}}
    annualData               = {"priceByYear": None,"countByYear": None}
    statistics               = {"min": None,"q1": None,"median": None,"q3": None,"max": None,"mean": None}
    yearlyBreakdown          = {"thisYear": [],"oneYear": [],"threeYears": [],"fiveYears": []}
    barmanpre_last_year_mark = []
    barmanpre_all_mark       = []
    
    if not data_transacciones.empty:
        
        data_transacciones['valor_mt2'] = None
        idd                             = (data_transacciones['cuantia']>0) & (data_transacciones['preaconst']>0)
        data_transacciones.loc[idd,'valor_mt2'] = data_transacciones.loc[idd,'cuantia']/data_transacciones.loc[idd,'preaconst']
      
        # Filtrar transacciones relevantes y excluir parqueaderos/depósitos
        data_transacciones = data_transacciones[data_transacciones['codigo'].isin(['125', '126', '164', '168', '169', '0125', '0126', '0164', '0168', '0169'])]
        idd                = data_transacciones['precuso'].isin(['048', '049', '051', '098'])  # Parqueaderos PH y Depósitos
        data_transacciones = data_transacciones[~idd]
        data_transacciones = data_transacciones.sort_values(by=['docid', 'year', 'valor_mt2'], ascending=False).drop_duplicates(subset='docid', keep='first')

        # Filtrar último año
        data_transacciones_last_year = pd.DataFrame(columns=['docid', 'barmanpre', 'chip', 'codigo', 'fecha_documento_publico', 'year', 'precuso', 'preaconst', 'preaterre', 'cuantia', 'valor_mt2', 'valormt2_terreno', 'latitud', 'longitud'])
        if 'fecha_documento_publico' in data_transacciones:
            fecha_limite = datetime.now() - timedelta(days=365)
            data_transacciones['fecha_documento_publico'] = pd.to_datetime(data_transacciones['fecha_documento_publico'])
            data_transacciones_last_year = data_transacciones[(data_transacciones['fecha_documento_publico']>=fecha_limite) & (data_transacciones['fecha_documento_publico']<=datetime.now())]
        elif 'year' in data_transacciones:
            data_transacciones_last_year = data_transacciones[data_transacciones['year'] >= datetime.now().year]
        
        #---------------------------------------------------------------------#
        # Marcar predios con transacciones
        if not data_transacciones_last_year.empty:
            df          = data_transacciones_last_year.groupby('barmanpre').agg({'valor_mt2':['count','median']}).reset_index()
            df.columns  = ['barmanpre','transacciones','valor_mt2']
            barmanpre_last_year_mark = df.to_dict(orient='records')
            
        if not data_transacciones.empty:
            df          = data_transacciones.groupby('barmanpre').agg({'valor_mt2':['count','median']}).reset_index()
            df.columns  = ['barmanpre','transacciones','valor_mt2']
            barmanpre_all_mark = df.to_dict(orient='records')
        
        #---------------------------------------------------------------------#
        # Estadísticas último año
        summary = {
            "ultimoAno": {"valor_promedio_mt2": float(data_transacciones_last_year['valor_mt2'].median()) if not data_transacciones_last_year.empty and 'valor_mt2' in data_transacciones_last_year else None,"total_compraventas": int(len(data_transacciones_last_year)) if not data_transacciones_last_year.empty else 0},
            "historico": {"valor_promedio_mt2": float(data_transacciones['valor_mt2'].median()) if not data_transacciones.empty and 'valor_mt2' in data_transacciones else None,"total_compraventas": int(len(data_transacciones)) if not data_transacciones.empty else 0},
        }
        
        # Datos para gráfica por año
        if 'year' in data_transacciones:
            years_data = data_transacciones[data_transacciones['year'] > 2020]

            if not years_data.empty:
                valor_anual  = years_data.groupby('year')['valor_mt2'].median().reset_index()
                conteo_anual = years_data.groupby('year').size().reset_index(name='count')
                annualData   = { "priceByYear": valor_anual.to_dict(orient='records'),"countByYear": conteo_anual.to_dict(orient='records')}
        
        # Estadísticas descriptivas para boxplot
        valores_mt2 = data_transacciones['valor_mt2'].dropna()
        if not valores_mt2.empty:
            statistics = {
                "min": float(valores_mt2.min()),
                "q1": float(valores_mt2.quantile(0.25)),
                "median": float(valores_mt2.median()),
                "q3": float(valores_mt2.quantile(0.75)),
                "max": float(valores_mt2.max()),
                "mean": float(valores_mt2.mean())
            }
            
        # Transacciones por ano de construido del edificio [edificios recientes]
        if 'preveustz' not in data_transacciones:
            try: data_transacciones['prevetustz'] = data_transacciones['prevetustzmin'].copy()
            except: pass
        if 'preveustz' not in data_transacciones:
            try: data_transacciones['prevetustz'] = data_transacciones['prevetustzmax'].copy()
            except: pass
        if 'preveustz' not in data_transacciones:
            data_transacciones = get_prevetustz(data=data_transacciones.copy())
            
        yearlyBreakdown = {}
        if 'prevetustz' in data_transacciones:
            df          = data_transacciones[data_transacciones['prevetustz']>=datetime.now().year]
            df          = df.groupby(['precuso']).agg({'valor_mt2':['count','min','median','max']}).reset_index()
            df.columns  = ['precuso','obs','valormt2_min','valormt2_median','valormt2_max']
            output_paso = df.to_dict(orient='records')
            yearlyBreakdown['thisYear'] = output_paso
            
            df          = data_transacciones[data_transacciones['prevetustz']>=(datetime.now().year-1)]
            df          = df.groupby('precuso').agg({'valor_mt2':['count','min','median','max']}).reset_index()
            df.columns  = ['precuso','obs','valormt2_min','valormt2_median','valormt2_max']
            output_paso = df.to_dict(orient='records')
            yearlyBreakdown['oneYear'] = output_paso
            
            df          = data_transacciones[data_transacciones['prevetustz']>=(datetime.now().year-3)]
            df          = df.groupby(['precuso']).agg({'valor_mt2':['count','min','median','max']}).reset_index()
            df.columns  = ['precuso','obs','valormt2_min','valormt2_median','valormt2_max']
            output_paso = df.to_dict(orient='records')
            yearlyBreakdown['threeYears'] = output_paso
            
            df          = data_transacciones[data_transacciones['prevetustz']>=(datetime.now().year-5)]
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
        "transactions": [],
        "summary": summary,
        "annualData": annualData,
        "statistics": statistics,
        "yearlyBreakdown": yearlyBreakdown,
        "barmanpreMark": {
            "lastYearMark":barmanpre_last_year_mark,
            "allMark":barmanpre_all_mark,
            }
    }
    
    output = clean_json(output)


    return output


def polygon_filter(data=pd.DataFrame(), polygon=None):
    
    load_dotenv()
    
    user     = os.getenv("user")
    password = os.getenv("password")
    host     = os.getenv("host")
    schema   = os.getenv("schema")
    port     = os.getenv("port")

    if not data.empty:
        
        lista       = "','".join(data['barmanpre'].astype(str).unique())
        query       = f" barmanpre IN ('{lista}')"
        engine      = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{schema}')
        datafilter  = pd.read_sql_query(f"SELECT barmanpre,ST_AsText(geometry) as wkt FROM  bigdata.bogota_data_lotes_fastsearch WHERE {query} ", engine)
        engine.dispose()
    
        if not datafilter.empty:
            datafilter['geometry'] = gpd.GeoSeries.from_wkt(datafilter['wkt'])
            datafilter             = gpd.GeoDataFrame(datafilter.drop(columns=['wkt']),geometry='geometry', crs="EPSG:4326")
            datafilter             = datafilter[datafilter.geometry.within(wkt.loads(polygon))]
            
            if not datafilter.empty:
                data = data[data['barmanpre'].isin(datafilter['barmanpre'])]
        
    return data

def get_prevetustz(data=pd.DataFrame()):
    
    load_dotenv()
    
    user     = os.getenv("user")
    password = os.getenv("password")
    host     = os.getenv("host")
    schema   = os.getenv("schema")
    port     = os.getenv("port")

    if not data.empty and 'barmanpre' in data:
        lista     = "','".join(data['barmanpre'].astype(str).unique())
        query     = f" barmanpre IN ('{lista}')"
        engine    = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{schema}')
        datamerge = pd.read_sql_query(f"SELECT barmanpre, prevetustzmin as prevetustz FROM  bigdata.bogota_data_caracteristicas WHERE {query} ", engine)
        engine.dispose()
    
        if not datamerge.empty:
            datamerge = datamerge.sort_values(by=['barmanpre','prevetustz'],ascending=False).drop_duplicates(subset='barmanpre',keep='first')
            data      = data.merge(datamerge,on='barmanpre',how='left',validate='m:1')

    return data