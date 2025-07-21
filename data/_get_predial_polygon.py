import pandas as pd
import uuid
import os
import geopandas as gpd
from shapely import wkt
from dotenv import load_dotenv
from sqlalchemy import create_engine
from datetime import datetime

from functions.general_functions import  get_multiple_data_bucket, generar_codigo, selectdata
from functions.clean_json import clean_json

def main(inputvar={}):

    # ——————————————————————————————————————————————————————————————————————— #
    # Variables inputvar: 
    # ——————————————————————————————————————————————————————————————————————— #
    barmanpre             = inputvar['barmanpre'] if 'barmanpre' in inputvar and isinstance(inputvar['barmanpre'],str) else None
    poligono              = inputvar['polygon'] if 'polygon' in inputvar and isinstance(inputvar['polygon'],str) else None

    # ——————————————————————————————————————————————————————————————————————— #
    # Estructura de datos
    # ——————————————————————————————————————————————————————————————————————— #
    data_prediales_actuales = pd.DataFrame()
    if isinstance(barmanpre,str) and barmanpre!='':

        lista            = [x.strip() for x in barmanpre.split('|') if isinstance(x, str)]
        lista_manzcodigo = list(set([x[:9] for x in lista]))
        ruta             = "_prediales/_bogota_prediales_estadisticas_manzana"
        formato          = []
        for items in lista_manzcodigo:
            filename   = generar_codigo(items)
            formato.append({
                "file":   f"{ruta}/{filename}.parquet",
                "name":   "prediales_actuales",
                "barmanpre": None,
                "data":   pd.DataFrame(),
                "run": True,
            })
            
        resultado               = get_multiple_data_bucket(formato, barmanpre=None, max_workers=10)
        data_prediales_actuales = selectdata(resultado,"prediales_actuales", barmanpre=None)
        if not data_prediales_actuales.empty:
            variables               = ['barmanpre', 'year', 'precuso', 'preaconst', 'prevetustz', 'predios', 'estrato']
            data_prediales_actuales = data_prediales_actuales.sort_values(by=variables+['avaluocatastral_mt2'],ascending=False).drop_duplicates(subset=variables,keep='first')
            
             
    # ——————————————————————————————————————————————————————————————————————— #
    # Filtros a la data
    # ——————————————————————————————————————————————————————————————————————— #
    
        # Filtro segun parametros
    if not data_prediales_actuales.empty:
        areamin       = inputvar.get('areamin', 0)
        areamax       = inputvar.get('areamax', 0)
        prevetustzmin = inputvar.get('desde_antiguedad', 0)
        prevetustzmax = inputvar.get('hasta_antiguedad', 0)
        estratomin    = inputvar.get('estratomin', 0)
        estratomax    = inputvar.get('estratomax', 0)
        precuso       = inputvar.get('precuso', [])
    
        if areamin>0 and 'preaconst' in data_prediales_actuales:
            data_prediales_actuales = data_prediales_actuales[data_prediales_actuales['preaconst']>=areamin]
        if areamax>0 and 'preaconst' in data_prediales_actuales:
            data_prediales_actuales = data_prediales_actuales[data_prediales_actuales['preaconst']<=areamax]

        if prevetustzmin>0 and 'prevetustz' in data_prediales_actuales:
            data_prediales_actuales = data_prediales_actuales[data_prediales_actuales['prevetustz']>=prevetustzmin]
        if prevetustzmax>0 and 'prevetustz' in data_prediales_actuales:
            data_prediales_actuales = data_prediales_actuales[data_prediales_actuales['prevetustz']<=prevetustzmax]

        if estratomin>0 and 'estrato' in data_prediales_actuales:
            data_prediales_actuales = data_prediales_actuales[data_prediales_actuales['estrato']>=estratomin]
        if estratomax>0 and 'estrato' in data_prediales_actuales:
            data_prediales_actuales = data_prediales_actuales[data_prediales_actuales['estrato']<=estratomax]

        if isinstance(precuso,list) and precuso!=[] and 'precuso' in data_prediales_actuales:
            data_prediales_actuales = data_prediales_actuales[data_prediales_actuales['precuso'].isin(precuso)]
            
        # Filtro de poligono
    if poligono is not None:
        if 'latitud' in data_prediales_actuales and 'longitud' in data_prediales_actuales:
            from shapely.geometry import Point
            polygon_contains               = wkt.loads(poligono)
            data_prediales_actuales['idx'] = data_prediales_actuales.apply(lambda row: polygon_contains.contains(Point(row['longitud'], row['latitud'])), axis=1)
            data_prediales_actuales        = data_prediales_actuales[data_prediales_actuales['idx']]
            data_prediales_actuales.drop(columns=['idx'],inplace=True)
        else:
            data_prediales_actuales = polygon_filter(data=data_prediales_actuales, polygon=poligono)
    
    # ——————————————————————————————————————————————————————————————————————— #
    # Estructura de la API
    # ——————————————————————————————————————————————————————————————————————— #
    avaluo_catastral      = {"valorMt2": None, "totalAvaluo": None}
    impuesto_predial      = {"valorMt2": None, "totalAvaluo": None}
    suelo                 = {"valorAvaluoSueloMt2": None, "valorPredialSueloMt2": None}
    propietarios          = None
    avaluo_mt2_historico  = []
    predial_mt2_historico = []
    estadisticas_area     = {"min":None,"q1": None,"median": None, "q3": None, "max":None,"mean":None}
        
    if not data_prediales_actuales.empty:
        
        #------------------#
        # Avalúo catastral #        
        df         = data_prediales_actuales[data_prediales_actuales['avaluocatastral_mt2'].notnull()]
        idd        = df['precuso'].isin(['048','049','051','098'])
        df         = df[~idd]
        if not df.empty:
            df['valor_avaluo_mt2'] = df['avaluocatastral_mt2']*df['predios']/df['predios'].sum()
            avaluo_catastral       = {"valorMt2": float(df['valor_avaluo_mt2'].sum()) , "totalAvaluo": float(df['avaluo_catastral_suma'].sum()) }
    
        #------------------#
        # Impuesto predial #
        df         = data_prediales_actuales[data_prediales_actuales['predial_mt2'].notnull()]
        idd        = df['precuso'].isin(['048','049','051','098'])
        df         = df[~idd]
        if not df.empty:
            df['valor_predial_mt2'] = df['predial_mt2']*df['predios']/df['predios'].sum()
            impuesto_predial        = {"valorMt2": float(df['valor_predial_mt2'].sum()) , "totalAvaluo": float(df['impuesto_predial_suma'].sum()) }

        # Estadisticas de avaluo por ano
        df = data_prediales_actuales[data_prediales_actuales['avaluocatastral_mt2'].notnull()]
        if not df.empty:
            idd = df['precuso'].isin(['048','049','051','098'])
            df  = df[~idd]
            if not df.empty:
                df['valor_avaluo_mt2'] = df['avaluocatastral_mt2']*df['predios']
                anos_avaluo            = df.groupby('year').agg({'valor_avaluo_mt2':'sum','predios':'sum'}).reset_index()
                anos_avaluo.columns    = ['year','valor_avaluo_mt2','predios']
                anos_avaluo['valor_avaluo_mt2'] = anos_avaluo['valor_avaluo_mt2']/anos_avaluo['predios']
                anos_avaluo.rename(columns={'valor_avaluo_mt2':'valorMt2'},inplace=True)
                avaluo_mt2_historico = anos_avaluo[['year','valorMt2']].to_dict(orient='records')
                
        # Estadisticas de predial por ano
        df = data_prediales_actuales[data_prediales_actuales['predial_mt2'].notnull()]
        if not df.empty:
            idd = df['precuso'].isin(['048','049','051','098'])
            df  = df[~idd]
            if not df.empty:
                df['valor_predial_mt2'] = df['predial_mt2']*df['predios']
                anos_predial             = df.groupby('year').agg({'valor_predial_mt2':'sum','predios':'sum'}).reset_index()
                anos_predial.columns     = ['year','valor_predial_mt2','predios']
                anos_predial['valor_predial_mt2'] = anos_predial['valor_predial_mt2']/anos_predial['predios']
                anos_predial.rename(columns={'valor_predial_mt2':'valorMt2'},inplace=True)
                predial_mt2_historico = anos_predial[['year','valorMt2']].to_dict(orient='records')

        # Estadisticas por area
        df  = data_prediales_actuales.copy()
        idd = df['precuso'].isin(['048','049','051','098'])
        df  = df[~idd]
        if not df.empty:
            stats_area = {
                "min": float(df['preaconst'].min()) if not df.empty and 'preaconst' in df else None,
                "q1": float(df['preaconst'].quantile(0.25)) if not df.empty and 'preaconst' in df else None,
                "median": float(df['preaconst'].median()) if not df.empty and 'preaconst' in df else None,
                "q3": float(df['preaconst'].quantile(0.75)) if not df.empty and 'preaconst' in df else None,
                "max": float(df['preaconst'].max()) if not df.empty and 'preaconst' in df else None,
                "mean": float(df['preaconst'].mean()) if not df.empty and 'preaconst' in df else None
            }
            
        estadisticas_area = stats_area

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
        "data":[],
        "avaluoCatastral": avaluo_catastral,
        "impuestoPredial":impuesto_predial,
        "suelo":suelo,
        "propietarios":propietarios,
        "avaluoMt2Historico":avaluo_mt2_historico,
        "predialMt2Historico":predial_mt2_historico,
        "estadisticasArea":estadisticas_area
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