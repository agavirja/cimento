import pandas as pd
import geopandas as gpd
import os
import uuid
from shapely import wkt
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine

from functions.get_barmanpre_from_polygon import main as get_barmanpre_from_polygon
from functions.getuso_destino import usosuelo_class
from functions.clean_json import clean_json

load_dotenv()

user     = os.getenv("user")
password = os.getenv("password")
host     = os.getenv("host")
schema   = os.getenv("schema")
port     = os.getenv("port")
    
def main(inputvar={}):
    
    #inputvar = {'barmanpre': '008412025008', 'areamin': 0, 'areamax': 0, 'desde_antiguedad': 0, 'hasta_antiguedad': 2025, 'pisosmin': 0, 'pisosmax': 0, 'estratomin': 0, 'estratomax': 0, 'precuso': [], 'polygon': 'POLYGON ((-74.05657133663108 4.690271440307401, -74.05650279351995 4.68949061251738, -74.05629924683434 4.68873350978464, -74.05596688123727 4.688023136292604, -74.05551579548923 4.687381076380362, -74.0549596956024 4.686826838713447, -74.05431547839048 4.686377263522912, -74.05360271806742 4.686046010923426, -74.05284307149499 4.685843145857601, -74.05205962014988 4.685774832277808, -74.05127616880476 4.685843145857601, -74.05051652223233 4.686046010923426, -74.04980376190927 4.686377263522912, -74.04915954469735 4.686826838713447, -74.04860344481052 4.687381076380362, -74.04815235906248 4.688023136292604, -74.04781999346541 4.68873350978464, -74.0476164467798 4.68949061251738, -74.04754790366867 4.690271440307401, -74.0476164467798 4.6910522680974225, -74.04781999346541 4.691809370830162, -74.04815235906248 4.692519744322198, -74.04860344481052 4.693161804234441, -74.04915954469735 4.6937160419013555, -74.04980376190927 4.69416561709189, -74.05051652223233 4.694496869691377, -74.05127616880476 4.694699734757202, -74.05205962014988 4.694768048336995, -74.05284307149499 4.694699734757202, -74.05360271806742 4.694496869691377, -74.05431547839048 4.69416561709189, -74.0549596956024 4.6937160419013555, -74.05551579548923 4.693161804234441, -74.05596688123727 4.692519744322198, -74.05629924683434 4.691809370830162, -74.05650279351995 4.6910522680974225, -74.05657133663108 4.690271440307401))', 'latitud': 4.690271440307401, 'longitud': -74.05205962014988, 'segmentacion': 'radio'}
    
    polygon         = inputvar.get('polygon',None)
    areamin         = inputvar.get('areamin', 0)
    areamax         = inputvar.get('areamax', 0)
    tipoinmueble    = inputvar.get('tipoinmueble', [])
    antiguedadmin   = inputvar.get('antiguedadmin', 0)
    antiguedadmax   = inputvar.get('antiguedadmax', 0)
    estratomin      = inputvar.get('estratomin', 0)
    estratomax      = inputvar.get('estratomax', 0)
    precuso         = inputvar.get('precuso', [])
    spatialRelation = inputvar.get('spatialRelation','intersects') # intersects | contains 

    if 'tabla' not in inputvar: 
        inputvar['tabla'] = "bogota_data_lotes_fastsearch" # "bogota_data_lotes" | " bogota_data_lotes_fastsearch"
        
    inputvar['getWkt']          = False
    inputvar['spatialRelation'] = spatialRelation
    response = get_barmanpre_from_polygon(inputvar=inputvar)
    try:    
        databarmanpre = pd.DataFrame(response['data'])
        databarmanpre = databarmanpre.drop_duplicates(subset='barmanpre')
    except: databarmanpre = pd.DataFrame()
    
    datapredios  = pd.DataFrame(columns=['barmanpre', 'preaconst', 'preaterre', 'prevetustzmin', 'prevetustzmax', 'estrato', 'predios', 'connpisos', 'connsotano', 'contsemis', 'conelevaci', 'formato_direccion', 'nombre_conjunto', 'prenbarrio', 'precbarrio', 'locnombre', 'preusoph', 'manzcodigo', 'wkt'])
    engine       = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{schema}')

    if not databarmanpre.empty:
        lista            = "','".join(databarmanpre['barmanpre'].astype(str).unique())
        query_conditions = [f"barmanpre IN ('{lista}')"]
        if areamin>0:
            query_conditions.append(f"preaconst >= {areamin}")
        if areamax>0:
            query_conditions.append(f"preaconst <= {areamax}")
        if antiguedadmin>0:
            query_conditions.append(f"prevetustz >= {antiguedadmin}")
        if antiguedadmax>0:
            query_conditions.append(f"prevetustz <= {antiguedadmax}")
        if estratomin>0:
            query_conditions.append(f"estrato >= {estratomin}")
        if estratomax>0:
            query_conditions.append(f"estrato <= {estratomax}")
        if isinstance(precuso, list) and precuso!=[]:
            precuso_lista = "','".join(precuso)
            query_conditions.append(f"precuso IN ('{precuso_lista}')")
    
        query       = " AND ".join(query_conditions)
        datapredios = pd.read_sql_query(f"SELECT barmanpre,preaconst,prevetustz,precuso,precdestin FROM  bigdata.bogota_data_predios WHERE {query}" , engine)
        
        #-----------------------------------------------------------------#
        # Filtros
        if not datapredios.empty and 'precdestin' in datapredios:
            idd         = datapredios['precdestin'].isin(['65','66'])
            datapredios = datapredios[~idd]

        if not datapredios.empty and 'preaconst' in datapredios and areamin>0:
            datapredios = datapredios[datapredios['preaconst']>=areamin]
            
        if not datapredios.empty and 'preaconst' in datapredios and areamax>0:
            datapredios = datapredios[datapredios['preaconst']<=areamax]

        if not datapredios.empty and 'estrato' in datapredios and estratomin>0:
            datapredios = datapredios[datapredios['estrato']>=estratomin]
            
        if not datapredios.empty and 'estrato' in datapredios and estratomax>0:
            datapredios = datapredios[datapredios['estrato']<=estratomax]

        if not datapredios.empty and 'prevetustz' in  datapredios and antiguedadmin>0:
            datapredios = datapredios[datapredios['prevetustz']>=antiguedadmin]
            
        if not datapredios.empty and 'prevetustz' in  datapredios and antiguedadmax>0:
            datapredios = datapredios[datapredios['prevetustz']<=antiguedadmax]
            
        if isinstance(precuso,list) and precuso!=[]:
            datapredios = datapredios[datapredios['precuso'].isin(precuso)]

        if isinstance(tipoinmueble,list) and tipoinmueble!=[]:
            if not any([x for x in tipoinmueble if isinstance(x,str) and 'todo' in x.lower()]):
                datauso = usosuelo_class()
                lista   = list(datauso[datauso['clasificacion'].isin(tipoinmueble)]['precuso'].unique())
                if isinstance(lista,list) and lista!=[] and 'precuso' in datapredios:
                    datapredios = datapredios[datapredios['precuso'].isin(lista)]

        #---------------------------------------------------------------------#
        # Data caracteristicas 
        #---------------------------------------------------------------------#
        databarmanpre = databarmanpre[databarmanpre['barmanpre'].isin(datapredios['barmanpre'])]
        if not databarmanpre.empty:
            barmanpre           = list(map(str, databarmanpre['barmanpre'].unique()))
            lista               = "','".join(barmanpre)
            query               = f" barmanpre IN ('{lista}')"
            databarmanpre       = pd.read_sql_query(f"SELECT barmanpre,ST_AsText(geometry) as wkt  FROM  bigdata.bogota_data_lotes WHERE {query}" , engine)
            datacaracteristicas = pd.read_sql_query(f"SELECT * FROM bigdata.bogota_data_caracteristicas WHERE {query}" , engine)
            if not datacaracteristicas.empty:
                datacaracteristicas = datacaracteristicas.sort_values(by=['barmanpre','preaconst'],ascending=False).drop_duplicates(subset=['barmanpre'],keep='first')
                databarmanpre       = databarmanpre.merge(datacaracteristicas,on=['barmanpre'],how='left',validate='m:1')

        #---------------------------------------------------------------------#
        # Data caracteristicas 
        #---------------------------------------------------------------------#
        if not databarmanpre.empty and 'wkt' in databarmanpre: 
            df             = databarmanpre.copy()
            df['geometry'] = gpd.GeoSeries.from_wkt(df['wkt'])
            df             = gpd.GeoDataFrame(df.drop(columns=['wkt']),geometry='geometry', crs="EPSG:4326")
            
            if spatialRelation=="intersects":
                df = df[df.geometry.intersects(wkt.loads(polygon))]
            elif spatialRelation=="contains":
                df = df[df.geometry.within(wkt.loads(polygon))]
                
            datapredios   = datapredios[datapredios['barmanpre'].isin(df['barmanpre'])]
            databarmanpre = databarmanpre[databarmanpre['barmanpre'].isin(datapredios['barmanpre'])]
            
    engine.dispose()

    output = {
        "meta": {
            "timestamp": datetime.now().isoformat(),
            "requestId": str(uuid.uuid4()),
            "filtersApplied": {
                "barmanpre": inputvar.get("barmanpre"),
                "chip": inputvar.get("chip"),
                "matriculaInmobiliaria": inputvar.get("matriculainmobiliaria")
            }
        },
        "data": [],
        "prediosResumen": {
            "numeroLotes": datapredios['barmanpre'].nunique(),
            "numeroPredios": len(datapredios)
        },
        "prediosPorUso": [],
        "prediosPorDestino": [],
        "estadisticasAreaConstruida": {},
        "estadisticasAnoConstruccion": {}
    }
    
    
    if not datapredios.empty:
    
        # Resumen general
        output['prediosResumen']["numeroLotes"]   = datapredios['barmanpre'].nunique()
        output['prediosResumen']["numeroPredios"] =  len(datapredios)

        # Agrupación por uso
        if 'precuso' in datapredios:
            predios_uso = datapredios.groupby('precuso').agg({'barmanpre': ['nunique', 'count']}).reset_index()
            predios_uso.columns = ['usoPrincipal', 'numeroLotes', 'numeroPredios']
            output["prediosPorUso"] = predios_uso.to_dict('records')
        
        # Agrupación por destino
        if 'precdestin' in datapredios:
            predios_destino = datapredios.groupby('precdestin').agg({'barmanpre': ['nunique', 'count']}).reset_index()
            predios_destino.columns = ['destinoPrincipal', 'numeroLotes', 'numeroPredios']
            output["prediosPorDestino"] = predios_destino.to_dict('records')
        
        # Función para boxplot
        def calcular_estadisticas_boxplot(serie, nombreCampo):
            valores = serie.dropna()
            if len(valores) == 0:
                return None
        
            q1 = valores.quantile(0.25)
            q3 = valores.quantile(0.75)
            iqr = q3 - q1
            yMin = max(0, q1 - 1.5 * iqr)
            yMax = q3 + 1.5 * iqr
        
            return {
                "campo": nombreCampo,
                "min": float(yMin),
                "q1": float(q1),
                "median": float(valores.median()),
                "mean": float(valores.mean()),
                "q3": float(q3),
                "max": float(yMax),
                "count": len(valores),
                "std": float(valores.std())
            }
        
        # Estadísticas de área construida
        if 'preaconst' in datapredios:
            try: output["estadisticasAreaConstruida"] = calcular_estadisticas_boxplot( datapredios['preaconst'],"areaConstruidaM2")
            except:  pass
        
        # Estadísticas de año de construcción
        if 'prevetustz' in datapredios:
            try: output["estadisticasAnoConstruccion"] = calcular_estadisticas_boxplot(datapredios['prevetustz'],"anoConstruccion")
            except: pass
            
        
    if not databarmanpre.empty:
        output['data'] = databarmanpre.to_dict(orient='records')
        
        
    output = clean_json(output)
    
    return output