import pandas as pd
import uuid
import geopandas as gpd
from shapely import wkt
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

from functions.general_functions import  get_multiple_data_bucket, generar_codigo, selectdata
from functions.clean_json import clean_json

def main(inputvar={}):

    # ——————————————————————————————————————————————————————————————————————— #
    # Variables inputvar: 
    # ——————————————————————————————————————————————————————————————————————— #
    barmanpre = inputvar.get('barmanpre',None)
    if isinstance(barmanpre,list) and barmanpre!=[]:
        barmanpre = '|'.join(barmanpre)
        
    poligono      = inputvar.get('polygon', None)
    areamin       = inputvar.get('areamin', 0)
    areamax       = inputvar.get('areamax', 0)
    antiguedadmin = inputvar.get('antiguedadmin', 0)
    antiguedadmax = inputvar.get('antiguedadmax', 0)
    estratomin    = inputvar.get('estratomin', 0)
    estratomax    = inputvar.get('estratomax', 0)
    precuso       = inputvar.get('precuso', [])
    
    # ——————————————————————————————————————————————————————————————————————— #
    # Estructura de datos
    # ——————————————————————————————————————————————————————————————————————— #
    data = pd.DataFrame()
    if isinstance(barmanpre,str) and barmanpre!='':

        lista            = [x.strip() for x in barmanpre.split('|') if isinstance(x, str)]
        lista_manzcodigo = list(set([x[:9] for x in lista]))
        ruta             = "_propietarios/_bogota_propietarios_radio"
        formato          = []
        for items in lista_manzcodigo:
            filename   = generar_codigo(items)
            formato.append({
                "file":   f"{ruta}/{filename}.parquet",
                "name":   "propeitarios_radio",
                "barmanpre": None,
                "data":   pd.DataFrame(),
                "run": True,
            })
            
        resultado = get_multiple_data_bucket(formato, barmanpre=None, max_workers=10)
        data      = selectdata(resultado,"propeitarios_radio", barmanpre=None)

    # ——————————————————————————————————————————————————————————————————————— #
    # Filtros
    # ——————————————————————————————————————————————————————————————————————— #
    if not data.empty:

        if areamin>0 and 'areaconstruida' in data:
            data = data[data['areaconstruida']>=areamin]
        if areamax>0 and 'areaconstruida' in data:
            data = data[data['areaconstruida']<=areamax]

        if isinstance(precuso,list) and precuso!=[] and 'precuso' in data:
            data = data[data['precuso'].isin(precuso)]
            
        # Filtro de poligono
        if poligono is not None:
            if 'latitud' in data and 'longitud' in data:
                from shapely.geometry import Point
                polygon_contains = wkt.loads(poligono)
                data['idx']      = data.apply(lambda row: polygon_contains.contains(Point(row['longitud'], row['latitud'])), axis=1)
                data             = data[data['idx']]
                data.drop(columns=['idx'],inplace=True)

    # ——————————————————————————————————————————————————————————————————————— #
    # Estructura de la API
    # ——————————————————————————————————————————————————————————————————————— #
    numclientes  = 0
    numemails    = 0
    numtelefonos = 0
    numcreditos  = 0
    
    chart_tipo_propietario = []
    chart_propiedades      = []
    chart_valor            = []
    chart_edad             = []
    chart_heatmap          = []
    
    datageometry_output = []
    
    if not data.empty:
        numclientes = len(data['identificacion'].unique()) if 'identificacion' in data else 0
        numcreditos = int(data['idCredito'].sum()) if 'idCredito' in data else 0

        # Edad
        data['edad1'] = data['fechaDocumento'].apply(lambda x: calcular_edad(x))  if not data.empty and 'fechaDocumento' in data else 0
        data['edad2'] = data['fechaDocumentoS'].apply(lambda x: calcular_edad(x)) if not data.empty and 'fechaDocumentoS' in data else 0
        data['edad']  = data.apply(lambda x: max(x['edad1'],x['edad2']),axis=1)
        data.drop(columns=['edad1','edad2'],errors='ignore',inplace=True)
        idd                   = (data['edad']>18) & (data['tipoPropietario'].astype(str).str.lower().str.contains('natural')) if 'tipoPropietario' in data else data['edad']>18
        data.loc[~idd,'edad'] = None
                
        if 'email' in data:
            df          = data.copy()
            df['email'] = df['email'].str.split('|')
            df          = df.explode('email')
            df['email'] = df['email'].str.strip()
            idd         = df['email'].notnull()
            numemails   = len(df[idd]['email'].unique())
        
        if 'telefonos' in data:
            df              = data.copy()
            df['telefonos'] = df['telefonos'].str.split('|')
            df              = df.explode('telefonos')
            df['telefonos'] = df['telefonos'].str.strip()
            idd             = df['telefonos'].apply(lambda x: True if isinstance(x,str) and len(x)>7 and x.strip()[0]=='3' else False)
            numtelefonos    = len(df[idd]['telefonos'].unique())
                
        if 'tipoPropietario' in data:
            df         = data.copy()
            df['id']   = 1
            df         = df.groupby('tipoPropietario')['id'].count().reset_index()
            df.columns = ['label','value']
            df         = df.sort_values(by=['value'],ascending=False)
            chart_tipo_propietario = df.to_dict('records')
        
        if 'conteo_propiedades' in data:
            df                = data.copy()
            idd               = (df['conteo_propiedades'].isnull()) | (df['conteo_propiedades']<1)
            df.loc[idd,'conteo_propiedades'] = 1
            df                = df.groupby('identificacion')['conteo_propiedades'].max().reset_index()
            df.columns        = ['identificacion','propiedades']
            df['id']          = 1
            df                = df.groupby('propiedades')['id'].count().reset_index()
            df['propiedades'] = df['propiedades'].where(df['propiedades']<=5, '6 o más propiedades')
            df                = df.groupby('propiedades')['id'].sum().reset_index()
            df.columns        = ['label','value']
            chart_propiedades = df.to_dict('records')
                    
        if 'sum_avaluo_catastral' in data:
            df                = data.copy()
            bins              = [-float('inf'), 180_000_000, 250_000_000, 500_000_000, 800_000_000, 1_500_000_000, float('inf')]
            labels            = [
                'Menos de 180 millones',
                'Menos de 250 millones',
                'Menos de 500 millones',
                'Menos de 800 millones',
                'Menos de 1500 millones',
                '1500 millones o más'
            ]
            df['rango_valor'] = pd.cut(df['sum_avaluo_catastral'], bins=bins, labels=labels, right=False)
            df['id']          = 1
            df                = df.groupby('rango_valor')['id'].count().reset_index()
            df.columns        = ['label','value']
            chart_valor       = df.to_dict('records')
        
        if 'edad' in data:
            df = data[data['edad'].notnull()]
            if not df.empty:
                edad_stats = {
                    'min'    : float(df['edad'].min()),
                    'q1'     : float(df['edad'].quantile(0.25)),
                    'median' : float(df['edad'].median()),
                    'q3'     : float(df['edad'].quantile(0.75)),
                    'max'    : float(df['edad'].max())
                }
                chart_edad = [edad_stats]
        
        if 'barmanpre' in data:
            df            = data.copy()
            df            = df.drop_duplicates(subset=['barmanpre','identificacion'],keep='first')
            df            = df.groupby(['barmanpre']).agg({'identificacion':'nunique'}).reset_index()
            df.columns    = ['barmanpre','conteo']
            chart_heatmap = df.to_dict('records')
            
        if 'wkt' in data:
            df                   = data[data['wkt'].notnull()].copy()
            df['geometry']       = gpd.GeoSeries.from_wkt(df['wkt'])
            df['predios_count']  = df.get('predios', 1)
            df_grouped           = df.groupby('wkt').agg({'predios_count': 'first'}).reset_index()
            df_grouped.columns   = ['wkt', 'predios']
            datageometry_output  = df_grouped[['wkt', 'predios']].to_dict('records')
    
    latitud  = None
    longitud = None
    if not data.empty and 'latitud' in data and 'longitud' in data:
        latitud  = float(data['latitud'].iloc[0]) if pd.notnull(data['latitud'].iloc[0]) else None
        longitud = float(data['longitud'].iloc[0]) if pd.notnull(data['longitud'].iloc[0]) else None
    
    meta = {
        "timestamp"      : datetime.now().isoformat(),
        "requestId"      : str(uuid.uuid4()),
        "filtersApplied" : {
            "barmanpre"             : inputvar.get("barmanpre"),
            "chip"                  : inputvar.get("chip"),
            "matriculaInmobiliaria" : inputvar.get("matriculainmobiliaria"),
        }
    }
    
    data_export = []
    if not data.empty:
        export_columns = ['tipo','identificacion','nombre','tipoPropietario','email','telefonos',
                         'numero','propiedades','credito','edad','valor','fechaDocumento',
                         'precuso','areamin','areamax','minyear','maxyear']
        
        available_columns = [col for col in export_columns if col in data.columns]
        df_export         = data[available_columns].copy()
        
        df_export   = df_export.to_dict('records')
        data_export = [{k: v for k, v in record.items() if pd.notna(v)} for record in df_export]
    
    output = {
        "meta"       : meta,
        "statistics" : {
            "numClientes"  : numclientes,
            "numEmails"    : numemails,
            "numTelefonos" : numtelefonos,
            "numCreditos"  : numcreditos
        },
        "charts"     : {
            "tipoPropietario" : chart_tipo_propietario,
            "propiedades"     : chart_propiedades,
            "valor"           : chart_valor,
            "edad"            : chart_edad,
            "heatmap"         : chart_heatmap,
        },
        "mapData"    : {
            "latitud"      : latitud,
            "longitud"     : longitud,
            "polygon"      : inputvar.get('polygon', None),
            "datageometry" : datageometry_output
        },
        "data": data_export
    }
    
    output = clean_json(output)

    return output    


def calcular_edad(fecha_doc_str, referencia=None):
    if pd.isna(fecha_doc_str):
        return None
    try:
        fecha_doc        = pd.to_datetime(fecha_doc_str).date()
        fecha_nacimiento = fecha_doc - relativedelta(years=18)
    
        if referencia is None:
            referencia = date.today()
            
        edad = relativedelta(referencia, fecha_nacimiento).years
    except: edad = None
    return edad