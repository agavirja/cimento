import pandas as pd
import json
import geopandas as gpd
from shapely.geometry import Point
from functions.general_functions import get_multiple_data_bucket
from functions.clean_json import clean_json

def main(inputvar={}):
    # ——————————————————————————————————————————————————————————————————————— #
    # Variables inputvar: 
    # ——————————————————————————————————————————————————————————————————————— #
    codigos    = inputvar['codigos']    if 'codigos'    in inputvar and (isinstance(inputvar['codigos'], list) or isinstance(inputvar['codigos'], str)) else None
    dpto_ccdgo = inputvar['dpto_ccdgo'] if 'dpto_ccdgo' in inputvar and (isinstance(inputvar['dpto_ccdgo'], list) or isinstance(inputvar['dpto_ccdgo'], str)) else None
    mpio_ccdgo = inputvar['mpio_ccdgo'] if 'mpio_ccdgo' in inputvar and (isinstance(inputvar['mpio_ccdgo'], list) or isinstance(inputvar['mpio_ccdgo'], str)) else None
    barrio     = inputvar['barrio']     if 'barrio' in inputvar and (isinstance(inputvar['barrio'], list) or isinstance(inputvar['barrio'], str)) else None
    nombre     = inputvar['nombre']     if 'nombre' in inputvar and (isinstance(inputvar['nombre'], list) or isinstance(inputvar['nombre'], str)) else None

    # ——————————————————————————————————————————————————————————————————————— #
    # Estructura de datos
    # ——————————————————————————————————————————————————————————————————————— #
    data_brand = pd.DataFrame()
   
    if codigos:
        
        if isinstance(codigos,str) and codigos!='':
            codigos = codigos.split('|')

        lista = [x.strip() for x in codigos if isinstance(x, str)]
        formato = []
        for items in lista:
            formato.append({
                "file": f"_brand/_data_by_brand/{items}.parquet",
                "name": "brand",
                "barmanpre": None,
                "data": pd.DataFrame(),
                "run": True,
            })
        resultado = get_multiple_data_bucket(formato, barmanpre=None, max_workers=10)
        data_brand = selectdata(resultado, "brand")
    
    # ——————————————————————————————————————————————————————————————————————— #
    # Estructura de la API
    # ——————————————————————————————————————————————————————————————————————— #
    output = generate_output(data_brand, dpto_ccdgo=dpto_ccdgo, mpio_ccdgo=mpio_ccdgo, barrio=barrio, nombre=nombre)
    output = clean_json(output)
    return output

def generate_output(data_brand, dpto_ccdgo=None, mpio_ccdgo=None, barrio=None, nombre=None):
    if data_brand.empty:
        return {
            "ubicacion": [],
            "numeroLocaciones": 0,
            "numeroBarrios": "Sin información",
            "latitud": None,
            "longitud": None,
            "geopoints": [],
            "data": [],
            "brandDistribution": [],
            "brandColors": {}
        }
    
    output = {}
    
    # Ubicacion
    df                  = data_brand[['dpto_ccdgo','dpto_cnmbr','mpio_ccdgo','mpio_cnmbr','prenbarrio','empresa','nombre']].drop_duplicates(keep='first')
    idd                 = (df['dpto_ccdgo'].isnull()) | (df['dpto_cnmbr'].isnull()) | (df['mpio_ccdgo'].isnull()) | (df['mpio_cnmbr'].isnull())
    df                  = df[~idd]
    output["ubicacion"] = df.to_dict(orient='records')
    
    # Filtros
    if isinstance(dpto_ccdgo,str) and dpto_ccdgo!='':
        dpto_ccdgo = dpto_ccdgo.split('|')
    if isinstance(dpto_ccdgo,list) and dpto_ccdgo!=[] and 'dpto_ccdgo' in data_brand:
        data_brand = data_brand[data_brand['dpto_ccdgo'].isin(dpto_ccdgo)]
    else:
        data_brand = data_brand[data_brand['dpto_ccdgo']=='11'] # Por defecto Bogota
        output["default"] = [{"dpto_ccdgo":"11","mpio_ccdgo":"11001","dpto_cnmbr":"BOGOTÁ, D.C.", "mpio_cnmbr":"BOGOTÁ, D.C."}]
        
    if isinstance(mpio_ccdgo,str) and mpio_ccdgo!='':
        mpio_ccdgo = mpio_ccdgo.split('|')
    if isinstance(mpio_ccdgo,list) and mpio_ccdgo!=[] and 'mpio_ccdgo' in data_brand:
        data_brand = data_brand[data_brand['mpio_ccdgo'].isin(mpio_ccdgo)]
    else:
        data_brand = data_brand[data_brand['mpio_ccdgo']=='11001'] # Por defecto Bogota
        output["default"] = [{"dpto_ccdgo":"11","mpio_ccdgo":"11001","dpto_cnmbr":"BOGOTÁ, D.C.", "mpio_cnmbr":"BOGOTÁ, D.C."}]

    if isinstance(barrio,str) and barrio!='':
        barrio = barrio.split('|')
    if isinstance(barrio,list) and barrio!=[] and 'prenbarrio' in data_brand:
        data_brand = data_brand[data_brand['prenbarrio'].isin(barrio)]

    if isinstance(nombre,str) and nombre!='':
        nombre = nombre.split('|')
    if isinstance(nombre,list) and nombre!=[] and 'nombre' in data_brand:
        data_brand = data_brand[data_brand['nombre'].isin(nombre)]

    # Output
    output["numeroLocaciones"] = len(data_brand)

    if 'prenbarrio' in data_brand.columns:
        barrios_unicos = data_brand[data_brand['prenbarrio'].notnull()]['prenbarrio'].nunique()
        if barrios_unicos > 0:
            output["numeroBarrios"] = barrios_unicos
        else:
            output["numeroBarrios"] = "Sin información"
    else:
        output["numeroBarrios"] = "Sin información"
    
    output["brandColors"]                 = extract_brand_colors(data_brand)
    output["latitud"], output["longitud"] = calculate_map_center(data_brand)
    output["geopoints"]                   = generate_geopoints(data_brand)
    output["data"]                        = generate_table_data(data_brand)
    output["brandDistribution"]           = generate_brand_distribution(data_brand)
    
    return output

def extract_brand_colors(data_brand):
    brand_colors = {}
    if 'empresa' in data_brand.columns and 'marker_color' in data_brand.columns:
        brand_color_map = data_brand[data_brand['empresa'].notnull() &  data_brand['marker_color'].notnull()].drop_duplicates('empresa')
        
        for _, row in brand_color_map.iterrows():
            empresa = row['empresa']
            color = row['marker_color']
            if pd.notna(empresa) and pd.notna(color):
                brand_colors[empresa] = color
    
    return brand_colors

def calculate_map_center(data_brand):
    if 'latitud' in data_brand.columns and 'longitud' in data_brand.columns:
        coords_data = data_brand[(data_brand['latitud'].notnull()) & (data_brand['longitud'].notnull())]
        
        if not coords_data.empty:
            return float(coords_data['latitud'].mean()), float(coords_data['longitud'].mean())
    
    return None, None

def generate_geopoints(data_brand):
    if ('latitud' not in data_brand.columns or 
        'longitud' not in data_brand.columns):
        return []
    

    data_map = data_brand[(data_brand['latitud'].notnull()) & (data_brand['longitud'].notnull())].copy()
    
    if data_map.empty:
        return []
    
    data_map['geometry'] = data_map.apply(lambda x: Point(x['longitud'], x['latitud']), axis=1)
    data_map             = gpd.GeoDataFrame(data_map, geometry='geometry')
    data_map['popup']    = data_map.apply(generate_popup, axis=1)
    map_columns          = ['geometry', 'popup']
    
    if 'marker' in data_map.columns:
        map_columns.append('marker')
    if 'marker_color' in data_map.columns:
        map_columns.append('marker_color')
    if 'empresa' in data_map.columns:
        map_columns.append('empresa')
    
    available_columns = [col for col in map_columns if col in data_map.columns]
    
    return data_map[available_columns].to_json()

def generate_popup(row):
    popup_parts = []

    column_labels = {
        'empresa': 'Empresa',
        'nombre': 'Nombre', 
        'direccion': 'Dirección',
        'prenbarrio': 'Barrio',
        'telefono': 'Teléfono',
        'nit': 'NIT'
    }
    
    for column, label in column_labels.items():
        if column in row.index and pd.notna(row[column]) and str(row[column]).strip():
            popup_parts.append(f"<b>{label}:</b> {row[column]}<br>")
    
    if not popup_parts:
        popup_parts.append("Información no disponible")
    
    popup_content = f"""
    <!DOCTYPE html>
    <html>
    <body>
        <div style="cursor:pointer; display: flex; flex-direction: column; flex: 1; width:250px; font-size: 12px;">
            <h5 style="text-align: center; margin-bottom: 10px;">Detalle de la estación:</h5>
            {''.join(popup_parts)}
        </div>
    </body>
    </html>
    """
    return popup_content.strip()

def generate_table_data(data_brand):

    if data_brand.empty:
        return []
    
    priority_vars  = ['id', 'nombre', 'direccion', 'telefono', 'empresa', 'latitud', 'longitud', 'prenbarrio', 'marker_color', 'dpto_cnmbr', 'mpio_cnmbr']
    available_vars = [var for var in priority_vars if var in data_brand.columns]

    if not available_vars:
        available_vars = list(data_brand.columns)
    
    table_data = data_brand[available_vars].copy()
    table_data = table_data.fillna('')
    db_records = table_data.to_dict(orient='records')
    
    return json.dumps(db_records, ensure_ascii=False)

def generate_brand_distribution(data_brand):

    if data_brand.empty or 'empresa' not in data_brand.columns:
        return []
    
    count_column = 'id' if 'id' in data_brand.columns else data_brand.columns[0]
    brand_counts = data_brand.groupby('empresa').agg({count_column: 'count'}).reset_index()
    brand_counts.columns = ['name', 'count']

    if 'marker_color' in data_brand.columns:
        color_map         = data_brand[data_brand['empresa'].notnull() &  data_brand['marker_color'].notnull()].drop_duplicates('empresa')[['empresa', 'marker_color']]
        color_map.columns = ['name', 'color']
        brand_counts      = brand_counts.merge(color_map, on='name', how='left')
    
    if 'color' not in brand_counts.columns:
        brand_counts['color'] = '#808080'
    
    brand_counts['color'] = brand_counts['color'].fillna('#808080')
    brand_counts          = brand_counts.sort_values('count', ascending=False)
    
    return json.dumps(brand_counts.to_dict(orient='records'), ensure_ascii=False)

def selectdata(resultado, file):
    result = pd.DataFrame()
    for item in resultado:
        if file == item['name']:
            datapaso = item['data']
            result = pd.concat([result, datapaso])
    
    if not result.empty:
        result = result.drop_duplicates()
    
    return result