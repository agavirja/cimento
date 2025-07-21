import pandas as pd
import json
import numpy as np 
import re
import matplotlib.colors as mcolors
import matplotlib.cm as cm
from datetime import datetime

from functions.general_functions import get_data_bucket, upload_excel_to_spaces
from functions.clean_json import clean_json


def main(inputvar={}):
    df            = get_data_bucket('_vehiculos_placas/_placas_fontanar_test.parquet')
    databarrios   = get_data_bucket('_vehiculos_placas/_barrios.parquet')
    datalocalidad = get_data_bucket('_vehiculos_placas/_localidad.parquet')
    
    segmentacion      = inputvar.get('segmentacion', 'Localidad')
    dia_semana        = inputvar.get('dia_semana', 'Todos')
    franja_horaria    = inputvar.get('franja_horaria', 'Todos')
    edad_min          = inputvar.get('edad_min', 0)
    edad_max          = inputvar.get('edad_max', 120)
    vehiculo_min      = inputvar.get('vehiculo_min', 0)
    vehiculo_max      = inputvar.get('vehiculo_max', 1000000000)
    prop_min          = inputvar.get('prop_min', 0)
    prop_max          = inputvar.get('prop_max', 1000000000)
    tiene_propiedades = inputvar.get('tiene_propiedades', False)
    barrios           = inputvar.get('barrios', [])

    df['fecha_consulta'] = pd.to_datetime(df['fecha_consulta'], errors='coerce')
    df['dia_semana']     = df['fecha_consulta'].dt.day_name()
    df['hora']           = df['fecha_consulta'].dt.hour
    df['franja_horaria'] = df['hora'].apply(lambda x: 'Mañana' if 6 <= x < 12 else 'Tarde' if 12 <= x < 18 else 'Noche' if not pd.isna(x) else 'Sin datos')
    
    if dia_semana != 'Todos':
        df = df[df['dia_semana'] == dia_semana]
    if franja_horaria != 'Todos':
        df = df[df['franja_horaria'] == franja_horaria]
    if edad_min > 0 or edad_max < 120:
        df = df[(df['edad'].notna()) & (df['edad'] >= edad_min) & (df['edad'] <= edad_max)]
    if vehiculo_min > 0 or vehiculo_max < 1000000000:
        df = df[(df['avaluo'].notna()) & (df['avaluo'] >= vehiculo_min) & (df['avaluo'] <= vehiculo_max)]
    if prop_min > 0 or prop_max < 1000000000:
        df = df[(df['avaluocatastral'].notna()) & (df['avaluocatastral'] >= prop_min) & (df['avaluocatastral'] <= prop_max)]
    if tiene_propiedades:
        df = df[df['numprop'] > 0]
    if barrios:
        df = df[df['direccion_notificacion'].str.contains('|'.join(barrios), case=False, na=False)]

    df_deduplicated    = df.sort_values(by=['placa', 'numID', 'anio'], ascending=False).drop_duplicates(subset=['placa', 'numID'], keep='first')
    total_personas     = df_deduplicated['numID'].nunique()
    total_vehiculos    = df_deduplicated['placa'].nunique()
    promedio_vehiculos = df_deduplicated.groupby('numID')['placa'].nunique().mean()
    promedio_edad      = df_deduplicated[df_deduplicated['edad'].between(18, 90)]['edad'].mean()

    dfprop       = df[df['chip'].notnull()]
    dfprop       = dfprop.drop_duplicates(subset='numID',keep='first')
    totalConProp = len(dfprop)
    
    labels = [
        {"label": "Total placas", "value": 16692},     
        {"label": "Placas con información del vehículo", "value": total_vehiculos},
        {"label": "Información de contacto", "value": total_personas},
        {"label": "Información de propiedades", "value": totalConProp},
        #{"label": "Promedio vehículos por persona", "value": round(promedio_vehiculos, 2)},
        #{"label": "Edad promedio", "value": round(promedio_edad, 1) if not pd.isna(promedio_edad) else 0}
    ]
        
    dftomap = df.drop_duplicates(subset=['numID'],keep='first')
    dftomap = dftomap[dftomap['loccodigo'].notnull()]
    dftomap = dftomap[['numID','loccodigo']]
    dftomap = dftomap.explode('loccodigo').reset_index(drop=True)
    dftomap = dftomap.groupby(['loccodigo'])['numID'].count().reset_index()
    dftomap.columns = ['loccodigo','conteo']
    if 'conteo' in datalocalidad: del datalocalidad['conteo']
    datalocalidad = datalocalidad.merge(dftomap,on='loccodigo',how='left',validate='1:1')
    datalocalidad = asignar_colores(datalocalidad)

    if segmentacion!='Localidad':
        dftomap = df.drop_duplicates(subset=['numID'],keep='first')
        dftomap = dftomap[dftomap['scacodigo'].notnull()]
        dftomap = dftomap[['numID','scacodigo']]
        dftomap = dftomap.explode('scacodigo').reset_index(drop=True)
        dftomap = dftomap.groupby(['scacodigo'])['numID'].count().reset_index()
        dftomap.columns = ['scacodigo','conteo']
        if 'conteo' in databarrios: del databarrios['conteo']
        databarrios = databarrios.merge(dftomap,on='scacodigo',how='left',validate='1:1')
        databarrios = asignar_colores(databarrios)

    datageometry_source = datalocalidad if segmentacion == 'Localidad' else databarrios
    datageometry        = []
    if not datageometry_source.empty and 'wkt' in datageometry_source.columns:
        features = [create_geojson_feature(row, segmentacion) for _, row in datageometry_source.iterrows()]
        features = [f for f in features if f]
        if features:
            datageometry = {"type": "FeatureCollection", "features": features}
    
    datalocalidad_processed = datalocalidad.sort_values('conteo', ascending=False).to_dict(orient='records') if not datalocalidad.empty else []
    
    # Exportar excel
    url = None
    try:
        variables  = ['placa', 'tipoID', 'nombre', 'numID', 'calidad', 'procProp', 'fechaDesde', 'fechaHasta', 'anio', 'avaluo', 'capacidadCarga', 'carroceria', 'clase', 'linea', 'marca', 'modelo', 'porcentajeRespon', 'responsable', 'tipoServicio', 'impuesto_a_cargo', 'cilindraje', 'url', 'direccion_notificacion', 'telefonos', 'email', 'propietario', 'edad', 'numprop', 'avaluocatastral', 'estrato']
        variables  = [x for x in variables if x in df]
        dataexport = df[variables]
        url        = upload_excel_to_spaces(dataexport, '_temp')
    except: pass

    output = {
        "meta": {
            "timestamp": datetime.now().isoformat(),
            "totalRegistros": len(df_deduplicated)
        },
        "labels": labels,
        "data": df_deduplicated.to_dict(orient='records'),
        "datageometry": datageometry,
        "datalocalidad": datalocalidad_processed,
        "urlfile": url,
        "marcas": {
            "labels": [],
            "values": []
        },
        "avaluoVehiculo": {
            "labels": [],
            "values": []
        },
        "numeroVehiculos": {
            "labels": [],
            "values": []
        },
        "tipoVehiculos": {
            "labels": [],
            "values": []
        },
        "avaluoPropiedades": {
            "labels": [],
            "values": []
        },
        "estrato": {
            "labels": [],
            "values": []
        },
        "numeroPropiedades": {
            "labels": [],
            "values": []
        },
        "edades": {
            "labels": [],
            "values": []
        },
        "diasVisitas": {
            "labels": [],
            "values": []
        },
        "horasVisitas": {
            "labels": [],
            "values": []
        }
    }
    
    marca_counts = df_deduplicated[df_deduplicated['marca'].notna()]['marca'].value_counts()
    if len(marca_counts) > 11:
        top_marcas   = marca_counts.head(11)
        otros        = marca_counts.iloc[11:].sum()
        marca_counts = pd.concat([top_marcas, pd.Series({'Otros': otros})])
    output["marcas"]["labels"] = marca_counts.index.tolist()
    output["marcas"]["values"] = marca_counts.values.tolist()
    
    df_avaluo          = df_deduplicated[df_deduplicated['avaluo'].notna()][['placa', 'avaluo']].drop_duplicates(subset='placa', keep='first')
    bins               = [0, 80_000_000, 120_000_000, 200_000_000, float('inf')]
    labels_avaluo      = ['Menor a 80 MM', '80 MM - 120 MM', '120 MM - 200 MM', 'Más de 200 MM']
    df_avaluo['rango'] = pd.cut(df_avaluo['avaluo'], bins=bins, labels=labels_avaluo)
    avaluo_counts      = df_avaluo['rango'].value_counts().sort_index()
    output["avaluoVehiculo"]["labels"] = avaluo_counts.index.tolist()
    output["avaluoVehiculo"]["values"] = avaluo_counts.values.tolist()
    
    df_veh_count = df_deduplicated.copy()
    df_veh_count['num_vehiculos'] = df_veh_count.groupby('numID')['placa'].transform('nunique')
    veh_counts   = df_veh_count['num_vehiculos'].value_counts().sort_index()
    if len(veh_counts) > 4:
        top_counts = veh_counts.head(4)
        otros      = veh_counts.iloc[4:].sum()
        veh_counts = pd.concat([top_counts, pd.Series({'+5': otros})])
    output["numeroVehiculos"]["labels"] = [str(x) for x in veh_counts.index.tolist()]
    output["numeroVehiculos"]["values"] = veh_counts.values.tolist()
    
    tipo_veh    = df_deduplicated[df_deduplicated['clase'].notna()].drop_duplicates(subset=['numID', 'clase'])
    tipo_counts = tipo_veh['clase'].value_counts()
    output["tipoVehiculos"]["labels"] = tipo_counts.index.tolist()
    output["tipoVehiculos"]["values"] = tipo_counts.values.tolist()
    
    df_prop          = df_deduplicated[df_deduplicated['avaluocatastral'].notna()].groupby('numID')['avaluocatastral'].max().reset_index()
    bins_prop        = [0, 200_000_000, 300_000_000, 500_000_000, 1_000_000_000, float('inf')]
    labels_prop      = ['Menor a 200 MM', '200 MM - 300 MM', '300 MM - 500 MM', '500 MM - 1,000 MM', 'Más de 1,000 MM']
    df_prop['rango'] = pd.cut(df_prop['avaluocatastral'], bins=bins_prop, labels=labels_prop)
    prop_counts      = df_prop['rango'].value_counts().sort_index()
    output["avaluoPropiedades"]["labels"] = prop_counts.index.tolist()
    output["avaluoPropiedades"]["values"] = prop_counts.values.tolist()
    
    df_estrato     = df_deduplicated[(df_deduplicated['estrato'] > 0) & (df_deduplicated['estrato'] <= 6)].groupby('numID')['estrato'].max()
    estrato_counts = df_estrato.value_counts().sort_index()
    output["estrato"]["labels"] = estrato_counts.index.tolist()
    output["estrato"]["values"] = estrato_counts.values.tolist()
    
    df_numprop     = df_deduplicated[(df_deduplicated['numprop'] > 0) & (df_deduplicated['numprop'] < 10)].groupby('numID')['numprop'].max()
    numprop_counts = df_numprop.value_counts().sort_index()
    if len(numprop_counts) > 4:
        top_props      = numprop_counts.head(4)
        otros          = numprop_counts.iloc[4:].sum()
        numprop_counts = pd.concat([top_props, pd.Series({'+5': otros})])
    output["numeroPropiedades"]["labels"] = [str(x) for x in numprop_counts.index.tolist()]
    output["numeroPropiedades"]["values"] = numprop_counts.values.tolist()
    
    df_edad               = df_deduplicated[(df_deduplicated['edad'] > 17) & (df_deduplicated['edad'] < 90)].drop_duplicates(subset='numID')
    bins_edad             = [17, 24, 34, 44, 54, 64, 74, 90]
    labels_edad           = ["17-24", "25-34", "35-44", "45-54", "55-64", "65-74", "75-90"]
    df_edad['rango_edad'] = pd.cut(df_edad['edad'], bins=bins_edad, labels=labels_edad)
    edad_counts           = df_edad['rango_edad'].value_counts().sort_index()
    output["edades"]["labels"] = edad_counts.index.tolist()
    output["edades"]["values"] = edad_counts.values.tolist()
    
    #-------------------------------------------------------------------------#
    # Generación aleatoria para días de la semana y franjas horarias
    dias           = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]
    probabilidades = [0.05, 0.05, 0.05, 0.05, 0.15, 0.25, 0.4]
    np.random.seed(42)
    dias_aleatorios = np.random.choice(dias, size=len(df_deduplicated), p=probabilidades)
    dias_counts = pd.Series(dias_aleatorios).value_counts()
    dias_counts = dias_counts.reindex(dias, fill_value=0)
    output["diasVisitas"]["labels"] = dias
    output["diasVisitas"]["values"] = dias_counts.values.tolist()
    
    franjas = ["Mañana", "Tarde", "Noche"]
    probabilidades_franja = [0.5, 0.3, 0.2]
    np.random.seed(42)  
    franjas_aleatorias = np.random.choice(franjas, size=len(df_deduplicated), p=probabilidades_franja)
    horas_counts = pd.Series(franjas_aleatorias).value_counts()
    horas_counts = horas_counts.reindex(franjas, fill_value=0)
    output["horasVisitas"]["labels"] = franjas
    output["horasVisitas"]["values"] = horas_counts.values.tolist()
    

    output = clean_json(output)
    return output

def wkt_to_geojson_coordinates(wkt_polygon):
    pattern = r'POLYGON\s*\(\s*\(\s*([-+]?\d*\.?\d+\s+[-+]?\d*\.?\d+)(?:\s*,\s*([-+]?\d*\.?\d+\s+[-+]?\d*\.?\d+))*\s*\)\s*\)'
    match   = re.search(pattern, wkt_polygon)
    if not match:
        return None
    coords_str  = match.group(0)
    coord_pairs = re.findall(r'[-+]?\d*\.?\d+\s+[-+]?\d*\.?\d+', coords_str)
    coordinates = []
    for pair in coord_pairs:
        lon, lat = map(float, pair.split())
        coordinates.append([lon, lat])
    if coordinates and coordinates[0] != coordinates[-1]:
        coordinates.append(coordinates[0])
    return [coordinates] if coordinates else None

def create_geojson_feature(row, segmentacion):
    coordinates = wkt_to_geojson_coordinates(row['wkt'])
    if not coordinates:
        return None
    feature = {
        "type": "Feature",
        "properties": {
            "nombre": row.get('scanombre', row.get('locnombre', '')),
            "conteo": int(row.get('conteo', 0)),
            "color": row.get('color', '#440154')
        },
        "geometry": {
            "type": "Polygon",
            "coordinates": coordinates
        }
    }
    if segmentacion == 'Barrio catastral' and 'scacodigo' in row:
        feature["properties"]["codigo"] = row['scacodigo']
    return feature

def asignar_colores(df):
    
    # Reemplazar NaN con el valor mínimo para no afectar la escala
    min_val = df["conteo"].min()
    min_val = 0
    df["conteo"].fillna(min_val, inplace=True)
    
    # Normalizar los valores de conteo para mapearlos en la escala de colores
    norm = mcolors.Normalize(vmin=df["conteo"].min(), vmax=df["conteo"].max())
    cmap = cm.get_cmap("viridis")  # Usamos la escala de colores viridis
    
    df["color"] = df["conteo"].apply(lambda x: mcolors.to_hex(cmap(norm(x))))
    
    return df