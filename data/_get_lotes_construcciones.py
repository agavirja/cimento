import pandas as pd
import geopandas as gpd
import shapely.wkt as wkt
import json
import uuid
from datetime import datetime
from shapely.geometry import Polygon, MultiPolygon
from shapely.ops import unary_union

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
    data_lote_polygon   = pd.DataFrame()
    data_construcciones = pd.DataFrame()
    if isinstance(barmanpre,str) and barmanpre!='':
        file_configs = [
            ("_caracteristicas/_bogota_lotes_colindantes","barmanpre_colindante"),
            ("_caracteristicas/_bogota_construcciones_building","construcciones"),
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
                
        resultado           = get_multiple_data_bucket(formato, barmanpre=None, max_workers=10)
        data_lote_polygon   = selectdata(resultado,"barmanpre_colindante", barmanpre=lista)
        data_construcciones = selectdata(resultado,"construcciones", barmanpre=lista)

    # ——————————————————————————————————————————————————————————————————————— #
    # Estructura de la API
    # ——————————————————————————————————————————————————————————————————————— #
    # Lotes colindantes | construcciones
    if not data_construcciones.empty:
        data_construcciones['referencia'] = 1
        
    if not data_lote_polygon.empty and 'barmanpre_colindante' in data_lote_polygon:
        barmanpre_colindante = [valor for texto in data_lote_polygon['barmanpre_colindante'].dropna() for valor in texto.split('|')]
        barmanpre_colindante = list(set(barmanpre_colindante))

        formato = []
        conteo  = 0
        for item in barmanpre_colindante:
            conteo    += 1 
            manzcodigo = item[0:9]
            filename   = generar_codigo(manzcodigo)
            formato.append({'file': f"_caracteristicas/_bogota_construcciones_building/{filename}.parquet", "name": item, "barmanpre": item, "data": pd.DataFrame(), "run":True})
        
        if formato!=[]:
            resultado = get_multiple_data_bucket(formato, barmanpre=None, max_workers=10)
            
        data_construcciones_colindantes = pd.DataFrame()
        for item in resultado:
            if 'data' in item and not item['data'].empty:
                datapaso = item['data'].copy()
                datapaso = datapaso[datapaso['barmanpre']==item['name']]
                data_construcciones_colindantes = pd.concat([data_construcciones_colindantes,datapaso])
                data_construcciones_colindantes = data_construcciones_colindantes.drop_duplicates()
                
        if not data_construcciones_colindantes.empty:
            data_construcciones_colindantes['referencia'] = 0 
            data_construcciones = pd.concat([data_construcciones,data_construcciones_colindantes])
            data_construcciones = data_construcciones.drop_duplicates()
    
    
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
        "location": {
            "latitud": None,
            "longitud": None,
            },
        "plot": {
            "areaPolygon":None,
            "wkt": None
        },
        "geometry": {
            "geojson":None,
            "googleCoords":None,
            },
        "constructions": {}
    }
        
    # Geometry
    if not data_lote_polygon.empty:
        output["location"]["latitud"]  = float(data_lote_polygon['latitud'].median()) if 'latitud' in data_lote_polygon else None
        output["location"]["longitud"] = float(data_lote_polygon['longitud'].median()) if 'longitud' in data_lote_polygon else None
        output["plot"]["areaPolygon"]  = float(data_lote_polygon['areapolygon'].sum()) if 'areapolygon' in data_lote_polygon else None
        output["plot"]["wkt"]          = data_lote_polygon['wkt'].iloc[0] if 'wkt' in data_lote_polygon else None

        # Generar GeoJSON para mapa
        if 'wkt' in data_lote_polygon:
            try:
                datapaso              = data_lote_polygon.copy()
                #datapaso['geometry'] = datapaso['wkt'].apply(wkt.loads)
                datapaso['geometry']  = gpd.GeoSeries.from_wkt(datapaso['wkt'])
                datapaso              = gpd.GeoDataFrame(datapaso, geometry='geometry')
                datapaso              = datapaso.explode(index_parts=False)
                
                if len(datapaso) > 1:
                    polygon = unary_union(datapaso.geometry)
                else:
                    polygon = datapaso['geometry'].iloc[0]
                output["plot"]["wkt"] = polygon.wkt
                
                # Convertir a GeoJSON para uso en cliente
                if isinstance(polygon, (Polygon, MultiPolygon)):
                    output["geometry"]["geojson"] = json.loads(gpd.GeoSeries([polygon]).to_json())
                    
                # Coordenadas para Google Maps
                if isinstance(polygon, Polygon):
                    coords = list(polygon.exterior.coords)
                    google_coords = [{"lat": lat, "lng": lng} for lng, lat in coords]
                    output["geometry"]["googleCoords"] = google_coords
                elif isinstance(polygon, MultiPolygon):
                    all_coords = []
                    for poly in polygon.geoms:
                        coords = list(poly.exterior.coords)
                        google_coords = [{"lat": lat, "lng": lng} for lng, lat in coords]
                        all_coords.append(google_coords)
                    output["geometry"]["googleCoords"] = all_coords
                    
            except: pass
        
        
    # Construcciones
    if not data_construcciones.empty:
        data_construcciones = data_construcciones.sort_values(by=['barmanpre', 'connpisos'], ascending=False).drop_duplicates(subset=['wkt'], keep='first')

        if 'wkt' in data_construcciones:
            output_construcciones = []
            try:
                data_construcciones['geometry'] = data_construcciones['wkt'].apply(wkt.loads)
                data_construcciones = gpd.GeoDataFrame(data_construcciones, geometry='geometry')
                
                # Generar color por defecto
                data_construcciones['color'] = "#E1E5F2"
                
                # Marcar referencia si existe
                if 'referencia' in data_construcciones:
                    idd = data_construcciones['referencia'] == 1
                    if sum(idd) > 0:
                        data_construcciones.loc[idd, 'color'] = "#A16CFF"
                
                # Procesar cada construcción
                for idx, row in data_construcciones.iterrows():
                    item = {
                        "id": int(idx),
                        "connpisos": int(row['connpisos']) if 'connpisos' in row and isinstance(row['connpisos'], (int, float)) else 1,
                        "color": row['color'],
                        "barmanpre": row['barmanpre'] if 'barmanpre' in row else None,
                        "wkt": row['wkt'] if 'wkt' in row else None,
                        "geometry": json.loads(gpd.GeoSeries([row.geometry]).to_json())
                    }
                    
                    # Coordenadas para Google Maps/Mapbox
                    if isinstance(row.geometry, Polygon):
                        coords = list(row.geometry.exterior.coords)
                        google_coords = [{"lat": lat, "lng": lng} for lng, lat in coords]
                        item["googleCoords"] = google_coords
                    
                    output_construcciones.append(item)
            except: pass
        output["constructions"] = output_construcciones
            
    output = clean_json(output)

    return output