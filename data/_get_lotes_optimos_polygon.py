import geopandas as gpd
import pandas as pd
from shapely import wkt

from functions.circle_polygon import circle_polygon
from functions.get_barmanpre_from_polygon import main as get_barmanpre_from_polygon

def main(inputvar={}):
    
    polygon = inputvar.get('polygon',None)
    output  = {'barmanpre':[]}
    
    if 'tabla' not in inputvar: 
        inputvar['tabla'] = "bogota_data_lotes_fastsearch" # "bogota_data_lotes" | " bogota_data_lotes_fastsearch"
        
    response = get_barmanpre_from_polygon(inputvar=inputvar)
    try:    
        data = pd.DataFrame(response['data'])
        data = data.drop_duplicates(subset='barmanpre')
    except: data = pd.DataFrame()
    
    if not data.empty and 'wkt' in data:

        data['geometry'] = gpd.GeoSeries.from_wkt(data['wkt'])
        data             = gpd.GeoDataFrame(data, geometry='geometry', crs="EPSG:4326")
        data             = data[['barmanpre','geometry']]
        
        data['latitud']  = data['geometry'].apply(lambda x: x.centroid.y)
        data['longitud'] = data['geometry'].apply(lambda x: x.centroid.x)
        
        metros           = 500
        data['geometry'] = data.apply(lambda x: circle_polygon(metros,x['latitud'],x['longitud']),axis=1)
        data             = gpd.GeoDataFrame(data, geometry='geometry', crs="EPSG:4326")
        data             = data[['barmanpre','geometry']]
        lista_lotes      = list(data['barmanpre'].unique()) if 'barmanpre' in data and not data.empty else []
        
        reference_poly   = wkt.loads(polygon)
        lista_lotes      = greedy_cover(reference_poly, data, tol_area=1e-8)
        output           = {'barmanpre':lista_lotes}
        
    return output
    

def greedy_cover(reference_poly, buffers_gdf: gpd.GeoDataFrame, tol_area: float = 1e-8, verbose: bool = True):

    sindex  = buffers_gdf.sindex
    idx     = list(sindex.intersection(reference_poly.bounds))
    buffers = buffers_gdf.iloc[idx]
    buffers = buffers[buffers.intersects(reference_poly)].copy().reset_index(drop=True)

    uncovered      = reference_poly
    selected_codes = []
    iteration      = 0

    while uncovered.area > tol_area:
        buffers["gain"] = buffers.geometry.intersection(uncovered).area
        best_idx        = buffers["gain"].idxmax()
        best_code       = buffers.at[best_idx, "barmanpre"]
        best_geom       = buffers.at[best_idx, "geometry"]

        selected_codes.append(best_code)
        uncovered = uncovered.difference(best_geom)
        if verbose:
            iteration += 1
        buffers = buffers.drop(best_idx).reset_index(drop=True)
    return selected_codes