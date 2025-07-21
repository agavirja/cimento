import pandas as pd
import geopandas as gpd
import os
import uuid
from shapely import wkt
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine

from functions.clean_json import clean_json

load_dotenv()

user     = os.getenv("user")
password = os.getenv("password")
host     = os.getenv("host")
schema   = os.getenv("schema")
port     = os.getenv("port")
    
def main(inputvar={}):
    
    #inputvar = {'polygon':'POLYGON ((-74.05657133663108 4.690271440307401, -74.05650279351995 4.68949061251738, -74.05629924683434 4.68873350978464, -74.05596688123727 4.688023136292604, -74.05551579548923 4.687381076380362, -74.0549596956024 4.686826838713447, -74.05431547839048 4.686377263522912, -74.05360271806742 4.686046010923426, -74.05284307149499 4.685843145857601, -74.05205962014988 4.685774832277808, -74.05127616880476 4.685843145857601, -74.05051652223233 4.686046010923426, -74.04980376190927 4.686377263522912, -74.04915954469735 4.686826838713447, -74.04860344481052 4.687381076380362, -74.04815235906248 4.688023136292604, -74.04781999346541 4.68873350978464, -74.0476164467798 4.68949061251738, -74.04754790366867 4.690271440307401, -74.0476164467798 4.6910522680974225, -74.04781999346541 4.691809370830162, -74.04815235906248 4.692519744322198, -74.04860344481052 4.693161804234441, -74.04915954469735 4.6937160419013555, -74.04980376190927 4.69416561709189, -74.05051652223233 4.694496869691377, -74.05127616880476 4.694699734757202, -74.05205962014988 4.694768048336995, -74.05284307149499 4.694699734757202, -74.05360271806742 4.694496869691377, -74.05431547839048 4.69416561709189, -74.0549596956024 4.6937160419013555, -74.05551579548923 4.693161804234441, -74.05596688123727 4.692519744322198, -74.05629924683434 4.691809370830162, -74.05650279351995 4.6910522680974225, -74.05657133663108 4.690271440307401))','segmentacion':'radio','spatialRelation': 'intersects'}
    
    polygon         = inputvar.get('polygon',None)
    segmentacion    = inputvar.get('segmentacion', None)
    spatialRelation = inputvar.get('spatialRelation','intersects') # intersects | contains 
    tabla           = inputvar.get('tabla','bogota_data_lotes_fastsearch')  # "bogota_data_lotes" | " bogota_data_lotes_fastsearch"
    getWkt          = inputvar.get('getWkt',True)
    
    databarmanpre  = pd.DataFrame()
    engine         = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{schema}')
            
    if isinstance(segmentacion,str) and 'radio' in segmentacion and isinstance(polygon, str) and polygon!='' and not 'none' in polygon.lower():        
        squarePolygon = polygon_to_square(polygon)
        #comprobante   = wkt.loads(squarePolygon).difference(wkt.loads(polygon))
        if isinstance(squarePolygon,str) and squarePolygon!='' and not 'none' in squarePolygon.lower():
            if spatialRelation=="intersects":
                query         = f" ST_INTERSECTS(ST_GEOMFROMTEXT('{squarePolygon}',4326),geometry)"
                databarmanpre = pd.read_sql_query(f"SELECT barmanpre, ST_AsText(geometry) as wkt FROM  bigdata.{tabla} WHERE {query}" , engine)
            
            elif spatialRelation=="contains":
                query         = f" ST_WITHIN(geometry,ST_GEOMFROMTEXT('{squarePolygon}',4326))"
                databarmanpre = pd.read_sql_query(f"SELECT barmanpre, ST_AsText(geometry) as wkt FROM  bigdata.{tabla} WHERE {query}" , engine)

    if databarmanpre.empty and isinstance(polygon, str) and polygon!='' and not 'none' in polygon.lower():

        if spatialRelation=="intersects":
            query         = f" ST_INTERSECTS(ST_GEOMFROMTEXT('{polygon}',4326),geometry)"
            databarmanpre = pd.read_sql_query(f"SELECT barmanpre, ST_AsText(geometry) as wkt FROM  bigdata.{tabla} WHERE {query}" , engine)
        
        elif spatialRelation=="contains":
            query         = f" ST_WITHIN(geometry,ST_GEOMFROMTEXT('{polygon}',4326))"
            databarmanpre = pd.read_sql_query(f"SELECT barmanpre, ST_AsText(geometry) as wkt FROM  bigdata.{tabla} WHERE {query}" , engine)

    if not databarmanpre.empty and 'wkt' in databarmanpre: 
        df             = databarmanpre.copy()
        df['geometry'] = gpd.GeoSeries.from_wkt(df['wkt'])
        df             = gpd.GeoDataFrame(df.drop(columns=['wkt']),geometry='geometry', crs="EPSG:4326")
        
        if spatialRelation=="intersects":
            df = df[df.geometry.intersects(wkt.loads(polygon))]
        elif spatialRelation=="contains":
            df = df[df.geometry.within(wkt.loads(polygon))]
            
        databarmanpre = databarmanpre[databarmanpre['barmanpre'].isin(df['barmanpre'])]

    engine.dispose()
    
    if getWkt is False and not databarmanpre.empty and 'wkt' in databarmanpre:
        del databarmanpre['wkt']
        
    output = {
        "meta": {
            "timestamp": datetime.now().isoformat(),
            "requestId": str(uuid.uuid4()),
            "filtersApplied": inputvar,
        },
        "data": databarmanpre.to_dict(orient='records') if not databarmanpre.empty else [],
    }
    
    output = clean_json(output)
    
    return output

def polygon_to_square(polygon_wkt):
    coords_text = polygon_wkt.replace("'", "").replace("POLYGON ((", "").replace("))", "")
    coord_pairs = coords_text.split(", ")
    lons        = []
    lats        = []
    for pair in coord_pairs:
        if pair.strip():
            coords = pair.strip().split()
            if len(coords) == 2:
                lons.append(float(coords[0]))
                lats.append(float(coords[1]))
    
    min_lon, max_lon = min(lons), max(lons)
    min_lat, max_lat = min(lats), max(lats)
    center_lon       = (min_lon + max_lon) / 2
    center_lat       = (min_lat + max_lat) / 2
    
    side_length = max(max_lon - min_lon, max_lat - min_lat) * 1.1
    half_side = side_length / 2
    
    square_coords = [
        (center_lon - half_side, center_lat - half_side),
        (center_lon + half_side, center_lat - half_side),
        (center_lon + half_side, center_lat + half_side),
        (center_lon - half_side, center_lat + half_side),
        (center_lon - half_side, center_lat - half_side)
    ]
    
    wkt_coords = ", ".join([f"{lon} {lat}" for lon, lat in square_coords])
    return f"POLYGON (({wkt_coords}))"