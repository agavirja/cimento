import pandas as pd
import uuid
from datetime import datetime, timedelta
from numbers import Number

from functions.coddir import coddir
from functions.general_functions import  get_multiple_data_bucket, generar_codigo, selectdata
from functions.clean_json import clean_json

from data._get_listings import bycode

def main(inputvar={}):

    # ——————————————————————————————————————————————————————————————————————— #
    # Variables inputvar: 
    # ——————————————————————————————————————————————————————————————————————— #
    barmanpre = inputvar['barmanpre'] if 'barmanpre' in inputvar and isinstance(inputvar['barmanpre'],str) else None
    direccion = inputvar['direccion'] if 'direccion' in inputvar and isinstance( inputvar['direccion'],str) and  inputvar['direccion']!='' else None
    poligono  = inputvar['polygon'] if 'polygon' in inputvar and isinstance(inputvar['polygon'],str) else None
    precuso   = inputvar['precuso'] if 'precuso' in inputvar and isinstance(inputvar['precuso'],list) else []
    areamin   = inputvar.get('areamin', 0)
    areamax   = inputvar.get('areamax', 0)

    # ——————————————————————————————————————————————————————————————————————— #
    # Estructura de datos
    # ——————————————————————————————————————————————————————————————————————— #
    data_market_activos   = pd.DataFrame()
    data_market_historico = pd.DataFrame()
    if isinstance(barmanpre,str) and barmanpre!='':
        file_configs = [
            ("_market/_bogota_market_activos_manzana", "market"),
            ("_market/_bogota_market_historico_manzana", "market_historico"),
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
                
        resultado             = get_multiple_data_bucket(formato, barmanpre=None, max_workers=10)
        data_market_activos   = selectdata(resultado,"market", barmanpre=lista)
        data_market_historico = selectdata(resultado,"market_historico", barmanpre=lista)

    # ——————————————————————————————————————————————————————————————————————— #
    # Estructura de la API
    # ——————————————————————————————————————————————————————————————————————— #    
    
    # Filtros
    if not data_market_activos.empty and areamin>0 and 'areaconstruida' in data_market_activos:
        data_market_activos = data_market_activos[data_market_activos['areaconstruida']>=areamin]
    if not data_market_activos.empty and areamax>0 and 'areaconstruida' in data_market_activos:
        data_market_activos = data_market_activos[data_market_activos['areaconstruida']<=areamax]

    if not data_market_historico.empty and areamin>0 and 'areaconstruida' in data_market_historico:
        data_market_historico = data_market_historico[data_market_historico['areaconstruida']>=areamin]
    if not data_market_historico.empty and areamax>0 and 'areaconstruida' in data_market_historico:
        data_market_historico = data_market_historico[data_market_historico['areaconstruida']<=areamax]

    datatipoinmueble = {
    "tipoinmueble": [
        "Oficina",
        "Local",
        "Bodega",
        "Apartamento",
        "Casa"
    ],
    "precuso": [
        ["015", "020", "045", "080", "081", "082", "092"],  # Oficina
        ['003', '004', '039', '040', '005', '006', '007', '041', '042', '056', '060', '094', '095', '066', '067', '058', '059', '066'],  # Local
        ["008", "022", "024", "025", "033", "091", "093", "097", "098"],  # Bodega
        ["002", "038"],  # Apartamento
        ["001", "037"]   # Casa
    ]}
    datatipoinmueble = pd.DataFrame(datatipoinmueble).explode("precuso", ignore_index=True)
    if isinstance(precuso,list) and precuso!=[]:
        datatipoinmueble = datatipoinmueble[datatipoinmueble['precuso'].isin(precuso)]
        if not datatipoinmueble.empty:
            data_market_activos   = data_market_activos[data_market_activos['tipoinmueble'].isin(list(datatipoinmueble['tipoinmueble'].unique()))]
            data_market_historico = data_market_historico[data_market_historico['tipoinmueble'].isin(list(datatipoinmueble['tipoinmueble'].unique()))]

    # Filtro de listings que esten dentro del poligono
    if poligono is not None:
        
        from shapely.geometry import Point
        from shapely import wkt
            
        polygon_contains = wkt.loads(poligono)

        if not data_market_activos.empty and 'latitud' in data_market_activos and 'longitud' in data_market_activos:
            data_market_activos['idx'] = data_market_activos.apply(lambda row: polygon_contains.contains(Point(row['longitud'], row['latitud'])) if isinstance(row['longitud'], Number) and isinstance(row['latitud'], Number) else False, axis=1)
            data_market_activos        = data_market_activos[data_market_activos['idx']]
            data_market_activos.drop(columns=['idx'],inplace=True)

        if not data_market_historico.empty and 'latitud' in data_market_historico and 'longitud' in data_market_historico:
            data_market_historico['idx'] = data_market_historico.apply(lambda row: polygon_contains.contains(Point(row['longitud'], row['latitud'])) if isinstance(row['longitud'], Number) and isinstance(row['latitud'], Number) else False, axis=1)
            data_market_historico        = data_market_historico[data_market_historico['idx']]
            data_market_historico.drop(columns=['idx'],inplace=True)


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
        "resumenEjecutivo": {
            "fechaAnalisis"              : datetime.now().isoformat(),
            "totalPropiedades"           : 0,
            "propiedadesActivas"         : 0,
            "propiedadesHistoricas"      : 0,
            "tiposInmuebleDisponibles"   : [],
            "rangoPrecios": {
                "venta"   : {"minimo": None, "maximo": None, "promedio": None},
                "arriendo": {"minimo": None, "maximo": None, "promedio": None}
            }
        },
        
        "indicadoresMercado": {
            "venta": {
                "valorPromedioPorM2"     : None,
                "valorMedianoPorM2"      : None,
                "desviacionEstandar"     : None,
                "coeficienteVariacion"   : None,
                "totalObservaciones"     : 0,
                "propiedadesActivas"     : 0,
                "propiedadesHistoricas"  : 0
            },
            "arriendo": {
                "valorPromedioPorM2"     : None,
                "valorMedianoPorM2"      : None,
                "desviacionEstandar"     : None,
                "coeficienteVariacion"   : None,
                "totalObservaciones"     : 0,
                "propiedadesActivas"     : 0,
                "propiedadesHistoricas"  : 0
            }
        },
        
        "distribucionPorTipo": {
            "venta"   : {},
            "arriendo": {}
        },
        
        "analisisEspacial": {
            "coordenadasPromedio": {"latitud": None, "longitud": None},
            "rangoGeografico": {
                "latitudMin"  : None,
                "latitudMax"  : None,
                "longitudMin" : None,
                "longitudMax" : None
            },
            "densidadPropiedades": None
        },
        
        "tendenciasTemporal": {
            "venta": {
                "datosPorAno"      : [],
                "tendenciaPrecios" : None,
                "variacionAnual"   : None,
                "estacionalidad"   : {}
            },
            "arriendo": {
                "datosPorAno"      : [],
                "tendenciaPrecios" : None,
                "variacionAnual"   : None,
                "estacionalidad"   : {}
            }
        },
        
        "metricsDeRendimiento": {
            "rentabilidadPromedio" : None,
            "multiplicadorRenta"   : None,
            "yieldPromedio"        : None
        },
        
        "caracteristicasPropiedades": {
            "areaConstruidaPromedio" : None,
            "habitacionesPromedio"   : None,
            "banosPromedio"          : None,
            "garajesPromedio"        : None,
            "distribucionAreas": {
                "pequenas"     : 0,
                "medianas"     : 0,
                "grandes"      : 0,
                "extraGrandes" : 0
            }
        },
        
        "liquidezMercado": {
            "tiempoPromedioEnMercado"    : None,
            "tasaRotacion"               : None,
            "propiedadesNuevasUltimoMes" : 0,
            "propiedadesRetiradas"       : 0
        },
        
        "listaCodigosActivos":{
            "venta":'',
            "arriendo":''
            },
        
        "listadoPropiedades" : [],
    }
    
    active_listings             = pd.DataFrame()
    data_market_activo_building = pd.DataFrame()
    data_activos_agrupados      = pd.DataFrame()
    df_venta_activos            = pd.DataFrame()
    df_arriendo_activos         = pd.DataFrame()

    address_filter = False
    if not data_market_activos.empty:
        if isinstance(direccion, str) and direccion != '':
            fcoddir = [coddir(x) for x in direccion.split('|') if isinstance(x,str)]
            if isinstance(fcoddir, list) and fcoddir!=[]:
                data_market_activo_building = data_market_activos[data_market_activos['coddir'].isin(fcoddir)]
                active_listings             = data_market_activos[data_market_activos['coddir'].isin(fcoddir)]
                address_filter              = True
        else:
            data_market_activo_building = data_market_activos.copy()
            active_listings             = data_market_activos.copy()
        
        if not data_market_activo_building.empty:
            data_market_activo_building['valormt2'] = None
            idd = (data_market_activo_building['valor'] > 0) & (data_market_activo_building['areaconstruida'] > 0)
            data_market_activo_building.loc[idd, 'valormt2'] = data_market_activo_building.loc[idd, 'valor'] / data_market_activo_building.loc[idd, 'areaconstruida']
            
            active_listings             = data_market_activo_building.copy()
            data_activos_agrupados      = data_market_activo_building[data_market_activo_building['valormt2'].notnull()]
            data_activos_agrupados      = data_activos_agrupados.groupby(['tiponegocio']).agg({'valormt2': 'median','code': 'count'}).reset_index()
            data_activos_agrupados.columns = ['tiponegocio', 'valormt2', 'obs']
            
            df_venta_activos    = data_activos_agrupados[data_activos_agrupados['tiponegocio'] == 'Venta']
            df_arriendo_activos = data_activos_agrupados[data_activos_agrupados['tiponegocio'] == 'Arriendo']
    
    if not data_market_activo_building.empty and 'code' in data_market_activo_building:
        output["listaCodigosActivos"]["venta"]    = '|'.join([str(x) for x in list(data_market_activo_building[data_market_activo_building['tiponegocio']=='Venta']['code'].unique())])
        output["listaCodigosActivos"]["arriendo"] = '|'.join([str(x) for x in list(data_market_activo_building[data_market_activo_building['tiponegocio']=='Arriendo']['code'].unique())])
            
    historic_listings                = pd.DataFrame()
    data_market_historico_building   = pd.DataFrame()
    data_historicos_agrupados        = pd.DataFrame()
    df_venta_historicos              = pd.DataFrame()
    df_arriendo_historicos           = pd.DataFrame()
    
    if not data_market_historico.empty:
        data_market_historico_building = data_market_historico.copy()
        data_market_historico_building['valormt2'] = None
        
        for i in ['valor', 'areaconstruida']:
            if i in data_market_historico_building:
                data_market_historico_building[i] = pd.to_numeric(data_market_historico_building[i], errors='coerce')
        
        idd = (data_market_historico_building['valor'] > 0) & (data_market_historico_building['areaconstruida'] > 0)
        data_market_historico_building.loc[idd, 'valormt2'] = data_market_historico_building.loc[idd, 'valor'] / data_market_historico_building.loc[idd, 'areaconstruida']
        
        historic_listings              = data_market_historico_building.copy()
        data_historicos_agrupados      = data_market_historico_building[data_market_historico_building['valormt2'].notnull()]
        data_historicos_agrupados      = data_historicos_agrupados.groupby(['tiponegocio']).agg({'valormt2': 'median','code': 'count'}).reset_index()
        data_historicos_agrupados.columns = ['tiponegocio', 'valormt2', 'obs']
        
        if not data_market_activo_building.empty and 'code' in data_market_activo_building and 'code' in data_market_historico_building:
            idd               = data_market_historico_building['code'].isin(data_market_activo_building['code'])
            historic_listings = historic_listings[~idd]
        
        df_venta_historicos    = data_historicos_agrupados[data_historicos_agrupados['tiponegocio'] == 'Venta']
        df_arriendo_historicos = data_historicos_agrupados[data_historicos_agrupados['tiponegocio'] == 'Arriendo']
    
    venta_activos_valormt2      = float(df_venta_activos['valormt2'].median()) if not df_venta_activos.empty else None
    venta_activos_obs           = int(df_venta_activos['obs'].sum()) if not df_venta_activos.empty else 0
    venta_historicos_valormt2   = float(df_venta_historicos['valormt2'].median()) if not df_venta_historicos.empty else None
    venta_historicos_obs        = int(df_venta_historicos['obs'].sum()) if not df_venta_historicos.empty else 0
    
    arriendo_activos_valormt2   = float(df_arriendo_activos['valormt2'].median()) if not df_arriendo_activos.empty else None
    arriendo_activos_obs        = int(df_arriendo_activos['obs'].sum()) if not df_arriendo_activos.empty else 0
    arriendo_historicos_valormt2 = float(df_arriendo_historicos['valormt2'].median()) if not df_arriendo_historicos.empty else None
    arriendo_historicos_obs     = int(df_arriendo_historicos['obs'].sum()) if not df_arriendo_historicos.empty else 0
    
    output["indicadoresMercado"]["venta"].update({
        "valorMedianoPorM2"     : venta_activos_valormt2 if venta_activos_valormt2 else venta_historicos_valormt2,
        "totalObservaciones"    : venta_activos_obs + venta_historicos_obs,
        "propiedadesActivas"    : venta_activos_obs,
        "propiedadesHistoricas" : venta_historicos_obs
    })
    
    output["indicadoresMercado"]["arriendo"].update({
        "valorMedianoPorM2"     : arriendo_activos_valormt2 if arriendo_activos_valormt2 else arriendo_historicos_valormt2,
        "totalObservaciones"    : arriendo_activos_obs + arriendo_historicos_obs,
        "propiedadesActivas"    : arriendo_activos_obs,
        "propiedadesHistoricas" : arriendo_historicos_obs
    })
    
    if not active_listings.empty and address_filter:
        try:
            lista     = list(active_listings['code'].unique())
            datamerge = bycode(code=lista)
            if not datamerge.empty:
                variables = [x for x in list(datamerge) if x not in list(active_listings)]
                variables = ['code'] + variables
                datamerge = datamerge[variables].drop_duplicates(subset=['code'], keep='last')
                active_listings = active_listings.merge(datamerge, on='code', how='left', validate='m:1')
        except Exception as e:
            print(f"Error al obtener datos adicionales: {e}")
        
        for _, item in active_listings.iterrows():
            listing_item = {
                "codigoPropiedad"     : item.get('code', None),
                "estadoListado"       : "Activo",
                "tipoNegocio"         : item.get('tiponegocio', None),
                "tipoInmueble"        : item.get('tipoinmueble', None),
                "direccionCompleta"   : item.get('direccion', None),
                "ubicacion": {
                    "latitud"  : float(item.get('latitud', 0)) if pd.notnull(item.get('latitud', None)) else None,
                    "longitud" : float(item.get('longitud', 0)) if pd.notnull(item.get('longitud', None)) else None
                },
                "precioTotal"         : float(item.get('valor', 0)) if pd.notnull(item.get('valor', None)) else None,
                "precioPorM2"         : float(item.get('valormt2', 0)) if pd.notnull(item.get('valormt2', None)) else None,
                "caracteristicas": {
                    "areaConstruida"      : float(item.get('areaconstruida', 0)) if pd.notnull(item.get('areaconstruida', None)) else None,
                    "numeroHabitaciones"  : int(float(item.get('habitaciones', 0))) if pd.notnull(item.get('habitaciones', None)) else None,
                    "numeroBanos"         : int(float(item.get('banos', 0))) if pd.notnull(item.get('banos', None)) else None,
                    "numeroGarajes"       : int(float(item.get('garajes', 0))) if pd.notnull(item.get('garajes', None)) else None
                },
                "fechaPublicacion"    : item.get('fecha_inicial').strftime('%Y-%m-%d') if pd.notnull(item.get('fecha_inicial', None)) else None,
                "diasEnMercado"       : (datetime.now() - pd.to_datetime(item.get('fecha_inicial'))).days if pd.notnull(item.get('fecha_inicial', None)) else None,
                "urlImagenes"         : item.get('url_img', None),
                "valorAdministracion" : float(item.get('valoradministracion', 0)) if pd.notnull(item.get('valoradministracion', None)) else None
            }
            output["listadoPropiedades"].append(listing_item)
    
    if not historic_listings.empty and not active_listings.empty:
        idd               = historic_listings['code'].isin(active_listings['code'])
        historic_listings = historic_listings[~idd]
    
    if not historic_listings.empty and address_filter:
        for _, item in historic_listings.iterrows():
            listing_item = {
                "codigoPropiedad"     : item.get('code', None),
                "estadoListado"       : "Inactivo",
                "tipoNegocio"         : item.get('tiponegocio', None),
                "tipoInmueble"        : item.get('tipoinmueble', None),
                "direccionCompleta"   : item.get('direccion', None),
                "ubicacion": {
                    "latitud"  : float(item.get('latitud', 0)) if pd.notnull(item.get('latitud', None)) else None,
                    "longitud" : float(item.get('longitud', 0)) if pd.notnull(item.get('longitud', None)) else None
                },
                "precioTotal"         : float(item.get('valor', 0)) if pd.notnull(item.get('valor', None)) else None,
                "precioPorM2"         : float(item.get('valormt2', 0)) if pd.notnull(item.get('valormt2', None)) else None,
                "caracteristicas": {
                    "areaConstruida"      : float(item.get('areaconstruida', 0)) if pd.notnull(item.get('areaconstruida', None)) else None,
                    "numeroHabitaciones"  : int(float(item.get('habitaciones', 0))) if pd.notnull(item.get('habitaciones', None)) else None,
                    "numeroBanos"         : int(float(item.get('banos', 0))) if pd.notnull(item.get('banos', None)) else None,
                    "numeroGarajes"       : int(float(item.get('garajes', 0))) if pd.notnull(item.get('garajes', None)) else None
                },
                "fechaPublicacion"    : item.get('fecha_inicial').strftime('%Y-%m-%d') if pd.notnull(item.get('fecha_inicial', None)) else None,
                "diasEnMercado"       : (datetime.now() - pd.to_datetime(item.get('fecha_inicial'))).days if pd.notnull(item.get('fecha_inicial', None)) else None,
                "urlImagenes"         : item.get('url_img', None),
                "valorAdministracion" : float(item.get('valoradministracion', 0)) if pd.notnull(item.get('valoradministracion', None)) else None
            }
            output["listadoPropiedades"].append(listing_item)
    
    if not active_listings.empty or not historic_listings.empty:
        all_listings = pd.concat([active_listings, historic_listings]) if not active_listings.empty and not historic_listings.empty else active_listings if not active_listings.empty else historic_listings
        
        if 'fecha_inicial' in all_listings:
            all_listings['fecha_inicial'] = pd.to_datetime(all_listings['fecha_inicial'], errors='coerce')
            all_listings['year']          = all_listings['fecha_inicial'].dt.year
            
            venta_data = all_listings[all_listings['tiponegocio'] == 'Venta']
            if not venta_data.empty:
                venta_por_ano = venta_data.groupby('year')['valormt2'].median().reset_index()
                venta_count   = venta_data.groupby('year').size().reset_index(name='count')
                output["tendenciasTemporal"]["venta"]["datosPorAno"]  = venta_por_ano.to_dict(orient='records')
                output["tendenciasTemporal"]["venta"]["conteoAnual"]  = venta_count.to_dict(orient='records')
            
            arriendo_data = all_listings[all_listings['tiponegocio'] == 'Arriendo']
            if not arriendo_data.empty:
                arriendo_por_ano = arriendo_data.groupby('year')['valormt2'].median().reset_index()
                arriendo_count   = arriendo_data.groupby('year').size().reset_index(name='count')
                output["tendenciasTemporal"]["arriendo"]["datosPorAno"] = arriendo_por_ano.to_dict(orient='records')
                output["tendenciasTemporal"]["arriendo"]["conteoAnual"] = arriendo_count.to_dict(orient='records')
    
    all_data = pd.concat([active_listings, historic_listings], ignore_index=True) if not active_listings.empty and not historic_listings.empty else active_listings if not active_listings.empty else historic_listings
    
    if not all_data.empty:
        numeric_cols = ['valor', 'areaconstruida', 'habitaciones', 'banos', 'garajes', 'latitud', 'longitud']
        for col in numeric_cols:
            if col in all_data:
                all_data[col] = pd.to_numeric(all_data[col], errors='coerce')
        
        output["resumenEjecutivo"].update({
            "totalPropiedades"         : len(all_data),
            "propiedadesActivas"       : len(active_listings),
            "propiedadesHistoricas"    : len(historic_listings),
            "tiposInmuebleDisponibles" : all_data['tipoinmueble'].unique().tolist()
        })
        
        for tipo_negocio in ['Venta', 'Arriendo']:
            subset = all_data[all_data['tiponegocio'] == tipo_negocio]
            if not subset.empty and 'valor' in subset:
                valores_validos = subset['valor'].dropna()
                if not valores_validos.empty:
                    key = tipo_negocio.lower()
                    output["resumenEjecutivo"]["rangoPrecios"][key] = {
                        "minimo"   : float(valores_validos.min()),
                        "maximo"   : float(valores_validos.max()),
                        "promedio" : float(valores_validos.mean())
                    }
        
        for tipo_negocio in ['Venta', 'Arriendo']:
            subset = all_data[all_data['tiponegocio'] == tipo_negocio]
            if not subset.empty and 'valormt2' in subset:
                valores_m2 = subset['valormt2'].dropna()
                if not valores_m2.empty:
                    key      = tipo_negocio.lower()
                    mean_val = valores_m2.mean()
                    std_val  = valores_m2.std()
                    
                    output["indicadoresMercado"][key].update({
                        "valorPromedioPorM2"   : float(mean_val),
                        "desviacionEstandar"   : float(std_val),
                        "coeficienteVariacion" : float(std_val / mean_val) if mean_val != 0 else None
                    })
                    
            if not subset.empty and 'tipoinmueble' in subset:
                distribucion_tipo = subset.groupby('tipoinmueble').agg({
                    'code': 'count',
                    'valor': 'mean',
                    'valormt2': 'median',
                    'areaconstruida': 'mean'
                }).reset_index()
                
                total_propiedades = len(subset)
                distribucion_dict = {}
                
                for _, row in distribucion_tipo.iterrows():
                    tipo_inmueble = row['tipoinmueble']
                    cantidad      = int(row['code'])
                    porcentaje    = round((cantidad / total_propiedades) * 100, 2) if total_propiedades > 0 else 0
                
                    distribucion_dict[tipo_inmueble] = {
                        "cantidad": cantidad,
                        "porcentaje": porcentaje,
                        "precioPromedio": float(row['valor']) if pd.notnull(row['valor']) else None,
                        "precioPorM2Mediano": float(row['valormt2']) if pd.notnull(row['valormt2']) else None,
                        "areaPromedio": float(row['areaconstruida']) if pd.notnull(row['areaconstruida']) else None
                    }
                output["distribucionPorTipo"][key] = distribucion_dict
                    
        coords_validas = all_data[['latitud', 'longitud']].dropna()
        if not coords_validas.empty:
            output["analisisEspacial"].update({
                "coordenadasPromedio": {
                    "latitud"  : float(coords_validas['latitud'].mean()),
                    "longitud" : float(coords_validas['longitud'].mean())
                },
                "rangoGeografico": {
                    "latitudMin"  : float(coords_validas['latitud'].min()),
                    "latitudMax"  : float(coords_validas['latitud'].max()),
                    "longitudMin" : float(coords_validas['longitud'].min()),
                    "longitudMax" : float(coords_validas['longitud'].max())
                },
                "densidadPropiedades": len(coords_validas)
            })
        
        areas_validas = all_data['areaconstruida'].dropna()
        if not areas_validas.empty:
            output["caracteristicasPropiedades"].update({
                "areaConstruidaPromedio" : float(areas_validas.mean()),
                "habitacionesPromedio"   : float(all_data['habitaciones'].mean()) if all_data['habitaciones'].notna().any() else None,
                "banosPromedio"          : float(all_data['banos'].mean()) if all_data['banos'].notna().any() else None,
                "garajesPromedio"        : float(all_data['garajes'].mean()) if all_data['garajes'].notna().any() else None,
                "distribucionAreas": {
                    "pequenas"     : len(areas_validas[areas_validas < 50]),
                    "medianas"     : len(areas_validas[(areas_validas >= 50) & (areas_validas < 100)]),
                    "grandes"      : len(areas_validas[(areas_validas >= 100) & (areas_validas < 200)]),
                    "extraGrandes" : len(areas_validas[areas_validas >= 200])
                }
            })
        
        venta_data    = all_data[all_data['tiponegocio'] == 'Venta']
        arriendo_data = all_data[all_data['tiponegocio'] == 'Arriendo']
        
        if not venta_data.empty and not arriendo_data.empty:
            precio_venta_promedio    = venta_data['valor'].mean()
            precio_arriendo_promedio = arriendo_data['valor'].mean()
            
            if precio_venta_promedio and precio_arriendo_promedio and precio_arriendo_promedio > 0:
                multiplicador_renta = precio_venta_promedio / (precio_arriendo_promedio * 12)
                yield_promedio      = (precio_arriendo_promedio * 12) / precio_venta_promedio * 100
                
                output["metricsDeRendimiento"].update({
                    "multiplicadorRenta"   : float(multiplicador_renta),
                    "yieldPromedio"        : float(yield_promedio),
                    "rentabilidadPromedio" : float(precio_arriendo_promedio * 12 / precio_venta_promedio * 100) if precio_venta_promedio > 0 else None
                })
        
        if not active_listings.empty:
            fecha_actual   = datetime.now()
            fechas_validas = pd.to_datetime(active_listings['fecha_inicial'], errors='coerce').dropna()
            
            if not fechas_validas.empty:
                tiempo_en_mercado = (fecha_actual - fechas_validas).dt.days
                tiempo_promedio   = tiempo_en_mercado.mean()
                fecha_limite      = fecha_actual - timedelta(days=30)
                propiedades_nuevas = len(fechas_validas[fechas_validas >= fecha_limite])
                
                output["liquidezMercado"].update({
                    "tiempoPromedioEnMercado"    : float(tiempo_promedio),
                    "propiedadesNuevasUltimoMes" : propiedades_nuevas,
                    "tasaRotacion"               : float(propiedades_nuevas / len(active_listings) * 100) if len(active_listings) > 0 else 0
                })
    
    output = clean_json(output)
    
    return output