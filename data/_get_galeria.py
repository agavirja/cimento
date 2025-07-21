import pandas as pd
import uuid
from shapely import wkt
from datetime import datetime, timedelta
from sqlalchemy import create_engine 

from functions.general_functions import  get_multiple_data_bucket, generar_codigo, selectdata
from functions.clean_json import clean_json

def main(inputvar={}):

    # ——————————————————————————————————————————————————————————————————————— #
    # Variables inputvar: 
    # ——————————————————————————————————————————————————————————————————————— #
    barmanpre     = inputvar['barmanpre'] if 'barmanpre' in inputvar and isinstance(inputvar['barmanpre'],str) else None
    poligono      = inputvar.get('polygon', None)
    areamin       = inputvar.get('areamin', 0)
    areamax       = inputvar.get('areamax', 0)
    prevetustzmin = inputvar.get('desde_antiguedad', 0)
    prevetustzmax = inputvar.get('hasta_antiguedad', 0)
    estratomin    = inputvar.get('estratomin', 0)
    estratomax    = inputvar.get('estratomax', 0)
    precuso       = inputvar.get('precuso', [])

    # ——————————————————————————————————————————————————————————————————————— #
    # Estructura de datos
    # ——————————————————————————————————————————————————————————————————————— #
    data_proyectos_galeria = pd.DataFrame()
    if isinstance(barmanpre,str) and barmanpre!='':

        lista   = [x.strip() for x in barmanpre.split('|') if isinstance(x, str)]
        ruta    = "_market/_bogota_proyectos_galeria"
        formato = []
        for items in lista:
            manzcodigo = items[:9]
            filename   = generar_codigo(manzcodigo)
            formato.append({
                "file":   f"{ruta}/{filename}.parquet",
                "name":   "galeria",
                "barmanpre": items,
                "data":   pd.DataFrame(),
                "run": True,
            })
            
        resultado          = get_multiple_data_bucket(formato, barmanpre=None, max_workers=10)
        data_proyectos_galeria = selectdata(resultado,"galeria", barmanpre=lista)

    # ——————————————————————————————————————————————————————————————————————— #
    # Filtros
    # ——————————————————————————————————————————————————————————————————————— #
    if not data_proyectos_galeria.empty:

        if areamin>0 and 'areaconstruida' in data_proyectos_galeria:
            data_proyectos_galeria = data_proyectos_galeria[data_proyectos_galeria['areaconstruida']>=areamin]
        if areamax>0 and 'areaconstruida' in data_proyectos_galeria:
            data_proyectos_galeria = data_proyectos_galeria[data_proyectos_galeria['areaconstruida']<=areamax]

        if isinstance(precuso,list) and precuso!=[] and 'precuso' in data_proyectos_galeria:
            data_proyectos_galeria = data_proyectos_galeria[data_proyectos_galeria['precuso'].isin(precuso)]
            
        # Filtro de poligono
        if poligono is not None:
            if 'latitud' in data_proyectos_galeria and 'longitud' in data_proyectos_galeria:
                from shapely.geometry import Point
                polygon_contains               = wkt.loads(poligono)
                data_proyectos_galeria['idx'] = data_proyectos_galeria.apply(lambda row: polygon_contains.contains(Point(row['longitud'], row['latitud'])), axis=1)
                data_proyectos_galeria        = data_proyectos_galeria[data_proyectos_galeria['idx']]
                data_proyectos_galeria.drop(columns=['idx'],inplace=True)

    # ——————————————————————————————————————————————————————————————————————— #
    # Estructura de la API - Datos Originales
    # ——————————————————————————————————————————————————————————————————————— #
    pricing_by_tipo_estado = []
    pricing_estadisticas   = []
    
    # ——————————————————————————————————————————————————————————————————————— #
    # Nuevas Estadísticas para Dashboard
    # ——————————————————————————————————————————————————————————————————————— #
    proyectos_por_estado = []
    proyectos_por_constructora = []
    proyectos_por_proyecto = []
    proyectos_por_tipo = []
    resumen_proyectos_mapa = []
    tendencia_precio_tiempo = []
    tipologia_inmuebles = {}
    estadisticas_generales = {}
    estadisticas_por_zona = []
    indicadores_mercado = {}
    
    if not data_proyectos_galeria.empty:
        data_proyectos_galeria = get_pricig_proyectos(data=data_proyectos_galeria)
        data_proyectos_galeria = data_proyectos_galeria.drop_duplicates() if not data_proyectos_galeria.empty else data_proyectos_galeria
    
        # Filtrar proyectos activos (Construcción/Preventa)
        data_activos = data_proyectos_galeria[data_proyectos_galeria['estado'].isin(['Const./','Prev./'])]
        valormin = 3000000
        valormax = 100000000
        
        # Asegurar columna de precio
        if 'precio' not in data_proyectos_galeria and 'valor_P' in data_proyectos_galeria: 
            data_proyectos_galeria['precio'] = data_proyectos_galeria['valor_P']
        if 'precio' not in data_activos and 'valor_P' in data_activos: 
            data_activos['precio'] = data_activos['valor_P']
            
        # Calcular valor por m2
        def calcular_valormt2(df):
            if df.empty:
                return df
            df = df.copy()
            if 'precio' in df.columns and 'areaconstruida' in df.columns:
                # Crear la columna valormt2
                df['valormt2'] = None
                idd = (df['precio'] > 0) & (df['areaconstruida'] > 0) & (df['precio'].notna()) & (df['areaconstruida'].notna())
                if idd.any():
                    df.loc[idd, 'valormt2'] = df.loc[idd, 'precio'] / df.loc[idd, 'areaconstruida']
                    # Filtrar valores razonables solo si hay datos válidos
                    idd_filtro = (df['valormt2'] >= valormin) & (df['valormt2'] <= valormax) & (df['valormt2'].notna())
                    if idd_filtro.any():
                        df = df[idd_filtro]
            return df

        data_proyectos_galeria = calcular_valormt2(data_proyectos_galeria)
        data_activos = calcular_valormt2(data_activos)
        
        # ——————————————————————————————————————————————————————————————————————— #
        # 1. Estadísticas Originales (solo proyectos activos)
        # ——————————————————————————————————————————————————————————————————————— #
        if not data_activos.empty and 'valormt2' in data_activos.columns:
            # Pricing por tipo y estado
            df_tipo_estado = data_activos.groupby(['tipo','estado']).agg({'valormt2':'median'}).reset_index()
            df_tipo_estado.columns = ['tipo','estado','valormt2']
            pricing_by_tipo_estado = df_tipo_estado.to_dict(orient='records')
            
            # Pricing estadísticas generales
            df_estadisticas = data_activos.groupby('tipo').agg({'valormt2':['min','median','max']}).reset_index()
            df_estadisticas.columns = ['tipo','valormt2_min','valormt2_median','valormt2_max']
            pricing_estadisticas = df_estadisticas.to_dict(orient='records')

        # ——————————————————————————————————————————————————————————————————————— #
        # 2. Proyectos por Estado
        # ——————————————————————————————————————————————————————————————————————— #
        df_estado = data_proyectos_galeria.groupby('estado').agg({
            'codproyecto': 'nunique',
            'precio': ['median', 'count'],
            'areaconstruida': 'median'
        }).reset_index()
        df_estado.columns = ['estado', 'num_proyectos', 'precio_mediano', 'num_unidades', 'area_mediana']
        
        # Si tenemos valormt2, agregarlo
        if 'valormt2' in data_proyectos_galeria.columns:
            df_valormt2 = data_proyectos_galeria.groupby('estado')['valormt2'].median().reset_index()
            df_estado = df_estado.merge(df_valormt2, on='estado', how='left')
            df_estado.rename(columns={'valormt2': 'valormt2_mediano'}, inplace=True)
        
        proyectos_por_estado = df_estado.to_dict(orient='records')

        # ——————————————————————————————————————————————————————————————————————— #
        # 3. Proyectos por Constructora y Estado
        # ——————————————————————————————————————————————————————————————————————— #
        df_constructora = data_proyectos_galeria.groupby(['construye', 'estado']).agg({
            'codproyecto': 'nunique',
            'precio': 'median',
            'areaconstruida': 'median'
        }).reset_index()
        df_constructora.columns = ['constructora', 'estado', 'num_proyectos', 'precio_mediano', 'area_mediana']
        
        if 'valormt2' in data_proyectos_galeria.columns:
            df_valormt2_const = data_proyectos_galeria.groupby(['construye', 'estado'])['valormt2'].median().reset_index()
            df_constructora = df_constructora.merge(df_valormt2_const, left_on=['constructora', 'estado'], right_on=['construye', 'estado'], how='left')
            df_constructora.rename(columns={'valormt2': 'valormt2_mediano'}, inplace=True)
            df_constructora.drop('construye', axis=1, inplace=True)
            
        proyectos_por_constructora = df_constructora.to_dict(orient='records')

        # ——————————————————————————————————————————————————————————————————————— #
        # 4. Proyectos individuales
        # ——————————————————————————————————————————————————————————————————————— #
        df_proyecto = data_proyectos_galeria.groupby(['codproyecto', 'proyecto', 'construye', 'estado']).agg({
            'precio': 'median',
            'areaconstruida': 'median'
        }).reset_index()
        df_proyecto.columns = ['codproyecto', 'proyecto', 'constructora', 'estado', 'precio_mediano', 'area_mediana']
        
        if 'valormt2' in data_proyectos_galeria.columns:
            df_valormt2_proy = data_proyectos_galeria.groupby(['codproyecto', 'proyecto', 'construye', 'estado'])['valormt2'].median().reset_index()
            df_proyecto = df_proyecto.merge(df_valormt2_proy, left_on=['codproyecto', 'proyecto', 'constructora', 'estado'], right_on=['codproyecto', 'proyecto', 'construye', 'estado'], how='left')
            df_proyecto.rename(columns={'valormt2': 'valormt2_mediano'}, inplace=True)
            df_proyecto.drop('construye', axis=1, inplace=True)
            
        proyectos_por_proyecto = df_proyecto.to_dict(orient='records')

        # ——————————————————————————————————————————————————————————————————————— #
        # 4.1. Proyectos por Tipo de Inmueble
        # ——————————————————————————————————————————————————————————————————————— #
        if 'tipo' in data_proyectos_galeria.columns:
            # Estadísticas generales por tipo
            df_tipo = data_proyectos_galeria.groupby(['tipo', 'estado']).agg({
                'codproyecto': 'nunique',
                'precio': ['median', 'count'],
                'areaconstruida': 'median'
            }).reset_index()
            df_tipo.columns = ['tipo', 'estado', 'num_proyectos', 'precio_mediano', 'num_unidades', 'area_mediana']
            
            if 'valormt2' in data_proyectos_galeria.columns:
                df_valormt2_tipo = data_proyectos_galeria.groupby(['tipo', 'estado'])['valormt2'].median().reset_index()
                df_tipo = df_tipo.merge(df_valormt2_tipo, on=['tipo', 'estado'], how='left')
                df_tipo.rename(columns={'valormt2': 'valormt2_mediano'}, inplace=True)
            
            # Estadísticas adicionales por tipo
            df_tipo_detalle = data_proyectos_galeria.groupby('tipo').agg({
                'codproyecto': 'nunique',
                'precio': ['min', 'median', 'max', 'mean'],
                'areaconstruida': ['min', 'median', 'max', 'mean'],
                'habitaciones': ['min', 'max', 'median'],
                'banos': ['min', 'max', 'median'],
                'construye': 'nunique'  # Número de constructoras por tipo
            }).reset_index()
            
            # Aplanar columnas para estadísticas detalladas
            df_tipo_detalle.columns = ['tipo', 'num_proyectos_total', 'precio_min', 'precio_mediano', 
                                     'precio_max', 'precio_promedio', 'area_min', 'area_mediana', 
                                     'area_max', 'area_promedio', 'habitaciones_min', 'habitaciones_max', 
                                     'habitaciones_mediana', 'banos_min', 'banos_max', 'banos_mediana', 
                                     'num_constructoras']
            
            if 'valormt2' in data_proyectos_galeria.columns:
                df_valormt2_tipo_det = data_proyectos_galeria.groupby('tipo')['valormt2'].agg(['min', 'median', 'max', 'mean']).reset_index()
                df_valormt2_tipo_det.columns = ['tipo', 'valormt2_min', 'valormt2_mediano', 'valormt2_max', 'valormt2_promedio']
                df_tipo_detalle = df_tipo_detalle.merge(df_valormt2_tipo_det, on='tipo', how='left')
            
            # Agregar información de garajes si existe
            if 'garajes' in data_proyectos_galeria.columns:
                df_garajes_tipo = data_proyectos_galeria.groupby('tipo')['garajes'].agg(['min', 'max', 'median']).reset_index()
                df_garajes_tipo.columns = ['tipo', 'garajes_min', 'garajes_max', 'garajes_mediana']
                df_tipo_detalle = df_tipo_detalle.merge(df_garajes_tipo, on='tipo', how='left')
            
            proyectos_por_tipo = {
                'por_estado': df_tipo.to_dict(orient='records'),
                'resumen_detallado': df_tipo_detalle.to_dict(orient='records'),
                'solo_activos': []
            }
            
            # Estadísticas solo para proyectos activos por tipo
            if not data_activos.empty and 'tipo' in data_activos.columns:
                df_tipo_activos = data_activos.groupby('tipo').agg({
                    'codproyecto': 'nunique',
                    'precio': ['median', 'count'],
                    'areaconstruida': 'median'
                }).reset_index()
                df_tipo_activos.columns = ['tipo', 'num_proyectos_activos', 'precio_mediano_activos', 'num_unidades_activas', 'area_mediana_activos']
                
                if 'valormt2' in data_activos.columns:
                    df_valormt2_activos = data_activos.groupby('tipo')['valormt2'].median().reset_index()
                    df_tipo_activos = df_tipo_activos.merge(df_valormt2_activos, on='tipo', how='left')
                    df_tipo_activos.rename(columns={'valormt2': 'valormt2_mediano_activos'}, inplace=True)
                
                proyectos_por_tipo['solo_activos'] = df_tipo_activos.to_dict(orient='records')

        # ——————————————————————————————————————————————————————————————————————— #
        # 5. Resumen para Mapa
        # ——————————————————————————————————————————————————————————————————————— #
        columnas_mapa_base   = ['codproyecto', 'proyecto', 'construye', 'estado', 'latitud', 'longitud']
        columnas_disponibles = [col for col in columnas_mapa_base if col in data_proyectos_galeria.columns]
        
        if len(columnas_disponibles) >= 4:  # Al menos proyecto, constructora, estado y alguna coordenada
            agg_dict = {
                'precio': ['min', 'median', 'max'],
                'habitaciones': ['min', 'max'],
                'banos': ['min', 'max'],
                'areaconstruida': ['min', 'median', 'max']
            }
            
            # Solo agregar valormt2 si existe
            if 'valormt2' in data_proyectos_galeria.columns:
                agg_dict['valormt2'] = ['min', 'median', 'max']
            
            # Solo agregar garajes si existe
            if 'garajes' in data_proyectos_galeria.columns:
                agg_dict['garajes'] = ['min', 'max']
                
            df_mapa = data_proyectos_galeria.groupby(columnas_disponibles).agg(agg_dict).reset_index()
            
            # Aplanar columnas
            df_mapa.columns = [col[0] if col[1] == '' else f"{col[0]}_{col[1]}" for col in df_mapa.columns]
            resumen_proyectos_mapa = df_mapa.to_dict(orient='records')

        # ——————————————————————————————————————————————————————————————————————— #
        # 6. Tendencia de Precios por Tiempo
        # ——————————————————————————————————————————————————————————————————————— #
        if 'ano' in data_proyectos_galeria.columns and 'mes' in data_proyectos_galeria.columns:
            # Filtrar datos válidos de tiempo
            df_tiempo = data_proyectos_galeria[
                (data_proyectos_galeria['ano'].notna()) & 
                (data_proyectos_galeria['mes'].notna()) &
                (data_proyectos_galeria['precio'].notna()) &
                (data_proyectos_galeria['precio'] > 0)
            ].copy()
            
            if not df_tiempo.empty:
                agg_tiempo = {
                    'precio': 'median',
                    'codproyecto': 'nunique'
                }
                
                if 'valormt2' in df_tiempo.columns:
                    agg_tiempo['valormt2'] = 'median'
                
                df_tendencia = df_tiempo.groupby(['ano', 'mes', 'estado']).agg(agg_tiempo).reset_index()
                
                # Renombrar columnas según lo que tengamos
                rename_dict = {
                    'precio': 'precio_mediano',
                    'codproyecto': 'num_proyectos'
                }
                if 'valormt2' in agg_tiempo:
                    rename_dict['valormt2'] = 'valormt2_mediano'
                    
                df_tendencia.rename(columns=rename_dict, inplace=True)
                tendencia_precio_tiempo = df_tendencia.to_dict(orient='records')

        # ——————————————————————————————————————————————————————————————————————— #
        # 7. Tipología de Inmuebles
        # ——————————————————————————————————————————————————————————————————————— #
        columnas_tipologia_base = ['habitaciones', 'banos', 'estado']
        columnas_tip_disponibles = [col for col in columnas_tipologia_base if col in data_proyectos_galeria.columns]
        
        if len(columnas_tip_disponibles) >= 2:  # Al menos habitaciones y algún otro campo
            # Tipología para proyectos activos
            if not data_activos.empty:
                group_cols = [col for col in ['habitaciones', 'banos'] if col in data_activos.columns]
                if 'garajes' in data_activos.columns:
                    group_cols.append('garajes')
                    
                if group_cols:
                    df_tipologia_activos = data_activos.groupby(group_cols).agg({
                        'codproyecto': 'nunique',
                        'codinmueble': 'count'
                    }).reset_index()
                    df_tipologia_activos.columns = group_cols + ['num_proyectos', 'num_unidades']
                else:
                    df_tipologia_activos = pd.DataFrame()
            else:
                df_tipologia_activos = pd.DataFrame()
            
            # Tipología para todos los estados
            group_cols_todos = [col for col in ['habitaciones', 'banos', 'estado'] if col in data_proyectos_galeria.columns]
            if 'garajes' in data_proyectos_galeria.columns:
                group_cols_todos.insert(-1, 'garajes')  # Insertar garajes antes de estado
                
            if group_cols_todos:
                df_tipologia_todos = data_proyectos_galeria.groupby(group_cols_todos).agg({
                    'codproyecto': 'nunique',
                    'codinmueble': 'count'
                }).reset_index()
                df_tipologia_todos.columns = group_cols_todos + ['num_proyectos', 'num_unidades']
            else:
                df_tipologia_todos = pd.DataFrame()
            
            tipologia_inmuebles = {
                'activos': df_tipologia_activos.to_dict(orient='records') if not df_tipologia_activos.empty else [],
                'todos_estados': df_tipologia_todos.to_dict(orient='records') if not df_tipologia_todos.empty else []
            }

        # ——————————————————————————————————————————————————————————————————————— #
        # 8. Estadísticas por Zona
        # ——————————————————————————————————————————————————————————————————————— #
        if 'zona' in data_proyectos_galeria.columns:
            agg_zona = {
                'codproyecto': 'nunique',
                'precio': 'median',
                'areaconstruida': 'median'
            }
            
            if 'valormt2' in data_proyectos_galeria.columns:
                agg_zona['valormt2'] = 'median'
            
            df_zona = data_proyectos_galeria.groupby(['zona', 'estado']).agg(agg_zona).reset_index()
            
            rename_zona = {
                'codproyecto': 'num_proyectos',
                'precio': 'precio_mediano',
                'areaconstruida': 'area_mediana'
            }
            if 'valormt2' in agg_zona:
                rename_zona['valormt2'] = 'valormt2_mediano'
                
            df_zona.rename(columns=rename_zona, inplace=True)
            estadisticas_por_zona = df_zona.to_dict(orient='records')

        # ——————————————————————————————————————————————————————————————————————— #
        # 9. Estadísticas Generales
        # ——————————————————————————————————————————————————————————————————————— #
        estadisticas_generales = {
            'total_proyectos': int(data_proyectos_galeria['codproyecto'].nunique()) if not data_proyectos_galeria.empty else 0,
            'total_unidades': int(len(data_proyectos_galeria)) if not data_proyectos_galeria.empty else 0,
            'proyectos_activos': int(data_activos['codproyecto'].nunique()) if not data_activos.empty else 0,
            'unidades_activas': int(len(data_activos)) if not data_activos.empty else 0,
            'constructoras_activas': int(data_activos['construye'].nunique()) if not data_activos.empty and 'construye' in data_activos.columns else 0,
            'zonas_activas': int(data_activos['zona'].nunique()) if not data_activos.empty and 'zona' in data_activos.columns else 0,
            'precio_promedio': float(data_proyectos_galeria['precio'].mean()) if not data_proyectos_galeria.empty and 'precio' in data_proyectos_galeria.columns else 0,
            'precio_mediano': float(data_proyectos_galeria['precio'].median()) if not data_proyectos_galeria.empty and 'precio' in data_proyectos_galeria.columns else 0,
            'area_promedio': float(data_proyectos_galeria['areaconstruida'].mean()) if not data_proyectos_galeria.empty and 'areaconstruida' in data_proyectos_galeria.columns else 0
        }
        
        # Agregar estadísticas de valormt2 solo si existe y tiene datos válidos
        if 'valormt2' in data_proyectos_galeria.columns and not data_proyectos_galeria['valormt2'].isna().all():
            estadisticas_generales.update({
                'precio_m2_min_mercado': float(data_proyectos_galeria['valormt2'].min()),
                'precio_m2_max_mercado': float(data_proyectos_galeria['valormt2'].max()),
                'precio_m2_mediano_mercado': float(data_proyectos_galeria['valormt2'].median()),
                'precio_m2_promedio_mercado': float(data_proyectos_galeria['valormt2'].mean()),
            })
            
            if not data_activos.empty and 'valormt2' in data_activos.columns and not data_activos['valormt2'].isna().all():
                estadisticas_generales['precio_m2_mediano_activos'] = float(data_activos['valormt2'].median())
                estadisticas_generales['precio_m2_promedio_activos'] = float(data_activos['valormt2'].mean())

        # ——————————————————————————————————————————————————————————————————————— #
        # 10. Indicadores de Mercado
        # ——————————————————————————————————————————————————————————————————————— #
        indicadores_mercado = {
            'distribucion_precios': {},
            'crecimiento_precio_m2': 0,
            'promedio_area_construida': float(data_activos['areaconstruida'].mean()) if not data_activos.empty and 'areaconstruida' in data_activos.columns else 0,
            'tipo_inmueble_mas_comun': data_activos['tipo'].mode().iloc[0] if not data_activos.empty and 'tipo' in data_activos.columns and not data_activos['tipo'].mode().empty else 'N/A',
            'estado_mas_comun': data_proyectos_galeria['estado'].mode().iloc[0] if not data_proyectos_galeria.empty and not data_proyectos_galeria['estado'].mode().empty else 'N/A',
            'constructora_mas_activa': data_activos['construye'].mode().iloc[0] if not data_activos.empty and 'construye' in data_activos.columns and not data_activos['construye'].mode().empty else 'N/A'
        }
        
        # Distribución de precios por rangos (solo si tenemos valormt2)
        if not data_activos.empty and 'valormt2' in data_activos.columns and not data_activos['valormt2'].isna().all():
            # Distribución por rangos de precio
            rangos_precio = [
                (0, 5000000, 'Bajo'),
                (5000000, 10000000, 'Medio'),
                (10000000, 15000000, 'Medio-Alto'),
                (15000000, float('inf'), 'Alto')
            ]
            
            def clasificar_precio(precio):
                if pd.isna(precio):
                    return 'No clasificado'
                for min_val, max_val, categoria in rangos_precio:
                    if min_val <= precio < max_val:
                        return categoria
                return 'No clasificado'
            
            data_activos_temp = data_activos.copy()
            data_activos_temp['rango_precio'] = data_activos_temp['valormt2'].apply(clasificar_precio)
            distribucion_precios = data_activos_temp['rango_precio'].value_counts().to_dict()
            indicadores_mercado['distribucion_precios'] = distribucion_precios
            
            # Crecimiento de precios (si hay datos de tendencia)
            if len(tendencia_precio_tiempo) > 1:
                # Filtrar tendencias de proyectos activos que tengan valormt2_mediano
                tendencia_activos = [t for t in tendencia_precio_tiempo if t['estado'] in ['Const./', 'Prev./'] and 'valormt2_mediano' in t and t['valormt2_mediano'] is not None]
                if len(tendencia_activos) >= 2:
                    # Ordenar por año y mes
                    tendencia_activos = sorted(tendencia_activos, key=lambda x: (x['ano'], x['mes']))
                    precio_actual = tendencia_activos[-1]['valormt2_mediano']
                    precio_anterior = tendencia_activos[-2]['valormt2_mediano']
                    if precio_anterior and precio_anterior > 0:
                        crecimiento = ((precio_actual - precio_anterior) / precio_anterior) * 100
                        indicadores_mercado['crecimiento_precio_m2'] = round(crecimiento, 2)
        
        # Si no hay valormt2, usar precio directo para distribución básica
        elif not data_activos.empty and 'precio' in data_activos.columns:
            # Distribución básica por precio total
            precio_q25 = data_activos['precio'].quantile(0.25)
            precio_q50 = data_activos['precio'].quantile(0.50)
            precio_q75 = data_activos['precio'].quantile(0.75)
            
            def clasificar_precio_total(precio):
                if pd.isna(precio):
                    return 'No clasificado'
                if precio <= precio_q25:
                    return 'Económico'
                elif precio <= precio_q50:
                    return 'Medio'
                elif precio <= precio_q75:
                    return 'Medio-Alto'
                else:
                    return 'Premium'
            
            data_activos_temp = data_activos.copy()
            data_activos_temp['rango_precio'] = data_activos_temp['precio'].apply(clasificar_precio_total)
            distribucion_precios = data_activos_temp['rango_precio'].value_counts().to_dict()
            indicadores_mercado['distribucion_precios'] = distribucion_precios

    # ——————————————————————————————————————————————————————————————————————— #
    # Metadata
    # ——————————————————————————————————————————————————————————————————————— #
    meta = {
        "timestamp": datetime.now().isoformat(),
        "requestId": str(uuid.uuid4()),
        "filtersApplied": {
            "barmanpre": inputvar.get("barmanpre"),
            "chip": inputvar.get("chip"),
            "matriculaInmobiliaria": inputvar.get("matriculainmobiliaria"),
        },
        "dataInfo": {
            "totalRecords": len(data_proyectos_galeria) if not data_proyectos_galeria.empty else 0,
            "activeRecords": len(data_activos) if not data_activos.empty else 0,
            "dateRange": {
                "minDate": data_proyectos_galeria[['ano', 'mes']].min().to_dict() if not data_proyectos_galeria.empty and 'ano' in data_proyectos_galeria.columns else None,
                "maxDate": data_proyectos_galeria[['ano', 'mes']].max().to_dict() if not data_proyectos_galeria.empty and 'ano' in data_proyectos_galeria.columns else None
            }
        }
    } 
    
    # ——————————————————————————————————————————————————————————————————————— #
    # Output Final para Dashboard
    # ——————————————————————————————————————————————————————————————————————— #
    output = {
        "meta": meta,
        
        # Estadísticas originales
        "pricingResumen": pricing_estadisticas,
        "pricingByType": pricing_by_tipo_estado,
        
        # Nuevas estadísticas para dashboard
        "estadisticasGenerales": estadisticas_generales,
        "indicadoresMercado": indicadores_mercado,
        
        # Análisis por segmentos
        "proyectosPorEstado": proyectos_por_estado,
        "proyectosPorConstructora": proyectos_por_constructora,
        "proyectosPorProyecto": proyectos_por_proyecto,
        "proyectosPorTipo": proyectos_por_tipo,
        "estadisticasPorZona": estadisticas_por_zona,
        
        # Para mapas y visualizaciones
        "resumenProyectosMapa": resumen_proyectos_mapa,
        "tendenciaPrecioTiempo": tendencia_precio_tiempo,
        
        # Tipología de inmuebles
        "tipologiaInmuebles": tipologia_inmuebles
    }
    
    output = clean_json(output)
    
    return output

def get_pricig_proyectos(data=pd.DataFrame()):
    
    user        = 'doadmin' #st.secrets["user_bigdata"]
    password    = 'AVNS_3p-UcEv0kzXLYLfzPIF' # st.secrets["password_bigdata"]
    host        = 'urbex-production-do-user-15728169-0.k.db.ondigitalocean.com' # st.secrets["host_bigdata_lectura"]
    schema      = 'bigdata' #st.secrets["schema_bigdata"]
    port        = 25060 #st.secrets["port_bigdata"]
    
    dataprecios = pd.DataFrame()
    lista       = list(data['codproyecto'].unique()) if not data.empty and 'codproyecto' in data else []
    if isinstance(lista,list) and lista!=[]:
        lista       = ",".join([str(x) for x in lista])
        query       = f" codproyecto IN ({lista})"
        engine      = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{schema}')
        dataprecios = pd.read_sql_query(f"SELECT *  FROM  bigdata.bogota_galeria_precios WHERE {query}" , engine)
        engine.dispose()
        
    if not data.empty and not dataprecios.empty:
        variables       = [x for x in  list(dataprecios) if x not in list(data)]
        variables       = ['codproyecto','codinmueble'] + variables
        data.index      = range(len(data))
        data['id_base'] = range(len(data))
        data      = dataprecios[variables].merge(data,on=['codproyecto','codinmueble'],how='outer')
    
    return data
