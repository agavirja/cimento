import pandas as pd
import uuid
from datetime import datetime

from functions.general_functions import  get_multiple_data_bucket, generar_codigo, selectdata
from functions.clean_json import clean_json

def ctl(inputvar={}):

    # ——————————————————————————————————————————————————————————————————————— #
    # Variables inputvar: 
    # ——————————————————————————————————————————————————————————————————————— #
    barmanpre             = inputvar['barmanpre'] if 'barmanpre' in inputvar and isinstance(inputvar['barmanpre'],str) else None
    chip                  = inputvar['chip'] if 'chip' in inputvar and isinstance(inputvar['chip'],str) else None
    matriculainmobiliaria = inputvar['matriculainmobiliaria'] if 'matriculainmobiliaria' in inputvar and isinstance(inputvar['matriculainmobiliaria'],str) else None
    output                = {}
        
    # ——————————————————————————————————————————————————————————————————————— #
    # Estructura de datos
    # ——————————————————————————————————————————————————————————————————————— #
    datactl = pd.DataFrame()
    if isinstance(barmanpre,str) and barmanpre!='':

        lista   = [x.strip() for x in barmanpre.split('|') if isinstance(x, str)]
        ruta    = "_snr/_bogota_ctl_manzana"
        formato = []
        for items in lista:
            manzcodigo = items[:9]
            filename   = generar_codigo(manzcodigo)
            formato.append({
                "file":   f"{ruta}/{filename}.parquet",
                "name":   "ctl",
                "barmanpre": items,
                "data":   pd.DataFrame(),
                "run": True,
            })
            
        resultado = get_multiple_data_bucket(formato, barmanpre=None, max_workers=10)
        datactl   = selectdata(resultado,"ctl", barmanpre=lista)

    # ——————————————————————————————————————————————————————————————————————— #
    # Estructura de la API
    # ——————————————————————————————————————————————————————————————————————— #
    
        # Filtro por chip:
    if not datactl.empty and isinstance(chip,str) and chip!='' and 'chip' in datactl:
        datactl = datactl[datactl['chip']==chip]

        # Filtro por matriculainmobiliaria:
    if not datactl.empty and isinstance(matriculainmobiliaria,str) and matriculainmobiliaria!='' and 'matriculainmobiliaria' in datactl:
        datactl = datactl[datactl['matriculainmobiliaria']==matriculainmobiliaria]
        
        
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
        "certificados": None,
        "estadisticas": None
    }
    
    if not datactl.empty:
        lista_ctls = []
        
        if 'fecha' in datactl:
            datactl['fecha'] = pd.to_datetime(datactl['fecha'], errors='coerce')
            datactl['fecha'] = datactl['fecha'].dt.strftime('%Y-%m-%d')
        
        # Ordenar por fecha descendente
        if 'fecha' in datactl:
            datactl = datactl.sort_values('fecha', ascending=False)
        
        
        if 'docid' in datactl:
            datactl = datactl.sort_values(by=['docid'], ascending=False)
            if 'link' not in datactl:
                datactl['link'] = datactl['docid'].apply(lambda x: f"https://radicacion.supernotariado.gov.co/app/static/ServletFilesViewer?docId={x}") if 'docid' in datactl  else ''
        if 'link' not in datactl:
            datactl['link'] = ''

        # Convertir datos para tabla
        for _, row in datactl.iterrows():
            item = {
                'link': row.get('link', ''),
                "docid": row.get('docid', None),
                "url": row['url'] if 'url' in row else None,
                "fecha": row['fecha'] if 'fecha' in row else None,
                "predirecc": row['predirecc'] if 'predirecc' in row else None,
                "direccion": row['direccion'] if 'direccion' in row else None,  # Campo alternativo
                "chip": row['chip'] if 'chip' in row else None,
                "matriculainmobiliaria": row['matriculainmobiliaria'] if 'matriculainmobiliaria' in row else None,
                "cedula_catastral": row['cedula_catastral'] if 'cedula_catastral' in row and pd.notnull(row['cedula_catastral']) else row['precedcata'] if 'precedcata' in row and pd.notnull(row['precedcata']) else None,  # Campo alternativo
                "preaconst": float(row['preaconst']) if 'preaconst' in row and pd.notnull(row['preaconst']) else None
            }
            lista_ctls.append(item)
        
        # Estadísticas adicionales
        estadisticas = {
            "totalCertificados": len(lista_ctls),
            "rangoFechas": {
                "fecha_mas_antigua": datactl['fecha'].min() if 'fecha' in datactl and not datactl['fecha'].isnull().all() else None,
                "fecha_mas_reciente": datactl['fecha'].max() if 'fecha' in datactl and not datactl['fecha'].isnull().all() else None
            },
            "porAno": {}
        }
        
        if 'fecha' in datactl and not datactl['fecha'].isnull().all():
            datactl['ano'] = pd.to_datetime(datactl['fecha']).dt.year
            por_ano = datactl['ano'].value_counts().sort_index()
            estadisticas["porAno"] = por_ano.to_dict()
        
        output["certificados"] = lista_ctls
        output["estadisticas"] = estadisticas
        
    output = clean_json(output)
    
    return output 


def licencias_construccion(inputvar={}):

    # ——————————————————————————————————————————————————————————————————————— #
    # Variables inputvar: 
    # ——————————————————————————————————————————————————————————————————————— #
    barmanpre = inputvar['barmanpre'] if 'barmanpre' in inputvar and isinstance(inputvar['barmanpre'],str) else None
    output    = {}
        
    # ——————————————————————————————————————————————————————————————————————— #
    # Estructura de datos
    # ——————————————————————————————————————————————————————————————————————— #
    datalicencias = pd.DataFrame()
    if isinstance(barmanpre,str) and barmanpre!='':

        lista   = [x.strip() for x in barmanpre.split('|') if isinstance(x, str)]
        ruta    = "_caracteristicas/_bogota_licencias_construccion_manzana"
        formato = []
        for items in lista:
            manzcodigo = items[:9]
            filename   = generar_codigo(manzcodigo)
            formato.append({
                "file":   f"{ruta}/{filename}.parquet",
                "name":   "licencias",
                "barmanpre": items,
                "data":   pd.DataFrame(),
                "run": True,
            })
            
        resultado     = get_multiple_data_bucket(formato, barmanpre=None, max_workers=10)
        datalicencias = selectdata(resultado,"licencias", barmanpre=lista)

    # ——————————————————————————————————————————————————————————————————————— #
    # Estructura de la API
    # ——————————————————————————————————————————————————————————————————————— #

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
        "licencias": None,
        "estadisticas": None
        }
    if not datalicencias.empty:
        
        lista_licencias = []
        
        if 'fecha' in datalicencias:
            datalicencias['fecha'] = pd.to_datetime(datalicencias['fecha'], errors='coerce')
            datalicencias['fecha'] = datalicencias['fecha'].dt.strftime('%Y-%m-%d')
        
        # Ordenar por fecha descendente
        if 'fecha' in datalicencias:
            datalicencias = datalicencias.sort_values('fecha', ascending=False)
        
        # Convertir datos para tabla
        for _, row in datalicencias.iterrows():
            item = {
                "licencia": row['licencia'] if 'licencia' in row else None,
                "numero_licencia": row['numero_licencia'] if 'numero_licencia' in row else None,  # Campo alternativo
                "formato_direccion": row['formato_direccion'] if 'formato_direccion' in row else None,
                "direccion": row['direccion'] if 'direccion' in row else None,  # Campo alternativo
                "tramite": row['tramite'] if 'tramite' in row else None,
                "tipo_tramite": row['tipo_tramite'] if 'tipo_tramite' in row else None,  # Campo alternativo
                "propietarios": row['propietarios'] if 'propietarios' in row else None,
                "propietario": row['propietario'] if 'propietario' in row else None,  # Campo alternativo
                "proyecto": row['proyecto'] if 'proyecto' in row else None,
                "nombre_proyecto": row['nombre_proyecto'] if 'nombre_proyecto' in row else None,  # Campo alternativo
                "estado": row['estado'] if 'estado' in row else None,
                "estado_tramite": row['estado_tramite'] if 'estado_tramite' in row else None,  # Campo alternativo
                "curaduria": row['curaduria'] if 'curaduria' in row else None,
                "fecha": row['fecha'] if 'fecha' in row else None,
                "fecha_expedicion": row['fecha_expedicion'] if 'fecha_expedicion' in row else None,  # Campo alternativo
                "area_construir": float(row['area_construir']) if 'area_construir' in row and pd.notnull(row['area_construir']) else None,
                "area_total": float(row['area_total']) if 'area_total' in row and pd.notnull(row['area_total']) else None,
                "pisos": int(row['pisos']) if 'pisos' in row and pd.notnull(row['pisos']) else None,
                "unidades": int(row['unidades']) if 'unidades' in row and pd.notnull(row['unidades']) else None
            }
            lista_licencias.append(item)
        
        # Estadísticas adicionales
        estadisticas = {
            "totalLicencias": len(lista_licencias),
            "rangoFechas": {
                "fecha_mas_antigua": datalicencias['fecha'].min() if 'fecha' in datalicencias and not datalicencias['fecha'].isnull().all() else None,
                "fecha_mas_reciente": datalicencias['fecha'].max() if 'fecha' in datalicencias and not datalicencias['fecha'].isnull().all() else None
            },
            "licenciasAno": {},
            "estado": {},
            "curaduria": {},
            "tipoTramite": {}
        }
        
        if 'fecha' in datalicencias and not datalicencias['fecha'].isnull().all():
            datalicencias['ano'] = pd.to_datetime(datalicencias['fecha']).dt.year
            por_ano = datalicencias['ano'].value_counts().sort_index()
            estadisticas["licenciasAno"] = por_ano.to_dict()
        
        if 'estado' in datalicencias:
            estado = datalicencias['estado'].value_counts()
            estadisticas["estado"] = estado.to_dict()
        elif 'estado_tramite' in datalicencias:
            estado = datalicencias['estado_tramite'].value_counts()
            estadisticas["estado"] = estado.to_dict()
        
        if 'curaduria' in datalicencias:
            curaduria = datalicencias['curaduria'].value_counts()
            estadisticas["curaduria"] = curaduria.to_dict()
        
        if 'tramite' in datalicencias:
            por_tramite = datalicencias['tramite'].value_counts()
            estadisticas["tipoTramite"] = por_tramite.to_dict()
        elif 'tipo_tramite' in datalicencias:
            por_tramite = datalicencias['tipo_tramite'].value_counts()
            estadisticas["tipoTramite"] = por_tramite.to_dict()
        
        # Estadísticas numéricas
        if 'area_construir' in datalicencias:
            areas = datalicencias['area_construir'].dropna()
            if not areas.empty:
                estadisticas["areaConstruir"] = {
                    "total": float(areas.sum()),
                    "promedio": float(areas.mean()),
                    "mediana": float(areas.median()),
                    "min": float(areas.min()),
                    "max": float(areas.max())
                }
        
        if 'unidades' in datalicencias:
            unidades = datalicencias['unidades'].dropna()
            if not unidades.empty:
                estadisticas["unidades"] = {
                    "total": int(unidades.sum()),
                    "promedio": float(unidades.mean()),
                    "mediana": float(unidades.median()),
                    "min": int(unidades.min()),
                    "max": int(unidades.max())
                }
        
        output["licencias"]    = lista_licencias
        output["estadisticas"] = estadisticas

    output = clean_json(output)
    
    return output