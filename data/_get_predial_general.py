import pandas as pd
import uuid
from datetime import datetime

from functions.general_functions import  get_multiple_data_bucket, generar_codigo, selectdata
from functions.getuso_destino import usosuelo_class
from functions.clean_json import clean_json

def main(inputvar={}):

    # ——————————————————————————————————————————————————————————————————————— #
    # Variables inputvar: 
    # ——————————————————————————————————————————————————————————————————————— #
    barmanpre             = inputvar['barmanpre'] if 'barmanpre' in inputvar and isinstance(inputvar['barmanpre'],str) else None
    chip                  = inputvar['chip'] if 'chip' in inputvar and isinstance(inputvar['chip'],str) else None
    matriculainmobiliaria = inputvar['matriculainmobiliaria'] if 'matriculainmobiliaria' in inputvar and isinstance(inputvar['matriculainmobiliaria'],str) else None
    tabla_export          = inputvar['get_tabla'] if 'get_tabla' in  inputvar else True
    tabla_lastyear        = inputvar.get('get_tabla_last_year',True)

    # ——————————————————————————————————————————————————————————————————————— #
    # Estructura de datos
    # ——————————————————————————————————————————————————————————————————————— #
    data_prediales_actuales = pd.DataFrame()
    if isinstance(barmanpre,str) and barmanpre!='':

        lista            = [x.strip() for x in barmanpre.split('|') if isinstance(x, str)]
        lista_manzcodigo = list(set([x[:9] for x in lista]))
        ruta             = "_prediales/_bogota_prediales_actuales_manzana"
        formato          = []
        for items in lista_manzcodigo:
            filename   = generar_codigo(items)
            formato.append({
                "file":   f"{ruta}/{filename}.parquet",
                "name":   "prediales_actuales",
                "barmanpre": None,
                "data":   pd.DataFrame(),
                "run": True,
            })
            
        resultado               = get_multiple_data_bucket(formato, barmanpre=None, max_workers=10)
        data_prediales_actuales = selectdata(resultado,"prediales_actuales", barmanpre=lista)
        if not data_prediales_actuales.empty:
            data_prediales_actuales = data_prediales_actuales[data_prediales_actuales['barmanpre'].isin(lista)]


    # ——————————————————————————————————————————————————————————————————————— #
    # Filtros a la data
    # ——————————————————————————————————————————————————————————————————————— #
    
        # Filtro por chip:
    if not data_prediales_actuales.empty and isinstance(chip,str) and chip!='' and 'chip' in data_prediales_actuales:
        data_prediales_actuales = data_prediales_actuales[data_prediales_actuales['chip']==chip]

        # Filtro por matriculainmobiliaria:
    if not data_prediales_actuales.empty and isinstance(matriculainmobiliaria,str) and matriculainmobiliaria!='' and 'matriculainmobiliaria' in data_prediales_actuales:
        data_prediales_actuales = data_prediales_actuales[data_prediales_actuales['matriculainmobiliaria']==matriculainmobiliaria]
        
        # Valor por metro cuadrado
    if not data_prediales_actuales.empty:

        for j in ['avaluo_catastral','impuesto_predial', 'preaconst']:
            if j in data_prediales_actuales:
                data_prediales_actuales[j] = pd.to_numeric(data_prediales_actuales[j], errors='coerce')

        for i in ['valor_avaluo_mt2','valor_predial_mt2','valor_avaluo_suelo_mt2','valor_predial_suelo_mt2']:
            data_prediales_actuales[i] = None
            
        idd = (data_prediales_actuales['avaluo_catastral']>0) & (data_prediales_actuales['preaconst']>0)
        data_prediales_actuales.loc[idd,'valor_avaluo_mt2'] = data_prediales_actuales.loc[idd,'avaluo_catastral']/data_prediales_actuales.loc[idd,'preaconst']
        
        idd = (data_prediales_actuales['impuesto_predial']>0) & (data_prediales_actuales['preaconst']>0)
        data_prediales_actuales.loc[idd,'valor_predial_mt2'] = data_prediales_actuales.loc[idd,'impuesto_predial']/data_prediales_actuales.loc[idd,'preaconst']
        
        idd = (data_prediales_actuales['avaluo_catastral'] > 0) & (data_prediales_actuales['preaterre'] > 0)
        data_prediales_actuales.loc[idd,'valor_avaluo_suelo_mt2'] = data_prediales_actuales.loc[idd, 'avaluo_catastral'] / data_prediales_actuales.loc[idd, 'preaterre']
        
        idd = (data_prediales_actuales['impuesto_predial'] > 0) & (data_prediales_actuales['preaterre'] > 0)
        data_prediales_actuales.loc[idd, 'valor_predial_suelo_mt2'] = data_prediales_actuales.loc[idd, 'impuesto_predial'] / data_prediales_actuales.loc[idd, 'preaterre']


        # Filtro segun parametros
    if not data_prediales_actuales.empty:
        areamin       = inputvar.get('areamin', 0)
        areamax       = inputvar.get('areamax', 0)
        prevetustzmin = inputvar.get('desde_antiguedad', 0)
        prevetustzmax = inputvar.get('hasta_antiguedad', 0)
        estratomin    = inputvar.get('estratomin', 0)
        estratomax    = inputvar.get('estratomax', 0)
        precuso       = inputvar.get('precuso', [])
    
        if areamin>0 and 'preaconst' in data_prediales_actuales:
            data_prediales_actuales = data_prediales_actuales[data_prediales_actuales['preaconst']>=areamin]
        if areamax>0 and 'preaconst' in data_prediales_actuales:
            data_prediales_actuales = data_prediales_actuales[data_prediales_actuales['preaconst']<=areamax]

        if prevetustzmin>0 and 'prevetustz' in data_prediales_actuales:
            data_prediales_actuales = data_prediales_actuales[data_prediales_actuales['prevetustz']>=prevetustzmin]
        if prevetustzmax>0 and 'prevetustz' in data_prediales_actuales:
            data_prediales_actuales = data_prediales_actuales[data_prediales_actuales['prevetustz']<=prevetustzmax]

        if estratomin>0 and 'estrato' in data_prediales_actuales:
            data_prediales_actuales = data_prediales_actuales[data_prediales_actuales['estrato']>=estratomin]
        if estratomax>0 and 'estrato' in data_prediales_actuales:
            data_prediales_actuales = data_prediales_actuales[data_prediales_actuales['estrato']<=estratomax]

        if isinstance(precuso,list) and precuso!=[] and 'precuso' in data_prediales_actuales:
            data_prediales_actuales = data_prediales_actuales[data_prediales_actuales['precuso'].isin(precuso)]
            
            
    # ——————————————————————————————————————————————————————————————————————— #
    # Estructura de la API
    # ——————————————————————————————————————————————————————————————————————— #
    tabla_prediales       = []
    avaluo_catastral      = {"valorMt2": None, "totalAvaluo": None}
    impuesto_predial      = {"valorMt2": None, "totalAvaluo": None}
    suelo                 = {"valorAvaluoSueloMt2": None, "valorPredialSueloMt2": None}
    propietarios          = None
    avaluo_mt2_historico  = []
    predial_mt2_historico = []
    estadisticas_area     = {"min":None,"q1": None,"median": None, "q3": None, "max":None,"mean":None}
        
    if not data_prediales_actuales.empty:
        
        #---------------------------------------------------------------------#
        # Tabla
        if tabla_export:
            
            variables      = ['chip','matriculainmobiliaria','direccion', 'year', 'avaluo_catastral', 'impuesto_predial', 'impuesto_ajustado', 'impuesto_total', 'url', 'tipo', 'identificacion', 'nombre', 'copropiedad', 'tipoPropietario', 'pagado', 'indPago', 'preaconst', 'preaterre', 'prevetustz', 'precuso', 'telefonos', 'email']
            variables      = [x for x in variables if x in data_prediales_actuales]
            df             = data_prediales_actuales[variables]
            datos_tabla    = df.where(pd.notnull(df), None)
            datos_tabla    = datos_tabla.sort_values(by=['chip', 'year','preaconst'], ascending=False)
            data_uso_suelo = usosuelo_class()
            
            if tabla_lastyear:
                datos_tabla = datos_tabla.sort_values(by=['chip', 'year'], ascending=False).drop_duplicates(subset=['chip'], keep='first')
            
            if not data_uso_suelo.empty:
                data_uso_suelo = data_uso_suelo.drop_duplicates(subset='precuso')
                if 'precuso' in datos_tabla:
                    datos_tabla = datos_tabla.merge(data_uso_suelo[['precuso', 'usosuelo']], on='precuso', how='left', validate='m:1')
            if not datos_tabla.empty:
                tabla_prediales = datos_tabla.to_dict(orient='records')

        #------------------#
        # Avalúo catastral #        
        df         = data_prediales_actuales[data_prediales_actuales['avaluo_catastral'].notnull()].sort_values(by=['chip', 'year', 'avaluo_catastral'], ascending=False).drop_duplicates(subset=['chip'], keep='first')
        idd        = df['precuso'].isin(['048','049','051','098'])
        df         = df[~idd]
        if not df.empty:
            df['isin'] = 1
            data_prediales_avaluo  = df.groupby(['isin']).agg({'valor_avaluo_mt2': 'median','avaluo_catastral': 'sum'}).reset_index()
            avaluo_catastral      = {"valorMt2": float(data_prediales_avaluo['valor_avaluo_mt2'].iloc[0]) if not data_prediales_avaluo.empty and 'valor_avaluo_mt2' in data_prediales_avaluo else None,"totalAvaluo": float(data_prediales_avaluo['avaluo_catastral'].iloc[0]) if not data_prediales_avaluo.empty and 'avaluo_catastral' in data_prediales_avaluo else None}
    
        #------------------#
        # Impuesto predial #
        df         = data_prediales_actuales[data_prediales_actuales['impuesto_predial'].notnull()].sort_values(by=['chip', 'year', 'impuesto_predial'], ascending=False).drop_duplicates(subset=['chip'], keep='first')
        idd        = df['precuso'].isin(['048','049','051','098'])
        df         = df[~idd]
        if not df.empty:
            df['isin'] = 1
            data_prediales_prediales = df.groupby(['isin']).agg({'valor_predial_mt2': 'median','impuesto_predial': 'sum'}).reset_index()
            impuesto_predial         = {"valorMt2": float(data_prediales_prediales['valor_predial_mt2'].iloc[0]) if not data_prediales_prediales.empty and 'valor_predial_mt2' in data_prediales_prediales else None,"totalImpuesto": float(data_prediales_prediales['impuesto_predial'].iloc[0]) if not data_prediales_prediales.empty and 'impuesto_predial' in data_prediales_prediales else None}
    
        #---------------#
        # Valores suelo #
        df         = data_prediales_actuales[data_prediales_actuales['avaluo_catastral'].notnull()].sort_values(by=['chip', 'year', 'avaluo_catastral'], ascending=False).drop_duplicates(subset=['chip'], keep='first')
        idd        = df['precuso'].isin(['048','049','051','098'])
        df         = df[~idd]
        if not df.empty:
            df['isin'] = 1
            data_prediales_suelo = df.groupby(['isin']).agg({'valor_avaluo_suelo_mt2': 'median','valor_predial_suelo_mt2': 'median'}).reset_index()
            suelo                = {"valorAvaluoSueloMt2": float(data_prediales_suelo['valor_avaluo_suelo_mt2'].iloc[0]) if not data_prediales_suelo.empty and 'valor_avaluo_suelo_mt2' in data_prediales_suelo else None,"valorPredialSueloMt2": float(data_prediales_suelo['valor_predial_suelo_mt2'].iloc[0]) if not data_prediales_suelo.empty and 'valor_predial_suelo_mt2' in data_prediales_suelo else None}
    
        #--------------#
        # Propietarios #
        idd = data_prediales_actuales['precuso'].isin(['048','049','051','098']) # Parqueaderos PH y Depositos o lockers PH
        df  = data_prediales_actuales[~idd]
        if not df.empty:
            dv         = df.groupby(['chip'])['year'].max().reset_index()
            dv.columns = ['chip','maxyear']
            df         = df.merge(dv,on='chip',how='left',validate='m:1')
            df         = df[df['year']==df['maxyear']]
            df         = df[df['identificacion'].notnull()].sort_values(by=['chip', 'year', 'identificacion'], ascending=False).drop_duplicates(subset=['identificacion'], keep='first')
            propietarios = int(len(df)) if not df.empty else None
    
        # Estadisticas de avaluo por ano
        df = data_prediales_actuales[data_prediales_actuales['valor_avaluo_mt2'].notnull()].sort_values(by=['chip', 'year', 'valor_avaluo_mt2'], ascending=False).drop_duplicates(subset=['chip','year'], keep='first')
        if not df.empty:
            idd = df['precuso'].isin(['048','049','051','098'])
            df  = df[~idd]
            if not df.empty:
                anos_avaluo          = df.groupby('year')['valor_avaluo_mt2'].median().reset_index()
                anos_avaluo.rename(columns={'valor_avaluo_mt2':'valorMt2'},inplace=True)
                avaluo_mt2_historico = anos_avaluo.to_dict(orient='records')
                
        # Estadisticas de predial por ano
        df = data_prediales_actuales[data_prediales_actuales['valor_predial_mt2'].notnull()].sort_values(by=['chip', 'year', 'valor_predial_mt2'], ascending=False).drop_duplicates(subset=['chip','year'], keep='first')
        if not df.empty:
            idd = df['precuso'].isin(['048','049','051','098'])
            df  = df[~idd]
            if not df.empty:
                anos_predial          = df.groupby('year')['valor_predial_mt2'].median().reset_index()
                anos_predial.rename(columns={'valor_predial_mt2':'valorMt2'},inplace=True)
                predial_mt2_historico = anos_predial.to_dict(orient='records')
                
        # Estadisticas por area
        df  = data_prediales_actuales.sort_values(by=['chip','year'],ascending=False).drop_duplicates(subset='chip',keep='first')
        idd = df['precuso'].isin(['048','049','051','098'])
        df  = df[~idd]
        if not df.empty:
            stats_area = {
                "min": float(df['preaconst'].min()) if not df.empty and 'preaconst' in df else None,
                "q1": float(df['preaconst'].quantile(0.25)) if not df.empty and 'preaconst' in df else None,
                "median": float(df['preaconst'].median()) if not df.empty and 'preaconst' in df else None,
                "q3": float(df['preaconst'].quantile(0.75)) if not df.empty and 'preaconst' in df else None,
                "max": float(df['preaconst'].max()) if not df.empty and 'preaconst' in df else None,
                "mean": float(df['preaconst'].mean()) if not df.empty and 'preaconst' in df else None
            }
        estadisticas_area = stats_area

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
        "meta": meta,
        "data": tabla_prediales,
        "avaluoCatastral": avaluo_catastral,
        "impuestoPredial":impuesto_predial,
        "suelo":suelo,
        "propietarios":propietarios,
        "avaluoMt2Historico":avaluo_mt2_historico,
        "predialMt2Historico":predial_mt2_historico,
        "estadisticasArea":estadisticas_area
    }
    
    output = clean_json(output)

    return output