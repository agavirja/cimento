import pandas as pd
import json
import os
import mysql.connector
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text

load_dotenv()

user     = os.getenv("user_bigdata")
password = os.getenv("password_bigdata")
host     = os.getenv("host_bigdata_lectura")
schema   = 'urbex'

# ——————————————————————————————————————————————————————————————————————————— #
# Permisos para acceder a informacion de propietarios 
# ——————————————————————————————————————————————————————————————————————————— #
def otorgar_permisos_download(usuarios_acceso=[]):
    try:
        if not isinstance(usuarios_acceso, list) or usuarios_acceso == []:
            return {"success": False, "error": "Invalid input", "message": "usuarios_acceso debe ser una lista no vacía"}
        
        host_escritura = os.getenv("host_bigdata_escritura")
        lista = "','".join(usuarios_acceso)
        query = f" email IN ('{lista}')"
        engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host_escritura}/{schema}')
        
        datainput = pd.read_sql_query(f"""SELECT id,email,token FROM {schema}.users WHERE {query} AND validate_email=1 ;""", engine)
        datastock = pd.read_sql_query(f"""SELECT email FROM {schema}.user_access_download WHERE {query};""", engine)
        
        idd = datainput['email'].isin(datastock['email'])
        datainput = datainput[~idd]
        
        if not datainput.empty:
            datainput['fecha_creacion'] = datetime.now().strftime('%Y-%m-%d')
            datainput['fecha_modificacion'] = datetime.now().strftime('%Y-%m-%d')
            datainput['validate_download'] = 1
            datainput.to_sql('user_access_download', engine, if_exists='append', index=False, chunksize=1)
            engine.dispose()
            return {"success": True, "message": "Permisos otorgados exitosamente", "data": {"usuarios_procesados": len(datainput), "usuarios_input": usuarios_acceso}}
        else:
            engine.dispose()
            return {"success": True, "message": "No hay usuarios nuevos para procesar", "data": {"usuarios_procesados": 0, "usuarios_input": usuarios_acceso}}
            
    except Exception as e:
        return {"success": False, "error": str(e), "message": "Error al otorgar permisos de descarga"}

def eliminar_permisos_download(usuarios_delete=[]):
    try:
        if not isinstance(usuarios_delete, list) or usuarios_delete == []:
            return {"success": False, "error": "Invalid input", "message": "usuarios_delete debe ser una lista no vacía"}
        
        host_escritura = os.getenv("host_bigdata_escritura")
        conn = mysql.connector.connect(
            host=host_escritura,
            user=user,
            password=password,
            database=schema
        )
        cursor = conn.cursor()
        placeholders = ','.join(['%s'] * len(usuarios_delete))
        cursor.execute(f"DELETE FROM user_access_download WHERE email IN ({placeholders})", tuple(usuarios_delete))
        rows_affected = cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()
        
        return {"success": True, "message": "Permisos eliminados exitosamente", "data": {"usuarios_eliminados": rows_affected, "usuarios_input": usuarios_delete}}
        
    except Exception as e:
        return {"success": False, "error": str(e), "message": "Error al eliminar permisos de descarga"}

def get_usuario_download(token=None):
    try:
        if not isinstance(token, str) or not token:
            return {"success": False, "error": "Invalid token", "message": "Token debe ser una cadena no vacía"}
        
        engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{schema}')
        with engine.connect() as connection:
            query = text(f"""SELECT 1 FROM {schema}.user_access_download WHERE token = :token AND validate_download = 1 LIMIT 1""")
            result = connection.execute(query, {"token": token}).fetchone()
        engine.dispose()
        
        has_permission = result is not None
        return {"success": True, "message": "Consulta realizada exitosamente", "data": {"has_permission": has_permission, "token": token}}
        
    except Exception as e:
        return {"success": False, "error": str(e), "message": "Error al verificar permisos de descarga"}


# ——————————————————————————————————————————————————————————————————————————— #
# Permisos para acceder a informacion de galeria
# ——————————————————————————————————————————————————————————————————————————— #
def otorgar_permisos_galeria(empresas_acceso=[], usuarios_acceso=[]):
    try:
        host_escritura = os.getenv("host_bigdata_escritura")
        engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host_escritura}/{schema}')
        empresas_procesadas = 0
        usuarios_procesados = 0
        
        if isinstance(empresas_acceso, list) and empresas_acceso != []:
            condiciones = " OR ".join([f"email LIKE '%@{empresa}%'" for empresa in empresas_acceso])
            where_clause = f"({condiciones})"
            
            datastock = pd.read_sql_query(f""" SELECT empresa FROM {schema}.user_access_galeria WHERE {where_clause}""", engine)
            datainput = pd.DataFrame(empresas_acceso, columns=['empresa'])
            idd = datainput['empresa'].isin(datastock['empresa'])
            datainput = datainput[~idd]
            
            if not datainput.empty:
                datainput = datainput.drop_duplicates(subset='empresa')
                datainput['fecha_creacion'] = datetime.now().strftime('%Y-%m-%d')
                datainput['fecha_modificacion'] = datetime.now().strftime('%Y-%m-%d')
                datainput['validate_download'] = 1
                datainput.to_sql('user_access_galeria', engine, if_exists='append', index=False, chunksize=1)
                empresas_procesadas = len(datainput)

        if isinstance(usuarios_acceso, list) and usuarios_acceso != []:
            lista = "','".join(usuarios_acceso)
            query = f" email IN ('{lista}')"
            datainput = pd.read_sql_query(f"""SELECT id,email,token FROM {schema}.users WHERE {query} AND validate_email=1 ;""", engine)
            datastock = pd.read_sql_query(f"""SELECT email FROM {schema}.user_access_galeria WHERE {query};""", engine)
            idd = datainput['email'].isin(datastock['email'])
            datainput = datainput[~idd]
            
            if not datainput.empty:
                datainput['fecha_creacion'] = datetime.now().strftime('%Y-%m-%d')
                datainput['fecha_modificacion'] = datetime.now().strftime('%Y-%m-%d')
                datainput['validate_download'] = 1
                datainput.to_sql('user_access_galeria', engine, if_exists='append', index=False, chunksize=1)
                usuarios_procesados = len(datainput)
        
        engine.dispose()
        
        if empresas_procesadas == 0 and usuarios_procesados == 0:
            return {"success": True, "message": "No hay nuevos elementos para procesar", "data": {"empresas_procesadas": empresas_procesadas, "usuarios_procesados": usuarios_procesados}}
        else:
            return {"success": True, "message": "Permisos de galería otorgados exitosamente", "data": {"empresas_procesadas": empresas_procesadas, "usuarios_procesados": usuarios_procesados}}
            
    except Exception as e:
        return {"success": False, "error": str(e), "message": "Error al otorgar permisos de galería"}

def get_usuario_galeria(token=None):
    try:
        if not isinstance(token, str) or not token:
            return {"success": False, "error": "Invalid token", "message": "Token debe ser una cadena no vacía"}
        
        engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{schema}')
        with engine.connect() as connection:
            query = text(f"""SELECT email FROM {schema}.users WHERE token = :token AND validate_email = 1 LIMIT 1""")
            result = connection.execute(query, {"token": token}).fetchone()
            
            if result:
                try:
                    email = result[0]
                    empresa = email.split('@')[-1].split('.')[0].strip().lower()
                    query = text(f"""SELECT 1 FROM {schema}.user_access_galeria WHERE (email = '{email}' OR empresa = '{empresa}') AND validate_download = 1 LIMIT 1""")
                    response = connection.execute(query, {"token": token}).fetchone()
                    has_permission = response is not None
                except:
                    has_permission = False
            else:
                has_permission = False
                
        engine.dispose()
        return {"success": True, "message": "Consulta realizada exitosamente", "data": {"has_permission": has_permission, "token": token}}
        
    except Exception as e:
        return {"success": False, "error": str(e), "message": "Error al verificar permisos de galería"}

def eliminar_permisos_galeria(empresas_delete=[], usuarios_delete=[]):
    try:
        if (not isinstance(empresas_delete, list) and not isinstance(usuarios_delete, list)) or (empresas_delete == [] and usuarios_delete == []):
            return {"success": False, "error": "Invalid input", "message": "Al menos una de las listas debe ser no vacía"}
        
        host_escritura = os.getenv("host_bigdata_escritura")
        conn = mysql.connector.connect(
            host=host_escritura,
            user=user,
            password=password,
            database=schema
        )
        cursor = conn.cursor()
        empresas_eliminadas = 0
        usuarios_eliminados = 0

        if isinstance(empresas_delete, list) and empresas_delete:
            placeholders = ", ".join(["%s"] * len(empresas_delete))
            sql = f"DELETE FROM {schema}.user_access_galeria WHERE empresa IN ({placeholders})"
            cursor.execute(sql, tuple(empresas_delete))
            empresas_eliminadas = cursor.rowcount

        if isinstance(usuarios_delete, list) and usuarios_delete:
            placeholders = ", ".join(["%s"] * len(usuarios_delete))
            sql = f"DELETE FROM {schema}.user_access_galeria WHERE email IN ({placeholders})"
            cursor.execute(sql, tuple(usuarios_delete))
            usuarios_eliminados = cursor.rowcount
            
        conn.commit()
        cursor.close()
        conn.close()
        
        return {"success": True, "message": "Permisos de galería eliminados exitosamente", "data": {"empresas_eliminadas": empresas_eliminadas, "usuarios_eliminados": usuarios_eliminados}}
        
    except Exception as e:
        return {"success": False, "error": str(e), "message": "Error al eliminar permisos de galería"}