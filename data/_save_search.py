import mysql.connector
import json
import os
import pandas as pd
import uuid
from datetime import datetime
from sqlalchemy import create_engine
from dotenv import load_dotenv
from functions.clean_json import clean_json

load_dotenv()

def main(inputvar={}):

    action = inputvar.get('action', 'save')
    token  = inputvar.get('token', None)
    
    if action == 'save':
        result = save_search(
            token=token,
            barmanpre=inputvar.get('barmanpre', None),
            seccion=inputvar.get('seccion', None),
            inputvar_data=inputvar.get('inputvar_data', None),
            id_consulta_defaul=inputvar.get('id_consulta_defaul', None)
        )
    elif action == 'update':
        result = update_save_status(
            id_consulta=inputvar.get('id_consulta', None),
            token=token,
            value=inputvar.get('save_value', 0)
        )
    else:
        result = {
            'success': False,
            'message': 'Invalid action. Use "save" or "update"',
            'data': None
        }
    
    output = {
        'meta': {
            "timestamp": datetime.now().isoformat(),
            "requestId": str(uuid.uuid4()),
            "action": action,
            "filtersApplied": {
                "token": token is not None,
                "action": action
            }
        },
        'tracking': result
    }
    
    output = clean_json(output)
    return output


def save_search(token, barmanpre, seccion, inputvar_data, id_consulta_defaul=None):
    email = None
    if isinstance(token, str) and token != '':
        email = get_email_from_token(token)
    
    if token is None and email is None:
        return {
            'success': False,
            'message': 'Invalid token or email not found',
            'data': None
        }
    
    try:
        user     = os.getenv("user")
        password = os.getenv("password")
        host     = os.getenv("host")
        
        if not all([user, password, host]):
            return {
                'success': False,
                'message': 'Database credentials not configured',
                'data': None
            }
        
        # Process inputvar_data
        inputvar_str = None
        try:
            inputvar_str = pd.io.json.dumps(inputvar_data)
            if inputvar_str == 'null':
                inputvar_str = None
        except:
            try:
                inputvar_str = json.dumps(inputvar_data) if isinstance(inputvar_data, dict) else None
                if inputvar_str is None:
                    inputvar_str = inputvar_data if isinstance(inputvar_data, str) and inputvar_data != '' else None
            except:
                inputvar_str = None
        
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        connection = mysql.connector.connect(
            user=user,
            password=password,
            host=host,
            database='urbexapp',
            port=25060,
        )
        
        cursor = connection.cursor()
        insert_query = """
        INSERT INTO urbexapp.tracking 
        (token, email, seccion, barmanpre, inputvar, save, fecha_consulta, fecha_update)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        values = (token, email, seccion, barmanpre, inputvar_str, 0, current_time, current_time)
        
        cursor.execute(insert_query, values)
        connection.commit()
        inserted_id = cursor.lastrowid
        
        if id_consulta_defaul is None:
            id_consulta_defaul = inserted_id
        
        if isinstance(id_consulta_defaul, int):
            update_query = """UPDATE urbexapp.tracking SET id_consulta = %s WHERE id = %s"""
            cursor.execute(update_query, (id_consulta_defaul, inserted_id))
            connection.commit()
        
        cursor.close()
        connection.close()
        
        return {
            'success': True,
            'message': 'Search saved successfully',
            'data': {
                'inserted_id': inserted_id,
                'id_consulta': id_consulta_defaul
            }
        }
        
    except Exception as e:
        # Close connection if open
        if 'connection' in locals() and connection.is_connected():
            if 'cursor' in locals():
                cursor.close()
            connection.close()
        
        return {
            'success': False,
            'message': f'Error saving search: {str(e)}',
            'data': None
        }


def update_save_status(id_consulta, token, value):
    
    if not isinstance(id_consulta, int) or not isinstance(token, str):
        return {
            'success': False,
            'message': 'Invalid parameters: id_consulta must be int and token must be string',
            'data': None
        }
    
    try:
        user = os.getenv("user")
        password = os.getenv("password")
        host = os.getenv("host")
        
        if not all([user, password, host]):
            return {
                'success': False,
                'message': 'Database credentials not configured',
                'data': None
            }
        
        connection = mysql.connector.connect(
            user=user,
            password=password,
            host=host,
            database='urbexapp',
            port=25060,
        )
        
        cursor = connection.cursor()
        
        # Verify record exists
        verify_query = """
        SELECT id FROM tracking 
        WHERE id = %s AND token = %s
        """
        cursor.execute(verify_query, (id_consulta, token))
        result = cursor.fetchone()
        
        if not result:
            cursor.close()
            connection.close()
            return {
                'success': False,
                'message': 'Record not found or token mismatch',
                'data': None
            }
        
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        update_query = """
        UPDATE tracking 
        SET save = %s,
            fecha_update = %s
        WHERE id = %s AND token = %s
        """
        cursor.execute(update_query, (value, current_time, id_consulta, token))
        
        if cursor.rowcount == 0:
            connection.rollback()
            cursor.close()
            connection.close()
            return {
                'success': False,
                'message': 'No rows updated',
                'data': None
            }
        
        connection.commit()
        cursor.close()
        connection.close()
        
        return {
            'success': True,
            'message': 'Save status updated successfully',
            'data': {
                'id_consulta': id_consulta,
                'updated_value': value,
                'timestamp': current_time
            }
        }
        
    except Exception as e:
        # Close connection if open
        if 'connection' in locals() and connection.is_connected():
            if 'cursor' in locals():
                cursor.close()
            connection.close()
        
        return {
            'success': False,
            'message': f'Error updating save status: {str(e)}',
            'data': None
        }


def get_email_from_token(token):
    email = None
    try:
        user     = os.getenv("user")
        password = os.getenv("password")
        host     = os.getenv("host")
        schema   = os.getenv("schema")
        port     = 25060
        
        if not all([user, password, host, schema]):
            return None
        
        engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{schema}')
        data = pd.read_sql_query(f"SELECT email FROM urbexapp.users WHERE token='{token}'", engine)
        engine.dispose()
        
        if not data.empty:
            email = data['email'].iloc[0]
    except Exception as e:
        print(f"Error getting email from token: {e}")
    
    return email