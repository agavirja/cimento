import streamlit as st
import pandas as pd
import numpy as np

from display.dashboard import main as generar_html


st.set_page_config(layout='wide')

# streamlit run D:\Dropbox\Empresa\Urbex\_APP_placas\Home.py
# https://streamlit.io/
# pipreqs --encoding utf-8 "D:\Dropbox\Empresa\Urbex\_APP_heroku"

from funciones.importdata import getdata

def download_excel(df):
    excel_file = df.to_excel('data_informacion.xlsx', index=False)
    with open('data_informacion.xlsx', 'rb') as f:
        data = f.read()
    st.download_button(
        label="Haz clic aquí para descargar",
        data=data,
        file_name='data_informacion.xlsx',
        mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
    
with st.spinner('Cargando...'):
    
    labels,data,databarrios,datalocalidad = getdata()
    
    #-------------------------------------------------------------------------#
    # Aleatorio para dias de la semana
    dias           = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]
    probabilidades = [0.05, 0.05, 0.05, 0.05, 0.15, 0.25, 0.4]
    np.random.seed(42)
    data['dia_semana'] = np.random.choice(dias, size=len(data), p=probabilidades)
    data['dia_semana'] = pd.Categorical(data['dia_semana'], categories=dias, ordered=True)


    franjas = ["Mañana", "Tarde", "Noche"]
    probabilidades_franja = [0.5, 0.3, 0.2]
    np.random.seed(42)  
    data['franja_horaria'] = np.random.choice(franjas, size=len(data), p=probabilidades_franja)
    data['franja_horaria'] = pd.Categorical(data['franja_horaria'], categories=franjas, ordered=True)

    col1, col2 = st.columns([8, 4]) 
    with col2:
        st.image('https://iconsapp.nyc3.digitaloceanspaces.com/_clientes_logos/canva-logo-CimentoFontanar.png', width=400)
    
    col1,col2,col3 = st.columns([4,4,4],vertical_alignment='center')
    with col3:
        if st.button('Descargar Excel'):
            variables  = ['placa', 'tipoID', 'nombre', 'numID', 'calidad', 'procProp', 'fechaDesde', 'fechaHasta', 'anio', 'avaluo', 'capacidadCarga', 'carroceria', 'clase', 'linea', 'marca', 'modelo', 'porcentajeRespon', 'responsable', 'tipoServicio', 'impuesto_a_cargo', 'cilindraje', 'url', 'direccion_notificacion', 'telefonos', 'email', 'propietario', 'edad', 'numprop', 'avaluocatastral', 'estrato']
            dataexport = data[variables]
            download_excel(dataexport)

    with col1:
        tipo = st.selectbox('Tipo de segmentación geográfica',options=['Localidad','Barrio catastral'])
        datageometry = datalocalidad.copy()
        if 'Barrio catastral' in tipo:
            datageometry = databarrios.copy()

    with col2:
        options = ["Todos","Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]
        diasem  = st.selectbox('Día de la semana',options=options)
        if "Todos" not in diasem:
            data = data[data['dia_semana']==diasem]
        
    if 'geometry' in datageometry: 
        del datageometry['geometry']
        datageometry = pd.DataFrame(datageometry)
        
    htmlrender = generar_html(labels, data, datageometry, datalocalidad)
    st.components.v1.html(htmlrender, height=2400, scrolling=True)