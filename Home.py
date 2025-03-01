import streamlit as st

from display.dashboard import main as generar_html


st.set_page_config(layout='wide')

# streamlit run D:\Dropbox\Empresa\Urbex\_APP_placas\Home.py
# https://streamlit.io/
# pipreqs --encoding utf-8 "D:\Dropbox\Empresa\Urbex\_APP_heroku"

from funciones.importdata import getdata

with st.spinner('Cargando...'):
    
    labels,data,databarrios,datalocalidad = getdata()
    
    col1, col2 = st.columns([8, 4]) 
    with col2:
        st.image('https://iconsapp.nyc3.digitaloceanspaces.com/_clientes_logos/canva-logo-CimentoFontanar.png', width=400)
    
    col1,col2 = st.columns([0.15,0.85])
    with col1:
        tipo = st.selectbox('Tipo de segmentación geográfica',options=['Localidad','Barrio catastral'])
        datageometry = datalocalidad.copy()
        if 'Barrio catastral' in tipo:
            datageometry = databarrios.copy()
    with col2:
        htmlrender = generar_html(labels, data, datageometry, datalocalidad)
        st.components.v1.html(htmlrender, height=2400, scrolling=True)
