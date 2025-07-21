import streamlit as st
import pandas as pd
import numpy as np
import requests
import json
from datetime import datetime

from display.style_white  import style

st.set_page_config(layout='wide')

def main():
    
    style()
    
    #-------------------------------------------------------------------------#
    # Variables 
    formato = {
               'html_estudio':'',
               'token':None,
               'boton_count_estudio':0,
               'url':None,
               }
    
    for key,value in formato.items():
        if key not in st.session_state: 
            st.session_state[key] = value
            
            
    _, collogo2 = st.columns([8, 4]) 
    _, colbo1, colbo2,_  = st.columns([2, 4, 4, 2]) 
    _,colbar,_  = st.columns(3)
    _,col2pdf,_ = st.columns([5,2,1])
    col01,col02 = st.columns([0.15,0.85])
    col1, col2  = st.columns([0.15,0.85])

    with collogo2:
        st.image('https://iconsapp.nyc3.digitaloceanspaces.com/_clientes_logos/canva-logo-CimentoFontanar.png', width=400)
    
    with st.spinner('Cargando...'):
        with col1:
            segmentacion      = st.selectbox('Tipo de segmentación geográfica', options=['Localidad', 'Barrio catastral'])
            dias_options      = ['Todos', 'Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']
            dia_semana        = st.selectbox('Día de la semana', options=dias_options)
            franja_options    = ['Todos', 'Mañana', 'Tarde', 'Noche']
            franja_horaria    = st.selectbox('Franja horaria', options=franja_options)
            edad_min          = st.number_input('Edad mínima', min_value=0, max_value=120, value=0)
            edad_max          = st.number_input('Edad máxima', min_value=0, max_value=120, value=120)
            vehiculo_min      = st.number_input('Valor mínimo vehículo', min_value=0, value=0)
            vehiculo_max      = st.number_input('Valor máximo vehículo', min_value=0, value=1000000000)
            prop_min          = st.number_input('Valor mínimo propiedades', min_value=0, value=0)
            prop_max          = st.number_input('Valor máximo propiedades', min_value=0, value=1000000000)
            tiene_propiedades = st.checkbox('Tiene propiedades')
            barrios           = st.multiselect('Barrios', options=['Santa Fe', 'Usaquén', 'Chapinero', 'Suba', 'Engativá', 'Fontibón', 'Kennedy', 'Teusaquillo', 'Barrios Unidos', 'Puente Aranda', 'Los Mártires', 'Otros'])
        
        inputvar = {
            "segmentacion": segmentacion,
            "dia_semana": dia_semana,
            "franja_horaria": franja_horaria,
            "edad_min": edad_min,
            "edad_max": edad_max,
            "vehiculo_min": vehiculo_min,
            "vehiculo_max": vehiculo_max,
            "prop_min": prop_min,
            "prop_max": prop_max,
            "tiene_propiedades": tiene_propiedades,
            "barrios": barrios
        }

    if st.session_state.boton_count_estudio==0 or st.session_state.html_estudio=='':
        with colbar:
            with st.spinner('Cargando...'):
                output                        = getdata(inputvar)
                st.session_state.html_estudio = get_html(output)
                st.session_state.url          = output['urlfile']
                st.session_state.boton_count_estudio = 1

    if st.session_state.boton_count_estudio>0:
        with col01:
            if st.button('Filtrar'):
                with colbar:
                    with st.spinner('Cargando...'):
                        output                        = getdata(inputvar)
                        st.session_state.html_estudio = get_html(output)
                        st.session_state.url          = output['urlfile']
                    
        with col1:
            if st.button('Filtrar '):
                with colbar:
                    with st.spinner('Cargando...'):
                        output                        = getdata(inputvar)
                        st.session_state.html_estudio = get_html(output)
                        st.session_state.url          = output['urlfile']
                        
    if isinstance(st.session_state.html_estudio,str) and st.session_state.html_estudio!='':
        with col2:
            with st.spinner('Cargando ...'):
                st.components.v1.html(st.session_state.html_estudio, height=3000, scrolling=True)

    with colbo1:
        if st.button('Generar PDF'):
            with st.spinner('Procesando PDF...'):            
                url = None
                try:
                    url = get_pdf_url(st.session_state.html_estudio)
                except: pass
                if isinstance(url,str) and url!='':
                    st.link_button('Descargar el PDF', url)        
                        
    with colbo2:
        if isinstance(st.session_state.url,str) and st.session_state.url!='':
            st.link_button('Descargar data', st.session_state.url, disabled=False)

            url = 'https://iconsapp.nyc3.digitaloceanspaces.com/_temp/b0e37782-98c8-41b8-bad0-df5b93b01003.xlsx'

def getdata(inputvar={}):
    response = requests.post("https://api.urbex.com.co/testFontanar", json=inputvar)
    if response.status_code != 200:
        st.write(f"API Error: Status {response.status_code}")
        return {}
    return response.json()

@st.cache_data(show_spinner=False)
def get_pdf_url(html_content):

    payload  = {'html_content': html_content}
    response = requests.post("https://api.urbex.com.co/html2pdf", json=payload)
    url      = None
    try: 
        r   = response.json() 
        url = r['url']
        if isinstance(url,str) and url!='':
            return url
        else:
            raise ValueError("La API no devolvió una URL de PDF")
    except Exception as e:
        raise RuntimeError(f"Error generando PDF: {e}")
        
def get_html(data):
    labels        = data.get('labels', [])
    datageometry  = data.get('datageometry', {"type": "FeatureCollection", "features": []})
    datalocalidad = data.get('datalocalidad', [])
    
    grafica_marcas             = get_marca_from_json(data)
    grafica_avaluo_vehiculo    = get_valor_vehiculo_from_json(data)
    grafica_numero_vehiculo    = get_numero_vehiculos_from_json(data)
    grafica_avaluo_propiedades = get_valor_propiedades_from_json(data)
    grafica_estrato            = get_estrato_from_json(data)
    grafica_numero_propiedades = get_numero_propiedades_from_json(data)
    grafica_localidades        = get_localidades_from_json(data)
    grafica_edades             = get_edades_from_json(data)
    grafica_tipo_vehiculos     = get_tipo_vehiculos_from_json(data)
    grafica_diasem             = get_dias_visitas_from_json(data)
    grafica_horasvisita        = get_horas_visitas_from_json(data)
    
    latitud   = 4.687115
    longitud  = -74.056937
    mapscript = map_function_from_json(datageometry, latitud, longitud)
    
    min_val = 0
    max_val = 100
    ticks   = [0, 25, 50, 75, 100]
    
    if datageometry and datageometry.get('features'):
        conteos = [feature.get('properties', {}).get('conteo', 0) for feature in datageometry['features']]
        conteos = [c for c in conteos if c > 0]  # Filtrar valores 0 para mejor visualización
        
        if conteos:
            min_val = min(conteos)
            max_val = max(conteos)
            
            # Generar ticks más inteligentes
            if max_val <= 10:
                ticks = list(range(0, max_val + 1))
            elif max_val <= 100:
                ticks = list(range(0, max_val + 1, max(1, max_val // 5)))
            else:
                # Para valores grandes, usar 5 puntos distribuidos uniformemente
                ticks = np.linspace(0, max_val, 6, dtype=int).tolist()
                # Asegurar que no haya duplicados y estén ordenados
                ticks = sorted(list(set(ticks)))
        else:
            # Si no hay conteos > 0, usar valores por defecto
            min_val = 0
            max_val = 10
            ticks = [0, 2, 4, 6, 8, 10]
    
    html_content = f'''
    <!DOCTYPE html>
    <html data-bs-theme="light" lang="en">
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0, shrink-to-fit=no">
        <link rel="stylesheet" href="https://iconsapp.nyc3.digitaloceanspaces.com/_estilo_dashboard_vehiculos/bootstrap.min.css">
        <link rel="stylesheet" href="https://iconsapp.nyc3.digitaloceanspaces.com/_estilo_dashboard_vehiculos/styles.css">
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-datalabels@2"></script>
        <link rel="stylesheet" href="https://unpkg.com/leaflet@1.7.1/dist/leaflet.css" />
        <script src="https://unpkg.com/leaflet@1.7.1/dist/leaflet.js"></script>
    </head>
    <body>
        <section>
            <div class="container-fluid">
                <div class="row">
                    <div class="col-12 col-lg-3 p-2">
                        <div id="box_shadow_default" class="h-100 d-flex flex-column" style="min-height: 100px;">
                            <h1 class="text-center">{"{:,.0f}".format(labels[0]['value']) if len(labels) > 0 else "0"}</h1>
                            <p class="text-center">{labels[0]['label'] if len(labels) > 0 else ""}</p>
                        </div>
                    </div>
                    <div class="col-12 col-lg-3 p-2">
                        <div id="box_shadow_default" class="h-100 d-flex flex-column" style="min-height: 100px;">
                            <h1 class="text-center">{"{:,.0f}".format(labels[1]['value']) if len(labels) > 1 else "0"}</h1>
                            <p class="text-center">{labels[1]['label'] if len(labels) > 1 else ""}</p>
                        </div>
                    </div>
                    <div class="col-12 col-lg-3 p-2">
                        <div id="box_shadow_default" class="h-100 d-flex flex-column" style="min-height: 100px;">
                            <h1 class="text-center">{"{:,.1f}".format(labels[2]['value']) if len(labels) > 2 else "0"}</h1>
                            <p class="text-center">{labels[2]['label'] if len(labels) > 2 else ""}</p>
                        </div>
                    </div>
                    <div class="col-12 col-lg-3 p-2">
                        <div id="box_shadow_default" class="h-100 d-flex flex-column" style="min-height: 100px;">
                            <h1 class="text-center">{"{:,.1f}".format(labels[3]['value']) if len(labels) > 3 else "0"}</h1>
                            <p class="text-center">{labels[3]['label'] if len(labels) > 3 else ""}</p>
                        </div>
                    </div>
                </div>
            </div>
        </section>
        <section>
            <div class="container-fluid">
                <div class="row">
                    <div class="col-12 col-lg-7 p-2">
                        <div id="box_shadow_default" class="h-100 d-flex flex-column" style="min-height: 500px; position: relative;">
                            <div style="position: absolute; top: 10px; right: 10px; background-color: white; padding: 10px; border: 1px solid #ccc; border-radius: 5px; font-family: sans-serif; font-size: 12px; z-index: 9999;">
                                <div style="font-weight: bold; margin-bottom: 5px;">Registros</div>
                                <div style="width: 200px; height: 20px; background: linear-gradient(to right, #440154, #443983, #31688e, #35b779, #fde725); margin-top: 5px; margin-bottom: 5px;"></div>
                                <div style="display: flex; justify-content: space-between; width: 200px;">
                                    {''.join([f"<span>{val:,}</span>" for val in ticks])}
                                </div>
                            </div>
                            <div id="leaflet-map" style="height: 100%;"></div>
                        </div>
                    </div>
                    <div class="col-12 col-lg-5 p-2">
                        <div id="box_shadow_default" class="h-100 d-flex flex-column" style="min-height: 500px;">
                            <canvas id="LocaChart"></canvas>
                        </div>
                    </div>
                </div>
            </div>
        </section>
        <section>
            <div class="container-fluid">
                <div class="row">
                    <div class="col-12 col-lg-3 p-2">
                        <div id="box_shadow_default" class="h-100 d-flex flex-column" style="min-height: 400px;">
                            <canvas id="EdadChart"></canvas>
                        </div>
                    </div>
                    <div class="col-12 col-lg-3 p-2">
                        <div id="box_shadow_default" class="h-100 d-flex flex-column" style="min-height: 400px;">
                            <canvas id="EstratoChart"></canvas>
                        </div>
                    </div>
                    <div class="col-12 col-lg-3 p-2">
                        <div id="box_shadow_default" class="h-100 d-flex flex-column" style="min-height: 400px;">
                            <canvas id="PropNumChart"></canvas>
                        </div>
                    </div>
                    <div class="col-12 col-lg-3 p-2">
                        <div id="box_shadow_default" class="h-100 d-flex flex-column" style="min-height: 400px;">
                            <canvas id="PropAvaluoChart"></canvas>
                        </div>
                    </div>
                </div>
            </div>
        </section>
        <section>
            <div class="container-fluid">
                <div class="row">
                    <div class="col-12 col-lg-4 p-2">
                        <div id="box_shadow_default" class="h-100 d-flex flex-column" style="min-height: 400px;">
                            <canvas id="VehNumChart"></canvas>
                        </div>
                    </div>
                    <div class="col-12 col-lg-4 p-2">
                        <div id="box_shadow_default" class="h-100 d-flex flex-column" style="min-height: 400px;">
                            <canvas id="VehAvaluoChart"></canvas>
                        </div>
                    </div>
                    <div class="col-12 col-lg-4 p-2">
                        <div id="box_shadow_default" class="h-100 d-flex flex-column" style="min-height: 400px;">
                            <canvas id="TipoVehiChart"></canvas>
                        </div>
                    </div>
                </div>
            </div>
        </section>
        <section>
            <div class="container-fluid">
                <div class="row">
                    <div class="col-12 col-lg-12 p-2">
                        <div id="box_shadow_default" class="h-100 d-flex flex-column" style="max-height: 400px;">
                            <canvas id="marcaChart"></canvas>
                        </div>
                    </div>
                </div>
            </div>
        </section>
        <section>
            <div class="container-fluid">
                <div class="row">
                    <div class="col-12 col-lg-6 p-2">
                        <div id="box_shadow_default" class="h-100 d-flex flex-column" style="min-height: 400px;">
                            <canvas id="DiaSemChart"></canvas>
                        </div>
                    </div>
                    <div class="col-12 col-lg-6 p-2">
                        <div id="box_shadow_default" class="h-100 d-flex flex-column" style="min-height: 400px;">
                            <canvas id="HorasVisitaChart"></canvas>
                        </div>
                    </div>
                </div>
            </div>
        </section>
        {grafica_marcas}
        {grafica_avaluo_vehiculo}
        {grafica_numero_vehiculo}
        {grafica_avaluo_propiedades}
        {grafica_estrato}
        {grafica_numero_propiedades}
        {grafica_localidades}
        {mapscript}
        {grafica_edades}
        {grafica_tipo_vehiculos}
        {grafica_diasem}
        {grafica_horasvisita}
    </body>
    </html>
    '''
    return html_content

def get_marca_from_json(data):
    marcas = data.get('marcas', {})
    labels = marcas.get('labels', [])
    values = marcas.get('values', [])
    labels_json = str(labels).replace("'", "\"")
    values_json = str(values)
    html = f"""
    <script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-datalabels@2"></script>
    <script>
        document.addEventListener("DOMContentLoaded", function() {{
            const ctx = document.getElementById('marcaChart').getContext('2d');
            const data = {{
                labels: {labels_json},
                datasets: [{{
                    label: 'Marca',
                    data: {values_json},
                    backgroundColor: 'rgba(54, 162, 235, 0.6)',
                    borderWidth: 1
                }}]
            }};
            new Chart(ctx, {{
                type: 'bar',
                data: data,
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {{
                        legend: {{ display: false }},
                        title: {{ display: true, text: 'Marca', font: {{ size: 16 }} }},
                        datalabels: {{ anchor: 'end', align: 'top', color: '#000', font: {{ weight: 'bold' }}, formatter: function(value) {{ return value; }} }}
                    }},
                    scales: {{
                        x: {{ beginAtZero: true, grid: {{ display: false }} }},
                        y: {{ grid: {{ display: false }} }}
                    }}
                }},
                plugins: [ChartDataLabels]
            }});
        }});
    </script>
    """
    return html

def get_valor_vehiculo_from_json(data):
    avaluo = data.get('avaluoVehiculo', {})
    labels = avaluo.get('labels', [])
    values = avaluo.get('values', [])
    labels_json = str(labels).replace("'", "\"")
    values_json = str(values)
    html = f"""
    <script>
        document.addEventListener("DOMContentLoaded", function() {{
            const ctx = document.getElementById('VehAvaluoChart').getContext('2d');
            const data = {{
                labels: {labels_json},
                datasets: [{{
                    label: 'Avalúo de los vehículos',
                    data: {values_json},
                    backgroundColor: ["#10564F", "#2F746A", "#E87E42", "#E8BD42"],
                    borderWidth: 1
                }}]
            }};
            new Chart(ctx, {{
                type: 'bar',
                data: data,
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {{
                        legend: {{ display: false }},
                        title: {{ display: true, text: 'Avalúo de los vehículos', font: {{ size: 16 }} }},
                        datalabels: {{ anchor: 'end', align: 'top', color: '#000', font: {{ weight: 'bold' }}, formatter: function(value) {{ return value; }} }}
                    }},
                    scales: {{
                        x: {{ beginAtZero: true, grid: {{ display: false }} }},
                        y: {{ grid: {{ display: false }} }}
                    }}
                }},
                plugins: [ChartDataLabels]
            }});
        }});
    </script>
    """
    return html

def get_numero_vehiculos_from_json(data):
    num_veh = data.get('numeroVehiculos', {})
    labels = num_veh.get('labels', [])
    values = num_veh.get('values', [])
    labels_json = str(labels).replace("'", "\"")
    values_json = str(values)
    html = f"""
    <script>
        document.addEventListener("DOMContentLoaded", function() {{
            const ctx = document.getElementById('VehNumChart').getContext('2d');
            const data = {{
                labels: {labels_json},
                datasets: [{{
                    label: 'Número de vehículos',
                    data: {values_json},
                    backgroundColor: ["#10564F", "#2F746A", "#E87E42", "#E8A142", "#E8BD42"],
                    borderWidth: 1
                }}]
            }};
            new Chart(ctx, {{
                type: 'bar',
                data: data,
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {{
                        legend: {{ display: false }},
                        title: {{ display: true, text: 'Número de vehículos', font: {{ size: 16 }} }},
                        datalabels: {{ anchor: 'end', align: 'top', color: '#000', font: {{ weight: 'bold' }}, formatter: function(value) {{ return value; }} }}
                    }},
                    scales: {{
                        x: {{ beginAtZero: true, grid: {{ display: false }} }},
                        y: {{ grid: {{ display: false }} }}
                    }}
                }},
                plugins: [ChartDataLabels]
            }});
        }});
    </script>
    """
    return html

def get_tipo_vehiculos_from_json(data):
    tipo_veh = data.get('tipoVehiculos', {})
    labels = tipo_veh.get('labels', [])
    values = tipo_veh.get('values', [])
    labels_json = str(labels).replace("'", "\"")
    values_json = str(values)
    html = f"""
    <script>
        document.addEventListener("DOMContentLoaded", function() {{
            const ctx = document.getElementById('TipoVehiChart').getContext('2d');
            const data = {{
                labels: {labels_json},
                datasets: [{{
                    label: 'Tipo de vehículos',
                    data: {values_json},
                    backgroundColor: ["#10564F", "#2F746A", "#E87E42", "#E8A142", "#E8BD42"],
                    borderWidth: 1
                }}]
            }};
            new Chart(ctx, {{
                type: 'bar',
                data: data,
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {{
                        legend: {{ display: false }},
                        title: {{ display: true, text: 'Tipo de vehículo', font: {{ size: 16 }} }},
                        datalabels: {{ anchor: 'end', align: 'top', color: '#000', font: {{ weight: 'bold' }}, formatter: function(value) {{ return value; }} }}
                    }},
                    scales: {{
                        x: {{ beginAtZero: true, grid: {{ display: false }} }},
                        y: {{ grid: {{ display: false }} }}
                    }}
                }},
                plugins: [ChartDataLabels]
            }});
        }});
    </script>
    """
    return html

def get_valor_propiedades_from_json(data):
    avaluo_prop = data.get('avaluoPropiedades', {})
    labels = avaluo_prop.get('labels', [])
    values = avaluo_prop.get('values', [])
    labels_json = str(labels).replace("'", "\"")
    values_json = str(values)
    html = f"""
    <script>
        document.addEventListener("DOMContentLoaded", function() {{
            const ctx = document.getElementById('PropAvaluoChart').getContext('2d');
            const data = {{
                labels: {labels_json},
                datasets: [{{
                    label: 'Avalúo catastral de las propiedades',
                    data: {values_json},
                    backgroundColor: ["#10564F", "#2F746A", "#E87E42", "#E8A142", "#E8BD42"],
                    borderWidth: 1
                }}]
            }};
            new Chart(ctx, {{
                type: 'bar',
                data: data,
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {{
                        legend: {{ display: false }},
                        title: {{ display: true, text: 'Avalúo catastral de las propiedades', font: {{ size: 16 }} }},
                        datalabels: {{ anchor: 'end', align: 'top', color: '#000', font: {{ weight: 'bold' }}, formatter: function(value) {{ return value; }} }}
                    }},
                    scales: {{
                        x: {{ beginAtZero: true, grid: {{ display: false }} }},
                        y: {{ grid: {{ display: false }} }}
                    }}
                }},
                plugins: [ChartDataLabels]
            }});
        }});
    </script>
    """
    return html

def get_estrato_from_json(data):
    estrato = data.get('estrato', {})
    labels = estrato.get('labels', [])
    values = estrato.get('values', [])
    labels_json = str(labels).replace("'", "\"")
    values_json = str(values)
    html = f"""
    <script>
        document.addEventListener("DOMContentLoaded", function() {{
            const ctx = document.getElementById('EstratoChart').getContext('2d');
            const data = {{
                labels: {labels_json},
                datasets: [{{
                    label: 'Estrato',
                    data: {values_json},
                    backgroundColor: ['#10564F', '#1F6D5E', '#2F746A', '#E87E42', '#E8A142', '#E8BD42'],
                    borderWidth: 1
                }}]
            }};
            new Chart(ctx, {{
                type: 'pie',
                data: data,
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {{
                        legend: {{ display: true, position: 'top' }},
                        title: {{ display: true, text: 'Distribución por Estrato', font: {{ size: 16 }} }},
                        datalabels: {{ color: 'white', font: {{ size: 16, weight: 'bold' }}, formatter: function(value) {{ return value; }} }}
                    }}
                }},
                plugins: [ChartDataLabels]
            }});
        }});
    </script>
    """
    return html

def get_numero_propiedades_from_json(data):
    num_prop = data.get('numeroPropiedades', {})
    labels = num_prop.get('labels', [])
    values = num_prop.get('values', [])
    labels_json = str(labels).replace("'", "\"")
    values_json = str(values)
    html = f"""
    <script>
        document.addEventListener("DOMContentLoaded", function() {{
            const ctx = document.getElementById('PropNumChart').getContext('2d');
            const data = {{
                labels: {labels_json},
                datasets: [{{
                    label: 'Número de propiedades',
                    data: {values_json},
                    backgroundColor: ["#10564F", "#2F746A", "#E87E42", "#E8A142", "#E8BD42"],
                    borderWidth: 1
                }}]
            }};
            new Chart(ctx, {{
                type: 'bar',
                data: data,
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {{
                        legend: {{ display: false }},
                        title: {{ display: true, text: 'Número de Propiedades', font: {{ size: 16 }} }},
                        datalabels: {{ anchor: 'end', align: 'top', color: '#000', font: {{ weight: 'bold' }}, formatter: function(value) {{ return value; }} }}
                    }},
                    scales: {{
                        x: {{ grid: {{ display: false }} }},
                        y: {{ beginAtZero: true, grid: {{ display: false }} }}
                    }}
                }},
                plugins: [ChartDataLabels]
            }});
        }});
    </script>
    """
    return html

def get_edades_from_json(data):
    edades = data.get('edades', {})
    labels = edades.get('labels', [])
    values = edades.get('values', [])
    labels_json = str(labels).replace("'", "\"")
    values_json = str(values)
    html = f"""
    <script>
        document.addEventListener("DOMContentLoaded", function() {{
            const ctx = document.getElementById('EdadChart').getContext('2d');
            const data = {{
                labels: {labels_json},
                datasets: [{{
                    label: 'Rangos de Edad',
                    data: {values_json},
                    backgroundColor: ['#10564F', '#1F6D5E', '#2F746A', '#E87E42', '#E88E42', '#E8A142', '#E8BD42'],
                    borderWidth: 1
                }}]
            }};
            new Chart(ctx, {{
                type: 'bar',
                data: data,
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {{
                        legend: {{ display: false }},
                        title: {{ display: true, text: 'Rangos de Edad', font: {{ size: 16 }} }},
                        datalabels: {{ anchor: 'end', align: 'top', color: '#000', font: {{ weight: 'bold' }}, formatter: function(value) {{ return value; }} }}
                    }},
                    scales: {{
                        x: {{ grid: {{ display: false }} }},
                        y: {{ beginAtZero: true, grid: {{ display: false }} }}
                    }}
                }},
                plugins: [ChartDataLabels]
            }});
        }});
    </script>
    """
    return html

def get_localidades_from_json(data):
    localidades = data.get('datalocalidad', [])
    if len(localidades) > 11:
        localidades = sorted(localidades, key=lambda x: x.get('conteo', 0), reverse=True)
        otros_conteo = sum([loc.get('conteo', 0) for loc in localidades[11:]])
        localidades = localidades[:11] + [{'locnombre': 'Otros', 'conteo': otros_conteo}]
    labels = [loc.get('locnombre', '') for loc in localidades]
    values = [loc.get('conteo', 0) for loc in localidades]
    labels_json = str(labels).replace("'", "\"")
    values_json = str(values)
    html = f"""
    <script>
        document.addEventListener("DOMContentLoaded", function() {{
            const ctx = document.getElementById('LocaChart').getContext('2d');
            const data = {{
                labels: {labels_json},
                datasets: [{{
                    label: 'Localidades',
                    data: {values_json},
                    backgroundColor: 'rgba(54, 162, 235, 0.6)',
                    borderWidth: 1
                }}]
            }};
            new Chart(ctx, {{
                type: 'bar',
                data: data,
                options: {{
                    indexAxis: 'y',
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {{
                        legend: {{ display: false }},
                        title: {{ display: true, text: 'Localidades', font: {{ size: 16 }} }},
                        datalabels: {{ anchor: 'end', align: 'end', color: '#000', font: {{ weight: 'bold' }}, formatter: function(value) {{ return value; }} }}
                    }},
                    scales: {{
                        x: {{ beginAtZero: true, grid: {{ display: false }} }},
                        y: {{ grid: {{ display: false }} }}
                    }}
                }},
                plugins: [ChartDataLabels]
            }});
        }});
    </script>
    """
    return html

def get_dias_visitas_from_json(data):
    dias = data.get('diasVisitas', {})
    labels = dias.get('labels', [])
    values = dias.get('values', [])
    labels_json = str(labels).replace("'", "\"")
    values_json = str(values)
    html = f"""
    <script>
        document.addEventListener("DOMContentLoaded", function() {{
            const ctx = document.getElementById('DiaSemChart').getContext('2d');
            const data = {{
                labels: {labels_json},
                datasets: [{{
                    label: 'Día de la semana',
                    data: {values_json},
                    backgroundColor: ['#10564F', '#1F6D5E', '#2F746A', '#E87E42', '#E88E42', '#E8A142', '#E8BD42'],
                    borderWidth: 1
                }}]
            }};
            new Chart(ctx, {{
                type: 'bar',
                data: data,
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {{
                        legend: {{ display: false }},
                        title: {{ display: true, text: 'Día de la semana', font: {{ size: 16 }} }},
                        datalabels: {{ anchor: 'end', align: 'top', color: '#000', font: {{ weight: 'bold' }}, formatter: function(value) {{ return value; }} }}
                    }},
                    scales: {{
                        x: {{ grid: {{ display: false }} }},
                        y: {{ beginAtZero: true, grid: {{ display: false }} }}
                    }}
                }},
                plugins: [ChartDataLabels]
            }});
        }});
    </script>
    """
    return html

def get_horas_visitas_from_json(data):
    horas = data.get('horasVisitas', {})
    labels = horas.get('labels', [])
    values = horas.get('values', [])
    labels_json = str(labels).replace("'", "\"")
    values_json = str(values)
    html = f"""
    <script>
        document.addEventListener("DOMContentLoaded", function() {{
            const ctx = document.getElementById('HorasVisitaChart').getContext('2d');
            const data = {{
                labels: {labels_json},
                datasets: [{{
                    label: 'Horas de visita',
                    data: {values_json},
                    backgroundColor: ['#10564F', '#E87E42', '#E8BD42'],
                    borderWidth: 1
                }}]
            }};
            new Chart(ctx, {{
                type: 'bar',
                data: data,
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {{
                        legend: {{ display: false }},
                        title: {{ display: true, text: 'Horas de visita', font: {{ size: 16 }} }},
                        datalabels: {{ anchor: 'end', align: 'top', color: '#000', font: {{ weight: 'bold' }}, formatter: function(value) {{ return value; }} }}
                    }},
                    scales: {{
                        x: {{ grid: {{ display: false }} }},
                        y: {{ beginAtZero: true, grid: {{ display: false }} }}
                    }}
                }},
                plugins: [ChartDataLabels]
            }});
        }});
    </script>
    """
    return html

def map_function_from_json(datageometry, latitud, longitud):
    map_leaflet = ""
    if datageometry and datageometry.get('features'):
        geojsonlotes = json.dumps(datageometry)
        map_leaflet = mapa_leaflet_from_json(latitud, longitud, geojsonlotes)
    return map_leaflet

def mapa_leaflet_from_json(latitud, longitud, geojsonlotes):
    html_mapa_leaflet = f"""
    <script>
        var geojsonLotes = {geojsonlotes};
        var map_leaflet = L.map('leaflet-map').setView([{latitud}, {longitud}], 11);
        L.tileLayer('https://{{s}}.basemaps.cartocdn.com/light_all/{{z}}/{{x}}/{{y}}{{r}}.png', {{
            attribution: '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors © <a href="https://carto.com/attributions">CARTO</a>',
            subdomains: 'abcd',
            maxZoom: 20
        }}).addTo(map_leaflet);
        function styleLotes(feature) {{
            return {{
                color: feature.properties.color || '#00ff00',
                weight: 1,
                fillOpacity: 0.4,
            }};
        }}
        function onEachFeature(feature, layer) {{
            if (feature.properties && feature.properties.nombre && feature.properties.conteo) {{
                layer.bindPopup("<b>" + (feature.properties.nombre.includes('SANTA') ? 'Barrio catastral' : 'Localidad') + ":</b> " + feature.properties.nombre + "<br><b>Registros:</b> " + feature.properties.conteo);
            }}
        }}
        L.geoJSON(geojsonLotes, {{
            style: styleLotes,
            onEachFeature: onEachFeature
        }}).addTo(map_leaflet);
    </script>
    """
    return html_mapa_leaflet

if __name__ == "__main__":
    main()