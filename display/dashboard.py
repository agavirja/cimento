import streamlit as st
import pandas as pd
import json
import geopandas as gpd

@st.cache_data(show_spinner=False)
def main(labels, data, datageometry, datalocalidad):

    #-------------------------------------------------------------------------#
    # CSS y HTML final
    #-------------------------------------------------------------------------#
 
    grafica_marcas             = get_marca(data)
    grafica_avaluo_vehiculo    = get_valor_vehiculo(data)
    grafica_numero_vehiculo    = get_numero_vehiculos(data)
    grafica_avaluo_propiedades = get_valor_propiedades(data)
    grafica_estrato            = get_estrato(data)
    grafica_numero_propiedades = get_numero_propiedades(data)
    grafica_localidades        = get_localidades(datalocalidad)
    grafica_edades             = get_edades(data)
    grafica_tipo_vehiculos     = get_tipo_vehiculos(data)
    
    
    latitud                    = 4.687115
    longitud                   = -74.056937
    mapscript                  = map_function(datageometry, latitud, longitud)

    html_content = f'''
    <!DOCTYPE html>
    <html data-bs-theme="light" lang="en">
    
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0, shrink-to-fit=no">
        <title>DashboarD_vehiculos</title>
        <link rel="stylesheet" href="https://iconsapp.nyc3.digitaloceanspaces.com/_estilo_dashboard_vehiculos/bootstrap.min.css">
        <link rel="stylesheet" href="https://iconsapp.nyc3.digitaloceanspaces.com/_estilo_dashboard_vehiculos/styles.css">
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-datalabels"></script>
    </head>
    
    <body>
        <section>
            <div class="container">
                <div class="row">
                    <div class="col-12 col-lg-3 p-2">
                        <div id="box_shadow_default" class="h-100 d-flex flex-column" style="min-height: 100px;">
                            <h1 class="text-center">{"{:,.0f}".format(labels[0]['value'])}</h1>
                            <p class="text-center">{labels[0]['label']}</p>
                        </div>
                    </div>
                    <div class="col-12 col-lg-3 p-2">
                        <div id="box_shadow_default" class="h-100 d-flex flex-column" style="min-height: 100px;">
                            <h1 class="text-center">{"{:,.0f}".format(labels[1]['value'])}</h1>
                            <p class="text-center">{labels[1]['label']}</p>
                        </div>
                    </div>
                    <div class="col-12 col-lg-3 p-2">
                        <div id="box_shadow_default" class="h-100 d-flex flex-column" style="min-height: 100px;">
                            <h1 class="text-center">{"{:,.0f}".format(labels[2]['value'])}</h1>
                            <p class="text-center">{labels[2]['label']}</p>
                        </div>
                    </div>
                    <div class="col-12 col-lg-3 p-2">
                        <div id="box_shadow_default" class="h-100 d-flex flex-column" style="min-height: 100px;">
                            <h1 class="text-center">{"{:,.0f}".format(labels[3]['value'])}</h1>
                            <p class="text-center">{labels[3]['label']}</p>
                        </div>
                    </div>
                </div>
            </div>
        </section>
        <section>
            <div class="container">
                <div class="row">
                    <div class="col-12 col-lg-7 p-2">
                        <div id="box_shadow_default" class="h-100 d-flex flex-column" style="min-height: 500px;">
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
            <div class="container">
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
            <div class="container">
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
            <div class="container">
                <div class="row">
                    <div class="col-12 col-lg-12 p-2">
                        <div id="box_shadow_default" class="h-100 d-flex flex-column" style="min-height: 300px;">
                            <canvas id="marcaChart"></canvas>
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
    </body>
    
    </html>
    '''
    
    return html_content

def get_marca(df):

    df = df[df['marca'].notnull()].drop_duplicates(subset='placa', keep='first')
    df = df['marca'].value_counts().reset_index()
    df.columns = ['marca', 'cantidad']
    df = df.sort_values(by='cantidad', ascending=False)
    
    if len(df) > 11:
        otros = df.iloc[11:]['cantidad'].sum()
        df = pd.concat([df.iloc[:11], pd.DataFrame([{'marca': 'Otros', 'cantidad': otros}])], ignore_index=True)
        
    labels = df['marca'].tolist()
    values = df['cantidad'].tolist()

    labels_json = str(labels).replace("'", "\"")
    values_json = str(values)
    
    html = f"""
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
                        legend: {{
                            display: false  // Desactiva la leyenda para evitar el color en el título
                        }},
                        title: {{
                            display: true,
                            text: 'Marca',
                            font: {{
                                size: 14
                            }}
                        }}
                    }},
                    scales: {{
                        x: {{
                            beginAtZero: true,
                            grid: {{
                                display: false
                            }}
                        }},
                        y: {{
                            grid: {{
                                display: false
                            }}
                        }}
                    }},
                }}
            }});
        }});
    </script>
    """
    return html


def get_valor_vehiculo(df):

    df         = df[df['avaluo'].notnull()].groupby('placa')['avaluo'].max().reset_index()
    df.columns = ['placa','avaluo']
    bins       = [0, 80_000_000, 120_000_000, 200_000_000, float('inf')]
    labels     = ['Menor a 80 MM', '80 MM - 120 MM', '120 MM - 200 MM', 'Más de 200 MM']
    
    # Crear la nueva columna con los rangos
    df['avaluo'] = pd.cut(df['avaluo'], bins=bins, labels=labels, right=True)
    df           = df.groupby('avaluo')['placa'].count().reset_index()
    df.columns   = ['avaluo', 'cantidad']


    labels = df['avaluo'].tolist()
    values = df['cantidad'].tolist()

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
                    backgroundColor: 'rgba(54, 162, 235, 0.6)',
                    borderWidth: 1,
                    backgroundColor: ["#ff6384", "#36a2eb", "#ffce56", "#4bc0c0"],
                }}]
            }};
            new Chart(ctx, {{
                type: 'bar',
                data: data,
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {{
                        legend: {{
                            display: false  // Desactiva la leyenda para evitar el color en el título
                        }},
                        title: {{
                            display: true,
                            text: 'Avalúo de los vehículos',
                            font: {{
                                size: 14
                            }}
                        }}
                    }},
                    scales: {{
                        x: {{
                            beginAtZero: true,
                            grid: {{
                                    display: false
                                }}
                        }},
                        y: {{
                            grid: {{
                                display: false
                            }}
                        }}
                    }}
                }}
            }});
        }});
    </script>
    """
    return html


def get_numero_vehiculos(df):

    def getNumVeh(x):
        try: 
            dd = pd.DataFrame(json.loads(x))
            dd = dd.drop_duplicates(subset='placa',keep='first')
            return len(dd) if len(dd)>0 else 1
            #return len(json.loads(x)) if len(json.loads(x))>0 else 1
        except: return 1

    df['valveh'] = df['vehiculos'].apply(lambda x: getNumVeh(x))
    df           = df.groupby('numID')['valveh'].max().reset_index()
    df.columns   = ['numID','numvehiculos']
    df           = df['numvehiculos'].value_counts().reset_index()
    df.columns   = ['numvehiculos', 'conteo']
    
    if len(df)>4:
        otros = df.iloc[4:]['conteo'].sum()
        df = pd.concat([df.iloc[:4], pd.DataFrame([{'numvehiculos': '+5', 'conteo': otros}])], ignore_index=True)
        
    labels = df['numvehiculos'].tolist()
    values = df['conteo'].tolist()

    labels_json = str(labels).replace("'", "\"")
    values_json = str(values)
    
    html = f"""
    <script>
        document.addEventListener("DOMContentLoaded", function() {{
            const ctx = document.getElementById('VehNumChart').getContext('2d');
            const data = {{
                labels: {labels_json},
                datasets: [{{
                    label: 'Númeo de vehículos',
                    data: {values_json},
                    backgroundColor: 'rgba(54, 162, 235, 0.6)',
                    borderWidth: 1,
                    backgroundColor: ["#ff6384", "#36a2eb", "#ffce56", "#4bc0c0", "#9966ff"],
                }}]
            }};
            new Chart(ctx, {{
                type: 'bar',
                data: data,
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {{
                        legend: {{
                            display: false  // Desactiva la leyenda para evitar el color en el título
                        }},
                        title: {{
                            display: true,
                            text: 'Númeo de vehículos',
                            font: {{
                                size: 14
                            }}
                        }}
                    }},
                    scales: {{
                        x: {{
                            beginAtZero: true,
                            grid: {{
                                    display: false
                                }}
                        }},
                        y: {{
                            grid: {{
                                display: false
                            }}
                        }}
                    }}
                }}
            }});
        }});
    </script>
    """
    return html


def get_tipo_vehiculos(df):

    df           = df[df['clase'].notnull()]
    df           = df.drop_duplicates(subset=['numID','clase'],keep='first')
    df           = df.groupby('clase')['numID'].count().reset_index()
    df.columns   = ['clase','conteo']

    labels = df['clase'].tolist()
    values = df['conteo'].tolist()

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
                    backgroundColor: 'rgba(54, 162, 235, 0.6)',
                    borderWidth: 1,
                    backgroundColor: ["#ff6384", "#36a2eb", "#ffce56", "#4bc0c0", "#9966ff"],
                }}]
            }};
            new Chart(ctx, {{
                type: 'bar',
                data: data,
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {{
                        legend: {{
                            display: false  // Desactiva la leyenda para evitar el color en el título
                        }},
                        title: {{
                            display: true,
                            text: 'Tipo de vehículo',
                            font: {{
                                size: 14
                            }}
                        }}
                    }},
                    scales: {{
                        x: {{
                            beginAtZero: true,
                            grid: {{
                                    display: false
                                }}
                        }},
                        y: {{
                            grid: {{
                                display: false
                            }}
                        }}
                    }}
                }}
            }});
        }});
    </script>
    """
    return html

def get_valor_propiedades(df):

    df         = df[df['avaluocatastral'].notnull()].groupby('numID')['avaluocatastral'].max().reset_index()
    df.columns = ['numID','avaluo']
    bins       = [0, 200_000_000, 300_000_000, 500_000_000, 100_000_000_000, float('inf')]
    labels     = ['Menor a 200 MM', '200 MM - 300 MM', '300 MM - 500 MM', '500 MM - 1,000 MM', 'Más de 1,000 MM']
    
    # Crear la nueva columna con los rangos
    df['avaluo'] = pd.cut(df['avaluo'], bins=bins, labels=labels, right=True)
    df           = df.groupby('avaluo')['numID'].count().reset_index()
    df.columns   = ['avaluo', 'cantidad']


    labels = df['avaluo'].tolist()
    values = df['cantidad'].tolist()

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
                    backgroundColor: 'rgba(54, 162, 235, 0.6)',
                    borderWidth: 1,
                    backgroundColor: ["#ff6384", "#36a2eb", "#ffce56", "#4bc0c0", "#9966ff"],
                }}]
            }};
            new Chart(ctx, {{
                type: 'bar',
                data: data,
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {{
                        legend: {{
                            display: false  // Desactiva la leyenda para evitar el color en el título
                        }},
                        title: {{
                            display: true,
                            text: 'Avalúo catastral de las propiedades',
                            font: {{
                                size: 14
                            }}
                        }}
                    }},
                    scales: {{
                        x: {{
                            beginAtZero: true,
                            grid: {{
                                    display: false
                                }}
                        }},
                        y: {{
                            grid: {{
                                display: false
                            }}
                        }}
                    }}
                }}
            }});
        }});
    </script>
    """
    return html

    
def get_estrato(df):
    df         = df[(df['estrato'] > 0) & (df['estrato'] <= 6)]
    df         = df[df['estrato'].notnull()].groupby('numID')['estrato'].max().reset_index()
    df.columns = ['numID', 'estrato']
    df         = df.groupby('estrato')['numID'].count().reset_index()
    df.columns = ['estrato', 'cantidad']
    
    labels = df['estrato'].tolist()
    values = df['cantidad'].tolist()
    
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
                    backgroundColor: ["#ff6384", "#36a2eb", "#ffce56", "#4bc0c0", "#9966ff", "#c9cbcf"],
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
                        legend: {{
                            display: true,
                            position: 'top'
                        }},
                        title: {{
                            display: true,
                            text: 'Distribución por Estrato',
                            font: {{
                                size: 14
                            }}
                        }},
                        datalabels: {{
                            color: 'white',
                            font: {{
                                size: 16
                            }},
                            formatter: (value, context) => {{
                                return context.chart.data.labels[context.dataIndex];
                            }}
                        }}
                    }}
                }}
            }});
        }});
    </script>
    """
    return html


def get_numero_propiedades(df):

    df           = df[(df['numprop']>0) & (df['numprop']<10)]
    df           = df.groupby('numID')['numprop'].max().reset_index()
    df.columns   = ['numID','numprop']
    df           = df['numprop'].value_counts().reset_index()
    df.columns   = ['numprop', 'conteo']
    
    if len(df)>4:
        otros = df.iloc[4:]['conteo'].sum()
        df = pd.concat([df.iloc[:4], pd.DataFrame([{'numprop': '+5', 'conteo': otros}])], ignore_index=True)
        
    labels = df['numprop'].tolist()
    values = df['conteo'].tolist()

    labels_json = str(labels).replace("'", "\"")
    values_json = str(values)
    
    html = f"""
    <script>
        document.addEventListener("DOMContentLoaded", function() {{
            const ctx = document.getElementById('PropNumChart').getContext('2d');
            const data = {{
                labels: {labels_json},
                datasets: [{{
                    label: 'Númeo de propiedades',
                    data: {values_json},
                    backgroundColor: 'rgba(54, 162, 235, 0.6)',
                    borderWidth: 1,
                    backgroundColor: ["#ff6384", "#36a2eb", "#ffce56", "#4bc0c0", "#9966ff"],
                }}]
            }};
            new Chart(ctx, {{
                type: 'bar',
                data: data,
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {{
                        legend: {{
                            display: false  // Desactiva la leyenda para evitar el color en el título
                        }},
                        title: {{
                            display: true,
                            text: 'Número de Propiedades',
                            font: {{
                                size: 14
                            }}
                        }}
                    }},
                    scales: {{
                        x: {{
                            beginAtZero: true,
                            grid: {{
                                display: false
                            }}
                        }},
                        y: {{
                            grid: {{
                                display: false
                            }}
                        }}
                    }},
                }}
            }});
        }});
    </script>
    """
    return html

def get_edades(df):

    df     = df[(df['edad']>17) & (df['edad']<90)]
    df     = df.drop_duplicates(subset='numID',keep='first')
    bins   = [17, 24, 34, 44, 54, 64, 74, 90]  # Rango de edades ajustado
    labels = ["17-24", "25-34", "35-44", "45-54", "55-64", "65-74", "75-90"]
    df["edad"] = pd.cut(df["edad"], bins=bins, labels=labels, right=True)
    df         = df.groupby('edad')['numID'].count().reset_index()
    df.columns = ['edad','conteo']
    
    labels = df['edad'].tolist()
    values = df['conteo'].tolist()

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
                    backgroundColor: 'rgba(54, 162, 235, 0.6)',
                    borderWidth: 1,
                    backgroundColor: ["#ff6384", "#36a2eb", "#ffce56", "#4bc0c0", "#ff9f40", "#c9cbcf"],
                }}]
            }};
            new Chart(ctx, {{
                type: 'bar',
                data: data,
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {{
                        legend: {{
                            display: false  // Desactiva la leyenda para evitar el color en el título
                        }},
                        title: {{
                            display: true,
                            text: 'Rangos de Edad',
                            font: {{
                                size: 14
                            }}
                        }}
                    }},
                    scales: {{
                        x: {{
                            beginAtZero: true,
                            grid: {{
                                    display: false
                                }}
                        }},
                        y: {{
                            grid: {{
                                display: false
                            }}
                        }}
                    }}
                }}
            }});
        }});
    </script>
    """
    return html


def get_localidades(df):

    df = df[df['conteo']>0].drop_duplicates(subset='locnombre', keep='first')
    df = df.sort_values(by='conteo', ascending=False)
    
    if len(df) > 11:
        otros = df.iloc[11:]['conteo'].sum()
        df = pd.concat([df.iloc[:11], pd.DataFrame([{'locnombre': 'Otros', 'conteo': otros}])], ignore_index=True)
        
    labels = df['locnombre'].tolist()
    values = df['conteo'].tolist()

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
                    indexAxis: 'y', // Hace que la gráfica sea horizontal
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {{
                        legend: {{
                            display: false  // Desactiva la leyenda para evitar el color en el título
                        }},
                        title: {{
                            display: true,
                            text: 'Localidades',
                            font: {{
                                size: 14
                            }}
                        }}
                    }},
                    scales: {{
                        x: {{
                            beginAtZero: true,
                            grid: {{
                                    display: false
                                }}
                        }},
                        y: {{
                            grid: {{
                                display: false
                            }}
                        }}
                    }}
                }}
            }});
        }});
    </script>
    """
    return html



def map_function(datageometry,latitud, longitud):
    map_leaflet = ""
    if (isinstance(latitud, float) or isinstance(latitud, int)) and (isinstance(longitud, float) or isinstance(longitud, int)):
        
        #---------------------------------------------------------------------#
        # Poligono de las otras propiedades
        geojsonlotes = '{"type": "FeatureCollection", "features": [{"id": "0", "type": "Feature", "properties": {"color": "blue"}, "geometry": {"type": "Polygon", "coordinates": []}}]}'
        
        if not datageometry.empty and 'wkt' in datageometry:
            datageometry             = datageometry[datageometry['wkt'].notnull()]
            datageometry['geometry'] = gpd.GeoSeries.from_wkt(datageometry['wkt'])
            datageometry             = gpd.GeoDataFrame(datageometry, geometry='geometry')
         
            for idd, items in datageometry.iterrows():
                
                buildinfinfo = ""
        
                try:    buildinfinfo += f"""<b> Localidad:</b> {items['locnombre']}<br>""" if isinstance(items['locnombre'],str) else ''
                except: pass
                try:    buildinfinfo += f"""<b> Barrio catastral:</b> {items['scanombre']}<br>""" if isinstance(items['scanombre'],str) else ''
                except: pass
                try:    buildinfinfo += f"""<b> Registros:</b> {items['conteo']}<br>""" if isinstance(items['conteo'],(float,int)) else ''
                except: pass     

                popup_content = f'''
                <!DOCTYPE html>
                <html>
                    <body>
                        {buildinfinfo}
                    </body>
                </html>
                '''
                datageometry.loc[idd, 'popup'] = popup_content
            
            geojsonlotes = datageometry.to_json()
        
        map_leaflet = mapa_leaflet(latitud, longitud, geojsonlotes)

    return map_leaflet

def mapa_leaflet(latitud, longitud, geojsonlotes):
    html_mapa_leaflet = f"""
        <link rel="stylesheet" href="https://unpkg.com/leaflet@1.7.1/dist/leaflet.css" />
        <script src="https://unpkg.com/leaflet@1.7.1/dist/leaflet.js"></script>
        <script>
            var geojsonLotes = {geojsonlotes};
    
            var map_leaflet = L.map('leaflet-map').setView([{latitud}, {longitud}], 11);
            
            L.tileLayer('https://{{s}}.basemaps.cartocdn.com/light_all/{{z}}/{{x}}/{{y}}{{r}}.png', {{
                attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
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
                if (feature.properties && feature.properties.popup) {{
                    layer.bindPopup(feature.properties.popup);
                }}
            }}
    
            L.geoJSON(geojsonLotes, {{
                style: styleLotes,
                onEachFeature: onEachFeature
            }}).addTo(map_leaflet);
        </script>
    """
    return html_mapa_leaflet
