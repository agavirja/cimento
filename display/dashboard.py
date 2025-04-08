import streamlit as st
import pandas as pd
import json
import geopandas as gpd
import numpy as np

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
    grafica_diasem             = get_dias_visitas(data)
    grafica_horasvisita        = get_horas_visitas(data)
    
    latitud                    = 4.687115
    longitud                   = -74.056937
    mapscript                  = map_function(datageometry, latitud, longitud)
        
    min_val = datageometry['conteo'].min()
    max_val = datageometry['conteo'].max()
    ticks   = np.linspace(min_val, max_val, 5, dtype=int)


    html_content = f'''
    <!DOCTYPE html>
    <html data-bs-theme="light" lang="en">
    
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0, shrink-to-fit=no">
        <link rel="stylesheet" href="https://iconsapp.nyc3.digitaloceanspaces.com/_estilo_dashboard_vehiculos/bootstrap.min.css">
        <link rel="stylesheet" href="https://iconsapp.nyc3.digitaloceanspaces.com/_estilo_dashboard_vehiculos/styles.css">
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-datalabels"></script>
        <script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-datalabels@2"></script>
    </head>
    
    <body>
        <section>
            <div class="container-fluid">
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
            <div class="container-fluid">
                <div class="row">
                    <div class="col-12 col-lg-7 p-2">
                    <div id="box_shadow_default" class="h-100 d-flex flex-column" style="min-height: 500px; position: relative;">
                    
                    <div style="position: absolute;top: 10px;right: 10px;background-color: white;padding: 10px;border: 1px solid #ccc;border-radius: 5px;font-family: sans-serif;font-size: 12px;z-index: 9999;">
                      <div style="width: 200px;height: 20px;background: linear-gradient(to right, #440154, #443983, #31688e, #35b779, #fde725);margin-top: 5px;margin-bottom: 5px;"></div>
                      <div style="display: flex; justify-content: space-between; width: 200px;">
                        {''.join([f"<span>{val}</span>" for val in ticks])}
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
    <!-- Plugin Chart.js Datalabels -->
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
                        legend: {{
                            display: false
                        }},
                        title: {{
                            display: true,
                            text: 'Marca',
                            font: {{
                                size: 16
                            }}
                        }},
                        datalabels: {{
                            anchor: 'end',
                            align: 'top',
                            color: '#000',
                            font: {{
                                weight: 'bold'
                            }},
                            formatter: function(value) {{
                                return value;
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
                }},
                plugins: [ChartDataLabels]
            }});
        }});
    </script>
    """
    return html

def get_valor_vehiculo(df):
    df = df[df['avaluo'].notnull()].groupby('placa')['avaluo'].max().reset_index()
    df.columns = ['placa','avaluo']
    bins = [0, 80_000_000, 120_000_000, 200_000_000, float('inf')]
    labels = ['Menor a 80 MM', '80 MM - 120 MM', '120 MM - 200 MM', 'Más de 200 MM']
    
    df['avaluo'] = pd.cut(df['avaluo'], bins=bins, labels=labels, right=True)
    df = df.groupby('avaluo')['placa'].count().reset_index()
    df.columns = ['avaluo', 'cantidad']

    labels = df['avaluo'].astype(str).tolist()
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
                        legend: {{
                            display: false
                        }},
                        title: {{
                            display: true,
                            text: 'Avalúo de los vehículos',
                            font: {{
                                size: 16
                            }}
                        }},
                        datalabels: {{
                            anchor: 'end',
                            align: 'top',
                            color: '#000',
                            font: {{
                                weight: 'bold'
                            }},
                            formatter: function(value) {{
                                return value;
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
                }},
                plugins: [ChartDataLabels]
            }});
        }});
    </script>
    """
    return html

def get_numero_vehiculos(df):
    def getNumVeh(x):
        try: 
            dd = pd.DataFrame(json.loads(x))
            dd = dd.drop_duplicates(subset='placa', keep='first')
            return len(dd) if len(dd) > 0 else 1
        except:
            return 1

    df['valveh'] = df['vehiculos'].apply(lambda x: getNumVeh(x))
    df = df.groupby('numID')['valveh'].max().reset_index()
    df.columns = ['numID', 'numvehiculos']
    df = df['numvehiculos'].value_counts().reset_index()
    df.columns = ['numvehiculos', 'conteo']

    if len(df) > 4:
        otros = df.iloc[4:]['conteo'].sum()
        df = pd.concat([df.iloc[:4], pd.DataFrame([{'numvehiculos': '+5', 'conteo': otros}])], ignore_index=True)

    labels = df['numvehiculos'].astype(str).tolist()
    values = df['conteo'].tolist()

    labels_json = str(labels).replace("'", "\"")
    values_json = str(values)

    html = f"""
    <!-- Plugin Chart.js Datalabels -->
    <script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-datalabels@2"></script>

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
                        legend: {{
                            display: false
                        }},
                        title: {{
                            display: true,
                            text: 'Número de vehículos',
                            font: {{
                                size: 16
                            }}
                        }},
                        datalabels: {{
                            anchor: 'end',
                            align: 'top',
                            color: '#000',
                            font: {{
                                weight: 'bold'
                            }},
                            formatter: function(value) {{
                                return value;
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
                }},
                plugins: [ChartDataLabels]
            }});
        }});
    </script>
    """
    return html

def get_tipo_vehiculos(df):
    df = df[df['clase'].notnull()]
    df = df.drop_duplicates(subset=['numID', 'clase'], keep='first')
    df = df.groupby('clase')['numID'].count().reset_index()
    df.columns = ['clase', 'conteo']

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
                        legend: {{
                            display: false
                        }},
                        title: {{
                            display: true,
                            text: 'Tipo de vehículo',
                            font: {{
                                size: 16
                            }}
                        }},
                        datalabels: {{
                            anchor: 'end',
                            align: 'top',
                            color: '#000',
                            font: {{
                                weight: 'bold'
                            }},
                            formatter: function(value) {{
                                return value;
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
                }},
                plugins: [ChartDataLabels]
            }});
        }});
    </script>
    """
    return html


def get_valor_propiedades(df):
    df = df[df['avaluocatastral'].notnull()].groupby('numID')['avaluocatastral'].max().reset_index()
    df.columns = ['numID','avaluo']
    
    bins = [0, 200_000_000, 300_000_000, 500_000_000, 100_000_000_000, float('inf')]
    labels = ['Menor a 200 MM', '200 MM - 300 MM', '300 MM - 500 MM', '500 MM - 1,000 MM', 'Más de 1,000 MM']
    
    df['avaluo'] = pd.cut(df['avaluo'], bins=bins, labels=labels, right=True)
    df = df.groupby('avaluo')['numID'].count().reset_index()
    df.columns = ['avaluo', 'cantidad']

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
                        legend: {{
                            display: false
                        }},
                        title: {{
                            display: true,
                            text: 'Avalúo catastral de las propiedades',
                            font: {{
                                size: 16
                            }}
                        }},
                        datalabels: {{
                            anchor: 'end',
                            align: 'top',
                            color: '#000',
                            font: {{
                                weight: 'bold'
                            }},
                            formatter: function(value) {{
                                return value;
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
                }},
                plugins: [ChartDataLabels]
            }});
        }});
    </script>
    """
    return html


def get_estrato(df):
    df = df[(df['estrato'] > 0) & (df['estrato'] <= 6)]
    df = df[df['estrato'].notnull()].groupby('numID')['estrato'].max().reset_index()
    df.columns = ['numID', 'estrato']
    df = df.groupby('estrato')['numID'].count().reset_index()
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
                        legend: {{
                            display: true,
                            position: 'top'
                        }},
                        title: {{
                            display: true,
                            text: 'Distribución por Estrato',
                            font: {{
                                size: 16
                            }}
                        }},
                        datalabels: {{
                            color: 'white',
                            font: {{
                                size: 16,
                                weight: 'bold'
                            }},
                            formatter: function(value) {{
                                return value;
                            }}
                        }}
                    }}
                }},
                plugins: [ChartDataLabels]
            }});
        }});
    </script>
    """
    return html

def get_numero_propiedades(df):

    df = df[(df['numprop'] > 0) & (df['numprop'] < 10)]
    df = df.groupby('numID')['numprop'].max().reset_index()
    df.columns = ['numID', 'numprop']
    df = df['numprop'].value_counts().reset_index()
    df.columns = ['numprop', 'conteo']

    if len(df) > 4:
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
                        legend: {{
                            display: false
                        }},
                        title: {{
                            display: true,
                            text: 'Número de Propiedades',
                            font: {{
                                size: 16
                            }}
                        }},
                        datalabels: {{
                            anchor: 'end',
                            align: 'top',
                            color: '#000',
                            font: {{
                                weight: 'bold'
                            }},
                            formatter: function(value) {{
                                return value;
                            }}
                        }}
                    }},
                    scales: {{
                        x: {{
                            grid: {{
                                display: false
                            }}
                        }},
                        y: {{
                            beginAtZero: true,
                            grid: {{
                                display: false
                            }}
                        }}
                    }}
                }},
                plugins: [ChartDataLabels]
            }});
        }});
    </script>
    """
    return html

def get_edades(df):

    df     = df[(df['edad'] > 17) & (df['edad'] < 90)]
    df     = df.drop_duplicates(subset='numID', keep='first')
    bins   = [17, 24, 34, 44, 54, 64, 74, 90]
    labels = ["17-24", "25-34", "35-44", "45-54", "55-64", "65-74", "75-90"]
    df["edad"] = pd.cut(df["edad"], bins=bins, labels=labels, right=True)
    df         = df.groupby('edad')['numID'].count().reset_index()
    df.columns = ['edad', 'conteo']

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
                        legend: {{
                            display: false
                        }},
                        title: {{
                            display: true,
                            text: 'Rangos de Edad',
                            font: {{
                                size: 16
                            }}
                        }},
                        datalabels: {{
                            anchor: 'end',
                            align: 'top',
                            color: '#000',
                            font: {{
                                weight: 'bold'
                            }},
                            formatter: function(value) {{
                                return value;
                            }}
                        }}
                    }},
                    scales: {{
                        x: {{
                            grid: {{
                                display: false
                            }}
                        }},
                        y: {{
                            beginAtZero: true,
                            grid: {{
                                display: false
                            }}
                        }}
                    }}
                }},
                plugins: [ChartDataLabels]
            }});
        }});
    </script>
    """
    return html

def get_localidades(df):

    df = df[df['conteo'] > 0].drop_duplicates(subset='locnombre', keep='first')
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
                    indexAxis: 'y',
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {{
                        legend: {{
                            display: false
                        }},
                        title: {{
                            display: true,
                            text: 'Localidades',
                            font: {{
                                size: 16
                            }}
                        }},
                        datalabels: {{
                            anchor: 'end',
                            align: 'end',
                            color: '#000',
                            font: {{
                                weight: 'bold'
                            }},
                            formatter: function(value) {{
                                return value;
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
                }},
                plugins: [ChartDataLabels]
            }});
        }});
    </script>
    """
    return html

def get_dias_visitas(df):
    
    df         = df.drop_duplicates(subset='placa', keep='first')
    df         = df.groupby('dia_semana')['numID'].count().reset_index()
    df.columns = ['dia_semana', 'conteo']

    labels = df['dia_semana'].tolist()
    values = df['conteo'].tolist()

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
                        legend: {{
                            display: false
                        }},
                        title: {{
                            display: true,
                            text: 'Día de la semana',
                            font: {{
                                size: 16
                            }}
                        }},
                        datalabels: {{
                            anchor: 'end',
                            align: 'top',
                            color: '#000',
                            font: {{
                                weight: 'bold'
                            }},
                            formatter: function(value) {{
                                return value;
                            }}
                        }}
                    }},
                    scales: {{
                        x: {{
                            grid: {{
                                display: false
                            }}
                        }},
                        y: {{
                            beginAtZero: true,
                            grid: {{
                                display: false
                            }}
                        }}
                    }}
                }},
                plugins: [ChartDataLabels]
            }});
        }});
    </script>
    """
    return html


def get_horas_visitas(df):

    df         = df.drop_duplicates(subset='placa', keep='first')
    df         = df.groupby('franja_horaria')['numID'].count().reset_index()
    df.columns = ['franja_horaria', 'conteo']

    labels = df['franja_horaria'].tolist()
    values = df['conteo'].tolist()

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
                    backgroundColor: ['#10564F', '#E87E42','#E8BD42'],
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
                            display: false
                        }},
                        title: {{
                            display: true,
                            text: 'Horas de visita',
                            font: {{
                                size: 16
                            }}
                        }},
                        datalabels: {{
                            anchor: 'end',
                            align: 'top',
                            color: '#000',
                            font: {{
                                weight: 'bold'
                            }},
                            formatter: function(value) {{
                                return value;
                            }}
                        }}
                    }},
                    scales: {{
                        x: {{
                            grid: {{
                                display: false
                            }}
                        }},
                        y: {{
                            beginAtZero: true,
                            grid: {{
                                display: false
                            }}
                        }}
                    }}
                }},
                plugins: [ChartDataLabels]
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
