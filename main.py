import uuid
import os
import boto3
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List, Optional, Dict, Any, Union
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

# Importar las funciones del archivo general_functions.py
from data._busqueda_general import main as busqueda_general_predios
from data._busqueda_lotes import main as _busqueda_lotes
from data._get_combinacion_lotes import main as get_combinacion_lotes

from data._get_snr_general import main as get_snr_general
from data._get_snr_polygon import main as get_snr_polygon
from data._get_predial_general import main as get_predial_general
from data._get_predial_polygon import main as get_predial_polygon
from data._get_predial_link import main as get_predial_link
from data._get_lotes_optimos_polygon import main as get_lotes_optimos_polygon

from data._get_lotes_caracteristicas import main as get_lotes_caracteristicas
from data._get_lotes_polygon import main as get_lotes_polygon
from data._get_lotes_construcciones import main as get_lotes_construcciones

from data._get_propietarios import main as get_propietarios
from data._get_properties_by_id import main as get_properties_by_id
from data._get_propietarios_radio import main as get_propietarios_radio
from data._get_vehiculos_by_id import main as get_vehiculos_by_id

from data._get_pot import main as get_pot
from data._get_ctl_licencias import ctl as get_ctl, licencias_construccion as get_licencias
from data._get_dane import main as get_dane

from data._get_market_analysis import main as get_market_analysis
from data._get_market_scacodigo import main as get_market_scacodigo
from data._get_property_info import main as get_property_info
from data._get_galeria import main as get_galeria
from data._get_sinupot_pdf import main as get_sinupot_pdf

from data._get_brand_data import main as get_brand_data


from data._get_latlng import main as get_latlng
from data._read_ctl import main as read_ctl
from data._html2PDF import main as _html2PDF
from data._html2PDF_asincronico import generate_and_upload_async

from data._save_search import main as save_search
from data._permisos import otorgar_permisos_download, eliminar_permisos_download, get_usuario_download, otorgar_permisos_galeria, get_usuario_galeria, eliminar_permisos_galeria

from data._html2PDF_hybrid import main_hybrid, main_sync, main_async, generate_and_upload_background, check_file_exists

from data._get_cabida_AI import main as get_cabida_AI

# APIs Agregadas por servicio
from data._get_detalle_building import main as get_detalle_building
from data._get_detalle_unidad import main as get_detalle_unidad
from data._get_estudio_mercado import main as get_estudio_mercado
from data._get_detalle_cabida import main as get_detalle_cabida
from data._get_detalle_propietarios import main as get_detalle_propietarios

# Test de placas Fontanar
from data._test_fontanar import main as test_fontanar


app = FastAPI(
    title="Urbex",
    description="API de propiedad de Urbex",
    version="1.0.0"
)

# powershell
# cd "D:\Dropbox\Empresa\Urbex\_API_Urbex"
# Get-ChildItem -Path . -Include __pycache__, *.pyc -Recurse | Remove-Item -Recurse -Force
# python -m venv venv
# .\venv\Scripts\Activate
# pip install -r requirements.txt
# uvicorn main:app --reload --host 0.0.0.0 --port 8000
# postman: http://127.0.0.1:8000/process  # Configuración de la base de datos
# pipreqs --encoding utf-8 "D:\Dropbox\Empresa\Urbex\_API_Urbex"

# Middleware para manejo de errores

@app.middleware("http")
async def handle_exceptions(request, call_next):
    try:
        return await call_next(request)
    except Exception as e:
        import traceback
        error_detail = traceback.format_exc()
        print(f"Error no manejado: {error_detail}")
        return JSONResponse(
            status_code=500,
            content={"detail": f"Error interno del servidor: {str(e)}"}
        )

# Ruta para verificar que la API está funcionando
@app.get("/")
async def root():
    return {
        "message": "Property Data API está funcionando correctamente",
        "version": "1.0.0",
        "endpoints": {
            "/property_data": "POST - Obtener datos de propiedades basado en barmanpre"
        }
    }

# ——————————————————————————————————————————————————————————————————————————— #
# Busqueda general de predios
# ——————————————————————————————————————————————————————————————————————————— #
class BusquedaGeneralInput(BaseModel):
    tipoinmueble: Optional[List[str]] = []
    areamin: Optional[float] = 0.0
    areamax: Optional[float] = 0.0
    antiguedadmin: Optional[int] = 0
    antiguedadmax: Optional[int] = 0
    estratomin: Optional[int] = 0
    estratomax: Optional[int] = 0
    precuso: Optional[List[str]] = []
    polygon: str

@app.post("/busqueda_general")  
async def busqueda_general(input_data: BusquedaGeneralInput):
    results = {}
    try:
        if not input_data.polygon:
            raise HTTPException(status_code=400, detail="Poligono es requerido")
            
        inputvar = {
            'tipoinmueble': input_data.tipoinmueble,
            'areamin': input_data.areamin,
            'areamax': input_data.areamax,
            'antiguedadmin': input_data.antiguedadmin,
            'antiguedadmax': input_data.antiguedadmax,
            'estratomin': input_data.estratomin,
            'estratomax': input_data.estratomax,
            'precuso': input_data.precuso,
            'polygon': input_data.polygon
         }
        
        results = busqueda_general_predios(inputvar=inputvar)

        return JSONResponse(content=results, status_code=200)
    except: 
        return JSONResponse(content=results, status_code=400)
    
    
# ——————————————————————————————————————————————————————————————————————————— #
# Get Detalle Building [API agregada]
# ——————————————————————————————————————————————————————————————————————————— #
class DetalleBuildingInput(BaseModel):
    barmanpre: str
    get_tabla: bool = True
    get_tabla_last_year: bool = True
    
@app.post("/getDetalleBuilding")  
async def getDetalleBuilding(input_data: DetalleBuildingInput):
    results = {}
    try:
        if not input_data.barmanpre:
            raise HTTPException(status_code=400, detail="barmanpre es requerido")
            
        inputvar = {
            'barmanpre':input_data.barmanpre,
            'get_tabla': input_data.get_tabla,
            'get_tabla_last_year': input_data.get_tabla_last_year, 
            }
        results = get_detalle_building(inputvar=inputvar)
        return JSONResponse(content=results, status_code=200)
    except: 
        return JSONResponse(content=results, status_code=400)
    
# ——————————————————————————————————————————————————————————————————————————— #
# Get Detalle Unidad [API agregada]
# ——————————————————————————————————————————————————————————————————————————— #   
@app.post("/getDetalleUnidad")  
async def getDetalleUnidad(input_data: DetalleBuildingInput):
    results = {}
    try:
        if not input_data.barmanpre:
            raise HTTPException(status_code=400, detail="barmanpre es requerido")
            
        inputvar = {
            'barmanpre':input_data.barmanpre,
            'get_tabla': input_data.get_tabla,
            'get_tabla_last_year': input_data.get_tabla_last_year, 
            }
        results = get_detalle_unidad(inputvar=inputvar)
        return JSONResponse(content=results, status_code=200)
    except: 
        return JSONResponse(content=results, status_code=400)

# ——————————————————————————————————————————————————————————————————————————— #
# Get Estudio de Mercado [API agregada]
# ——————————————————————————————————————————————————————————————————————————— #
class EstudioMercadoInput(BaseModel):
    polygon: str
    barmanpre: Optional[str] = None
    areamin: Optional[float] = 0
    areamax: Optional[float] = 0
    desde_antiguedad: Optional[int] = 0
    hasta_antiguedad: Optional[int] = 0
    antiguedadmin: Optional[int] = 0
    antiguedadmax: Optional[int] = 0
    pisosmin: Optional[int] = 0
    pisosmax: Optional[int] = 0
    estratomin: Optional[int] = 0
    estratomax: Optional[int] = 0
    precuso: Optional[List[str]] = []
    latitud: Optional[float] = None
    longitud: Optional[float] = None
    segmentacion: Optional[str] = None
    spatialRelation: Optional[str] = 'intersects' # intersects | contains 
    tabla: Optional[str] = "bogota_data_lotes_fastsearch"
    get_tabla: Optional[bool] = False

@app.post("/getEstudioMercado")  
async def getEstudioMercado(input_data: EstudioMercadoInput):
    results = {}
    try:

        inputvar = {
            'barmanpre': input_data.barmanpre,
            'polygon':input_data.polygon,
            'areamin':input_data.areamin,
            'areamax':input_data.areamax,
            'desde_antiguedad':input_data.desde_antiguedad,
            'hasta_antiguedad':input_data.hasta_antiguedad,
            'antiguedadmin': input_data.antiguedadmin,
            'antiguedadmax': input_data.antiguedadmax,
            'pisosmin':input_data.pisosmin,
            'pisosmax':input_data.pisosmax,
            'estratomin':input_data.estratomin,
            'estratomax':input_data.estratomax,
            'precuso':input_data.precuso,
            'latitud':input_data.latitud,
            'longitud':input_data.longitud,
            'segmentacion':input_data.segmentacion,
            'spatialRelation': input_data.spatialRelation,
            'tabla': input_data.tabla,
            'get_tabla': input_data.get_tabla
            }
        results = get_estudio_mercado(inputvar=inputvar)
        return JSONResponse(content=results, status_code=200)
    except: 
        return JSONResponse(content=results, status_code=400)
    
# ——————————————————————————————————————————————————————————————————————————— #
# Get Detalle Cabida [API agregada]
# ——————————————————————————————————————————————————————————————————————————— #   
@app.post("/getDetalleCabida")  
async def getDetalleCabida(input_data: DetalleBuildingInput):
    results = {}
    try:
        if not input_data.barmanpre:
            raise HTTPException(status_code=400, detail="barmanpre es requerido")
            
        inputvar = {
            'barmanpre':input_data.barmanpre,
            }
        results = get_detalle_cabida(inputvar=inputvar)
        return JSONResponse(content=results, status_code=200)
    except: 
        return JSONResponse(content=results, status_code=400)

# ——————————————————————————————————————————————————————————————————————————— #
# Get Detalle Cabida [API agregada]
# ——————————————————————————————————————————————————————————————————————————— #   
@app.post("/getDetallePropietarios")  
async def getDetallePropietarios(input_data: DetalleBuildingInput):
    results = {}
    try:
        if not input_data.barmanpre:
            raise HTTPException(status_code=400, detail="barmanpre es requerido")
            
        inputvar = {
            'barmanpre':input_data.barmanpre,
            }
        results = get_detalle_propietarios(inputvar=inputvar)
        return JSONResponse(content=results, status_code=200)
    except: 
        return JSONResponse(content=results, status_code=400)





# ——————————————————————————————————————————————————————————————————————————— #
# Busqueda lotes
# ——————————————————————————————————————————————————————————————————————————— #
class BusquedaLotesInput(BaseModel):
    areaminlote: Optional[float] = 0.0
    areamaxlote: Optional[float] = 0.0
    estratomin: Optional[int] = 0
    estratomax: Optional[int] = 0
    pisos:  Optional[Union[int, str]] = None
    altura_min_pot: Optional[Union[int, str]] = None
    tratamiento: Optional[Union[int, str]] = None
    actuacion_estrategica: Optional[Union[int, str]] = None
    area_de_actividad: Optional[Union[int, str]] = None
    via_principal: Optional[Union[int, str]] = None
    numero_propietarios: Optional[Union[int, str]] = None
    localidad: Optional[Union[str, List[str], int]] = None
    polygon:  Optional[str] = None
    precuso: Optional[List[str]] = []
    
@app.post("/busqueda_lotes")  
async def busqueda_lotes(input_data: BusquedaLotesInput):
    results = {}
    try:  
        inputvar = {
            'areaminlote':           input_data.areaminlote,
            'areamaxlote':           input_data.areamaxlote,
            'estratomin':            input_data.estratomin,
            'estratomax':            input_data.estratomax,
            'pisos':                 input_data.pisos,
            'altura_min_pot':        input_data.altura_min_pot,
            'tratamiento':           input_data.tratamiento,
            'actuacion_estrategica': input_data.actuacion_estrategica,
            'area_de_actividad':     input_data.area_de_actividad,
            'via_principal':         input_data.via_principal,
            'numero_propietarios':   input_data.numero_propietarios,
            'localidad':             input_data.localidad,
            'polygon':               input_data.polygon,
            'precuso':               input_data.precuso,
        }
        results = _busqueda_lotes(inputvar=inputvar)

        return JSONResponse(content=results, status_code=200)
    except: 
        return JSONResponse(content=results, status_code=400)
    
# ——————————————————————————————————————————————————————————————————————————— #
# Combinacion de lotes
# ——————————————————————————————————————————————————————————————————————————— #
class CombinacionLoteInput(BaseModel):
    barmanpre: str
    
@app.post("/combinacion_lotes")  
async def combinacion_lotes(input_data: CombinacionLoteInput):
    results = {}
    try:  
        if not input_data.barmanpre:
            raise HTTPException(status_code=400, detail="barmanpre es requerido")
            
        inputvar = {
            'barmanpre':input_data.barmanpre,
        }
        results = get_combinacion_lotes(inputvar=inputvar)

        return JSONResponse(content=results, status_code=200)
    except: 
        return JSONResponse(content=results, status_code=400)
    
    
# ——————————————————————————————————————————————————————————————————————————— #
# API general get predios SNR | poligono
# ——————————————————————————————————————————————————————————————————————————— #
class TransaccionesInput(BaseModel):
    barmanpre: str
    chip: Optional[str] = None
    matriculainmobiliaria: Optional[str] = None
    get_tabla: bool = True
    
    areamin: Optional[int] = 0
    areamax: Optional[int] = 0
    desde_antiguedad: Optional[int] = 0
    hasta_antiguedad: Optional[int] = 0
    estratomin: Optional[int] = 0
    estratomax: Optional[int] = 0
    precuso: Optional[List[str]] = []
    polygon: Optional[str] = None
    
@app.post("/transacciones")  
async def transacciones(input_data: TransaccionesInput):
    results = {}
    try:
        if not input_data.barmanpre:
            raise HTTPException(status_code=400, detail="barmanpre es requerido")
            
        inputvar = {
            'barmanpre':input_data.barmanpre,
            'chip':input_data.chip,
            'matriculainmobiliaria':input_data.matriculainmobiliaria,
            'get_tabla': input_data.get_tabla,
            'areamin': input_data.areamin,
            'areamax': input_data.areamax,
            'desde_antiguedad': input_data.desde_antiguedad,
            'hasta_antiguedad': input_data.hasta_antiguedad,
            'estratomin': input_data.estratomin,
            'estratomax': input_data.estratomax,
            'precuso': input_data.precuso,
            }
        results = get_snr_general(inputvar=inputvar)
        return JSONResponse(content=results, status_code=200)
    except: 
        return JSONResponse(content=results, status_code=400)

@app.post("/transacciones_polygon")
async def transacciones_polygon(input_data: TransaccionesInput):
    results = {}
    try:
        if not input_data.barmanpre:
            raise HTTPException(status_code=400, detail="barmanpre es requerido")
            
        inputvar = {
            'barmanpre':input_data.barmanpre,
            'chip':input_data.chip,
            'matriculainmobiliaria':input_data.matriculainmobiliaria,
            'get_tabla': input_data.get_tabla,
            'areamin': input_data.areamin,
            'areamax': input_data.areamax,
            'desde_antiguedad': input_data.desde_antiguedad,
            'hasta_antiguedad': input_data.hasta_antiguedad,
            'estratomin': input_data.estratomin,
            'estratomax': input_data.estratomax,
            'precuso': input_data.precuso,
            'polygon': input_data.polygon
            }
        results = get_snr_polygon(inputvar=inputvar)
        return JSONResponse(content=results, status_code=200)
    except: 
        return JSONResponse(content=results, status_code=400)
    

# ——————————————————————————————————————————————————————————————————————————— #
# API general get prediales | avaluo | poligono
# ——————————————————————————————————————————————————————————————————————————— #
class PredialesInput(BaseModel):
    barmanpre: str
    chip: Optional[str] = None
    matriculainmobiliaria: Optional[str] = None
    get_tabla: bool = True
    get_tabla_last_year: bool = True
    
    areamin: Optional[int] = 0
    areamax: Optional[int] = 0
    desde_antiguedad: Optional[int] = 0
    hasta_antiguedad: Optional[int] = 0
    estratomin: Optional[int] = 0
    estratomax: Optional[int] = 0
    precuso: Optional[List[str]] = []
    polygon: Optional[str] = None
    
@app.post("/prediales")  
async def prediales(input_data: PredialesInput):
    results = {}
    try:
        if not input_data.barmanpre:
            raise HTTPException(status_code=400, detail="barmanpre es requerido")
            
        inputvar = {
            'barmanpre':input_data.barmanpre,
            'chip':input_data.chip,
            'matriculainmobiliaria':input_data.matriculainmobiliaria,
            'get_tabla': input_data.get_tabla,
            'get_tabla_last_year': input_data.get_tabla_last_year, 
            'areamin': input_data.areamin,
            'areamax': input_data.areamax,
            'desde_antiguedad': input_data.desde_antiguedad,
            'hasta_antiguedad': input_data.hasta_antiguedad,
            'estratomin': input_data.estratomin,
            'estratomax': input_data.estratomax,
            'precuso': input_data.precuso,
            }
        results = get_predial_general(inputvar=inputvar)
        return JSONResponse(content=results, status_code=200)
    except: 
        return JSONResponse(content=results, status_code=400)
    
@app.post("/prediales_polygon")  
async def prediales_polygon(input_data: PredialesInput):
    results = {}
    try:
        if not input_data.barmanpre:
            raise HTTPException(status_code=400, detail="barmanpre es requerido")
            
        inputvar = {
            'barmanpre':input_data.barmanpre,
            'chip':input_data.chip,
            'matriculainmobiliaria':input_data.matriculainmobiliaria,
            'areamin': input_data.areamin,
            'areamax': input_data.areamax,
            'desde_antiguedad': input_data.desde_antiguedad,
            'hasta_antiguedad': input_data.hasta_antiguedad,
            'estratomin': input_data.estratomin,
            'estratomax': input_data.estratomax,
            'precuso': input_data.precuso,
            'polygon': input_data.polygon
            }
        results = get_predial_polygon(inputvar=inputvar)
        return JSONResponse(content=results, status_code=200)
    except: 
        return JSONResponse(content=results, status_code=400)
        

class PredialLinkInput(BaseModel):
    year: Optional[Union[int, float]] = None
    chip: Optional[str] = None
   
@app.post("/predial_link")  
async def predial_link(input_data: PredialLinkInput):
    results = {}
    try:
        if not input_data.chip:
            raise HTTPException(status_code=400, detail="Chip es requerido")

        inputvar = {
            'year':input_data.year,
            'chip':input_data.chip
            }
        results = get_predial_link(inputvar=inputvar)
        
        return JSONResponse(content=results, status_code=200)
    except: 
        return JSONResponse(content=results, status_code=400)
    
    
# ——————————————————————————————————————————————————————————————————————————— #
# API lista barmanpre optima en un poligono para estadisticas
# ——————————————————————————————————————————————————————————————————————————— #
class BarmanpreEstadisticasInput(BaseModel):
    polygon: str = None
    segmentacion: Optional[str] = None
    spatialRelation: Optional[str] = 'intersects' # intersects | contains
    tabla: Optional[str] = "bogota_data_lotes_fastsearch"
    
@app.post("/lista_optima_barmanpre")  
async def lista_optima_barmanpre(input_data: BarmanpreEstadisticasInput):
    results = {}
    try:
        if not input_data.polygon:
            raise HTTPException(status_code=400, detail="Poligono es requerido")
            
        inputvar = {
            'polygon': input_data.polygon,
            'segmentacion': input_data.segmentacion,
            'spatialRelation': input_data.spatialRelation,
            'tabla': input_data.tabla
            }
        results = get_lotes_optimos_polygon(inputvar=inputvar)
        return JSONResponse(content=results, status_code=200)
    except: 
        return JSONResponse(content=results, status_code=400)

class BarmanpreFromPolygonInput(BaseModel):
    polygon: str = None
    segmentacion: Optional[str] = None
    spatialRelation: Optional[str] = 'intersects' # intersects | contains 
    
# ——————————————————————————————————————————————————————————————————————————— #
# API get lotes caracteristicas
# ——————————————————————————————————————————————————————————————————————————— #
class PredioCaracteristicasInput(BaseModel):
    barmanpre: str

@app.post("/lotes_caracteristicas")  
async def lotes_caracteristicas(input_data: PredioCaracteristicasInput):
    results = {}
    try:
        if not input_data.barmanpre:
            raise HTTPException(status_code=400, detail="barmanpre es requerido")
            
        inputvar = {
            'barmanpre':input_data.barmanpre,
            }
        results = get_lotes_caracteristicas(inputvar=inputvar)
        return JSONResponse(content=results, status_code=200)
    except: 
        return JSONResponse(content=results, status_code=400)
    
# ——————————————————————————————————————————————————————————————————————————— #
# API get lotes caracteristicas por poligono
# ——————————————————————————————————————————————————————————————————————————— #
class PredioCaracteristicasPolygonInput(BaseModel):
    polygon: str
    areamin: Optional[float] = 0
    areamax: Optional[float] = 0
    desde_antiguedad: Optional[int] = 0
    hasta_antiguedad: Optional[int] = 0
    pisosmin: Optional[int] = 0
    pisosmax: Optional[int] = 0
    estratomin: Optional[int] = 0
    estratomax: Optional[int] = 0
    precuso: Optional[List[str]] = []
    latitud: Optional[float] = None
    longitud: Optional[float] = None
    segmentacion: Optional[str] = None
    spatialRelation: Optional[str] = 'intersects' # intersects | contains 
    tabla: Optional[str] = "bogota_data_lotes_fastsearch"

@app.post("/lotes_polygon")  
async def lotes_polygon(input_data: PredioCaracteristicasPolygonInput):
    results = {}
    try:
        if not input_data.polygon:
            raise HTTPException(status_code=400, detail="poligono es requerido")
            
        inputvar = {
            'polygon':input_data.polygon,
            'areamin':input_data.areamin,
            'areamax':input_data.areamax,
            'desde_antiguedad':input_data.desde_antiguedad,
            'hasta_antiguedad':input_data.hasta_antiguedad,
            'pisosmin':input_data.pisosmin,
            'pisosmax':input_data.pisosmax,
            'estratomin':input_data.estratomin,
            'estratomax':input_data.estratomax,
            'precuso':input_data.precuso,
            'latitud':input_data.latitud,
            'longitud':input_data.longitud,
            'segmentacion':input_data.segmentacion,
            'spatialRelation': input_data.spatialRelation,
            'tabla': input_data.tabla,
            }
        results = get_lotes_polygon(inputvar=inputvar)
        return JSONResponse(content=results, status_code=200)
    except: 
        return JSONResponse(content=results, status_code=400)

# ——————————————————————————————————————————————————————————————————————————— #
# API get lotes construcciones
# ——————————————————————————————————————————————————————————————————————————— #
class ConstruccionesInput(BaseModel):
    barmanpre: str

@app.post("/lotes_construcciones")  
async def lotes_construcciones(input_data: ConstruccionesInput):
    results = {}
    try:
        if not input_data.barmanpre:
            raise HTTPException(status_code=400, detail="barmanpre es requerido")
            
        inputvar = {
            'barmanpre':input_data.barmanpre,
            }
        results = get_lotes_construcciones(inputvar=inputvar)
        
        return JSONResponse(content=results, status_code=200)
    except: 
        return JSONResponse(content=results, status_code=400)
    
    
# ——————————————————————————————————————————————————————————————————————————— #
# API get propietarios
# ——————————————————————————————————————————————————————————————————————————— #
class PropietariosInput(BaseModel):
    barmanpre: str
    chip: Optional[str] = None
    matriculainmobiliaria: Optional[str] = None
    get_tabla: bool = True

@app.post("/propietarios")  
async def propietarios(input_data: PropietariosInput):
    results = {}
    try:
        if not input_data.barmanpre:
            raise HTTPException(status_code=400, detail="barmanpre es requerido")
            
        inputvar = {
            'barmanpre':input_data.barmanpre,
            'chip':input_data.chip,
            'matriculainmobiliaria':input_data.matriculainmobiliaria,
            'get_tabla': input_data.get_tabla
            }
        results = get_propietarios(inputvar=inputvar)
        
        return JSONResponse(content=results, status_code=200)
    except: 
        return JSONResponse(content=results, status_code=400)


# ——————————————————————————————————————————————————————————————————————————— #
# API get propiedades by ID
# ——————————————————————————————————————————————————————————————————————————— #
class PropertyByIDInput(BaseModel):
    identificacion: str

@app.post("/propertiesById")  
async def propertiesById(input_data: PropertyByIDInput):
    results = {}
    try:
        if not input_data.identificacion:
            raise HTTPException(status_code=400, detail="ID es requerido")
            
        inputvar = {
            'identificacion':input_data.identificacion,
            }
        results = get_properties_by_id(inputvar=inputvar)
        
        return JSONResponse(content=results, status_code=200)
    except: 
        return JSONResponse(content=results, status_code=400)

# ——————————————————————————————————————————————————————————————————————————— #
# API get propitarios | leads por radio o poligono
# ——————————————————————————————————————————————————————————————————————————— #
class PropietariosRadioInput(BaseModel):
    barmanpre: str
    areamin: Optional[float] = 0.0
    areamax: Optional[float] = 0.0
    antiguedadmin: Optional[int] = 0
    antiguedadmax: Optional[int] = 0
    estratomin: Optional[int] = 0
    estratomax: Optional[int] = 0
    precuso: Optional[List[str]] = []
    polygon: Optional[str] = None

@app.post("/getLeads")  
async def getLeads(input_data: PropietariosRadioInput):
    results = {}
    try:
        if not input_data.barmanpre:
            raise HTTPException(status_code=400, detail="barmanpre es requerido")
            
        inputvar = {
            'barmanpre': input_data.barmanpre,
            'areamin': input_data.areamin,
            'areamax': input_data.areamax,
            'antiguedadmin': input_data.antiguedadmin,
            'antiguedadmax': input_data.antiguedadmax,
            'estratomin': input_data.estratomin,
            'estratomax': input_data.estratomax,
            'precuso': input_data.precuso,
            'polygon': input_data.polygon,
            }
        results = get_propietarios_radio(inputvar=inputvar)
        
        return JSONResponse(content=results, status_code=200)
    except: 
        return JSONResponse(content=results, status_code=400)
    
# ——————————————————————————————————————————————————————————————————————————— #
# API get vehiculos by ID
# ——————————————————————————————————————————————————————————————————————————— #
class VehiculosByIDInput(BaseModel):
    identificacion: str

@app.post("/vehiculosById")  
async def vehiculosById(input_data: VehiculosByIDInput):
    results = {}
    try:
        if not input_data.identificacion:
            raise HTTPException(status_code=400, detail="ID es requerido")
            
        inputvar = {
            'identificacion':input_data.identificacion,
            }
        results = get_vehiculos_by_id(inputvar=inputvar)
        
        return JSONResponse(content=results, status_code=200)
    except: 
        return JSONResponse(content=results, status_code=400)
    
# ——————————————————————————————————————————————————————————————————————————— #
# API get market analysis
# ——————————————————————————————————————————————————————————————————————————— #
class MarketAnalysisInput(BaseModel):
    barmanpre: str
    direccion: Optional[str] = None
    polygon: Optional[str] = None
    areamin: Optional[float] = 0.0
    areamax: Optional[float] = 0.0
    precuso: Optional[List[str]] = []

@app.post("/market_analysis")  
async def market_analysis(input_data: MarketAnalysisInput):
    results = {}
    try:
        if not input_data.barmanpre:
            raise HTTPException(status_code=400, detail="barmanpre es requerido")
            
        inputvar = {
            'barmanpre':input_data.barmanpre,
            'direccion':input_data.direccion,
            'precuso':input_data.precuso,
            'polygon': input_data.polygon,
            'areamin':input_data.areamin,
            'areamax':input_data.areamax,
            }
        results = get_market_analysis(inputvar=inputvar)
        
        return JSONResponse(content=results, status_code=200)
    except: 
        return JSONResponse(content=results, status_code=400)
    
@app.post("/market_statistics")  
async def market_statistics(input_data: MarketAnalysisInput):
    results = {}
    try:
        if not input_data.barmanpre:
            raise HTTPException(status_code=400, detail="barmanpre es requerido")
            
        inputvar = {
            'barmanpre':input_data.barmanpre,
            }
        results = get_market_scacodigo(inputvar=inputvar)
        
        return JSONResponse(content=results, status_code=200)
    except: 
        return JSONResponse(content=results, status_code=400)
    
# ——————————————————————————————————————————————————————————————————————————— #
# API get property info
# ——————————————————————————————————————————————————————————————————————————— #
class PropertyInfoInput(BaseModel):
    code: Optional[str] = None
    tipoinmueble: Optional[str] = None
    tiponegocio: Optional[str] = None
    polygon: Optional[str] = None
    areamin: Optional[float] = 0.0
    areamax: Optional[float] = 0.0
    antiguedadmin: Optional[int] = 0
    antiguedadmax: Optional[int] = 0
    estratomin: Optional[int] = 0
    estratomax: Optional[int] = 0
    habitacionesmin: Optional[int] = 0
    habitacionesmax: Optional[int] = 0
    banosmin: Optional[int] = 0
    banosmax: Optional[int] = 0
    garajesmin: Optional[int] = 0
    garajesmax: Optional[int] = 0
       
@app.post("/propertyInfo")  
async def propertyInfo(input_data: PropertyInfoInput):
    results = {}
    try:
        inputvar = {
            'code': input_data.code,
            'tipoinmueble': input_data.tipoinmueble,
            'tiponegocio': input_data.tiponegocio,
            'polygon': input_data.polygon,
            'areamin': input_data.areamin,
            'areamax': input_data.areamax,
            'antiguedadmin': input_data.antiguedadmin,
            'antiguedadmax': input_data.antiguedadmax,
            'estratomin': input_data.estratomin,
            'estratomax': input_data.estratomax,
            'habitacionesmin': input_data.habitacionesmin,
            'habitacionesmax': input_data.habitacionesmax,
            'banosmin': input_data.banosmin,
            'banosmax': input_data.banosmax,
            'garajesmin': input_data.garajesmin,
            'garajesmax': input_data.garajesmax,
        }
        results = get_property_info(inputvar=inputvar)
        
        return JSONResponse(content=results, status_code=200)
    except: 
        return JSONResponse(content=results, status_code=400)
    
    
    
# ——————————————————————————————————————————————————————————————————————————— #
# API get galeria proyectos nuevos
# ——————————————————————————————————————————————————————————————————————————— #
class ProyectosNuevosInput(BaseModel):
    barmanpre: str
    areamin: Optional[float] = 0.0
    areamax: Optional[float] = 0.0
    antiguedadmin: Optional[int] = 0
    antiguedadmax: Optional[int] = 0
    estratomin: Optional[int] = 0
    estratomax: Optional[int] = 0
    precuso: Optional[List[str]] = []
    polygon: Optional[str] = None


@app.post("/market_proyectos")  
async def market_proyectos(input_data: ProyectosNuevosInput):
    results = {}
    try:
        if not input_data.barmanpre:
            raise HTTPException(status_code=400, detail="barmanpre es requerido")
            
        inputvar = {
            'barmanpre':input_data.barmanpre,
            'polygon': input_data.polygon,
            'areamin': input_data.areamin,
            'areamax': input_data.areamax,
            'antiguedadmin': input_data.antiguedadmin,
            'antiguedadmax': input_data.antiguedadmax,
            'estratomin': input_data.estratomin,
            'estratomax': input_data.estratomax,
            'precuso': input_data.precuso,

            }
        results = get_galeria(inputvar=inputvar)
        return JSONResponse(content=results, status_code=200)
    except: 
        return JSONResponse(content=results, status_code=400)
    
# ——————————————————————————————————————————————————————————————————————————— #
# API get brand
# ——————————————————————————————————————————————————————————————————————————— #
class BrandInput(BaseModel):
    codigos: Optional[Union[List[str], str]] = None
    dpto_ccdgo: Optional[Union[List[str], str]] = None
    mpio_ccdgo: Optional[Union[List[str], str]] = None
    barrio: Optional[Union[List[str], str]] = None
    nombre: Optional[Union[List[str], str]] = None
    
@app.post("/brand_data")  
async def brand_data(input_data: BrandInput):
    results = {}
    try:
        if not input_data.codigos:
            raise HTTPException(status_code=400, detail="Códigos es requerido")
            
        inputvar = {
            'codigos':input_data.codigos,
            'dpto_ccdgo':input_data.dpto_ccdgo,
            'mpio_ccdgo':input_data.mpio_ccdgo,
            'barrio':input_data.barrio,
            'nombre':input_data.nombre,
            }
        
        results = get_brand_data(inputvar=inputvar)
        return JSONResponse(content=results, status_code=200)
    except: 
        return JSONResponse(content=results, status_code=400)
    
# ——————————————————————————————————————————————————————————————————————————— #
# API get POT bogota
# ——————————————————————————————————————————————————————————————————————————— #
class POTBogotaInput(BaseModel):
    barmanpre: str

@app.post("/pot_bogota")  
async def pot_bogota(input_data: POTBogotaInput):
    results = {}
    try:
        if not input_data.barmanpre:
            raise HTTPException(status_code=400, detail="barmanpre es requerido")
            
        inputvar = {
            'barmanpre':input_data.barmanpre,
            }
        results = get_pot(inputvar=inputvar)
        
        return JSONResponse(content=results, status_code=200)
    except: 
        return JSONResponse(content=results, status_code=400)
    
# ——————————————————————————————————————————————————————————————————————————— #
# API get CTL
# ——————————————————————————————————————————————————————————————————————————— #
class CTLInput(BaseModel):
    barmanpre: str
    chip: Optional[str] = None
    matriculainmobiliaria: Optional[str] = None
    
@app.post("/ctl")  
async def ctl(input_data: CTLInput):
    results = {}
    try:
        if not input_data.barmanpre:
            raise HTTPException(status_code=400, detail="barmanpre es requerido")
            
        inputvar = {
            'barmanpre':input_data.barmanpre,
            'chip':input_data.chip,
            'matriculainmobiliaria':input_data.matriculainmobiliaria
            }
        results = get_ctl(inputvar=inputvar)
        
        return JSONResponse(content=results, status_code=200)
    except: 
        return JSONResponse(content=results, status_code=400)
    
# ——————————————————————————————————————————————————————————————————————————— #
# API get Licencias Construccion
# ——————————————————————————————————————————————————————————————————————————— #
class LicenciasConstruccionInput(BaseModel):
    barmanpre: str

@app.post("/licencias_construccion")  
async def licencias_construccion(input_data: LicenciasConstruccionInput):
    results = {}
    try:
        if not input_data.barmanpre:
            raise HTTPException(status_code=400, detail="barmanpre es requerido")
            
        inputvar = {
            'barmanpre':input_data.barmanpre,
            }
        results = get_licencias(inputvar=inputvar)
        
        return JSONResponse(content=results, status_code=200)
    except: 
        return JSONResponse(content=results, status_code=400)
    
# ——————————————————————————————————————————————————————————————————————————— #
# API get Licencias Construccion
# ——————————————————————————————————————————————————————————————————————————— #
class DaneInput(BaseModel):
    barmanpre: str

@app.post("/dane")  
async def dane(input_data: DaneInput):
    results = {}
    try:
        if not input_data.barmanpre:
            raise HTTPException(status_code=400, detail="barmanpre es requerido")
            
        inputvar = {
            'barmanpre':input_data.barmanpre,
            }
        results = get_dane(inputvar=inputvar)
        
        return JSONResponse(content=results, status_code=200)
    except: 
        return JSONResponse(content=results, status_code=400)
    

# ——————————————————————————————————————————————————————————————————————————— #
# API get latitud y longitud
# ——————————————————————————————————————————————————————————————————————————— #
class LatLngInput(BaseModel):
    direccion:  Optional[str] = None
    ciudad: Optional[str] = None
    chip: Optional[str] = None
    matricula: Optional[str] = None
@app.post("/getlatlng")  
async def getlatlng(input_data: LatLngInput):
    results = {}
    try:
        inputvar = {
            'direccion':input_data.direccion,
            'ciudad':input_data.ciudad,
            'chip': input_data.chip,
            'matricula': input_data.matricula
            }
        results = get_latlng(inputvar=inputvar)
        
        return JSONResponse(content=results, status_code=200)
    except: 
        return JSONResponse(content=results, status_code=400)
    
# ——————————————————————————————————————————————————————————————————————————— #
# API get sinupot pdf
# ——————————————————————————————————————————————————————————————————————————— #
class SinupotPDFInput(BaseModel):
    chip: str
    arealote: Optional[float] = 0.0
    
@app.post("/sinupot_pdf")  
async def sinupot_pdf(input_data: SinupotPDFInput):
    results = {}
    try:
        if not input_data.chip:
            raise HTTPException(status_code=400, detail="Chip es requerido")

        inputvar = {
            'chip':input_data.chip,
            'arealote':input_data.arealote,
            }
        results = get_sinupot_pdf(inputvar=inputvar)
        
        return JSONResponse(content=results, status_code=200)
    except: 
        return JSONResponse(content=results, status_code=400)
    
    
# ——————————————————————————————————————————————————————————————————————————— #
# API lectura de CTL
# ——————————————————————————————————————————————————————————————————————————— #
class ReadCTLInput(BaseModel):
    url: str

@app.post("/lectura_ctl")  
async def lectura_ctl(input_data: ReadCTLInput):
    results = {}
    try:
        if not input_data.url:
            raise HTTPException(status_code=400, detail="url es requerido")
            
        inputvar = {
            'url':input_data.url,
            }
        results = read_ctl(inputvar=inputvar)
        return JSONResponse(content=results, status_code=200)
    except: 
        return JSONResponse(content=results, status_code=400)
    
# ——————————————————————————————————————————————————————————————————————————— #
# API html 2 pdf
# ——————————————————————————————————————————————————————————————————————————— #
class Html2PDFInput(BaseModel):
    html_content: str

@app.post("/html2pdf")  
async def html2pdf(input_data: Html2PDFInput):
    results = {}
    try:
        if not input_data.html_content:
            raise HTTPException(status_code=400, detail="html content es requerido")
            
        inputvar = {
            'html_content':input_data.html_content,
            }
        results = _html2PDF(inputvar=inputvar)
        return JSONResponse(content=results, status_code=200)
    except: 
        return JSONResponse(content=results, status_code=400)
    
@app.post("/html2pdf_async")
async def html2pdf_async(input_data: Html2PDFInput, background_tasks: BackgroundTasks):
    if not input_data.html_content:
        raise HTTPException(status_code=400, detail="html content es requerido")

    file_id = str(uuid.uuid4())
    folder = "pdf_reports"
    filename = f"{folder}/{file_id}.pdf"
    REGION = os.getenv("REGION_iconos")
    BUCKET_NAME = os.getenv("BUCKET_NAME_iconos")
    url = f'https://{BUCKET_NAME}.{REGION}.digitaloceanspaces.com/{filename}'

    background_tasks.add_task(generate_and_upload_async, input_data.html_content, filename)

    return {
        "meta": {
            "timestamp": datetime.now().isoformat(),
            "requestId": file_id
        },
        "url": url
    }

@app.get("/html2pdf_status/{file_id}")
async def html2pdf_status(file_id: str):
    REGION = os.getenv("REGION_iconos")
    BUCKET_NAME = os.getenv("BUCKET_NAME_iconos")
    filename = f"pdf_reports/{file_id}.pdf"

    session = boto3.session.Session()
    client = session.client(
        's3',
        region_name=REGION,
        endpoint_url=f'https://{REGION}.digitaloceanspaces.com',
        aws_access_key_id=os.getenv("ACCESS_KEY_iconos"),
        aws_secret_access_key=os.getenv("SECRET_KEY_iconos")
    )

    try:
        client.head_object(Bucket=BUCKET_NAME, Key=filename)
        return {"exists": True}
    except client.exceptions.ClientError:
        return {"exists": False}
    
    
    
# ——————————————————————————————————————————————————————————————————————————— #
# API html 2 pdf - Versión Híbrida Mejorada
# ——————————————————————————————————————————————————————————————————————————— #
class Html2PDFInput(BaseModel):
    html_content: str
    async_mode: Optional[bool] = False

@app.post("/html2pdf_hybrid")  
async def html2pdf_hybrid(input_data: Html2PDFInput, background_tasks: BackgroundTasks):
    """
    Endpoint híbrido que decide automáticamente entre modo síncrono o asíncrono.
    - Modo síncrono: Para documentos pequeños (<50KB), respuesta inmediata con URL
    - Modo asíncrono: Para documentos grandes o cuando async_mode=True
    """
    try:
        if not input_data.html_content:
            raise HTTPException(status_code=400, detail="html_content es requerido")
        
        inputvar = {
            'html_content': input_data.html_content,
            'async_mode': input_data.async_mode
        }
        
        # Verificar si se usará modo asíncrono
        content_size = len(input_data.html_content)
        use_async = input_data.async_mode or content_size > 50000
        
        if use_async:
            # Llamar a main_async directamente
            result = main_async(inputvar)
            
            # Extraer el file_id del resultado
            file_id = result['meta']['requestId']
            
            # IMPORTANTE: Agregar la tarea en background
            background_tasks.add_task(
                generate_and_upload_background, 
                input_data.html_content, 
                file_id
            )
            
            return JSONResponse(content=result, status_code=202)  # 202 Accepted para async
        else:
            # Modo síncrono
            results = main_sync(inputvar)
            return JSONResponse(content=results, status_code=200)
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        print(f"Error en html2pdf_hybrid: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@app.post("/html2pdf_hybrid_sync")  
async def html2pdf_hybrid_sync(input_data: Html2PDFInput):
    """
    Endpoint síncrono forzado. Siempre espera a que el PDF se complete.
    Útil cuando necesitas garantía inmediata del resultado.
    """
    try:
        if not input_data.html_content:
            raise HTTPException(status_code=400, detail="html_content es requerido")
        
        inputvar = {'html_content': input_data.html_content}
        results = main_sync(inputvar=inputvar)
        return JSONResponse(content=results, status_code=200)
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        print(f"Error en html2pdf_sync: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@app.post("/html2pdf_hybrid_async")
async def html2pdf_hybrid_async(input_data: Html2PDFInput, background_tasks: BackgroundTasks):
    """
    Endpoint asíncrono forzado. Retorna inmediatamente con un requestId.
    Usar /html2pdf_status/{requestId} para verificar el estado.
    """
    try:
        if not input_data.html_content:
            raise HTTPException(status_code=400, detail="html_content es requerido")
        
        # Obtener la respuesta con el file_id
        inputvar = {'html_content': input_data.html_content}
        result = main_async(inputvar=inputvar)
        
        # Agregar tarea en background
        file_id = result['meta']['requestId']
        background_tasks.add_task(
            generate_and_upload_background, 
            input_data.html_content, 
            file_id
        )
        
        return JSONResponse(content=result, status_code=202)  # 202 Accepted
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        print(f"Error en html2pdf_async: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@app.get("/html2pdf_hybrid_status/{request_id}")
async def html2pdf_hybrid_status(request_id: str):
    """
    Verifica el estado de una generación de PDF asíncrona.
    
    Posibles estados:
    - pending: La solicitud fue recibida pero aún no ha comenzado
    - processing: El PDF se está generando
    - completed: El PDF fue generado exitosamente
    - failed: Hubo un error durante la generación
    - not_found: El requestId no existe
    """
    try:
        result = check_file_exists(request_id)
        
        if result["status"] == "not_found":
            raise HTTPException(status_code=404, detail="Request ID not found")
        
        return JSONResponse(content=result, status_code=200)
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error en html2pdf_status: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

# Endpoint adicional para limpiar estados antiguos (opcional)
@app.delete("/html2pdf_hybrid_cleanup")
async def html2pdf_hybrid_cleanup(hours: int = 24):
    """
    Limpia los estados de PDFs antiguos de la memoria.
    En producción, esto debería ser un job programado.
    """
    from data._html2PDF_hibrido import pdf_status_store
    
    cutoff_time = datetime.now() - timedelta(hours=hours)
    cleaned = 0
    
    for file_id in list(pdf_status_store.keys()):
        status_info = pdf_status_store[file_id]
        started_at = status_info.get("started_at")
        
        if started_at:
            try:
                start_time = datetime.fromisoformat(started_at)
                if start_time < cutoff_time:
                    del pdf_status_store[file_id]
                    cleaned += 1
            except:
                pass
    
    return {
        "message": f"Cleaned {cleaned} old PDF status records",
        "cutoff_time": cutoff_time.isoformat()
    }


# ——————————————————————————————————————————————————————————————————————————— #
# API guardar la consulta [tracking]
# ——————————————————————————————————————————————————————————————————————————— #
class TrackingInput(BaseModel):
    action: str
    token: Optional[str] = None
    barmanpre: Optional[str] = None
    seccion: Optional[str] = None
    inputvar_data: Optional[Dict[str, Any]] = None
    id_consulta: Optional[int] = None
    id_consulta_defaul: Optional[int] = None
    save_value: Optional[int] = None
    
@app.post("/savesearch")  
async def savesearch(input_data: TrackingInput):
    results = {}
    try:
        inputvar = {
            'action': input_data.action,
            'token': input_data.token,
            'barmanpre':input_data.barmanpre,
            'seccion': input_data.seccion,
            'inputvar_data': input_data.inputvar_data,
            'id_consulta':input_data.id_consulta,
            'id_consulta_defaul': input_data.id_consulta_defaul,
            'save_value': input_data.save_value,
            }
        results = save_search(inputvar=inputvar)
        return JSONResponse(content=results, status_code=200)
    except: 
        return JSONResponse(content=results, status_code=400)
    
# ——————————————————————————————————————————————————————————————————————————— #
# API permisos
# ——————————————————————————————————————————————————————————————————————————— #
class PermisosInput(BaseModel):
    token: Optional[str] = None
    usuarios_acceso: Optional[List[str]] = []
    empresas_acceso: Optional[List[str]] = []
    usuarios_delete: Optional[List[str]] = []
    empresas_delete: Optional[List[str]] = []
    
@app.post("/propietarios_permisos_download")  
async def propietarios_permisos_download(input_data: PermisosInput):
    results = {}
    try:
        if not input_data.usuarios_acceso:
            raise HTTPException(status_code=400, detail="Lista emails es requerido")

        results = otorgar_permisos_download(usuarios_acceso=input_data.usuarios_acceso)
        return JSONResponse(content=results, status_code=200)
    except: 
        return JSONResponse(content=results, status_code=400)
    
@app.post("/propietarios_permisos_delete")  
async def propietarios_permisos_delete(input_data: PermisosInput):
    results = {}
    try:
        if not input_data.usuarios_delete:
            raise HTTPException(status_code=400, detail="Lista emails es requerido")

        results = eliminar_permisos_download(usuarios_delete=input_data.usuarios_delete)
        return JSONResponse(content=results, status_code=200)
    except: 
        return JSONResponse(content=results, status_code=400)
    
@app.post("/propietarios_get_user")  
async def propietarios_get_user(input_data: PermisosInput):
    results = {}
    try:
        if not input_data.token:
            raise HTTPException(status_code=400, detail="Token es requerido")

        results = get_usuario_download(token=input_data.token)
        return JSONResponse(content=results, status_code=200)
    except: 
        return JSONResponse(content=results, status_code=400)
    
@app.post("/galeria_permisos")  
async def galeria_permisos(input_data: PermisosInput):
    results = {}
    try:
        if not input_data.empresas_acceso and not input_data.usuarios_acceso:
            raise HTTPException(status_code=400, detail="Empresa o usuario es requerido")
            
        results = otorgar_permisos_galeria(empresas_acceso=input_data.empresas_acceso, usuarios_acceso=input_data.usuarios_acceso)
        return JSONResponse(content=results, status_code=200)
    except: 
        return JSONResponse(content=results, status_code=400)
    
@app.post("/galeria_get_user")  
async def galeria_get_user(input_data: PermisosInput):
    results = {}
    try:
        if not input_data.token:
            raise HTTPException(status_code=400, detail="Token es requerido")

        results = get_usuario_galeria(token=input_data.token)
        return JSONResponse(content=results, status_code=200)
    except: 
        return JSONResponse(content=results, status_code=400)
    
@app.post("/galeria_permisos_delete")  
async def galeria_permisos_delete(input_data: PermisosInput):
    results = {}
    try:
        if not input_data.empresas_delete and not input_data.usuarios_delete:
            raise HTTPException(status_code=400, detail="Empresa o usuario es requerido")
            
        results = eliminar_permisos_galeria(empresas_delete=input_data.empresas_delete, usuarios_delete= input_data.usuarios_delete)
        return JSONResponse(content=results, status_code=200)
    except: 
        return JSONResponse(content=results, status_code=400)
    
# ——————————————————————————————————————————————————————————————————————————— #
# API cabida AI
# ——————————————————————————————————————————————————————————————————————————— #
class CabidaAiInput(BaseModel):
    pot_json_lote: Optional[Dict[str, Any]] = None
    input_lote: Optional[Dict[str, Any]] = None

@app.post("/cabidaAI")  
async def cabidaAI(input_data: CabidaAiInput):
    results = {}
    try:
        inputvar = {
            'pot_json_lote': input_data.pot_json_lote,
            'input_lote': input_data.input_lote,
            }
        results = get_cabida_AI(inputvar=inputvar)
        return JSONResponse(content=results, status_code=200)
    except: 
        return JSONResponse(content=results, status_code=400)
    
    
    
# ——————————————————————————————————————————————————————————————————————————— #
# Test placas Fontanar
# ——————————————————————————————————————————————————————————————————————————— #
class TestFontanarInput(BaseModel):
    segmentacion: Optional[str] = None
    dia_semana: Optional[str] = None
    franja_horaria: Optional[str] = None
    edad_min: Optional[int] = 0
    edad_max: Optional[int] = 0
    vehiculo_min: Optional[int] = 0
    vehiculo_max: Optional[int] = 0
    tiene_propiedades: Optional[bool] = False
    prop_min: Optional[int] = 0
    prop_max: Optional[int] = 0
    estratos_selected: Optional[List[str]] = []
    localidades_selected: Optional[List[str]] = []
    barrios: Optional[List[str]] = []
    
@app.post("/testFontanar")  
async def testFontanar(input_data: TestFontanarInput):
    results = {}
    try:

        inputvar = {
            'segmentacion': input_data.segmentacion,
            'dia_semana': input_data.dia_semana,
            'franja_horaria': input_data.franja_horaria,
            'edad_min': input_data.edad_min,
            'edad_max': input_data.edad_max,
            'vehiculo_min': input_data.vehiculo_min,
            'vehiculo_max': input_data.vehiculo_max,
            'tiene_propiedades': input_data.tiene_propiedades,
            'prop_min': input_data.prop_min,
            'prop_max': input_data.prop_max,
            'estratos_selected': input_data.estratos_selected,
            'localidades_selected': input_data.localidades_selected,
            'barrios': input_data.barrios,
         }
        
        results = test_fontanar(inputvar=inputvar)

        return JSONResponse(content=results, status_code=200)
    except: 
        return JSONResponse(content=results, status_code=400)
    
    