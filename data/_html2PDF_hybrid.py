import os
import base64
import tempfile
import uuid
import time
import boto3
import asyncio
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from enum import Enum
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait

load_dotenv()

class PDFStatus(Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"

# Almacenamiento temporal en memoria para estados
# En producción, usar Redis o base de datos
pdf_status_store: Dict[str, Dict[str, Any]] = {}

class PdfReportGenerator:
    """
    Clase unificada para convertir HTML a PDF usando Selenium + Chrome.
    """
    def __init__(self, headless: bool = True, folder: str = "pdf_reports"):
        self.folder = folder
        self.chrome_options = Options()
        if headless:
            self.chrome_options.add_argument('--headless=new')
        self.chrome_options.add_argument('--no-sandbox')
        self.chrome_options.add_argument('--disable-dev-shm-usage')
        self.chrome_options.add_argument('--disable-gpu')
        self.chrome_options.add_argument('--disable-web-security')
        self.chrome_options.add_argument('--allow-running-insecure-content')
        self.chrome_options.add_argument('--window-size=1280,1600')
        self.chrome_options.binary_location = os.getenv(
            "CHROME_BIN",
            "/usr/bin/google-chrome-stable"
        )
        self.service = Service(ChromeDriverManager().install())

    def html_to_pdf_bytes(self, html_content: str) -> bytes:
        """Genera PDF en bytes a partir de HTML."""
        driver = None
        temp_file = None
        try:
            driver = webdriver.Chrome(service=self.service, options=self.chrome_options)
            driver.set_window_size(1280, 1600)
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False, encoding='utf-8') as f:
                f.write(html_content)
                temp_file = f.name

            file_url = f"file:///{temp_file.replace(os.sep, '/')}"
            driver.get(file_url)
            
            WebDriverWait(driver, 15).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            time.sleep(5)

            print_options = {
                'landscape': False,
                'displayHeaderFooter': False,
                'printBackground': True,
                'preferCSSPageSize': False,
                'paperWidth': 8.5,
                'paperHeight': 11,
                'marginTop': 0.5,
                'marginBottom': 0.5,
                'marginLeft': 0.5,
                'marginRight': 0.5,
                'scale': 0.95
            }
            
            result = driver.execute_cdp_cmd('Page.printToPDF', print_options)
            return base64.b64decode(result['data'])
            
        except Exception as e:
            print(f"Error generando PDF: {str(e)}")
            raise
        finally:
            if driver:
                driver.quit()
            if temp_file and os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except:
                    pass

    def upload_to_spaces(self, pdf_bytes: bytes, filename: str = None) -> str:
        """Sube PDF a DigitalOcean Spaces y devuelve URL pública."""
        ACCESS_KEY = os.getenv("ACCESS_KEY_iconos")
        SECRET_KEY = os.getenv("SECRET_KEY_iconos")
        REGION = os.getenv("REGION_iconos")
        BUCKET_NAME = os.getenv("BUCKET_NAME_iconos")

        if not filename:
            filename = f"{self.folder}/{uuid.uuid4()}.pdf"

        temp_pdf = None
        try:
            with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as f:
                temp_pdf = f.name
                f.write(pdf_bytes)

            session = boto3.session.Session()
            client = session.client(
                's3',
                region_name=REGION,
                endpoint_url=f'https://{REGION}.digitaloceanspaces.com',
                aws_access_key_id=ACCESS_KEY,
                aws_secret_access_key=SECRET_KEY
            )

            expiration = datetime.utcnow() + timedelta(hours=24)

            with open(temp_pdf, 'rb') as data:
                client.upload_fileobj(
                    data,
                    BUCKET_NAME,
                    filename,
                    ExtraArgs={
                        'ContentType': 'application/pdf',
                        'ACL': 'public-read',
                        'Expires': expiration,
                        'CacheControl': 'max-age=86400'
                    }
                )

            return f'https://{BUCKET_NAME}.{REGION}.digitaloceanspaces.com/{filename}'
            
        except Exception as e:
            print(f"Error subiendo a Spaces: {str(e)}")
            raise
        finally:
            if temp_pdf and os.path.exists(temp_pdf):
                try:
                    os.remove(temp_pdf)
                except:
                    pass

    def generate_report_sync(self, html_content: str) -> str:
        """Flujo síncrono completo: HTML → PDF → Spaces URL"""
        pdf_bytes = self.html_to_pdf_bytes(html_content)
        return self.upload_to_spaces(pdf_bytes)

    async def generate_report_async(self, html_content: str, file_id: str) -> None:
        """Flujo asíncrono completo con actualización de estado"""
        filename = f"{self.folder}/{file_id}.pdf"
        
        try:
            # Actualizar estado a procesando
            pdf_status_store[file_id] = {
                "status": PDFStatus.PROCESSING.value,
                "url": None,
                "error": None,
                "started_at": datetime.now().isoformat(),
                "filename": filename
            }
            
            # Generar PDF
            pdf_bytes = self.html_to_pdf_bytes(html_content)
            
            # Subir a Spaces
            url = self.upload_to_spaces(pdf_bytes, filename)
            
            # Actualizar estado a completado
            pdf_status_store[file_id] = {
                "status": PDFStatus.COMPLETED.value,
                "url": url,
                "error": None,
                "started_at": pdf_status_store[file_id]["started_at"],
                "completed_at": datetime.now().isoformat(),
                "filename": filename
            }
            
        except Exception as e:
            # Actualizar estado a fallido
            pdf_status_store[file_id] = {
                "status": PDFStatus.FAILED.value,
                "url": None,
                "error": str(e),
                "started_at": pdf_status_store[file_id].get("started_at"),
                "failed_at": datetime.now().isoformat(),
                "filename": filename
            }
            print(f"Error en generate_report_async: {str(e)}")

def generate_and_upload_background(html_content: str, file_id: str):
    """Función para ejecutar en BackgroundTasks"""
    gen = PdfReportGenerator()
    # Ejecutar el método asíncrono en un nuevo loop de eventos
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(gen.generate_report_async(html_content, file_id))
    finally:
        loop.close()

def check_file_exists(file_id: str) -> Dict[str, Any]:
    """Verifica el estado del archivo"""
    # Primero verificar en el store local
    if file_id in pdf_status_store:
        status_info = pdf_status_store[file_id]
        
        # Si está completado, verificar que realmente existe en Spaces
        if status_info["status"] == PDFStatus.COMPLETED.value:
            try:
                REGION = os.getenv("REGION_iconos")
                BUCKET_NAME = os.getenv("BUCKET_NAME_iconos")
                
                session = boto3.session.Session()
                client = session.client(
                    's3',
                    region_name=REGION,
                    endpoint_url=f'https://{REGION}.digitaloceanspaces.com',
                    aws_access_key_id=os.getenv("ACCESS_KEY_iconos"),
                    aws_secret_access_key=os.getenv("SECRET_KEY_iconos")
                )
                
                client.head_object(
                    Bucket=BUCKET_NAME, 
                    Key=status_info["filename"]
                )
                
                return {
                    "exists": True,
                    "status": status_info["status"],
                    "url": status_info["url"],
                    "error": status_info["error"],
                    "metadata": {
                        "started_at": status_info.get("started_at"),
                        "completed_at": status_info.get("completed_at")
                    }
                }
                
            except client.exceptions.ClientError:
                # El archivo no existe en Spaces aunque el estado dice completado
                pdf_status_store[file_id]["status"] = PDFStatus.FAILED.value
                pdf_status_store[file_id]["error"] = "File not found in storage"
                
        return {
            "exists": False,
            "status": status_info["status"],
            "url": status_info.get("url"),
            "error": status_info.get("error"),
            "metadata": {
                "started_at": status_info.get("started_at"),
                "completed_at": status_info.get("completed_at"),
                "failed_at": status_info.get("failed_at")
            }
        }
    
    return {
        "exists": False,
        "status": "not_found",
        "url": None,
        "error": "Request ID not found",
        "metadata": None
    }

def main_sync(inputvar: Dict[str, Any]) -> Dict[str, Any]:
    """Función principal síncrona"""
    html_content = inputvar.get('html_content', '')
    
    if not html_content:
        raise ValueError("html_content es requerido")
    
    gen = PdfReportGenerator()
    url = gen.generate_report_sync(html_content)
    
    return {
        'meta': {
            'timestamp': datetime.now().isoformat(),
            'requestId': str(uuid.uuid4()),
            'mode': 'sync'
        },
        'url': url,
        'status': 'completed'
    }

def main_async(inputvar: Dict[str, Any]) -> Dict[str, Any]:
    """Función principal asíncrona"""
    html_content = inputvar.get('html_content', '')
    
    if not html_content:
        raise ValueError("html_content es requerido")
    
    file_id = str(uuid.uuid4())
    folder = "pdf_reports"
    filename = f"{folder}/{file_id}.pdf"
    
    REGION = os.getenv("REGION_iconos")
    BUCKET_NAME = os.getenv("BUCKET_NAME_iconos")
    url = f'https://{BUCKET_NAME}.{REGION}.digitaloceanspaces.com/{filename}'
    
    # Inicializar estado
    pdf_status_store[file_id] = {
        "status": PDFStatus.PENDING.value,
        "url": url,
        "error": None,
        "started_at": datetime.now().isoformat(),
        "filename": filename
    }
    
    return {
        'meta': {
            'timestamp': datetime.now().isoformat(),
            'requestId': file_id,
            'mode': 'async'
        },
        'url': url,
        'status': 'pending',
        'message': 'PDF generation started. Use the requestId to check status.'
    }

def main_hybrid(inputvar: Dict[str, Any]) -> Dict[str, Any]:
    """
    Función principal híbrida que decide entre modo síncrono o asíncrono
    basándose en el tamaño del contenido o preferencia del usuario.
    """
    html_content = inputvar.get('html_content', '')
    async_mode = inputvar.get('async_mode', False)
    
    if not html_content:
        raise ValueError("html_content es requerido")
    
    # Decidir modo basado en tamaño o preferencia
    content_size = len(html_content)
    use_async = async_mode or content_size > 50000  # > 50KB
    
    if use_async:
        return main_async(inputvar)
    else:
        return main_sync(inputvar)