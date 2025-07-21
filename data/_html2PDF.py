import os
import base64
import tempfile
import uuid
import time
import boto3
from datetime import datetime, timedelta
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys

load_dotenv()


def main(inputvar={}):
    html = inputvar.get('html_content', '')
    gen = PdfReportGenerator()
    url = gen.generate_report(html)
    return {
        'meta': {
            'timestamp': datetime.now().isoformat(),
            'requestId': str(uuid.uuid4())
        },
        'url': url
    }

class PdfReportGenerator:
    """
    Clase para convertir HTML a PDF usando Selenium + Chrome en DO Spaces.
    """
    def __init__(self, headless: bool = True, folder: str = "pdf_reports"):
        self.folder = folder
        # Configurar opciones de Chrome
        self.chrome_options = Options()
        if headless:
            self.chrome_options.add_argument('--headless=new')
        self.chrome_options.add_argument('--no-sandbox')
        self.chrome_options.add_argument('--disable-dev-shm-usage')
        self.chrome_options.add_argument('--disable-gpu')
        self.chrome_options.add_argument('--disable-web-security')
        self.chrome_options.add_argument('--allow-running-insecure-content')
        self.chrome_options.add_argument('--window-size=1280,1600')
        # Ubicación del binario de Chrome en contenedor
        self.chrome_options.binary_location = os.getenv(
            "CHROME_BIN",
            "/usr/bin/google-chrome-stable"
        )
        # Servicio de ChromeDriver gestionado automáticamente
        self.service = Service(ChromeDriverManager().install())

    def html_to_pdf_bytes(self, html_content: str) -> bytes:
        """
        Genera PDF en bytes a partir de HTML.
        """
        driver = webdriver.Chrome(service=self.service, options=self.chrome_options)
        driver.set_window_size(1280, 1600)
        try:
            # Crear archivo temporal HTML
            with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False, encoding='utf-8') as f:
                f.write(html_content)
                path = f.name

            file_url = f"file:///{path.replace(os.sep, '/')}"
            driver.get(file_url)
            # Esperar carga completa
            WebDriverWait(driver, 15).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            time.sleep(5)
            # Opciones de impresión
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
        finally:
            driver.quit()
            try:
                os.remove(path)
            except:
                pass

    def upload_to_spaces(self, pdf_bytes: bytes) -> str:
        """
        Sube PDF a DigitalOcean Spaces y devuelve URL pública.
        """
        ACCESS_KEY = os.getenv("ACCESS_KEY_iconos")
        SECRET_KEY = os.getenv("SECRET_KEY_iconos")
        REGION = os.getenv("REGION_iconos")
        BUCKET_NAME = os.getenv("BUCKET_NAME_iconos")

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

            filename = f"{self.folder}/{uuid.uuid4()}.pdf"
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
        finally:
            if temp_pdf and os.path.exists(temp_pdf):
                os.remove(temp_pdf)

    def generate_report(self, html_content: str) -> str:
        """
        Flujo completo: HTML → PDF → Spaces URL
        """
        pdf = self.html_to_pdf_bytes(html_content)
        return self.upload_to_spaces(pdf)