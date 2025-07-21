import re
import json
import uuid
import os
from datetime import datetime
from typing import Dict, List, Any
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
from unidecode import unidecode

from functions.clean_json import clean_json

def main(inputvar={}):
    url = inputvar.get('url','')
    extractor = CertificadoLibertadExtractor(headless=True)
    
    try:    
        data = extractor.extract_all(url)
    except Exception as e: 
        print(f"Error en extracción: {str(e)}")
        data = {"error": str(e)}
    
    meta = {
        "timestamp": datetime.now().isoformat(),
        "requestId": str(uuid.uuid4()),
        "filtersApplied": {
            "url": inputvar.get("url",''),
        }
    } 
    
    output = {
        "meta": meta,
        "data": data
    }
    
    output = clean_json(output)
    return output

class CertificadoLibertadExtractor:

    def __init__(self, headless=True):
        self.opciones = Options()
        
        # Configuraciones básicas
        if headless:
            self.opciones.add_argument('--headless=new')  # Usar nueva versión headless
        
        # Configuraciones críticas para servidor/Docker
        self.opciones.add_argument('--no-sandbox')
        self.opciones.add_argument('--disable-dev-shm-usage')
        self.opciones.add_argument('--disable-gpu')
        self.opciones.add_argument('--disable-software-rasterizer')
        self.opciones.add_argument('--disable-background-timer-throttling')
        self.opciones.add_argument('--disable-backgrounding-occluded-windows')
        self.opciones.add_argument('--disable-renderer-backgrounding')
        self.opciones.add_argument('--disable-features=TranslateUI')
        self.opciones.add_argument('--disable-ipc-flooding-protection')
        
        # Configuraciones de ventana y memoria
        self.opciones.add_argument('--window-size=1920,1080')
        self.opciones.add_argument('--start-maximized')
        self.opciones.add_argument('--memory-pressure-off')
        self.opciones.add_argument('--max_old_space_size=4096')
        
        # Configuraciones de red y seguridad
        self.opciones.add_argument('--disable-web-security')
        self.opciones.add_argument('--allow-running-insecure-content')
        self.opciones.add_argument('--disable-extensions')
        self.opciones.add_argument('--disable-plugins')
        self.opciones.add_argument('--disable-images')  # Para acelerar
        self.opciones.add_argument('--disable-javascript')  # Si no necesitas JS completo
        
        # User agent
        self.opciones.add_argument('--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        # Configuraciones para DigitalOcean/Docker
        self.opciones.add_argument('--remote-debugging-port=9222')
        self.opciones.add_argument('--disable-logging')
        self.opciones.add_argument('--disable-gpu-logging')
        self.opciones.add_argument('--silent')
        
        # Prefs para desactivar notificaciones y popups
        prefs = {
            "profile.default_content_setting_values": {
                "notifications": 2,
                "media_stream": 2,
            },
            "profile.managed_default_content_settings": {
                "images": 2
            }
        }
        self.opciones.add_experimental_option("prefs", prefs)
        
        # Logging
        self.opciones.add_experimental_option('excludeSwitches', ['enable-logging'])
        self.opciones.add_experimental_option('useAutomationExtension', False)
        
        # NUEVA CONFIGURACIÓN: Usar webdriver-manager para gestión automática de ChromeDriver
        self.service = Service(ChromeDriverManager().install())
        
        self.data = {
            "informacion_predio": {},
            "direccion": {},
            "cabida_linderos": {},
            "anotaciones": [],
            "metadata": {}
        }
        
    def get_chromedriver_path(self):
        """Buscar ChromeDriver en ubicaciones comunes"""
        possible_paths = [
            '/usr/local/bin/chromedriver',
            '/usr/bin/chromedriver',
            '/app/chromedriver',
            'chromedriver'
        ]
        
        for path in possible_paths:
            if os.path.exists(path) and os.access(path, os.X_OK):
                return path
        
        return None
    
    def extract_text_from_page(self, url: str) -> tuple:
        browser = None
        try:
            # Crear driver con configuración robusta
            if self.service:
                browser = webdriver.Chrome(service=self.service, options=self.opciones)
            else:
                browser = webdriver.Chrome(options=self.opciones)
            
            # Configurar timeouts más largos
            browser.set_page_load_timeout(60)  # 60 segundos para cargar página
            browser.implicitly_wait(10)
            
            print(f"Navegando a: {url}")
            browser.get(url)
            
            # Esperar a que aparezca el certificado con timeout más largo
            print("Esperando certificado...")
            WebDriverWait(browser, 30).until(
                EC.presence_of_element_located((
                    By.XPATH,
                    "//*[contains(translate(., 'CERTIFICADO', 'certificado'), 'certificado') and "
                    "contains(translate(., 'LIBERTAD', 'libertad'), 'libertad') and "
                    "(contains(translate(., 'TRADICION', 'tradicion'), 'tradicion') or "
                    "contains(translate(., 'TRADICIÓN', 'tradicion'), 'tradicion'))]"
                ))
            )
            print("Certificado encontrado")
            
            # Esperar a que aparezca la dirección del inmueble
            try:
                print("Esperando dirección...")
                WebDriverWait(browser, 30).until(
                    EC.presence_of_element_located((
                        By.XPATH, 
                        "//*[contains(translate(normalize-space(text()), 'DIRECCION DEL INMUEBLE', 'direccion del inmueble'), 'direccion del inmueble')]"
                    ))
                )
                print("Dirección encontrada")
            except Exception as e:
                print(f"No se encontró dirección: {e}")
                pass
            
            # Obtener HTML inicial
            htmlpage = browser.page_source
            soup = BeautifulSoup(htmlpage, 'html.parser')
            
            # Hacer scroll para cargar todas las anotaciones
            print("Haciendo scroll para cargar anotaciones...")
            body = browser.find_element(By.TAG_NAME, 'body')
            for i in range(30):
                body.send_keys(Keys.PAGE_DOWN)
                if i % 5 == 0:  # Log cada 5 scrolls
                    print(f"Scroll {i}/30")
                browser.implicitly_wait(0.5)
            
            # Esperar a que aparezcan las anotaciones
            try:
                print("Esperando anotaciones...")
                WebDriverWait(browser, 30).until(
                    EC.presence_of_element_located((
                        By.XPATH, 
                        "//*[contains(translate(normalize-space(text()), 'ANOTACIÓN', 'anotacion'), 'anotacion')]"
                    ))
                )
                print("Anotaciones encontradas")
            except Exception as e:
                print(f"No se encontraron anotaciones: {e}")
                pass
            
            # Obtener HTML completo después del scroll
            htmlpage_complete = browser.page_source
            soup_complete = BeautifulSoup(htmlpage_complete, 'html.parser')
            
            print("Extracción HTML completada")
            return soup, soup_complete
            
        except Exception as e:
            print(f"Error al cargar la página: {str(e)}")
            raise Exception(f"Error al cargar la página: {str(e)}")
        finally:
            if browser:
                try:
                    browser.quit()
                    print("Browser cerrado correctamente")
                except:
                    pass
    
    def extract_informacion_predio(self, soup: BeautifulSoup) -> Dict[str, Any]:
        info = {}
        
        # Buscar todos los spans
        for span in soup.find_all('span', {'dir': 'ltr'}):
            texto = span.get_text().strip()
            
            # Matrícula inmobiliaria
            if 'nro' in unidecode(texto.lower()) and 'matricula' in unidecode(texto.lower()):
                info["matricula_completa"] = texto.split(':')[-1].strip()
                partes = texto.split(':')[-1].strip().split('-')
                if len(partes) >= 2:
                    info["circulo_notarial"] = partes[0].strip()
                    info["matricula_inmobiliaria"] = partes[-1].strip()
            
            # CHIP
            chip_match = re.search(r'CHIP:\s*([A-Z0-9]+)', texto, re.IGNORECASE)
            if chip_match:
                info["chip"] = chip_match.group(1).strip()
            
            # Número predial
            predial_match = re.search(r'NUMERO PREDIAL.*?:\s*([0-9\-]+)', texto, re.IGNORECASE)
            if predial_match:
                info["numero_predial"] = predial_match.group(1).strip()
            
            # Círculo registral y ubicación
            if 'circulo' in unidecode(texto.lower()) and 'registral' in unidecode(texto.lower()):
                circulo_data = self.extract_circulo_info(texto)
                info.update(circulo_data)
        
        return info
    
    def extract_circulo_info(self, texto: str) -> Dict[str, str]:
        patron_circulo = re.compile(r'CIRCULO REGISTRAL: (.*?)(?= DEPTO|$)', re.IGNORECASE)
        patron_depto = re.compile(r'DEPTO: (.*?)(?= MUNICIPIO|$)', re.IGNORECASE)
        patron_municipio = re.compile(r'MUNICIPIO: (.*?)(?= VEREDA|$)', re.IGNORECASE)
        patron_vereda = re.compile(r'VEREDA: (.*)$', re.IGNORECASE)
        
        resultado = {
            "circulo": "",
            "departamento": "",
            "municipio": "",
            "vereda": ""
        }
        
        circulo_match = patron_circulo.search(texto)
        if circulo_match:
            resultado["circulo"] = circulo_match.group(1).strip()
            
        depto_match = patron_depto.search(texto)
        if depto_match:
            resultado["departamento"] = depto_match.group(1).strip()
            
        municipio_match = patron_municipio.search(texto)
        if municipio_match:
            resultado["municipio"] = municipio_match.group(1).strip()
            
        vereda_match = patron_vereda.search(texto)
        if vereda_match:
            resultado["vereda"] = vereda_match.group(1).strip()
            
        return resultado
    
    def extract_direccion(self, soup: BeautifulSoup) -> Dict[str, Any]:
        direccion_data = {
            "direccion_completa": "",
            "tipo_predio": "",
            "todas_direcciones": []
        }
        
        span_direccion = soup.find('span', text=re.compile('DIRECCION DEL INMUEBLE', re.IGNORECASE))
        if span_direccion:
            direcciones = []
            span_siguiente = span_direccion.find_next('span')
            
            while span_siguiente and 'SUPERINTENDENCIA DE NOTARIADO Y REGISTRO' not in span_siguiente.text:
                texto = span_siguiente.text.strip()
                if texto:
                    direcciones.append(texto)
                span_siguiente = span_siguiente.find_next('span')
            
            if direcciones:
                # Buscar tipo de predio
                patron_tipo_predio = re.compile(r'tipo\s*predio\s*:\s*(\w+)', re.IGNORECASE)
                for direccion in direcciones:
                    tipo_match = patron_tipo_predio.search(direccion)
                    if tipo_match:
                        direccion_data["tipo_predio"] = tipo_match.group(1).strip()
                
                # Limpiar direcciones
                direcciones_limpias = []
                for direccion in direcciones:
                    if not patron_tipo_predio.search(direccion):
                        # Eliminar números entre paréntesis
                        direccion = re.sub(r'\(\D*\d+\)|^\d+\)\s*', '', direccion).strip()
                        # Eliminar cualquier contenido entre paréntesis
                        direccion = re.sub(r'\([^)]*\)', '', direccion).strip()
                        if direccion:
                            direcciones_limpias.append(direccion)
                
                if direcciones_limpias:
                    direccion_data["direccion_completa"] = direcciones_limpias[0]
                    direccion_data["todas_direcciones"] = direcciones_limpias
        
        return direccion_data
    
    def extract_cabida_linderos(self, soup: BeautifulSoup) -> Dict[str, Any]:
        cabida_linderos = {
            "area": "",
            "unidad_area": "M2",
            "linderos": {}
        }
        
        texto_completo = soup.get_text()
        
        # Buscar área
        area_patterns = [
            r'AREA.*?:\s*([0-9,\.]+)\s*(M2|METROS|HECTAREAS)?',
            r'CABIDA.*?:\s*([0-9,\.]+)\s*(M2|METROS|HECTAREAS)?',
            r'SUPERFICIE.*?:\s*([0-9,\.]+)\s*(M2|METROS|HECTAREAS)?'
        ]
        
        for pattern in area_patterns:
            area_match = re.search(pattern, texto_completo, re.IGNORECASE)
            if area_match:
                cabida_linderos["area"] = area_match.group(1).replace(",", ".")
                if area_match.group(2):
                    cabida_linderos["unidad_area"] = area_match.group(2)
                break
        
        # Buscar linderos
        linderos_match = re.search(r'LINDEROS[:\s]*(.+?)(?=ANOTACION|PERSONAS QUE INTERVIENEN|$)', 
                                 texto_completo, re.IGNORECASE | re.DOTALL)
        if linderos_match:
            linderos_text = linderos_match.group(1).strip()
            
            # Extraer linderos por punto cardinal
            linderos = {}
            puntos_cardinales = {
                'norte': r'NORTE[:\s]*([^;]+)',
                'sur': r'SUR[:\s]*([^;]+)',
                'oriente': r'(ORIENTE|ESTE)[:\s]*([^;]+)',
                'occidente': r'(OCCIDENTE|OESTE)[:\s]*([^;]+)'
            }
            
            for cardinal, pattern in puntos_cardinales.items():
                match = re.search(pattern, linderos_text, re.IGNORECASE)
                if match:
                    if cardinal in ['oriente', 'occidente']:
                        linderos[cardinal] = match.group(2).strip() if match.group(2) else ""
                    else:
                        linderos[cardinal] = match.group(1).strip()
            
            cabida_linderos["linderos"] = linderos
            cabida_linderos["linderos"]["descripcion_completa"] = linderos_text
        
        return cabida_linderos
    
    def extract_anotaciones(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        anotaciones = []
        
        # Buscar todas las anotaciones
        coincidencias = soup.find_all(text=re.compile(r'ANOTACION.*?Nro.*?\d+', re.IGNORECASE | re.UNICODE))
        
        for texto_anotacion in coincidencias:
            # Extraer número de anotación
            num_match = re.search(r'Nro\s*\.?\s*(\d+)', texto_anotacion, re.IGNORECASE)
            if not num_match:
                continue
                
            numero = int(num_match.group(1))
            
            # Crear diccionario para esta anotación
            anotacion = {
                "numero": numero,
                "fecha": "",
                "tipo_acto": "",
                "especificacion": "",
                "valor_acto": "",
                "personas": {
                    "de": [],
                    "a": []
                },
                "estado": "VIGENTE",
                "radicacion": "",
                "contenido_completo": []
            }
            
            # Buscar el span que contiene esta anotación
            span_anotacion = soup.find('span', text=texto_anotacion)
            if span_anotacion:
                contenido = []
                span_actual = span_anotacion.find_next('span')
                
                while span_actual and 'SUPERINTENDENCIA DE NOTARIADO Y REGISTRO' not in span_actual.get_text():
                    texto = span_actual.get_text().strip()
                    if texto:
                        contenido.append(texto)
                        
                        # Extraer fecha
                        fecha_match = re.search(r'Fecha\s*:\s*(\d{2}-\d{2}-\d{4})', texto, re.IGNORECASE)
                        if fecha_match:
                            fecha = fecha_match.group(1)
                            try:
                                fecha = datetime.strptime(fecha, '%d-%m-%Y').strftime('%Y-%m-%d')
                                anotacion["fecha"] = fecha
                            except:
                                anotacion["fecha"] = fecha_match.group(1)
                        
                        # Extraer especificación/código
                        espec_match = re.search(r'ESPECIFICACION:(?:.*?:\s*)?(\d+)', texto, re.IGNORECASE)
                        if espec_match:
                            anotacion["especificacion"] = espec_match.group(1)
                            # Buscar el tipo de acto después del código
                            tipo_match = re.search(r'ESPECIFICACION:.*?:\s*\d+\s*(.+)', texto, re.IGNORECASE)
                            if tipo_match:
                                anotacion["tipo_acto"] = tipo_match.group(1).strip()
                        
                        # Extraer valor del acto
                        valor_match = re.search(r'VALOR ACTO:\s*\$?\s*([0-9,\.]+)', texto, re.IGNORECASE)
                        if valor_match:
                            anotacion["valor_acto"] = valor_match.group(1).replace(",", "")
                        
                        # Extraer radicación
                        rad_match = re.search(r'RADICACION:\s*([^\s]+)', texto, re.IGNORECASE)
                        if rad_match:
                            anotacion["radicacion"] = rad_match.group(1)
                        
                        # Extraer personas DE:
                        if re.match(r'^DE\s*:', texto, re.IGNORECASE):
                            persona = re.sub(r'^DE\s*:\s*', '', texto, flags=re.IGNORECASE).strip()
                            if persona:
                                anotacion["personas"]["de"].append(persona)
                        
                        # Extraer personas A:
                        if re.match(r'^A\s*:', texto, re.IGNORECASE):
                            persona = re.sub(r'^A\s*:\s*', '', texto, flags=re.IGNORECASE).strip()
                            if persona:
                                anotacion["personas"]["a"].append(persona)
                        
                        # Detectar si está cancelada
                        if re.search(r'CANCELAD[OA]', texto, re.IGNORECASE):
                            anotacion["estado"] = "CANCELADA"
                    
                    span_actual = span_actual.find_next('span')
                    
                    # Detener si encontramos otra anotación
                    if span_actual and re.search(r'ANOTACION.*?Nro.*?\d+', span_actual.get_text(), re.IGNORECASE):
                        break
                
                anotacion["contenido_completo"] = contenido
            
            anotaciones.append(anotacion)
        
        # Ordenar por número
        anotaciones.sort(key=lambda x: x["numero"])
        
        return anotaciones
    
    def extract_metadata(self, soup: BeautifulSoup) -> Dict[str, Any]:
        metadata = {}
        
        for span in soup.find_all('span', {'dir': 'ltr'}):
            texto = span.get_text().strip()
            
            # Fecha de impresión
            if 'impreso' in unidecode(texto.lower()) and 'el' in unidecode(texto.lower()):
                metadata["fecha_expedicion"] = self.extraer_fecha(texto)
            
            # Número de PIN/certificado
            pin_match = re.search(r'Pin\s*No\s*:\s*([0-9]+)', texto, re.IGNORECASE)
            if pin_match:
                metadata["numero_pin"] = pin_match.group(1)
        
        return metadata
    
    def extraer_fecha(self, texto: str) -> str:
        try:
            meses = {
                'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4,
                'mayo': 5, 'junio': 6, 'julio': 7, 'agosto': 8,
                'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12
            }
            
            numeros = [int(s) for s in texto.split() if s.isdigit()]
            nombre_mes = next((mes for mes in meses.keys() if mes in texto.lower()), None)
            
            if numeros and len(numeros) >= 2 and nombre_mes:
                mes = meses.get(nombre_mes, 1)
                fecha_string = f"{numeros[1]:04d}-{mes:02d}-{numeros[0]:02d}"
                return fecha_string
        except:
            pass
        
        return ""
    
    def extract_all(self, url: str) -> Dict[str, Any]:
        try:
            print(f"Iniciando extracción para URL: {url}")
            soup, soup_complete = self.extract_text_from_page(url)
            
            print("Extrayendo información del predio...")
            self.data["informacion_predio"] = self.extract_informacion_predio(soup)
            
            print("Extrayendo dirección...")
            self.data["direccion"] = self.extract_direccion(soup)
            
            print("Extrayendo cabida y linderos...")
            self.data["cabida_linderos"] = self.extract_cabida_linderos(soup_complete)
            
            print("Extrayendo anotaciones...")
            self.data["anotaciones"] = self.extract_anotaciones(soup_complete)
            
            print("Extrayendo metadata...")
            self.data["metadata"] = self.extract_metadata(soup)
            self.data["metadata"]["url_original"] = url
            self.data["metadata"]["fecha_procesamiento"] = datetime.now().isoformat()
            self.data["metadata"]["certificado_valido"] = True
            
            docid_match = re.search(r'docId=(\d+)', url)
            if docid_match:
                self.data["metadata"]["docid"] = docid_match.group(1)
            
            print("Extracción completada exitosamente")
            return self.data
            
        except Exception as e:
            print(f"Error al procesar el certificado: {str(e)}")
            raise Exception(f"Error al procesar el certificado: {str(e)}")
    
    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.data, ensure_ascii=False, indent=indent)