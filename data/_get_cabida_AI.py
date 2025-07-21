import anthropic
import os
import json
from typing import Dict, Any, Optional
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# https://console.anthropic.com/settings/billing

class ValidadorNormativaUrbana:
    def __init__(self, api_key: Optional[str] = None):
        """
        Inicializa el validador de normativa urbana con Claude API
        
        Args:
            api_key: Clave de API de Anthropic
        """
        self.client = anthropic.Anthropic(
            api_key=api_key or os.getenv('ANTHROPIC_API_KEY')
        )
        
        # Cargar normativas desde archivo
        self.normativas_texto = self._cargar_normativas()
    
    def _cargar_normativas(self) -> str:
        """
        Carga el texto de normativas desde archivo o variable
        
        Returns:
            Texto completo de las normativas
        """
        # Texto de normativas POT 555 (puedes cargar desde archivo o mantener aquí)
        normativas = """
# RESUMEN DETALLADO DE NORMAS URBANÍSTICAS - BOGOTÁ D.C.
## Manual de Normas Comunes a los Tratamientos Urbanísticos

**Fuente:** Decreto Distrital 555 de 2021 - Anexo No. 5  
**Fecha:** Actualización 2024  
**Secretaría Distrital de Planeación**

---

## 1. AISLAMIENTO FRONTAL (ANTEJARDÍN)

### 1.1 VARIABLES DE ENTRADA
- **Tratamiento urbanístico**: Consolidación, Renovación Urbana, Mejoramiento Integral, Desarrollo, Conservación
- **Tipología**: Aislada (TA), Continua (TC), No aplica
- **Tipo de predio**: Esquinero, Medianero
- **Ancho de vía**: Dimensión del perfil vial colindante
- **Antejardín existente**: Edificaciones vecinas de 3+ pisos
- **Mapa CU-5.5**: "Dimensionamiento de Antejardines"

### 1.2 REGLAS POR TRATAMIENTO
**Base normativa:** Artículos 258, 314, 315, 316 DD 555/2021 | Sección 1.7 Anexo 5

| Tratamiento | Exigencia de Antejardín | Fuente de Dimensionamiento | Referencia Normativa |
|-------------|-------------------------|----------------------------|---------------------|
| **Consolidación** | SÍ (obligatorio) | Mapa CU-5.5 "Dimensionamiento de Antejardines" | Art. 314 DD 555/2021 |
| **Renovación Urbana** | NO (salvo empates) | Solo aplican empates con predios colindantes | Sección 1.7.c Anexo 5 |
| **Mejoramiento Integral** | NO (salvo empates) | Solo aplican empates con predios colindantes | Sección 1.7.c Anexo 5 |
| **Desarrollo** | NO (salvo empates) | Solo aplican empates con predios colindantes | Sección 1.7.c Anexo 5 |
| **Conservación** | Según ficha normativa | Ficha específica del BIC | Anexo 6 DD 555/2021 |

### 1.3 CÁLCULO DE DIMENSIONES

#### 1.3.1 Tratamiento de Consolidación
**Base normativa:** Art. 258, 314 DD 555/2021 | Resolución SDP 1631/2023
- **Dimensión base**: Definida en Mapa CU-5.5
- **Cuando no hay dimensión en mapa**: Se aplica norma vigente antes del Decreto 555/2021

#### 1.3.2 Empates de Antejardines (Todos los tratamientos excepto Consolidación)
**Base normativa:** Sección 1.7.c Anexo 5 | Art. 258 DD 555/2021

**Condiciones para empate:**

| Situación | Acción Requerida | Referencia Normativa |
|-----------|------------------|---------------------|
| Colindancia lateral con edificaciones 3+ pisos con antejardín | Empate con dimensión del colindante en mínimo 3m de fachada | Sección 1.7.c - Ilustración 09A |
| Colindancia con 2 predios de diferente antejardín | Empate con el antejardín de MAYOR dimensión en mínimo 3m | Sección 1.7.c - Ilustración 09B |
| Colindancia con 2 predios de igual antejardín | Empate con la misma dimensión en toda la fachada | Sección 1.7.c - Ilustración 09C |

### 1.4 EXCEPCIONES Y CASOS ESPECIALES

#### 1.4.1 Predios Esquineros (Consolidación)
**Base normativa:** Resolución SDP 1631/2023 | Sección 1.7.b Anexo 5
```
SI antejardín ≥ 5.00m Y proyecto = costado completo de manzana
ENTONCES: Reducción permitida = 2.00m
PERO: Mantener empate con vecinos en mínimo 3.00m de longitud
Referencia: Sección 1.7.b - Ilustración 08
```

#### 1.4.2 Lotes Esquineros - Reducción especial
**Base normativa:** Resolución SDP 1631/2023
- **Lado de mayor dimensión**: Puede reducirse hasta mínimo 2.00m
- **Condición**: Empate con antejardines vecinos en ≥ 3.00m de fachada

#### 1.4.3 Excepciones donde NO se exige antejardín
**Base normativa:** Sección 1.7.a Anexo 5 | Art. 154 DD 555/2021
- Frentes con franja de control ambiental ejecutada
- Frentes derivados de obras de infraestructura
- Predios que colindan con BIC nivel N4

**Concordancias adicionales:** Art. 90, 128, 146, 147, 154, 176, 315, 316, 348, 358, 363, 368, 548 DD 555/2021

---

## 2. AISLAMIENTO LATERAL

### 2.1 VARIABLES DE ENTRADA
- **Altura de la edificación propuesta** (en metros)
- **Tratamiento urbanístico**
- **Tipología** (Aislada/Continua)
- **Nivel de exigencia** (desde qué piso aplica)
- **Edificaciones colindantes existentes**

### 2.2 FÓRMULA GENERAL
**Base normativa:** Sección 1.9.b Anexo 5
```
Aislamiento Lateral = (1/5) × Altura en metros de la edificación
Dimensión mínima ≥ 3.00m (Mejoramiento Integral) o 4.00m (otros tratamientos)
```

### 2.3 DIMENSIONES MÍNIMAS POR TRATAMIENTO
**Base normativa:** Secciones 2.1, 3.2, 4.3, 5.5 Anexo 5

| Tratamiento | Dimensión Mínima | Nivel de Exigencia | Referencia Normativa |
|-------------|------------------|-------------------|---------------------|
| **Desarrollo** | 4.00m | Desde nivel de terreno o empate | Sección 2.1.b Anexo 5 |
| **Renovación Urbana** | 4.00m | Desde > 11.40m de altura | Sección 3.2.a Anexo 5 |
| **Mejoramiento Integral** | 3.00m | Desde 3er piso (> 5 pisos) | Sección 4.3.a Anexo 5 |
| **Consolidación** | 4.00m | Desde 2do piso (solo TA) | Sección 5.5.a Anexo 5 |

### 2.4 REGLAS DE EXIGENCIA ESPECÍFICAS

#### 2.4.1 Tratamiento de Desarrollo
**Base normativa:** Sección 2.1 Anexo 5
- **Exigencia**: Desde nivel de terreno o empate con edificaciones colindantes > 3 pisos
- **Dimensión**: 1/3 de altura total, mínimo 4.00m

#### 2.4.2 Tratamiento de Renovación Urbana
**Base normativa:** Sección 3.2.a Anexo 5 | Decreto 582/2023
```
SI altura ≤ 11.40m: NO se exige aislamiento lateral
SI altura > 11.40m: Aislamiento = (1/5) × altura, mínimo 4.00m
INCENTIVO: Decreto 582/2023 permite disminución de exigencia
```

#### 2.4.3 Tratamiento de Mejoramiento Integral
**Base normativa:** Sección 4.3.a Anexo 5 | Decreto 582/2023
```
SI altura ≤ 5 pisos: NO se exige aislamiento lateral
SI altura > 5 pisos: Exigencia desde placa superior del 3er piso
Dimensión = (1/5) × altura desde nivel de exigencia, mínimo 3.00m
INCENTIVO: Aislamiento desde 4to piso con construcción sostenible
```

#### 2.4.4 Tratamiento de Consolidación
**Base normativa:** Sección 5.5.a Anexo 5 | Art. 310 DD 555/2021
- **Solo para tipología AISLADA** (Mapas CU-5.4.2 a CU-5.4.33)
- **Exigencia**: Desde 2do piso
- **Dimensión**: (1/5) × altura desde 2do piso, mínimo 4.00m

### 2.5 EMPATES DE AISLAMIENTO LATERAL
**Base normativa:** Secciones 2.1.c, 3.2.c, 4.3.c, 5.5.c Anexo 5

| Situación Colindante | Acción | Referencia |
|---------------------|--------|------------|
| Edificación sin aislamiento lateral | Empate volumétrico hasta altura existente, luego aislamiento reglamentario | Sección 1.9.c |
| Edificación con aislamiento lateral | Aislamiento desde nivel de terreno según altura propia | Sección 1.9.c |

### 2.6 AVANCES DE FACHADA PERMITIDOS
**Base normativa:** Sección 1.9.b Anexo 5 | Ilustración 12
```
SI aislamiento lateral > 4.00m:
   Avance permitido = 0.50m máximo
   Área del avance ≤ 30% del área de fachada sobre el aislamiento
   Proyección sobre otros niveles NO cuenta como área construida
```

---

## 3. AISLAMIENTO POSTERIOR

### 3.1 VARIABLES DE ENTRADA
- **Altura de la edificación** (metros o pisos según tratamiento)
- **Tratamiento urbanístico**
- **Geometría del predio** (fondo, forma)
- **Edificaciones colindantes posteriores**

### 3.2 DIMENSIONES POR TRATAMIENTO Y ALTURA

#### 3.2.1 Tratamiento de Renovación Urbana
**Base normativa:** Sección 3.1.b Anexo 5

| Altura de Edificación (metros) | Aislamiento Posterior (metros) | Referencia |
|--------------------------------|--------------------------------|------------|
| Hasta 12m | 4m | Sección 3.1.b tabla |
| > 12m y ≤ 18m | 5m | Sección 3.1.b tabla |
| > 18m y ≤ 27m | 6m | Sección 3.1.b tabla |
| > 27m y ≤ 36m | 8m | Sección 3.1.b tabla |
| > 36m y ≤ 45m | 10m | Sección 3.1.b tabla |
| > 45m y ≤ 54m | 12m | Sección 3.1.b tabla |
| > 54m y ≤ 66m | 14m | Sección 3.1.b tabla |
| > 66m y ≤ 75m | 16m | Sección 3.1.b tabla |
| > 75m y ≤ 84m | 18m | Sección 3.1.b tabla |
| > 84m | 20m | Sección 3.1.b tabla |

#### 3.2.2 Tratamiento de Mejoramiento Integral
**Base normativa:** Sección 4.2.b Anexo 5 | Art. 338 DD 555/2021

| Altura en Pisos | Aislamiento Posterior | Referencia |
|----------------|----------------------|------------|
| Hasta 3 pisos | NO se exige | Sección 4.2.a |
| 4 a 6 pisos | 5m | Sección 4.2.b tabla |
| 7 a 9 pisos | 6m | Sección 4.2.b tabla |
| 10 a 12 pisos | 8m | Sección 4.2.b tabla |

#### 3.2.3 Tratamiento de Consolidación
**Base normativa:** Sección 5.4.b Anexo 5 | Art. 310 DD 555/2021

| Altura en Pisos | Aislamiento Posterior | Mapas de Referencia |
|----------------|----------------------|-------------------|
| Hasta 3 pisos | 3m | CU-5.4.2 a CU-5.4.33 |
| 4 a 6 pisos | 5m | CU-5.4.2 a CU-5.4.33 |
| 7 a 9 pisos | 6m | CU-5.4.2 a CU-5.4.33 |
| 10 a 12 pisos | 8m | CU-5.4.2 a CU-5.4.33 |
| 13 a 15 pisos | 10m | CU-5.4.2 a CU-5.4.33 |
| 16 a 18 pisos | 12m | CU-5.4.2 a CU-5.4.33 |
| 19 a 22 pisos | 14m | CU-5.4.2 a CU-5.4.33 |
| 23 a 25 pisos | 16m | CU-5.4.2 a CU-5.4.33 |
| 26 a 28 pisos | 18m | CU-5.4.2 a CU-5.4.33 |
| 29+ pisos | 20m | CU-5.4.2 a CU-5.4.33 |

### 3.3 EXCEPCIONES DE EXIGENCIA
**Base normativa:** Secciones 3.1.a, 4.2.a, 5.4.a Anexo 5 | Ilustraciones 29 y 30

**NO se exige aislamiento posterior cuando:**
- Predio esquinero/medianero con lindero posterior = lindero lateral del vecino
- Fondo del predio ≤ 8.00m
- Geometría irregular que no permita definir claramente la localización
- Colindancia con suelos de protección, servicios públicos, infraestructura

### 3.4 EMPATES DE AISLAMIENTO POSTERIOR
**Base normativa:** Secciones 3.1.c, 4.2.c, 5.4.c Anexo 5 | Ilustración 28
```
SI edificación colindante posterior de 3+ pisos SIN aislamiento posterior:
   Empate volumétrico hasta altura existente (máx 11.40m en Renovación Urbana)
   Desde nivel de empate: aislamiento reglamentario para altura máxima propuesta
```

### 3.5 PREDIOS ESQUINEROS - REGLAS ESPECIALES
**Base normativa:** Sección 3.1.a, 5.4.a Anexo 5

#### 3.5.1 Renovación Urbana y Consolidación Continua
- **Aislamiento posterior = Patio en esquina interior**
- **Dimensión del patio = Dimensión del aislamiento posterior reglamentario**

#### 3.5.2 Consolidación Aislada
- **Aislamiento posterior = Patio en esquina interior**  
- **Dimensión del patio = Dimensión del aislamiento LATERAL reglamentario**

---

## 4. VOLADIZO FRONTAL

### 4.1 VARIABLES DE ENTRADA
- **Perfil vial del frente del predio**
- **Existencia de antejardín/APAUP**
- **Voladizos existentes en edificaciones colindantes**

### 4.2 TABLA DE DIMENSIONES MÁXIMAS
**Base normativa:** Sección 1.13.b Anexo 5 | Art. 155 DD 555/2021

| Perfil Vial (ancho) | Con Antejardín/APAUP/Control Ambiental | Sin Antejardín | Referencia |
|-------------------|---------------------------------------|----------------|------------|
| ≤ 6.00m | No se permite | No se permite | Sección 1.13.b tabla |
| > 6.00m y ≤ 10.00m | 0.60m | 0.60m | Sección 1.13.b tabla |
| > 10.00m y ≤ 15.00m | 0.80m | 0.60m | Sección 1.13.b tabla |
| > 15.00m y ≤ 22.00m | 1.00m | 0.60m | Sección 1.13.b tabla |
| > 22.00m y malla arterial | 1.50m | 0.60m | Sección 1.13.b tabla |

### 4.3 CONDICIONES DE APLICACIÓN
**Base normativa:** Sección 1.13.a Anexo 5
- **Aplica**: Solo en pisos diferentes del primero
- **Prohibido sobre**: Cesiones públicas para parques, equipamientos, EEP
- **Permitido sobre**: Áreas de control ambiental ejecutadas y entregadas

### 4.4 EMPATES DE VOLADIZOS
**Base normativa:** Sección 1.13.c Anexo 5
```
SI edificación colindante de 3+ pisos con voladizo diferente al reglamentario:
   Empate volumétrico en máximo 3.00m de fachada horizontal
   Resto de fachada: voladizo reglamentario
```

### 4.5 MANEJO DE VOLADIZOS
**Base normativa:** Sección 1.13.d Anexo 5 | Art. 128 DD 555/2021
- Sujeto a directrices para manejo de espacios privados afectos al uso público

---

## 5. VOLADIZO LATERAL

### 5.1 APLICACIÓN
**Base normativa:** Sección 1.13 Anexo 5 (aplicación por analogía)
Los voladizos laterales siguen las **mismas reglas que los frontales** cuando el predio tiene frente lateral a vía pública.

### 5.2 CASOS ESPECIALES
- **Predio esquinero**: Cada frente se evalúa independientemente según su perfil vial
- **Lado sin frente a vía**: No aplica voladizo lateral

---

## 6. VOLADIZO POSTERIOR

### 6.1 REGLA GENERAL
**Base normativa:** Sección 1.13.a Anexo 5
**NO se permiten voladizos posteriores** sobre predios colindantes.

### 6.2 EXCEPCIONES
- Solo se permiten si el predio posterior también tiene frente a vía pública
- En ese caso, aplican las reglas de voladizo frontal según el perfil vial posterior

---

## 7. ALTURA MÁXIMA DE PISOS A CONSTRUIR

### 7.1 VARIABLES DE ENTRADA
- **Tratamiento urbanístico**
- **Mapa CU-5.4.2 a CU-5.4.33 - Edificabilidad**
- **Limitantes de aeronáutica**
- **Cables aéreos de transporte**

### 7.2 DETERMINACIÓN POR TRATAMIENTO
**Base normativa:** Art. 260 DD 555/2021 | Sección 1.3 Anexo 5

| Tratamiento | Método de Determinación | Referencia Normativa |
|-------------|------------------------|---------------------|
| **Consolidación** | Pisos definidos en mapas CU-5.4.2 a CU-5.4.33 | Art. 310 DD 555/2021 |
| **Mejoramiento Integral** | Pisos definidos en mapas CU-5.4.2 a CU-5.4.33 | Art. 338 DD 555/2021 |
| **Conservación** | Pisos definidos en mapas CU-5.4.2 a CU-5.4.33 | Anexo 6 DD 555/2021 |
| **Desarrollo 4A y 4B** | Pisos definidos en mapas CU-5.4.2 a CU-5.4.33 | Art. 281 DD 555/2021 |
| **Renovación Urbana** | Resultante de aplicar normas del tratamiento | Art. 260 DD 555/2021 |
| **Desarrollo 1,2,3,4C,4D** | Resultante de aplicar normas del tratamiento | Art. 281 DD 555/2021 |

### 7.3 LIMITANTES GENERALES
**Base normativa:** Sección 1.2 Anexo 5 | Mapa Anexo No. 01
```
Altura máxima edificación ≤ MIN(
   Altura por tratamiento,
   Altura AEROCIVIL según mapa sectorización,
   Altura por cables aéreos (3 pisos hasta definición)
)
```

### 7.4 EXCEPCIONES EN ALTURA MÁXIMA

#### 7.4.1 Consolidación - Excepciones por área y frente
**Base normativa:** Art. 310, 311, 312 DD 555/2021 | Sección 5.1 Anexo 5
- **Condiciones para superar altura base**: Definidas en parágrafo 3 Art. 310
- **Conjuntos bajo copropiedad**: Art. 311 DD 555/2021

#### 7.4.2 Mejoramiento Integral - Condiciones especiales
**Base normativa:** Art. 338 DD 555/2021 | Sección 4.1 Anexo 5
```
Altura permitida = f(área predio, ancho vía, cumplimiento condiciones urbanísticas)
Referencia específica: Tabla Art. 338 DD 555/2021
```

### 7.5 EMPATES DE ALTURA (Consolidación y Mejoramiento Integral)
**Base normativa:** Sección 1.6 Anexo 5 | Ilustraciones 07A, 07B, 07C

| Situación Colindante | Altura Permitida | Referencia |
|---------------------|------------------|------------|
| Edificación > altura base sin aislamiento lateral | Empate volumétrico con la mayor altura | Ilustración 07A |
| Edificación < altura base sin aislamiento lateral | No se exige empate | Ilustración 07B |
| 2 edificaciones igual altura sin aislamiento | Misma altura que las existentes | Ilustración 07C |

### 7.6 CONCORDANCIAS NORMATIVAS
**Base normativa:** DD 555/2021
- Art. 76: Altura 3 pisos equipamientos Zona Articulación Río Bogotá
- Art. 166: Altura servicios conexos en infraestructuras
- Art. 176: Condiciones edificabilidad equipamientos
- Art. 217: Estaciones radioeléctricas
- Art. 248: Acción MU1 - altura libre 5m zonas transición
- Art. 260: Altura máxima edificaciones

---

## 8. ALTURA DEL EDIFICIO (EN METROS)

### 8.1 MÉTODO DE CÁLCULO
**Base normativa:** Sección 1.3.e Anexo 5 | Ilustración 04
```
Altura edificio = Distancia vertical desde:
   DESDE: Cruce del paramento propuesto con línea de inclinación del terreno
   HASTA: Parte superior de placa de cubierta o último piso
```

### 8.2 ALTURAS POR PISO PERMITIDAS
**Base normativa:** Sección 1.5 Anexo 5 | Ilustración 06

| Uso/Área | Altura Mínima Libre | Altura Máxima Libre | Referencia |
|----------|-------------------|-------------------|------------|
| **Residencial** | 2.30m | 3.80m | Sección 1.5 tabla |
| **Comercio y Servicios** | 2.30m | 4.20m | Sección 1.5 tabla |
| **Dotacional** | 2.30m | La requerida para el uso | Sección 1.5 tabla |
| **Industrial** | 2.30m | La requerida para el uso | Sección 1.5 tabla |
| **Estacionamientos** | 2.30m | 4.20m | Sección 1.5 tabla |

### 8.3 REGLA DE CONTABILIZACIÓN DE PISOS ADICIONALES
**Base normativa:** Sección 1.5 Anexo 5
```
SI altura libre > altura máxima permitida:
   Cada fracción de 3.80m (residencial) o 4.20m (otros) = 1 piso adicional
```

### 8.4 ELEMENTOS QUE NO CUENTAN COMO ALTURA
**Base normativa:** Sección 1.3.c Anexo 5
- Ductos, chimeneas, remates de cubiertas
- Puntos fijos, cerramientos, tanques
- Equipos técnicos, sobre-recorridos de ascensores
- Hall de cubierta

### 8.5 ALTURAS EN TERRENOS INCLINADOS
**Base normativa:** Sección 1.4 Anexo 5 | Ilustraciones 05A, 05B, 05C
```
Franja de ajuste = máximo 3.00m paralela a línea de inclinación
Retroceso obligatorio = mínimo 3.00m frente a espacio público
```

---

## 9. ESCALONAMIENTO DEL EDIFICIO

### 9.1 RETROCESOS DE FACHADA CONTRA ESPACIO PÚBLICO
**Base normativa:** Sección 1.11 Anexo 5 | Ilustración 14

### 9.2 VARIABLES DE ENTRADA
- **Distancia (D)**: Entre paramento propuesto y lindero contra espacio público
- **Tratamiento urbanístico**

### 9.3 FÓRMULAS DE ESCALONAMIENTO

#### 9.3.1 Consolidación y Renovación Urbana
**Base normativa:** Sección 1.11.a Anexo 5
```
SI D ≤ 30m:
   Altura límite de fachada (A) = 2.5 × D
   
SI altura edificio > A:
   Retroceso mínimo = (1/5) × (altura restante)
   Retroceso mínimo ≥ 4.00m
```

#### 9.3.2 Desarrollo  
**Base normativa:** Sección 1.11.a Anexo 5
```
SI D ≤ 30m:
   Altura límite de fachada (A) = 2.0 × D
   
SI altura edificio > A:
   Retroceso mínimo = (1/5) × (altura restante)
   Retroceso mínimo ≥ 4.00m
```

#### 9.3.3 Mejoramiento Integral
**Base normativa:** Sección 1.11.b Anexo 5 | Art. 338 DD 555/2021
- **Retrocesos específicos**: Según artículo 338 del DD 555/2021
- **Sobreancho de andenes**: Para costado completo de manzana > 3 pisos

### 9.4 TERRENOS INCLINADOS - ESCALONAMIENTO ESPECIAL
**Base normativa:** Sección 1.4 Anexo 5 | Ilustraciones 05A, 05B, 05C

#### 9.4.1 Franja de Ajuste
```
Franja de ajuste = 3.00m paralela a línea de inclinación
EN esta franja: Volumetría puede superar altura máxima
CONDICIÓN: Retroceso de fachada ≥ 3.00m frente a espacio público
```

---

## 10. REDUCCIÓN DEL LOTE PARA DESARROLLO

### 10.1 DEFINICIÓN DE ÁREA DE TERRENO
**Base normativa:** Sección 1.1 Anexo 5 | Art. 2.2.1.1 Decreto Nacional 1077/2015

#### 10.2 FÓRMULA GENERAL
```
Área de Terreno = Área predio privada 
                 - Áreas sistema vial principal y transporte
                 - Redes primarias servicios públicos
                 - Áreas conservación recursos naturales
                 - Malla vial intermedia y local (existente o proyectada)*
                 
* No se descuenta en procesos de reurbanización
Referencia: Sección 1.1 definición "Área de terreno"
```


### 10.3 APLICACIÓN POR TRATAMIENTO
**Base normativa:** Sección 1.1 Anexo 5

| Tratamiento | Área Base para Cálculos | Referencia Normativa |
|-------------|------------------------|---------------------|
| **Desarrollo** | Área Neta Urbanizable (ANU) | Art. 2.2.1.1 DN 1077/2015 |
| **Consolidación** | Área de Terreno | Sección 1.1 Anexo 5 |
| **Renovación Urbana** | Área de Terreno | Sección 1.1 Anexo 5 |
| **Mejoramiento Integral** | Área de Terreno | Sección 1.1 Anexo 5 |
| **Conservación N4** | Área de Terreno | Sección 1.1 Anexo 5 |

### 10.4 CESIONES OBLIGATORIAS EN DESARROLLO
**Base normativa:** Art. 269 DD 555/2021 | Sección 2.4 Anexo 5

#### 10.4.1 Cesiones Tipo A (Locales)
- **Parques**: 17 m²/hab (sobre área útil)
- **Equipamientos**: 4 m²/hab (sobre área útil)

#### 10.4.2 Cesiones Tipo B (Generales)  
- **Parques**: 2 m²/hab (sobre área neta urbanizable)
- **Equipamientos**: 2 m²/hab (sobre área neta urbanizable)

---

## 11. JARDINES (ÁREAS VERDES Y RECREATIVAS)

### 11.1 EQUIPAMIENTO COMUNAL PRIVADO (ECP)
**Base normativa:** Sección 1.18 Anexo 5 | Art. 190 DD 555/2021

#### 11.2 ÁREAS CONTABILIZABLES COMO JARDINES
**Base normativa:** Sección 1.18.a Anexo 5
- **Antejardines**
- **Zonas verdes**
- **Plazoletas**
- **Áreas de disfrute para la comunidad**
- **Áreas de acondicionamiento físico**

### 11.3 EXIGENCIAS DE ECP POR TRATAMIENTO

#### 11.3.1 Consolidación, Renovación Urbana, Mejoramiento Integral
**Base normativa:** Sección 1.18.b Anexo 5

| Tipo de Proyecto | Exigencia de ECP | Referencia |
|------------------|------------------|------------|
| **VIS/VIP hasta 150 viviendas** | 6.00 m²/vivienda | Sección 1.18.b tabla |
| **VIS/VIP > 150 viviendas** | 8.50 m²/vivienda (para exceso) | Sección 1.18.b tabla |
| **No VIS/VIP** | 10 m²/80 m² área construida uso | Sección 1.18.b tabla |
| **Usos no residenciales** | 10 m²/120 m² área construida uso | Sección 1.18.b tabla |

#### 11.3.2 Desarrollo
**Base normativa:** Sección 1.18.b Anexo 5

| Tipo de Proyecto | Exigencia de ECP | Referencia |
|------------------|------------------|------------|
| **VIS/VIP hasta 150 viv/ha ANU** | 6.00 m²/vivienda | Sección 1.18.b tabla |
| **VIS/VIP > 150 viv/ha ANU** | 8.50 m²/vivienda (para exceso) | Sección 1.18.b tabla |
| **No VIS/VIP** | 15 m²/80 m² área construida uso | Sección 1.18.b tabla |
| **Usos no residenciales** | 10 m²/120 m² área construida uso | Sección 1.18.b tabla |

### 11.4 DESTINACIÓN OBLIGATORIA
**Base normativa:** Sección 1.18.c Anexo 5
```
40% mínimo: Zonas verdes y recreativas (áreas libres)
20% mínimo: Servicios comunales (áreas construidas)
40% restante: Puede incluir estacionamientos especiales
```

### 11.5 EXCEPCIONES
**Base normativa:** Sección 1.18.b notas 4 y 5 | Art. 271, 559 DD 555/2021
- **BIC niveles 1, 2, 3**: No se exige ECP (Art. 559 numeral 3)
- **Reúso para vivienda**: No se exige ECP adicional (Art. 271)

---

## 12. RETROCESOS DEL LOTE

### 12.1 TIPOS DE RETROCESOS

#### 12.1.1 Antejardines (Ver Sección 1)
**Base normativa:** Sección 1.7 Anexo 5 | Art. 258, 314 DD 555/2021
- **Función**: Área libre entre lindero y paramento reglamentario
- **Cálculo**: Según Mapa CU-5.5 (Consolidación) o empates (otros tratamientos)

#### 12.1.2 Retrocesos por Altura (Ver Sección 9)
**Base normativa:** Sección 1.11 Anexo 5
- **Función**: Escalonamiento de edificaciones altas
- **Cálculo**: Según fórmulas de altura límite de fachada

#### 12.1.3 Franjas de Control Ambiental
**Base normativa:** Sección 7.7 Anexo 5 | Art. 154 DD 555/2021
```
Dimensión mínima = 5.00m
Aplicable en: Renovación Urbana y Mejoramiento Integral
Puede contar como: Parte de cesiones de espacio público (relación 1:1.5)
```

---

## 13. FÓRMULA PARA CEDER PARTE DEL LOTE PARA AUMENTAR ALTURA

## 13.1 MECANISMO DE REDUCCIÓN DE LOTE PARA MAYOR EDIFICABILIDAD

### 13.1.1 TRATAMIENTOS DE RENOVACIÓN URBANA Y DESARROLLO
**Base normativa:** Art. 260, 317-330, 273-307 DD 555/2021

**PRINCIPIO FUNDAMENTAL:**
En tratamientos de Renovación Urbana y Desarrollo, la altura de edificación y huella están directamente relacionadas con la cesión de área del lote para el desarrollo urbano.

**CÁLCULO DE REDUCCIÓN DE LOTE:**
reduccion_lote = 100 - porcentaje_cesion_requerido
Donde porcentaje_cesion_requerido depende de:

Altura de edificación deseada
Tratamiento específico
Densidad propuesta
Cesiones obligatorias de espacio público


**REGLA PARA AISLAMIENTOS EN ESTOS TRATAMIENTOS:**
- **NO se calculan aislamientos laterales y posteriores individualmente**
- **SE calcula "reduccion_lote" como parámetro principal (0-100%)**
- **Ejemplo**: Si requiero ceder 30% del lote para edificar X pisos, entonces reduccion_lote = 70%

### 13.1.2 RELACIÓN ANTEJARDÍN - AISLAMIENTO FRONTAL
**Base normativa:** Sección 1.7 Anexo 5 | Art. 258, 314, 315, 316 DD 555/2021

**REGLA CRÍTICA:**
aislamiento_frontal = antejardín_reglamentario
FUENTE: Mapa CU-5.5 "Dimensionamiento de Antejardines" (Consolidación)
FUENTE: Empates con edificaciones colindantes (otros tratamientos)

**APLICACIÓN POR TRATAMIENTO:**
- **Consolidación**: aislamiento_frontal = dimensión Mapa CU-5.5
- **Renovación/Desarrollo/Mejoramiento**: aislamiento_frontal = empate con colindantes o 0m
- **NUNCA**: aislamiento_frontal ≠ antejardín (son el mismo valor)

#### 13.2 CONDICIONES PARA RENOVACIÓN URBANA
**Base normativa:** Sección 1.22.a Anexo 5
```
BENEFICIO: Pago total de cesión de espacio público al fondo
CONDICIÓN: APAUP = 35% del área total del terreno en centro de manzana

REQUISITOS:
- Proyecto de manzana completa por licenciamiento directo
- Frente mínimo APAUP = 20m sobre vías circundantes
- Continuidad espacial desde centro hacia vías
- Acceso peatonal libre y sin obstáculos
```

### 13.3 TRANSFERENCIA DE DERECHOS DE CONSTRUCCIÓN
**Base normativa:** Sección 7.8.b Anexo 5 | Art. 477 DD 555/2021

#### 13.3.1 Aplicable en Suelo Rural
```
Predio generador: Cede derechos de construcción
Predio receptor: Recibe derechos adicionales de construcción
Subdivisión mínima en predios generadores: 0.04 Has
```

### 13.4 INCENTIVOS POR CONSTRUCCIÓN SOSTENIBLE
**Base normativa:** Decreto 582/2023

#### 13.4.1 Renovación Urbana y Mejoramiento Integral
**Beneficios disponibles según Decreto 582/2023:**
- Disminución de exigencia de aislamiento lateral
- Aumento de área para estacionamientos
- Otros incentivos por ecourbanismo

#### 13.4.2 Mejoramiento Integral - Incentivo específico
**Base normativa:** Sección 4.3.a Anexo 5 | Decreto 582/2023
```
INCENTIVO: Aislamiento lateral desde 4to piso (en lugar de 3er piso)
CONDICIÓN: Incorporación de medidas de construcción sostenible
```

---

## RESUMEN DE VARIABLES CRÍTICAS POR TRATAMIENTO

| Variable | Consolidación | Renovación Urbana | Mejoramiento Integral | Desarrollo | Conservación |
|----------|---------------|-------------------|----------------------|------------|--------------|
| **Antejardín** | Obligatorio (Mapa CU-5.5) | Solo empates | Solo empates | Solo empates | Según ficha |
| **Aislamiento Lateral** | Desde 2° piso (TA) | Desde >11.40m | Desde >5 pisos | Desde terreno | Según ficha |
| **Aislamiento Posterior** | Según tabla pisos | Según tabla metros | Según tabla pisos | Como lateral | Según ficha |
| **Altura Máxima** | Pisos en mapa | Resultante | Pisos en mapa | Resultante | Pisos en mapa |
| **Voladizo Máximo** | Según perfil vial | Según perfil vial | Según perfil vial | Según perfil vial | Según ficha |
| **ECP Exigido** | 10m²/80m² | 10m²/80m² | 10m²/80m² | 15m²/80m² | No aplica |

**Referencias normativas de la tabla:**
- Consolidación: Arts. 310-316 DD 555/2021 | Secciones 5.1-5.6 Anexo 5
- Renovación Urbana: Arts. 317-330 DD 555/2021 | Secciones 3.1-3.3 Anexo 5  
- Mejoramiento Integral: Arts. 331-340 DD 555/2021 | Secciones 4.1-4.5 Anexo 5
- Desarrollo: Arts. 273-307 DD 555/2021 | Secciones 2.1-2.8 Anexo 5
- Conservación: Arts. 341-376 DD 555/2021 | Anexo 6 | Sección 6.1-6.2 Anexo 5

---

## DOCUMENTOS DE REFERENCIA NORMATIVA

### Decretos Principales
1. **Decreto Distrital 555 de 2021** - Plan de Ordenamiento Territorial de Bogotá D.C.
2. **Decreto Distrital 582 de 2023** - Ecourbanismo y Construcción Sostenible
3. **Decreto Distrital 122 de 2023** - Vivienda Colectiva y Soluciones Habitacionales
4. **Decreto Nacional 1077 de 2015** - Normas nacionales de urbanismo

### Mapas Reglamentarios
5. **Mapa CU-5.5** - Dimensionamiento de Antejardines
6. **Mapas CU-5.4.2 a CU-5.4.33** - Edificabilidad
7. **Mapa Anexo No. 01** - Sectorización de obstáculos por altura AEROCIVIL

### Resoluciones y Circulares
8. **Resolución SDP 1631/2023** - Actualización Mapa CU-5.5
9. **Resolución SDP 1662/2023** - Sectores Consolidados
10. **Circular No. 007/2022 SDP** - Área mínima habitable

### Leyes y Normas Nacionales
11. **Ley 675 de 2001** - Régimen de Propiedad Horizontal
12. **Ley 160 de 1994** - Sistema Nacional de Reforma Agraria
13. **Acuerdo 338/2023 ANT** - Unidades Agrícolas Familiares

---

**Nota importante:** Este resumen incluye todas las referencias normativas específicas que respaldan cada cálculo, tabla, fórmula y regla. Para aplicaciones específicas, consulte siempre la normativa oficial vigente y los mapas reglamentarios correspondientes.

**Elaborado:** 2024  
**Fuente:** Secretaría Distrital de Planeación - Bogotá D.C.  
**Metodología:** Análisis técnico-jurídico del Anexo No. 5 - DD 555/2021
"""
        return normativas
    
    def _construir_prompt_validacion(self, json_lote: Dict[str, Any], input_lote: Dict[str, Any]) -> str:
        """
        Construye el prompt específico para validación de normativa urbana
        
        Args:
            json_lote: Información del lote desde la base de datos
            input_lote: Parámetros propuestos para validar
            
        Returns:
            Prompt estructurado para Claude
        """
        
        prompt = f"""Eres un experto en interpretación de la normativa urbana de Bogotá D.C. Tu tarea es validar si los parámetros de edificación propuestos cumplen con la normativa POT 555 de 2021.

## CONTEXTO NORMATIVO COMPLETO:
{self.normativas_texto}

## INFORMACIÓN DEL LOTE A ANALIZAR:
```json
{json.dumps(json_lote, indent=2, ensure_ascii=False)}
```

## PARÁMETROS DE EDIFICACIÓN PROPUESTOS PARA VALIDAR:
```json
{json.dumps(input_lote, indent=2, ensure_ascii=False)}
```

## INSTRUCCIONES ESPECÍFICAS:

1. **ANÁLISIS REQUERIDO**: Debes validar CADA parámetro propuesto contra la normativa específica según:
   - Tratamiento urbanístico del lote
   - Tipología (TA/TC)
   - Altura máxima permitida
   - Características del predio (esquinero/medianero)
   - Ancho de vía
   - Dimensión de antejardín según mapa CU-5.5
   - Edificaciones colindantes

2. **PARÁMETROS A VALIDAR**:

   **REGLAS ESPECÍFICAS POR TRATAMIENTO:**
   
   **A) TRATAMIENTOS DE RENOVACIÓN URBANA Y DESARROLLO:**
   - `numero_pisos`: Debe cumplir altura máxima según tratamiento
   - `alturapiso`: Debe estar entre 2.30m y 3.80m (residencial)
   - `aislamiento_frontal`: DEBE SER IGUAL al antejardín reglamentario (NO calcular independiente)
   - `reduccion_lote`: PARÁMETRO PRINCIPAL - Porcentaje (0-100%) que determina cesión requerida para la edificabilidad
   - `aislamiento_lateral/posterior`: NO CALCULAR - Dependen de la reduccion_lote
   - `voladizo_frontal/lateral/posterior`: Según ancho de vía
   
   **B) TRATAMIENTOS DE CONSOLIDACIÓN Y MEJORAMIENTO INTEGRAL:**
   - `numero_pisos`: Debe cumplir altura máxima según tratamiento
   - `alturapiso`: Debe estar entre 2.30m y 3.80m (residencial)
   - `aislamiento_frontal`: DEBE SER IGUAL al antejardín reglamentario según Mapa CU-5.5
   - `aislamiento_lateral`: Debe cumplir fórmula (1/5) × altura, mínimos según tratamiento
   - `aislamiento_posterior`: Debe cumplir tabla según tratamiento y altura
   - `voladizo_frontal/lateral/posterior`: Según ancho de vía
   - `reduccion_lote`: Generalmente 0% (no aplica cesión)

3. **VALIDACIÓN OBLIGATORIA**: Para cada parámetro debes:
   - Identificar la norma específica que aplica
   - Calcular el valor correcto según la normativa
   - Comparar con el valor propuesto
   - Determinar si es correcto o requiere corrección

4. **LÓGICA ESPECÍFICA DE CÁLCULO**:

   **PASO 1 - IDENTIFICAR MÉTODO DE CÁLCULO:**
    SI tratamiento == "Renovación Urbana" OR tratamiento == "Desarrollo":
    MÉTODO = "Reducción de Lote"
    CALCULAR: reduccion_lote (0-100%)
    aislamiento_frontal = antejardín_reglamentario
    aislamiento_lateral = "Depende de reduccion_lote"
    aislamiento_posterior = "Depende de reduccion_lote"
    SI tratamiento == "Consolidación" OR tratamiento == "Mejoramiento Integral":
    MÉTODO = "Aislamientos Individuales"
    CALCULAR: Cada aislamiento según fórmulas específicas
    aislamiento_frontal = antejardín_reglamentario (Mapa CU-5.5)
    reduccion_lote = 0% (no aplica)
    
    **PASO 2 - ANTEJARDÍN = AISLAMIENTO FRONTAL:**
    SIEMPRE: aislamiento_frontal = valor_antejardin_reglamentario
    NUNCA: aislamiento_frontal ≠ antejardín
    FUENTE: Mapa CU-5.5 (Consolidación) o empates (otros tratamientos)

5. **RESPUESTA REQUERIDA**: Debes responder ÚNICAMENTE con un JSON válido que contenga:

```json
{{
  "validacion_exitosa": true/false,
  "parametros_validados": {{
    "numero_pisos": {{
      "valor_propuesto": [valor del input],
      "valor_correcto": [valor según normativa],
      "es_valido": true/false,
      "norma_aplicable": "Artículo específico y sección",
      "observaciones": "Explicación detallada"
    }},
    "alturapiso": {{
      "valor_propuesto": [valor del input],
      "valor_correcto": [valor según normativa],
      "es_valido": true/false,
      "norma_aplicable": "Sección específica",
      "observaciones": "Explicación detallada"
    }},
    "aislamiento_frontal": {{
      "valor_propuesto": [valor del input],
      "valor_correcto": [valor según normativa],
      "es_valido": true/false,
      "norma_aplicable": "Artículo y mapa específico",
      "observaciones": "Explicación detallada"
    }},
    "aislamiento_lateral": {{
      "valor_propuesto": [valor del input],
      "valor_correcto": [valor según normativa],
      "es_valido": true/false,
      "norma_aplicable": "Sección específica",
      "observaciones": "Explicación detallada"
    }},
    "aislamiento_posterior": {{
      "valor_propuesto": [valor del input],
      "valor_correcto": [valor según normativa],
      "es_valido": true/false,
      "norma_aplicable": "Sección específica",
      "observaciones": "Explicación detallada"
    }},
    "voladizo_frontal": {{
      "valor_propuesto": [valor del input],
      "valor_correcto": [valor según normativa],
      "es_valido": true/false,
      "norma_aplicable": "Sección específica",
      "observaciones": "Explicación detallada"
    }},
    "voladizo_lateral": {{
      "valor_propuesto": [valor del input],
      "valor_correcto": [valor según normativa],
      "es_valido": true/false,
      "norma_aplicable": "Sección específica",
      "observaciones": "Explicación detallada"
    }},
    "voladizo_posterior": {{
      "valor_propuesto": [valor del input],
      "valor_correcto": [valor según normativa],
      "es_valido": true/false,
      "norma_aplicable": "Sección específica",
      "observaciones": "Explicación detallada"
    }},
    "reduccion_poligono": {{
      "valor_propuesto": [valor del input],
      "valor_correcto": [valor según normativa],
      "es_valido": true/false,
      "norma_aplicable": "Sección específica",
      "observaciones": "Explicación detallada"
    }}
  }},
  "input_lote_corregido": {{
    "numero_pisos": [valor corregido],
    "alturapiso": [valor corregido],
    "aislamiento_frontal": [valor corregido],
    "aislamiento_lateral": [valor corregido],
    "aislamiento_posterior": [valor corregido],
    "aislamiento_frontal_cara_larga": [valor corregido],
    "aislamiento_frontal_cara_corta": [valor corregido],
    "aislamiento_lateral_cara_larga": [valor corregido],
    "aislamiento_lateral_cara_corta": [valor corregido],
    "aislamiento_posterior_cara_larga": [valor corregido],
    "aislamiento_posterior_cara_corta": [valor corregido],
    "reduccion_poligono": [valor corregido],
    "voladizo_frontal": [valor corregido],
    "voladizo_lateral": [valor corregido],
    "voladizo_posterior": [valor corregido],
    "voladizo_frontal_cara_larga": [valor corregido],
    "voladizo_frontal_cara_corta": [valor corregido],
    "voladizo_lateral_cara_larga": [valor corregido],
    "voladizo_lateral_cara_corta": [valor corregido],
    "voladizo_posterior_cara_larga": [valor corregido],
    "voladizo_posterior_cara_corta": [valor corregido],
    "escalones_config": [valor corregido]
  }},
  "resumen_validacion": {{
    "total_parametros": [número],
    "parametros_validos": [número],
    "parametros_corregidos": [número],
    "cumple_normativa": true/false,
    "observaciones_generales": "Resumen ejecutivo de la validación"
  }}
}}
```

**IMPORTANTE**: 
- Tu respuesta debe ser ÚNICAMENTE el JSON anterior, sin texto adicional
- NO uses bloques de código markdown (```json o ```)
- NO agregues texto antes o después del JSON
- Responde SOLO con el JSON puro, comenzando con {{ y terminando con }}
- Usa los valores exactos de la normativa para las correcciones
- Si un parámetro es válido, `valor_correcto` = `valor_propuesto`
- Si requiere corrección, `valor_correcto` = valor según normativa
- Incluye siempre las referencias normativas específicas
- Los valores deben ser numéricos, no strings

**ELEMENTOS CRÍTICOS A CONSIDERAR**:
- Tratamiento: {json_lote.get('pot_tratamiento_urbanistico', {}).get('nombre_tra', 'No definido')}
- Tipología: {json_lote.get('pot_tratamiento_urbanistico', {}).get('tipologia', 'No definida')}
- Altura máxima: {json_lote.get('pot_tratamiento_urbanistico', {}).get('altura_max', 'No definida')} pisos
- Antejardín reglamentario: {json_lote.get('pot_antejardin', {}).get('dimension', 'No definido')}m
- Ancho de vía: {json_lote.get('caracteristicas_lote', {}).get('ancho_via', 'No definido')}m
- Predio esquinero: {json_lote.get('caracteristicas_lote', {}).get('esquinero', False)}

**NOTA CRÍTICA SOBRE REDUCCION_LOTE:**
- Si tratamiento es "Renovación Urbana" o "Desarrollo": El parámetro clave es `reduccion_lote`
- Si tratamiento es "Consolidación" o "Mejoramiento Integral": Los parámetros clave son aislamientos individuales
- SIEMPRE: `aislamiento_frontal` debe ser igual al antejardín reglamentario

Procede con la validación detallada."""

        return prompt
    
    def validar_normativa(self, json_lote: Dict[str, Any], input_lote: Dict[str, Any], 
                         modelo: str = "claude-sonnet-4-20250514", 
                         max_tokens: int = 4000) -> Dict[str, Any]:
        """
        Valida los parámetros de edificación contra la normativa urbana
        
        Args:
            json_lote: Información del lote desde la base de datos
            input_lote: Parámetros propuestos para validar
            modelo: Modelo de Claude a usar
            max_tokens: Máximo de tokens en la respuesta
            
        Returns:
            Diccionario con la validación completa
        """
        try:
            # Construir prompt especializado
            prompt = self._construir_prompt_validacion(json_lote, input_lote)
            
            # Hacer llamada a Claude
            message = self.client.messages.create(
                model=modelo,
                max_tokens=max_tokens,
                messages=[
                    {
                        "role": "user",
                        "content": prompt
                    }
                ]
            )
            
            # Extraer respuesta
            respuesta_texto = message.content[0].text
            
            # Limpiar respuesta (remover bloques de código markdown si existen)
            respuesta_limpia = respuesta_texto.strip()
            if respuesta_limpia.startswith('```json'):
                respuesta_limpia = respuesta_limpia[7:]  # Remover ```json
            if respuesta_limpia.startswith('```'):
                respuesta_limpia = respuesta_limpia[3:]   # Remover ```
            if respuesta_limpia.endswith('```'):
                respuesta_limpia = respuesta_limpia[:-3]  # Remover ``` final
            respuesta_limpia = respuesta_limpia.strip()
            
            # Parsear JSON de respuesta
            try:
                validacion_resultado = json.loads(respuesta_limpia)
                return validacion_resultado
            except json.JSONDecodeError as e:
                return {
                    "error": "Error al parsear respuesta JSON",
                    "respuesta_cruda": respuesta_texto,
                    "respuesta_limpia": respuesta_limpia,
                    "error_detalle": str(e)
                }
                
        except Exception as e:
            return {
                "error": "Error en la validación",
                "error_detalle": str(e),
                "json_lote": json_lote,
                "input_lote": input_lote
            }
    

def inicializar_validador(api_key: str) -> ValidadorNormativaUrbana:
    return ValidadorNormativaUrbana(api_key=api_key)

def validar_lote_cached(json_lote_str: str,input_lote_str: str,api_key: str) -> Dict[str, Any]:

    # Convertir strings de vuelta a diccionarios
    json_lote  = json.loads(json_lote_str)
    input_lote = json.loads(input_lote_str)
    
    # Usar validador cacheado
    validador = inicializar_validador(api_key)
    
    # Realizar validación
    return validador.validar_normativa(json_lote, input_lote)

def crear_tabla_validacion(parametros_validados):
    nombres_parametros = {'numero_pisos': 'Número de pisos', 'alturapiso': 'Altura por piso (m)', 'aislamiento_frontal': 'Aislamiento frontal (m)', 'aislamiento_lateral': 'Aislamiento lateral (m)', 'aislamiento_posterior': 'Aislamiento posterior (m)', 'aislamiento_frontal_cara_larga': 'Aislamiento frontal cara larga (m)', 'aislamiento_frontal_cara_corta': 'Aislamiento frontal cara corta (m)', 'aislamiento_lateral_cara_larga': 'Aislamiento lateral cara larga (m)', 'aislamiento_lateral_cara_corta': 'Aislamiento lateral cara corta (m)', 'aislamiento_posterior_cara_larga': 'Aislamiento posterior cara larga (m)', 'aislamiento_posterior_cara_corta': 'Aislamiento posterior cara corta (m)', 'voladizo_frontal': 'Voladizo frontal (m)', 'voladizo_lateral': 'Voladizo lateral (m)', 'voladizo_posterior': 'Voladizo posterior (m)', 'voladizo_frontal_cara_larga': 'Voladizo frontal cara larga (m)', 'voladizo_frontal_cara_corta': 'Voladizo frontal cara corta (m)', 'voladizo_lateral_cara_larga': 'Voladizo lateral cara larga (m)', 'voladizo_lateral_cara_corta': 'Voladizo lateral cara corta (m)', 'voladizo_posterior_cara_larga': 'Voladizo posterior cara larga (m)', 'voladizo_posterior_cara_corta': 'Voladizo posterior cara corta (m)', 'reduccion_poligono': 'Reducción del polígono (%)', 'escalones_config': 'Configuración de escalones'}
    tabla  = "| Parámetro | Valor | Norma Aplicable | Observaciones |\n"
    tabla += "|-----------|----------------|-----------------|---------------|\n"
    
    # Iterar sobre cada parámetro validado
    for key, validacion in parametros_validados.items():
        if key in nombres_parametros:
            nombre_parametro = nombres_parametros[key]
            valor_correcto = validacion.get('valor_correcto', '')
            norma_aplicable = validacion.get('norma_aplicable', '')
            observaciones = validacion.get('observaciones', '')
            
            tabla += f"| {nombre_parametro} | {valor_correcto} | {norma_aplicable} | {observaciones} |\n"
    
    return tabla

def main(inputvar={}):
    
    pot_json_lote = inputvar.get('pot_lote',{})
    input_lote    = inputvar.get('input_lote',{})
    api_key       = os.getenv('ANTHROPIC_API_KEY')
    
    if not api_key:
        return {
            "error": "API key no configurada",
            "error_detalle": "Configura ANTHROPIC_API_KEY en .env o pásala como parámetro"
        }
    
    try:
        # Usar validación cacheada para eficiencia
        resultado = validar_lote_cached(
            json.dumps(pot_json_lote, sort_keys=True),
            json.dumps(input_lote, sort_keys=True),
            api_key
        )
        
        try: 
            parametros_validados = resultado.get('parametros_validados',None)
            if parametros_validados is not None:
                resultado.update({'tabla_resumen':crear_tabla_validacion(parametros_validados)})
        except: pass
        return resultado
        
    except Exception as e:
        return {
            "error": "Error en validación",
            "error_detalle": str(e)
        }