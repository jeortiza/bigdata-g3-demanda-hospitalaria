# Multi-Horizon Hospital Demand Forecasting for Peru's National Health Insurance System Using Big Data Integration, Epidemiological Pattern Mining, and Ensemble Machine Learning: A Predictive Analytics Framework for Healthcare Resource Optimization

> **Título en español:** Pronóstico Multi-Horizonte de Demanda Hospitalaria en EsSalud mediante Big Data, Minería de Patrones Epidemiológicos y Machine Learning Ensemble: Un Marco de Analítica Predictiva para la Optimización de Recursos de Salud  
> **Curso:** Big Data DD283 | Universidad Autónoma del Perú | 2026-1  
> **Grupo:** 7 | **Sector:** Salud Pública / Healthcare Analytics / Gobierno

---

## Equipo

| Nombre | GitHub | Rol |
|--------|--------|-----|
| [Apellido Nombre 1] | [@usuario1](https://github.com/usuario1) | Líder + Arquitectura de Datos |
| [Apellido Nombre 2] | [@usuario2](https://github.com/usuario2) | Ingeniería de Datos (PySpark + ETL) |
| [Apellido Nombre 3] | [@usuario3](https://github.com/usuario3) | ML Predictivo (Prophet + Ensemble) |
| [Apellido Nombre 4] | [@usuario4](https://github.com/usuario4) | Dashboard Ejecutivo + Epidemiología |

---

## 1. Caso de Negocio

### Contexto Institucional
**EsSalud** (Seguro Social de Salud del Perú) es la institución de salud más grande del país:
- **11.4 millones** de asegurados activos (35% de la población peruana)
- **400+ establecimientos** de salud a nivel nacional (hospitales, policlínicos, postas)
- **42 millones** de atenciones anuales (consultas externas + emergencias + hospitalizaciones)
- Presupuesto anual: S/ 12,000 millones (2024)

A pesar de su escala, EsSalud opera en su mayoría de forma **reactiva**: los recursos (camas, médicos, medicamentos, equipos) se asignan con base en el historial del año anterior, sin modelos predictivos que anticipen variaciones estacionales, epidemiológicas o demográficas.

### El Problema

> La gestión hospitalaria en EsSalud enfrenta un **desbalance crítico entre oferta y demanda** de servicios de salud que genera:
> - Colapso de emergencias en meses pico (enero-marzo: dengue; julio: influenza)
> - Camas hospitalarias ociosas en meses de baja demanda
> - Listas de espera de 30-180 días en especialidades críticas (cardiología, oncología, traumatología)
> - Rotación de personal sin datos que justifiquen los turnos adicionales
> - Desabastecimiento de medicamentos en momentos de alta demanda epidemiológica

**Cifras que evidencian el problema:**
| Indicador | Valor | Fuente |
|-----------|-------|--------|
| Tiempo promedio de espera emergencias | 4.2 horas | SuSalud 2023 |
| Ocupación de camas en enero (pico dengue) | 112% (sobreocupado) | EsSalud 2024 |
| Ocupación de camas en agosto (mes bajo) | 67% (subutilizado) | EsSalud 2024 |
| Médicos especialistas faltantes | 8,200 a nivel nacional | MINSA 2023 |
| Costo de una cama hospitalaria ociosa/día | S/ 450 | EsSalud interno |

### El Reto de Big Data
> Diseñar un sistema de **pronóstico de demanda hospitalaria** que integre datos históricos de atenciones, variables epidemiológicas (dengue, influenza, COVID-19), climáticas (lluvia, temperatura) y demográficas para **predecir con 4 semanas de anticipación** la demanda por especialidad, establecimiento y tipo de atención en la red EsSalud.

**¿Por qué Big Data?**
| V | Justificación en EsSalud |
|---|--------------------------|
| **Volumen** | 42M atenciones/año × 3 años histórico = 126M+ registros |
| **Velocidad** | Actualización diaria de camas disponibles, alertas epidemiológicas semanales |
| **Variedad** | HIS (Sistema de Información en Salud, CSV), SENAMHI clima (API), MINSA epidemiología (JSON), RENIEC demográfico (CSV) |
| **Veracidad** | Diagnósticos inconsistentes (CIE-10 mal asignado), duplicados por re-consulta, datos faltantes |
| **Valor** | Reducir 20% sobreocupación = ahorro S/ 180M/año en gestión de crisis |

---

## 2. Objetivos del Proyecto

### Objetivo General
Desarrollar una plataforma Big Data que integre datos históricos de atenciones EsSalud con fuentes epidemiológicas y climáticas para generar predicciones de demanda hospitalaria con horizonte de 4 semanas, precisión MAPE < 15%, y un dashboard ejecutivo para directores de establecimientos y la Gerencia Central.

### Objetivos Específicos
1. Diseñar y generar un **dataset sintético realista** de 500,000 atenciones médicas (3 años, múltiples establecimientos Lima)
2. Construir el **pipeline ETL medallion** (Bronze→Silver→Gold) con PySpark para integrar 4 fuentes de datos
3. Implementar **análisis epidemiológico** Big Data: correlación dengue-clima, patrones estacionales de influenza, análisis de brotes
4. Entrenar modelos de **forecasting multi-horizonte**: Prophet (series temporales por especialidad) + Random Forest (factores de riesgo)
5. Almacenar en **MongoDB Atlas** con esquema optimizado para consultas de gestión hospitalaria
6. Implementar análisis geoespacial de la **distribución de demanda** en Lima Metropolitana
7. Construir **dashboard ejecutivo** con KPIs hospitalarios: ocupación, IRAB, productividad médica, lista de espera proyectada
8. Evaluar el impacto: ¿cuántas camas se optimizarían con una predicción correcta?

---

## 3. Solución Propuesta

### Marco Metodológico

```
┌─────────────────────────────────────────────────────────────────────┐
│            MARCO CONCEPTUAL: DEMANDA HOSPITALARIA                   │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  DEMANDA = f(Epidemiología, Clima, Demografía, Estacionalidad)      │
│                                                                     │
│  Epidemiología: brotes dengue, influenza, COVID, gastroenteritis    │
│  Clima:         temperatura, lluvia, humedad (SENAMHI Lima)         │
│  Demografía:    edad, densidad poblacional por zona, PEA            │
│  Estacionalidad: feriados, inicio escolar, temporada alta           │
│                                                                     │
│  OFERTA = Camas × Ocupación + Médicos × Productividad              │
│                                                                     │
│  BRECHA = DEMANDA predicha - OFERTA disponible                      │
│  → Si BRECHA > 0: activar protocolo de contingencia                 │
│  → Si BRECHA < -15%: redistribuir recursos a otro establecimiento   │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 4. Arquitectura del Sistema

```
┌──────────────────────────────────────────────────────────────────────────┐
│              ARQUITECTURA BIG DATA — PREDICCION DEMANDA ESSALUD           │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  FUENTES DE DATOS (4 fuentes integradas)                                 │
│                                                                          │
│  ┌─────────────────┐ ┌─────────────────┐ ┌──────────────┐ ┌──────────┐  │
│  │ HIS EsSalud     │ │ SENAMHI         │ │ MINSA/CDC    │ │ RENIEC   │  │
│  │ Histórico       │ │ Estación Lima   │ │ Boletín      │ │ Población│  │
│  │ atenciones      │ │ temperatura     │ │ Epidemio.    │ │ por zona │  │
│  │ 3 años / 500K  │ │ lluvia, humedad  │ │ dengue, flu  │ │ y edad   │  │
│  │ (CSV simulado)  │ │ (API / CSV)     │ │ (PDF→JSON)   │ │ (CSV)    │  │
│  └────────┬────────┘ └────────┬────────┘ └──────┬───────┘ └────┬─────┘  │
│           └──────────────────┴────────────────┴───────────────┘        │
│                                      │                                   │
│  ┌───────────────────────────────────▼──────────────────────────────┐   │
│  │                 PYSPARK ETL — ARQUITECTURA MEDALLION             │   │
│  │                                                                  │   │
│  │  BRONZE  → datos crudos exactamente como llegan                 │   │
│  │            (4 fuentes en formato original)                       │   │
│  │                                                                  │   │
│  │  SILVER  → limpieza + estandarización + join de fuentes          │   │
│  │            diagnósticos → CIE-10 normalizado                     │   │
│  │            edad → grupos etarios (0-5, 6-17, 18-59, 60+)        │   │
│  │            clima → variables rezagadas 7/14/21 días              │   │
│  │            deduplicación + imputación de nulos                   │   │
│  │                                                                  │   │
│  │  GOLD    → tablas analíticas para ML y dashboard                 │   │
│  │            demanda_semanal_por_especialidad                      │   │
│  │            correlacion_clima_enfermedad                          │   │
│  │            prediccion_proximas_4_semanas                         │   │
│  │            brechas_oferta_demanda_por_establecimiento            │   │
│  └───────────────────────────────────┬──────────────────────────────┘   │
│                                      │                                   │
│  ┌────────────────────────────────────────────────────────────────────┐  │
│  │                    CAPA DE ANÁLISIS Y ML                          │  │
│  │                                                                   │  │
│  │  ┌──────────────────┐  ┌──────────────────┐  ┌────────────────┐  │  │
│  │  │ ANÁLISIS         │  │ FORECASTING      │  │ CLASIFICACION  │  │  │
│  │  │ EPIDEMIOLÓGICO   │  │ MULTI-HORIZONTE  │  │ RIESGO         │  │  │
│  │  │                  │  │                  │  │ EPIDÉMICO      │  │  │
│  │  │ Correlación      │  │ Prophet por      │  │                │  │  │
│  │  │ clima-dengue     │  │ especialidad     │  │ Random Forest  │  │  │
│  │  │ Detección brotes │  │ ARIMA estacional │  │ ¿Semana de     │  │  │
│  │  │ (Isolation       │  │ (SARIMAX)        │  │  brote?        │  │  │
│  │  │  Forest)         │  │ Horizonte: 4 sem │  │  Si/No + proba │  │  │
│  │  │ Mapa calor Lima  │  │ MAPE objetivo    │  │                │  │  │
│  │  │ por enfermedad   │  │ < 15%            │  │                │  │  │
│  │  └──────────────────┘  └──────────────────┘  └────────────────┘  │  │
│  └────────────────────────────────────────────────────────────────────┘  │
│                                      │                                   │
│  ┌───────────────────────────────────▼──────────────────────────────┐   │
│  │   MongoDB Atlas (series tiempo) + Parquet Gold (análisis batch)  │   │
│  └───────────────────────────────────┬──────────────────────────────┘   │
│                                      │                                   │
│  ┌───────────────────────────────────▼──────────────────────────────┐   │
│  │              DASHBOARD EJECUTIVO — GERENCIA ESSALUD              │   │
│  │                                                                  │   │
│  │  Vista 1 — Director de Hospital:                                 │   │
│  │    Ocupación camas hoy vs predicción próximas 4 semanas          │   │
│  │    Alertas: "Se proyecta pico dengue semana 3 marzo (+35%)"      │   │
│  │                                                                  │   │
│  │  Vista 2 — Gerente de Red Asistencial:                           │   │
│  │    Mapa Lima: establecimientos con demanda proyectada alta/baja  │   │
│  │    Redistribución recomendada de recursos                        │   │
│  │                                                                  │   │
│  │  Vista 3 — Epidemiólogo:                                         │   │
│  │    Correlación clima-enfermedades | Vigilancia brotes            │   │
│  │    Comparativo: semana epidemiológica actual vs histórico         │   │
│  └──────────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## 5. Dataset Sintético — Modelo de Datos

### 5.1 Tabla principal: `atenciones_essalud` (500,000 registros, 3 años)

```python
{
  "atencion_id": "ATN-2022-0001234",
  "fecha_atencion": "2022-03-14",
  "semana_epidemiologica": 11,           # semana CDC 1-52
  "establecimiento_id": "HOSP-001",
  "establecimiento_nombre": "Hospital Almenara",
  "red_asistencial": "Lima Metropolitana",
  "tipo_atencion": "emergencia",          # consulta_externa | emergencia | hospitalizacion
  "especialidad": "Medicina Interna",
  "medico_id": "MED-2345",
  "paciente_edad": 67,
  "paciente_grupo_etario": "60+",
  "paciente_sexo": "M",
  "paciente_distrito": "La Victoria",
  "diagnostico_cie10": "A90",             # CIE-10: A90=Dengue, J11=Influenza
  "diagnostico_nombre": "Dengue",
  "diagnostico_grupo": "Enfermedades_infecciosas",
  "dias_estancia": 4,                     # hospitalizacion, null si ambulatorio
  "camas_disponibles_dia": 145,
  "camas_ocupadas_dia": 162,              # puede superar disponibles (colchoneta)
  "tasa_ocupacion": 1.12,
  "resultado_atencion": "alta",           # alta | fallecido | transferido | fuga
  "costo_atencion_soles": 850,
  "lista_espera_dias": 0,                 # 0 si atendido el mismo dia
  "temperatura_lima_max": 29.5,           # SENAMHI ese dia
  "precipitacion_mm": 0.2,
  "indice_humedad": 78,
  "casos_dengue_semana": 234,            # MINSA boletín epidemiológico
  "alerta_epidemiologica": False          # True si brote declarado
}
```

### 5.2 Tabla de oferta: `oferta_hospitalaria` (semanal por establecimiento)

```python
{
  "semana": "2022-W11",
  "establecimiento_id": "HOSP-001",
  "camas_totales": 450,
  "camas_disponibles": 145,
  "medicos_activos": 38,
  "enfermeras_activas": 120,
  "consultas_programadas": 1840,
  "consultas_ejecutadas": 2156,           # real vs programado
  "productividad_medica": 56.7,           # consultas/médico/semana
  "lista_espera_especialidades": {
    "Cardiologia": 45,
    "Oncologia": 120,
    "Traumatologia": 67
  }
}
```

---

## 6. Stack Tecnológico (100% Gratuito)

| Capa | Tecnología | Plataforma Gratuita | Para qué |
|------|-----------|---------------------|---------|
| Generación datos | Faker + Python | Local / Colab | 500K atenciones sintéticas realistas |
| ETL Medallion | PySpark 3.5 | Databricks Community | Bronze→Silver→Gold |
| Series Temporales | Prophet (Meta) + SARIMAX | Google Colab | Predicción demanda semanal |
| Epidemiología ML | Isolation Forest + Random Forest | Google Colab | Detección brotes + clasificación riesgo |
| Correlación | PySpark + scipy.stats | Databricks Community | Lag analysis clima-enfermedad |
| Mapas | Folium + Plotly Mapbox | pip install | Mapa calor Lima establecimientos |
| NoSQL | MongoDB Atlas M0 | Atlas Free | Series temporales, predicciones |
| Dashboard | Streamlit | Streamlit Cloud Free | Portal gerencial |
| Clima | SENAMHI datos abiertos | data.gob.pe | Variables meteorológicas Lima |
| Epidemiología | MINSA boletín | www.minsa.gob.pe/noticias | Casos dengue/influenza |
| Calidad datos | Great Expectations | pip install | Validar registros HIS |

---

## 7. Metodología Detallada

### 7.1 Análisis Epidemiológico con Big Data

```python
# Hipótesis a validar con datos:
# H1: Temperatura > 28°C + humedad > 75% → pico dengue en 2-3 semanas
# H2: Temperatura < 16°C → pico influenza en 1-2 semanas
# H3: Inicio escolar (marzo, agosto) → pico enfermedades respiratorias pediátricas
# H4: Cierre de año fiscal (diciembre) → pico estrés/cardiovascular
# H5: Feriados largos → pico emergencias traumatológicas (accidentes)

# Análisis de correlación con rezago (lag analysis):
# cor(casos_dengue[semana_t], temperatura[semana_t-k]) para k = 1,2,3,4 semanas
# Identificar k* = lag óptimo (mayor correlación)
```

### 7.2 Forecasting Multi-Horizonte con Prophet

```python
from prophet import Prophet

# Modelo por especialidad × establecimiento
# Ejemplo: Emergencias Hospital Almenara
modelo = Prophet(
    yearly_seasonality=True,    # patrones anuales (dengue en verano)
    weekly_seasonality=True,    # patrones semanales (más emergencias lunes)
    changepoint_prior_scale=0.1 # flexibilidad ante cambios bruscos (COVID)
)

# Variables exógenas (regressors):
modelo.add_regressor('temperatura_max_lag2')   # temperatura con 2 semanas de rezago
modelo.add_regressor('casos_dengue_lag1')       # casos dengue semana anterior
modelo.add_regressor('es_inicio_escolar')       # dummy: 1 en semanas de inicio escolar
modelo.add_regressor('es_feriado_largo')        # dummy: semanas con feriado largo

# Predicción: próximas 4 semanas con intervalos de confianza 80%/95%
forecast = modelo.predict(future_df)
# Output por semana: yhat, yhat_lower, yhat_upper, trend, yearly, weekly
```

### 7.3 Detección de Brotes — Isolation Forest

```python
from sklearn.ensemble import IsolationForest

# Features para detección de brotes:
# - atenciones_semana (vs promedio histórico mismo mes)
# - variacion_porcentual_vs_semana_anterior
# - casos_cie10_grupo (infecciosas, respiratorias, digestivas)
# - temperatura_promedio_semana
# - precipitacion_total_semana

# Isolation Forest detecta semanas con comportamiento anómalo:
# contamination=0.05 → ~5% de semanas son "anómalas" (posible brote)
```

### 7.4 KPIs de Gestión Hospitalaria

| KPI | Definición | Meta EsSalud |
|-----|-----------|-------------|
| **Tasa de Ocupación** | Camas ocupadas / Camas disponibles × 100 | 85-90% |
| **Índice de Rotación** | Egresos / Camas disponibles por período | > 40 egresos/cama/año |
| **Promedio de Estadía** | Suma días estancia / Egresos | < 5 días (promedio) |
| **Rendimiento Médico** | Consultas atendidas / Horas médico programadas | > 4 consultas/hora |
| **Productividad Cama** | Días cama ocupados / Días cama disponibles | > 0.85 |
| **Lista Espera (días)** | Días hasta primera consulta disponible | < 30 días (especialidad) |
| **Mortalidad Intrahospitalaria** | Fallecidos / Egresos × 1,000 | < 25 × 1,000 egresos |
| **IRAB** | Índice de Rechazo de Atención por Baja Capacidad | < 2% |

---

## 8. Plan de Entregables por Semana

| Semana | Entregable | Notebook | Criterio de éxito |
|--------|-----------|----------|-------------------|
| **S1** | Dataset 500K + EDA hospitales Lima | `01_EDA_atenciones.ipynb` | Distribución por especialidad, estacionalidad, análisis CIE-10 top 20 |
| **S2** | Pipeline PySpark Medallion | `02_pipeline_medallion.ipynb` | Bronze→Silver→Gold; join 4 fuentes; CIE-10 normalizado; Parquet Gold |
| **S3** | MongoDB Atlas + queries gerenciales | `03_mongodb_kpis.ipynb` | Series tiempo de ocupación, top 10 diagnósticos, alertas por establecimiento |
| **S4** | **EP: Arquitectura + EDA + Correlación clima-enfermedad** | `presentacion_EP_grupo7.pdf` | 60% proyecto: lag analysis validado, correlaciones significativas p<0.05 |
| **S5** | Spark SQL + análisis epidemiológico | `04_spark_sql_epidemio.ipynb` | Patrones estacionales, semanas epidemiológicas, hotspots por distrito |
| **S6** | Prophet + Isolation Forest | `05_forecasting_brotes.ipynb` | MAPE < 15% en 4 semanas; F1 > 0.75 en detección de brotes |
| **S7** | Scraping MINSA + Great Expectations | `06_scraping_calidad.ipynb` | Datos MINSA integrados, 8 validaciones Great Expectations |
| **S8** | **EF: Dashboard ejecutivo + resultados** | `presentacion_EF_grupo7.pdf` | Demo en vivo para "Director EsSalud", impacto cuantificado |

### Semana 4 — Evaluación Parcial EP (60% del proyecto)

Simular presentación ante el **Director de la Red Asistencial Lima** (20 minutos):

1. **El problema** — Con datos reales MINSA/SuSalud: ¿cuánto colapsa EsSalud en temporada dengue?
2. **El dataset** — 500K atenciones sintéticas: distribución, top diagnósticos, estacionalidad visible
3. **Arquitectura Medallion** — Demo: ticket HIS crudo → Gold con KPIs calculados
4. **Correlación clima-dengue** — Gráfico lag analysis: "temperatura con 14 días de rezago correlaciona 0.78 con casos dengue"
5. **MongoDB Atlas** — Query en vivo: "¿cuál fue la semana más crítica del año pasado?"
6. **Proyección preliminar** — Gráfico Prophet básico para 1 especialidad

### Semana 8 — Evaluación Final EF (proyecto completo)

Presentación de 25 minutos + demo interactiva para la **Gerencia Central de EsSalud**:

1. **Sistema end-to-end** — Pipeline completo desde datos crudos hasta predicción
2. **Predicciones por especialidad** — "Emergencias Almenara: +38% demanda semana del 15 enero"
3. **Mapa de brotes** — Lima con calor de demanda proyectada por establecimiento
4. **Cuantificación de impacto** — "Con este sistema, EsSalud podría evitar 3 situaciones de colapso por año = S/ 2.1M ahorrados"
5. **Recomendaciones de política pública** — Basadas en datos, no en intuición
6. **Draft paper Scopus** — Abstract 250 palabras en inglés + sección de metodología

---

## 9. Estructura del Repositorio

```
grupo7-demanda-hospitalaria-bd/
│
├── README.md                              ← guía completa del proyecto
│
├── notebooks/
│   ├── 01_EDA_atenciones.ipynb            ← S1: exploración 500K atenciones
│   ├── 02_pipeline_medallion.ipynb        ← S2: Bronze→Silver→Gold PySpark
│   ├── 03_mongodb_kpis.ipynb              ← S3: NoSQL + KPIs hospitalarios
│   ├── 04_spark_sql_epidemio.ipynb        ← S5: SQL avanzado + epidemiología
│   ├── 05_forecasting_brotes.ipynb        ← S6: Prophet + Isolation Forest
│   ├── 06_scraping_calidad.ipynb          ← S7: MINSA scraping + GX
│   └── 07_dashboard_ejecutivo.ipynb       ← S8: dashboard gerencial
│
├── src/
│   ├── generador_atenciones.py            ← Faker: 500K atenciones sintéticas
│   ├── pipeline_medallion.py             ← PySpark ETL 4 fuentes
│   ├── correlacion_clima_epidemia.py     ← lag analysis temperatura-dengue
│   ├── forecasting_prophet.py            ← modelos Prophet por especialidad
│   ├── deteccion_brotes.py               ← Isolation Forest semanas anómalas
│   └── kpis_hospitalarios.py             ← cálculo KPIs estándares MINSA
│
├── data/
│   ├── README_datos.md                   ← instrucciones para generar datos
│   ├── raw/                              ← datos crudos (NO en GitHub)
│   ├── processed/                        ← Gold layer (NO en GitHub)
│   └── sample/
│       ├── atenciones_muestra_200.csv    ← 200 registros ejemplo (en GitHub)
│       ├── cie10_codigos.json            ← catálogo CIE-10 principales
│       ├── establecimientos_essalud.json ← 20 establecimientos Lima mock
│       └── semanas_epidemiologicas.csv   ← calendario epidemiológico 2022-2024
│
├── docs/
│   ├── arquitectura_demanda_hospitalaria.png
│   ├── diccionario_datos_his.md          ← definición de campos HIS
│   ├── kpis_hospitalarios_minsa.md       ← KPIs estándar MINSA/EsSalud
│   ├── marco_conceptual_demanda.md       ← teoría de demanda sanitaria
│   ├── presentacion_EP_semana4.pdf
│   └── presentacion_EF_semana8.pdf
│
├── .gitignore
└── requirements.txt
```

---

## 10. Cómo Usar Este Repositorio

### Configuración inicial (una sola vez por integrante)

```bash
# Paso 1: Fork del repo en GitHub
# → Ir a: github.com/RubenCarty/grupo7-demanda-hospitalaria-bd
# → Clic "Fork" → "Create fork"
# → Ahora tienes: github.com/TU-USUARIO/grupo7-demanda-hospitalaria-bd

# Paso 2: Clonar TU fork
git clone https://github.com/TU-USUARIO/grupo7-demanda-hospitalaria-bd.git
cd grupo7-demanda-hospitalaria-bd

# Paso 3: Conectar con el repo del líder del grupo
git remote add upstream https://github.com/LIDER-GRUPO/grupo7-demanda-hospitalaria-bd.git

# Verificar:
git remote -v
# origin   → tu fork (donde subes tu trabajo)
# upstream → repo del líder (de donde sincronizas)

# Paso 4: Instalar dependencias
pip install -r requirements.txt

# Paso 5: Generar el dataset sintético
python src/generador_atenciones.py
# → Genera: data/raw/atenciones_essalud.csv (~280MB, 500K registros)
# → Genera: data/raw/clima_lima_2022_2024.csv (datos SENAMHI simulados)
# → Genera: data/raw/epidemio_minsa_2022_2024.csv (boletines epidemiológicos)
```

### Flujo semanal de trabajo

```bash
# INICIO DE SEMANA — sincronizar con el grupo
git checkout main
git pull upstream main           # traer avances de los compañeros
git push origin main             # actualizar tu fork

# Crear tu rama de trabajo
git checkout -b feature/forecasting-prophet-tuapellido
# Formato: feature/descripcion-tuapellido

# Trabajar en el notebook...
# jupyter notebook notebooks/05_forecasting_brotes.ipynb

# Guardar avance (hacerlo frecuentemente)
git add notebooks/05_forecasting_brotes.ipynb src/forecasting_prophet.py
git commit -m "feat: Prophet MAPE 12% en Emergencias Almenara - TuApellido"

# Subir tu rama
git push origin feature/forecasting-prophet-tuapellido
# → En GitHub: crear Pull Request → líder revisa → merge a main
```

### Después de que el líder aprueba un PR

```bash
# Descargar los cambios aprobados a tu local
git checkout main
git pull upstream main
git push origin main    # sincronizar tu fork también
```

### Comandos útiles para este proyecto

```bash
# Ver el progreso del grupo en el tiempo
git log --oneline --graph --all --since="2026-01-01"

# Comparar tu trabajo con la rama main
git diff main notebooks/05_forecasting_brotes.ipynb

# Si necesitas deshacer tu último commit (sin perder trabajo)
git reset HEAD~1 --soft

# Ver qué archivos cambiaron en el último commit del líder
git show --name-only HEAD
```

---

## 11. Recursos y Referencias

### Datos reales disponibles (públicos)
- [MINSA — Sala Situacional Dengue](https://www.dge.gob.pe/sala-situacional-dengue/)
- [MINSA — Boletín Epidemiológico Semanal](https://www.dge.gob.pe/portal/index.php?option=com_content&view=article&id=598)
- [SENAMHI — Datos Históricos Lima](https://www.senamhi.gob.pe/?&p=data-historica)
- [SuSalud — Datos de Establecimientos](https://www.susalud.gob.pe/)
- [SIGA MEF — Datos Presupuestales EsSalud](https://apps.mef.gob.pe/siaf-siga/)
- [data.gob.pe — Datos MINSA Abiertos](https://www.datosabiertos.gob.pe/organization/ministerio-de-salud-minsa)
- [OPS PAHO — Indicadores Salud Perú](https://www.paho.org/data/index.php/es/)

### Dataset UCI para complementar
- [Hospital Readmission Dataset — UCI](https://archive.ics.uci.edu/dataset/296/diabetes+130-us+hospitals+for+years+1999-2008)
- [Length of Hospital Stay — Kaggle](https://www.kaggle.com/datasets/nehaprabhavalkar/av-healthcare-analytics-ii)

### Repositorios GitHub similares
- [Hospital Demand Forecasting — Prophet](https://github.com/facebook/prophet/tree/main/notebooks)
- [Healthcare Analytics PySpark](https://github.com/cartershanklin/pyspark-cheatsheet)
- [Dengue Forecasting — DrivenData](https://github.com/drivendataorg/dengue-fever-prediction)
- [Epidemiological Surveillance ML](https://github.com/cdcepi/FluSight-ensemble)
- [Time Series Healthcare](https://github.com/awslabs/gluonts)
- [Folium Healthcare Maps](https://github.com/python-visualization/folium/tree/main/examples)
- [Isolation Forest Anomaly](https://github.com/scikit-learn/scikit-learn/blob/main/sklearn/ensemble/_iforest.py)
- [SARIMAX Time Series](https://github.com/statsmodels/statsmodels)

### Papers clave para artículo Scopus Q1

| Paper | Revista | Factor Impacto | Relevancia |
|-------|---------|---------------|------------|
| Wanigatunga et al. (2022) — "Forecasting Emergency Department Crowding Using Big Data" | BMC Health Services Research | Q1 | Predicción demanda ED |
| Rostami-Tabar & Farnoosh (2021) — "To aggregate or not to aggregate: Forecasting of finite autocorrelated demand" | Journal of Operational Research | Q1 | Metodología forecasting |
| García-Marcos et al. (2023) — "Machine Learning for Epidemiological Surveillance: A Systematic Review" | Int. Journal of Epidemiology | Q1 | NLP + ML epidemiología |
| Rustam et al. (2020) — "COVID-19 Hospital Demand Forecasting Using ML Approaches" | IEEE Access | Q1 | Arquitectura similar |
| Chae et al. (2018) — "Predicting Infectious Disease Using Deep Learning and Big Data" | Int. Journal of Environmental Research | Q1 | Clima + epidemiología |

---

## 12. Consideraciones Éticas y de Privacidad

Este proyecto maneja datos sintéticos de salud. En un escenario real, se deben considerar:

### Marco Legal Perú
- **Ley 29733** (Protección de Datos Personales): datos de salud son "datos sensibles" → categoría especial de protección
- **Ley 26842** (Ley General de Salud): confidencialidad de la historia clínica
- **Resolución Ministerial N° 214-2018/MINSA**: lineamientos de gobierno de datos en salud

### Controles requeridos en producción
```
✅ Anonimización: paciente_id → hash irreversible (SHA-256)
✅ Pseudonimización: diagnóstico individual → solo agregados por semana/área
✅ Control de acceso por rol: médico / director / epidemiólogo / TI
✅ Auditoría: log de quién consulta qué dato y cuándo
✅ Retención: datos clínicos individuales: mínimo 5 años (normativa MINSA)
✅ Consentimiento: para uso analítico, informar al paciente
```

### En este proyecto (datos sintéticos)
- Todos los registros son **100% sintéticos** — ningún paciente real
- Los nombres y DNI son generados por Faker
- Los patrones estadísticos son realistas pero no provienen de bases de datos reales

---

## 13. Catálogo de Diagnósticos CIE-10 Principales (para el generador)

```python
DIAGNOSTICOS_ESSALUD = {
    # Enfermedades infecciosas (pico enero-marzo)
    "A90": "Dengue (sin signo de alarma)",
    "A91": "Dengue hemorrágico",
    "J11": "Influenza",
    "A09": "Gastroenteritis de presunto origen infeccioso",
    # Respiratorias (pico junio-agosto)
    "J06": "Infección aguda vías respiratorias superiores",
    "J18": "Neumonía no especificada",
    "J45": "Asma",
    # Cardiovascular
    "I10": "Hipertensión esencial",
    "I21": "Infarto agudo de miocardio",
    "I50": "Insuficiencia cardiaca",
    # Crónicas (distribución uniforme)
    "E11": "Diabetes mellitus tipo 2",
    "N18": "Enfermedad renal crónica",
    "C34": "Tumor maligno bronquios/pulmón",
    # Traumatología (pico feriados)
    "S00-S99": "Traumatismos",
    "M54": "Dorsalgia",
    # Maternidad
    "O80": "Parto espontáneo único",
    "O21": "Vómitos excesivos en embarazo",
}
```

---

## 14. Tracker de Progreso del Grupo

| Semana | Entregable | Responsable | Estado | Link PR |
|--------|-----------|-------------|--------|---------|
| S1 | Dataset 500K + EDA | [Nombre] | ⬜ Pendiente | — |
| S2 | Pipeline Medallion | [Nombre] | ⬜ Pendiente | — |
| S3 | MongoDB Atlas KPIs | [Nombre] | ⬜ Pendiente | — |
| S4 | **Sustentación EP** | Todos | ⬜ Pendiente | — |
| S5 | Spark SQL Epidemiología | [Nombre] | ⬜ Pendiente | — |
| S6 | Prophet + Brotes | [Nombre] | ⬜ Pendiente | — |
| S7 | Scraping + Calidad | [Nombre] | ⬜ Pendiente | — |
| S8 | **Sustentación EF** | Todos | ⬜ Pendiente | — |

---

## 15. Checklist de Entrega Final (Semana 8)

**Repositorio:**
- [ ] README actualizado con resultados reales del grupo
- [ ] Los 7 notebooks ejecutados con outputs visibles
- [ ] `docs/arquitectura_demanda_hospitalaria.png` — diagrama final
- [ ] `docs/presentacion_EF_semana8.pdf`

**Resultados mínimos:**
- [ ] 500K atenciones generadas con patrones estacionales realistas
- [ ] Correlación clima-dengue validada estadísticamente (p-value < 0.05)
- [ ] Predicción Prophet con MAPE < 15% para al menos 3 especialidades
- [ ] Al menos 5 semanas de brote correctamente identificadas (Isolation Forest)
- [ ] Dashboard con mínimo 7 KPIs hospitalarios funcionales
- [ ] Mapa Lima con demanda proyectada por establecimiento
- [ ] Abstract paper Scopus redactado (250 palabras, inglés)

---

*Big Data DD283 | Universidad Autónoma del Perú | 2026-1*  
*Dudas: abrir un Issue en este repositorio | Docente: github.com/RubenCarty*
