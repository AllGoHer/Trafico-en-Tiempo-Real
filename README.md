# 🚦Analisis del Trafico-en-Tiempo-Real

![image](https://github.com/user-attachments/assets/83557ea5-ad23-4092-897a-ece9f663089e)

________________________________________________________________________________________________________________________________________________________________________________________________________________
## 🚀 Arquitectura Lakehouse de Streaming en Tiempo Real: Tráfico Vehicular
________________________________________________________________________________________________________________________________________________________________________________________________________________

Una plataforma de análisis de tráfico en tiempo real de producción, desarrollada con Apache Kafka, PySpark Structured Streaming, Delta Lake, Hive Metastore y Power BI.

El proyecto simula eventos de tráfico continuos generados por vehículos que circulan por múltiples zonas urbanas y carreteras. Los datos en flujo continuo se recogen, validan, transforman, enriquecen y modelan en conjuntos de datos listos para el análisis mediante una arquitectura moderna de tipo «Lakehouse».

**Enfoque:** El proyecto simula la caótica realidad del mundo real ingestando un flujo contaminado con datos nulos, tipados incorrectamente, fuera de tiempo y JSONs corruptos, aplicando tolerancia a fallos estricta y patrones de diseño empresariales.

![Image](https://github.com/user-attachments/assets/8d9480d3-3790-4677-8a12-0ee6d19b9396)  ![Image](https://github.com/user-attachments/assets/2d66f89a-bca0-454d-b1d3-1366e0702a8b)  ![Image](https://github.com/user-attachments/assets/89552d90-3caf-4c16-b6ba-b3e52b78d91d)  ![Image](https://github.com/user-attachments/assets/fce18da2-78eb-4470-a27b-ceb93f706fce)

________________________________________________________________________________________________________________________________________________________________________________________________________________
## 🎯 Problema Empresarial
________________________________________________________________________________________________________________________________________________________________________________________________________________

Los sistemas de monitorización del tráfico generan miles de eventos cada segundo.

Estos eventos suelen contener:

* Valores que faltan
* Registros duplicados
* Mensajes dañados
* Marcas de tiempo no válidas
* Problemas de evolución del esquema
* Eventos desordenados

El objetivo de este proyecto es crear una plataforma de datos en tiempo real escalable capaz de:

* Ingerir datos de tráfico en streaming.
* Gestionar datos de baja calidad.
* Procesar eventos casi en tiempo real.
* Crear conjuntos de datos analíticos.
* Dar soporte a paneles de BI y a la elaboración de informes operativos.

________________________________________________________________________________________________________________________________________________________________________________________________________________
## 🎯 Valor empresarial y capacidades técnicas
________________________________________________________________________________________________________________________________________________________________________________________________________________

**Valor Aportado**

| Área de Negocio	| Impacto |
|-----------------|---------|
| Monitoreo en Tiempo Real |	Dashboard operativo con latencia < 5 segundos |
| Calidad de Datos |	Detección automática del 30% de datos corruptos |
| Escalabilidad	| Pipeline preparado para procesar millones de eventos/día |
| Toma de Decisiones |	Datos estructurados para análisis estratégico|

**Capacidades Técnicas**

|Componente |	Implementación |
|-----------|----------------|
|Streaming |	Kafka + Spark Structured Streaming con watermarking |
| Data Quality |	Validación en tiempo real con flags de calidad |
| Storage |	Delta Lake con ACID transactions y time travel |
| Modelado	| Star Schema para Business Intelligence |
| Visualización |	Integración nativa con Power BI |
________________________________________________________________________________________________________________________________________________________________________________________________________________
 ## 🏗️ Arquitectura Técnica
________________________________________________________________________________________________________________________________________________________________________________________________________________
 
![Image](https://github.com/user-attachments/assets/aa9ce6c2-4a88-4f1b-bd71-d73ccc5f32aa)

________________________________________________________________________________________________________________________________________________________________________________________________________________
## 🛠️ Stack Tecnológico
________________________________________________________________________________________________________________________________________________________________________________________________________________

**Infraestructura**

| Componente |	Tecnología |	Versión |	Función |
|------------|------------|---------|---------|
| Orquestación |	Docker Compose |	3.8 |	Gestión de contenedores |
| Streaming Broker |	Apache Kafka	| Latest (KRaft) |	Ingesta en tiempo real |
| Procesamiento |	Apache Spark |	3.5.1 |	Procesamiento distribuido |
| Almacenamiento |	Delta Lake |	3.2.0 |	Tablas ACID en Data Lake |
| Metadatos |	Hive Metastore |	3.1.3	| Catálogo de datos |
| Metastore DB |	PostgreSQL |	13 |	Persistencia de metadatos |
| BI / Visualización |	Power BI | 	Desktop |	Dashboards interactivos |
| Lenguajes |	Python / PySpark |	3.9+ |	Desarrollo de pipelines |

________________________________________________________________________________________________________________________________________________________________________________________________________________
## Patrones de Diseño Implementados
________________________________________________________________________________________________________________________________________________________________________________________________________________

✅ Lambda Architecture (Batch + Speed layers unificados con Delta).

✅ Medallion Architecture (Bronze → Silver → Gold).

✅ Star Schema (Fact + Dimension tables).

✅ Change Data Capture (CDC simulation).

✅ Idempotent Writes (Deduplicación en streaming).

✅ Stateful Processing (Windowing + Watermarking).


________________________________________________________________________________________________________________________________________________________________________________________________________________
## 📂 Estructura del Proyecto
________________________________________________________________________________________________________________________________________________________________________________________________________________

![Image](https://github.com/user-attachments/assets/8f40a368-a44c-4c8f-a7c6-ba8555e47d05)

________________________________________________________________________________________________________________________________________________________________________________________________________________
### 📂 Estructura del Proyecto y Explicación Profunda

El pipeline está dividido en scripts modulares que representan las diferentes etapas de la vida de los datos:

**1. Simulación de Datos ([traffic_dirty_producer.py](https://github.com/AllGoHer/Trafico-en-Tiempo-Real/blob/main/producer/traffic_dirty_producer.py))**

Este script simula la dura realidad del mundo real de los datos.

70% Datos Limpios: Eventos realistas de tráfico (IDs de vehículos, velocidad, zonas, clima).

30% Datos Sucios: Inyecta anomalías intencionales para probar el pipeline:

Velocidades nulas, negativas o extremas (ej. 420 km/h).

Eventos en el futuro (errores de reloj en sensores).

Tipos de datos incorrectos (Texto en vez de Números).

Deriva de esquema (Columnas inesperadas).

JSONs corruptos (Datos inparseables).


**2. Capa Bronze ([traffic_bronze.py](https://github.com/AllGoHer/Trafico-en-Tiempo-Real/blob/main/apps/traffic_bronze.py))**

*Objetivo*: Ingerir todo sin descartar nada.

Se conecta a Kafka, consume el flujo binario y lo convierte a texto JSON.

Aplica un esquema estricto y guarda los datos de forma inmutable en Delta Lake.

**3. Capa Silver ([traffic_silver.py](https://github.com/AllGoHer/Trafico-en-Tiempo-Real/blob/main/apps/traffic_silver.py))**

*Objetivo*: Limpiar, validar y enriquecer.

Banderas de Calidad: Etiqueta los datos faltantes o corruptos.

Conversión Segura: Transforma textos a Números/Fechas sin que el pipeline explote si encuentra un error.

Reglas de Negocio: Rechaza velocidades menores a 0 o mayores a 160 km/h. Rechacha eventos que lleguen más de 10 minutos en el futuro.

Operaciones con Estado: Usa Watermarking (marca de agua de 15 minutos) para manejar datos que llegan tarde y elimina duplicados exactos.

Ingeniería de Variables: Crea banderas de "Hora Pico" y categoriza la velocidad (BAJA/MEDIA/ALTA).

### 🧪 Gestión de Calidad de Datos

**Tipos de Datos Sucios Simulados**

| Tipo	| Descripción	| Frecuencia	| Manejo en Silver |
|------|-------------|------------|------------------|
| null_speed	| Velocidad nula	| 3.3%	| Flag + filtrado |
| negative_speed	| Velocidad negativa	| 3.3%	| Validación fallida |
| extreme_speed |	420 km/h	| 3.3%	| Validación fallida |
| duplicate_vehicle	| ID duplicado	| 3.3%	| Deduplicación |
| late_event	| Timestamp pasado (>10min)	| 3.3%	| Watermark + filtrado |
| future_event	| Timestamp futuro (>10min)	| 3.3%	| Time_valid = 0 |
| wrong_datatype	| "FAST" en speed |	3.3%	| Cast a null |
| schema_drift	| Campo extra	| 3.3%	| Ignorado en Gold |
| corrupt_json	| JSON inválido	| 3.3%	| CORRUPT_JSON |

Proporción: 70% datos limpios / 30% datos sucios


**Pipeline de Limpieza:**

| Etapa |	Operación	| Técnica |
|-------|-----------|---------|
| 1	|Calidad de Datos |	Flags: MISSING_VEHICLE, MISSING_TIME, CORRUPT_JSON |
| 2 |	Tipado Seguro |	CAST con manejo de nulos (speed → int) |
| 3	| Reglas de Negocio	| Validación: speed 0-160 km/h, timestamp ≤ now + 10min |
| 4	| Watermarking |	Tolerancia de 15 minutos para datos tardíos |
| 5	| Deduplicación	| Eliminación de duplicados por (vehicle_id, event_ts) |
| 6	| Feature Engineering	| hour, peak_flag, speed_band (LOW/MEDIUM/HIGH) |


**Métricas de Calidad:**

* Velocidades en rango válido: ✅

* Timestamps consistentes: ✅

* IDs únicos: ✅

* Datos corruptos: ❌ (filtrados)

  

**4. Capa Gold ([traffic_gold.py](https://github.com/AllGoHer/Trafico-en-Tiempo-Real/blob/main/apps/traffic_gold.py))**

*Objetivo*: Modelado analítico para el negocio.

Transforma los datos limpios de Silver en un ***Esquema Estrella (Star Schema)***:

***dim_zone***: Enriquece las zonas con el tipo de zona (Comercial, Hub de transporte) y el riesgo de tráfico.

***dim_road***: Enriquece las carreteras con el tipo (Autopista/Ciudad) y el límite de velocidad legal.

***fact_traffic***: La tabla central de hechos, extrayendo la fecha para optimizar las consultas.

**Esquema Estrella (Star Schema)**
| Tabla	| Tipo |	Clave	| Atributos |
|-------|------|-------|-----------|
| dim_zone |	Dimensión	| city_zone |	zone_type, traffic_risk |
| dim_road	| Dimensión	| road_id	| road_type, speed_limit |
| fact_traffic	| Hechos	| (vehicle_id, event_ts) |	speed, congestion_level, weather, hour, peak_flag, speed_band |

**5. Capa de Servicio ([SQL.txt](https://github.com/AllGoHer/Trafico-en-Tiempo-Real/blob/main/SQL.txt) y [commands.txt](https://github.com/AllGoHer/Trafico-en-Tiempo-Real/blob/main/commands.txt))**

Inicializa el Metastore de Hive y el Thrift Server para exponer las tablas de Delta Lake.

Crea Vistas optimizadas para BI (bi_fact_traffic, etc.) asegurando que los tipos de datos sean perfectamente leídos por Power BI.

________________________________________________________________________________________________________________________________________________________________________________________________________________
## 🚀 Cómo ejecutar el proyecto
________________________________________________________________________________________________________________________________________________________________________________________________________________

**Prerrequisitos**

* Docker desktop (v20+) y [Docker Compose](https://github.com/AllGoHer/Trafico-en-Tiempo-Real/blob/main/docker-compose.yaml) instalados y corriendo.

  Código:

           docker compose up -d

* VS Code (o IDE similar)
  
* Python 3.9+ instalado localmente (para el productor).
  
* Los contenedores de infraestructura (Spark, Kafka, [Hive](https://github.com/AllGoHer/Trafico-en-Tiempo-Real/blob/main/hive-conf/hive-site.xml)) deben estar levantados.

* Power BI Desktop (para visualización)
  

**1. Configurar entorno local**

Instala las librerías necesarias para generar los datos falsos:

Código:

        pip install kafka-python faker pytz
        
**2. Crear el tópico en Kafka**

Código:

        docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic traffic-topic --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1

**3. Ejecutar el Pipeline (En orden)**

Abre terminales separadas en tu contenedor de Spark y ejecuta los trabajos. Espera unos segundos entre cada uno.

Iniciar Capa Bronze:

Código:

         docker exec -it spark-worker /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /opt/spark-apps/traffic_bronze.py

Iniciar Capa Silver:

Código:

        docker exec -it spark-worker /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /opt/spark-apps/traffic_silver.py

Iniciar Capa Gold:

Código:

        docker exec -it spark-worker /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /opt/spark-apps/traffic_gold.py

**4. Iniciar la generación de datos**

Ejecuta esto en tu computadora local (NO dentro de docker) para empezar a enviar datos sucios a Kafka:

Código:

        python traffic_dirty_producer.py

**5. Conexión a Power BI**

Usa los comandos detallados en [commands.txt](https://github.com/AllGoHer/Trafico-en-Tiempo-Real/blob/main/commands.txt) para iniciar el Thrift Server y ejecuta las consultas de [SQL.txt](https://github.com/AllGoHer/Trafico-en-Tiempo-Real/blob/main/SQL.txt) para crear las vistas que conectará Power BI.

____________________________________________________________________________________________________________________________________________________________________________________________________________________________
## 🔍 Monitoreo y Observabilidad
____________________________________________________________________________________________________________________________________________________________________________________________________________________________

**Interfaces de Usuario**

| Servicio	| URL	| Propósito |
|----------|-----|-----------|
| Kafka UI	| http://localhost:8090	| Monitoreo de topics y mensajes |
| Spark Master	| http://localhost:8080	| Estado del cluster y workers |
| Spark Worker	| http://localhost:8081	| Logs y métricas de ejecución |
| Spark History	| http://localhost:18080	| Historial de aplicaciones |


![Image]()

![Image]()

![Image]()

![Image]()

![Image]()

![Image]()

![Image]()

![Image]()

![Image]()

![Image]()

![Image]()

![Image]()

![Image]()

## 👨‍💻 Author

**Allan Gonzales (AllGoHer)**

Data Engineer | Streaming Data Engineer | Spark | Kafka | Delta Lake | Databricks | Airflow | Lakehouse Architecture

*"Building scalable real-time data platforms one event at a time."*
