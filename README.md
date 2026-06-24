# 🚦Análisis del Tráfico en Tiempo Real

![image](https://github.com/user-attachments/assets/83557ea5-ad23-4092-897a-ece9f663089e)

________________________________________________________________________________________________________________________________________________________________________________________________________________
## 🚀 Arquitectura Lakehouse de Streaming en Tiempo Real: Tráfico Vehicular
________________________________________________________________________________________________________________________________________________________________________________________________________________

Una plataforma de análisis de tráfico en tiempo real de producción, desarrollada con Apache Kafka, PySpark Structured Streaming, Delta Lake, Hive Metastore y Power BI.

El proyecto simula eventos de tráfico continuos generados por vehículos que circulan por múltiples zonas urbanas y carreteras. Los datos en flujo continuo se recogen, validan, transforman, enriquecen y modelan en conjuntos de datos listos para el análisis mediante una arquitectura moderna de tipo «Lakehouse».

**Enfoque:** El proyecto simula la caótica realidad del mundo real ingestando un flujo contaminado con datos nulos, tipados incorrectamente, fuera de tiempo y JSONs corruptos, aplicando tolerancia a fallos estricta y patrones de diseño empresariales.

![Image](https://github.com/user-attachments/assets/8d9480d3-3790-4677-8a12-0ee6d19b9396)  ![Image](https://github.com/user-attachments/assets/2d66f89a-bca0-454d-b1d3-1366e0702a8b)  ![Image](https://github.com/user-attachments/assets/89552d90-3caf-4c16-b6ba-b3e52b78d91d)  ![Image](https://github.com/user-attachments/assets/fce18da2-78eb-4470-a27b-ceb93f706fce)

________________________________________________________________________________________________________________________________________________________________________________________________________________
## 🎯 Problema de Negocio
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

___________________________________________________________________________________________________________________________________________________________________________________________________________________________

**Pipeline de Limpieza:**

| Etapa |	Operación	| Técnica |
|-------|-----------|---------|
| 1	|Calidad de Datos |	Flags: MISSING_VEHICLE, MISSING_TIME, CORRUPT_JSON |
| 2 |	Tipado Seguro |	CAST con manejo de nulos (speed → int) |
| 3	| Reglas de Negocio	| Validación: speed 0-160 km/h, timestamp ≤ now + 10min |
| 4	| Watermarking |	Tolerancia de 15 minutos para datos tardíos |
| 5	| Deduplicación	| Eliminación de duplicados por (vehicle_id, event_ts) |
| 6	| Feature Engineering	| hour, peak_flag, speed_band (LOW/MEDIUM/HIGH) |

____________________________________________________________________________________________________________________________________________________________________________________________________________________________

**Métricas de Calidad:**

* Velocidades en rango válido: ✅

* Timestamps consistentes: ✅

* IDs únicos: ✅

* Datos corruptos: ❌ (filtrados)

__________________________________________________________________________________________________________________________________________________________________________________________________________________________  

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

* Inicializa el Metastore de Hive y el Thrift Server para exponer las tablas de Delta Lake.

* Crea Vistas optimizadas para BI (bi_fact_traffic, etc.) asegurando que los tipos de datos sean perfectamente leídos por Power BI.

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


___________________________________________________________________________________________________________________________________________________________________________________________________________________________
## 👨‍💻 Autor
___________________________________________________________________________________________________________________________________________________________________________________________________________________________

**Allan Gonzales (All GoHer)**

Data Engineer | Streaming Data Engineer | Spark | Kafka | Delta Lake | Databricks | Airflow | Lakehouse Architecture

*"Creación de plataformas de datos en tiempo real escalables, evento a evento."*

📧 Contacto
Para preguntas, sugerencias o colaboraciones:

Email: [allgoher007@gmail.com]

LinkedIn: [  ]

GitHub: [https://github.com/AllGoHer]

___________________________________________________________________________________________________________________________________________________________________________________________________________________________
## 🧠 DESARROLLO DEL PROYECTO
___________________________________________________________________________________________________________________________________________________________________________________________________________________________

### 🚦Analisis del Trafico-en-Tiempo-Real

1.	Primero creamos nuestra carpeta de trabajo desde la terminal

Código:

        mkdir Real_Time_Traffic

luego ingresamos a la carpeta.

Código:

        cd Real_Time_Traffic

![Image](https://github.com/user-attachments/assets/ed83eddb-ae9f-406f-bb54-86baf9d5aae2)

2.	Activamos nuestro docker desktop

![Image](https://github.com/user-attachments/assets/28cc2678-06be-409e-bb3e-6296856c4d28)

3.	Abrimos visual estudio code y vinculamos con la carpeta principal (Real_Time_Traffic) y, creamos un archivo docker-compose.yaml con el siguiente código.

Código:

        services:

         # =========================
         # HIVE METASTORE DATABASE
         # =========================
         postgres-metastore:
           image: postgres:13
           container_name: hive-metastore-db
           environment:
             POSTGRES_DB: metastore
             POSTGRES_USER: hive
             POSTGRES_PASSWORD: hive
           ports:
             - "5435:5432"

         # =========================
         # HIVE METASTORE SERVICE
         # =========================
         hive-metastore:
           image: apache/hive:3.1.3
           container_name: hive-metastore
           depends_on:
             - postgres-metastore
           environment:
             SERVICE_NAME: metastore
             DB_DRIVER: postgres
             IS_RESUME: "false"
           ports:
             - "9083:9083"
           volumes:
             - ./hive-conf/hive-site.xml:/opt/hive/conf/hive-site.xml
             - ./warehouse:/opt/hive/warehouse

         # =========================
         # SPARK MASTER
         # =========================
         spark-master:
           image: apache/spark:3.5.1
           container_name: spark-master
           hostname: spark-master
           command: >
             bash -c "
             /opt/spark/sbin/start-master.sh &&
             tail -f /opt/spark/logs/spark--org.apache.spark.deploy.master*.out
             "
           ports:
             - "8080:8080"   # Spark Master UI
             - "7077:7077"   # Spark Master port
           volumes:
             - ./warehouse:/opt/spark/warehouse
             - ./apps:/opt/spark-apps
             - ./spark-ivy:/tmp/.ivy

         # =========================
         # SPARK WORKER
         # =========================
         spark-worker:
           image: apache/spark:3.5.1
           container_name: spark-worker
           hostname: spark-worker
           command: >
             bash -c "/opt/spark/sbin/start-worker.sh spark://spark-master:7077 --cores 2 --memory 2g &&
             tail -f /opt/spark/logs/spark--org.apache.spark.deploy.worker*.out"
           ports:
             - "8081:8081"
             - "4040:4040"
             - "10000:10000"
           depends_on:
             - spark-master
           volumes:
             - ./warehouse:/opt/spark/warehouse
             - ./apps:/opt/spark-apps
             - ./spark-ivy:/tmp/.ivy

         # =========================
         # KAFKA (KRaft mode)
         # =========================
         kafka:
           image: apache/kafka:latest
           container_name: kafka
           ports:
             - "9092:9092"
             - "29092:29092"
           environment:
             KAFKA_NODE_ID: 1
             KAFKA_PROCESS_ROLES: broker,controller
             KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
             KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,PLAINTEXT_EXTERNAL://0.0.0.0:29092,CONTROLLER://0.0.0.0:9093
             KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_EXTERNAL://localhost:29092
             KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
             KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
             KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT
             KAFKA_LOG_DIRS: /tmp/kraft-combined-logs

         # =========================
         # KAFKA UI
         # =========================
         kafka-ui:
           image: provectuslabs/kafka-ui:latest
           container_name: kafka-ui
           depends_on:
             - kafka
           ports:
             - "8090:8080"
           environment:
             KAFKA_CLUSTERS_0_NAME: local
             KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092

___________________________________________________________________________________________________________________________________________________________________________________________________________________________

### 📣Ahora paso a explicar el código del docker-compose por cada container creado.

**1. HIVE METASTORE DATABASE (El Cerebro del sistema)**

•	La tecnología: Usas la imagen oficial de PostgreSQL (postgres:13).

•	El propósito: Almacena los "Metadatos". No guarda los datos reales. Guarda qué tablas existen, cuántas particiones tienen y cuáles son los tipos de datos. Sin esto, Spark no sabe cómo leer tus datos.

•	Variables de entorno: Configura el usuario, la contraseña y el nombre de la base de datos.

•	Puerto 5435: Exposición a tu computadora para que puedas entrar a ver la base de datos con herramientas como DBeaver o DataGrip.


**2. HIVE METASTORE SERVICE (El Traductor)**

•	La tecnología: Usas la imagen oficial de Apache Hive 3.1.3.

•	El propósito: Es el cerebro motor de la vieja escuela. Cuando tú escribes spark.sql("SELECT * FROM tabla"), Spark le pregunta a este servicio: "¿Dónde están físicamente los datos de esa tabla?". El Metastore le responde: "Están en el Worker 1, particiones 1 al 10".

•	depends_on - postgres-metastore: Garantiza que Hive no intente arrancar hasta que la base de datos PostgreSQL esté 100% levantada. Si arrancan al mismo tiempo, Hive fallará porque no sabe quién es quién manda.

•	IS_RESUME: "false": Si Hive se cae y lo reinicias, empieza desde cero. Es vital en clústers estáticos. Si estuviera en "true", intentar recuperar el estado anterior a veces rompe el clúster.

•	Puerto 9083: El puerto por defecto por el que escucha peticiones de Spark o Hive.

•	Los Volumes:

o hive-site.xml: Sobreescribe el archivo de configuración nativo de Hive para que se conecte a tu PostgreSQL, o a un S3/MinIO si estás usando el optimizador de KRaft.
	
o warehouse: Es la carpeta donde se guardan los datos reales si usaras Hive nativo. Aquí es donde Spark escribe los datos procesados.

•	**El nombre de la variable SERVICE_NAME: metastore: Es un identificador interno. En sistemas Hadoop clásicos, puedes tener varios Metastores (uno para desarrollo, otro para producción). Aquí tienes uno solo.


**3. SPARK MASTER (El Gerente del clúster)**

•	La tecnología: Usas la imagen de Apache Spark 3.5.1.

•	El propósito: Es el "Director de Orquestación". Asigna las tareas a los Workers y mantiene el estado global de la aplicación Spark.

•	**El comando command: Le dice a la máquina: "Arranca el proceso Maestro y quédate mirando los logs".

•	Puerto 8080: Exposición del servidor web de la interfaz de usuario de Spark.

•	Puerto 7077: EL PUERTO CLAVE DE COMUNICACIÓN INTERNA. Los Workers usarán este puerto para reportarse con el Master y pedir trabajo.

•	Los Volumes:
    
o warehouse: Comparte la carpeta de datos con los Workers. Si el Master necesita procesar algo, lo hace directamente en este directorio sin pasar por la red. A esto se le llama Compute o Shuffle I/O Local. Es extremadamente rápido.
    
o apps: Donde pones tus scripts .py de Airflow o Scala.
    
o spark-ivy: Almacena las dependencias de Java (la librería de Spark) de forma cacheada para no descargarlas cada vez que mandas un trabajo.

•	hostname: spark-master: Fija el nombre de la máquina. Los Workers lo usarán para conectarse al puerto 7077.

**4. SPARK WORKER (Los Operarios)**

•	La tecnología: Misma versión de Spark (3.5.1).

•	El propósito: Son los que ejecutan el código real (Transformaciones, Joins, GroupBys). Son los que consumen la memoria y la CPU.

•	**El comando command: Le dice al Worker: "Conéctate al Master en el puerto 7077. Usa 2 núcleos (cores) y 2GB de RAM". "Quédate mirando los logs".

•	Puertos del Worker:

o 4040: Puerto interno por si un trabajo se desborda.

o 10000: Puerto para el backend del Spark UI (si lo usas).

o 8081: Puerto por defecto para métricas internas.

•	depends_on - spark-master: Un Worker no puede trabajar sin un Master vivo. Tiene que esperar a que el Master esté 100% arriba.


**5. KAFKA (El Transportador)**

•	La tecnología: Usas apache/kafka:latest (Modo KRaft).

•	El propósito: Mover datos masivos a velocidad extrema entre sistemas diferentes, sin que nadie pierda datos. En este entorno, suele usarse para transportar los datos crudos antes de que Spark los procese.

•	KRaft (La innovación): Tradicionalmente, Kafka necesitaba un clúster de ZooKeeper aparte solo para saber quién es el "Jefe" (Controller). KRaft elimina ZooKeeper. Aquí ves cómo configurar el modo combinado: Este único contenedor actúa como nodo de red (broker) Y como "Jefe" (controller) al mismo tiempo.

•	Puertos:

o 9092: Usado para la comunicación interna entre los contenedores Spark.

o 29092: Expuesto a tu computadora local (host.docker.internal:29092) para que tus scripts de Python en Windows puedan leer o escribir en Kafka sin instalar Kafka en tu PC.

•	**CONTROLLER (Puerto 9093): Es un canal oculto que Kafka usa internamente para que los nodos se pongan de acuerdo en cuanto a quién es el "Jefe".

•	ADVERTISED_LISTENERS: Aquí está el truco de red. Si te conectas desde Windows, Kafka te dirá: "Yo soy 'host.docker.internal:29092, conéctate por ahí". Si te conectas internamente (Docker a Docker), te dirá: "Yo soy 'kafka:9092, conéctate aquí".

•	**KAFKA_LOG_DIRS: Donde Kafka guarda internamente los datos en el contenedor (en /tmp/...).


**6. KAFKA UI (El Visor)**

•	La tecnología: Una herramienta web de terceros (provectuslabs/kafka-ui:Puerto 8090 en tu código).

•	**depends_on - kafka: No tiene sentido arrancar la interfaz si Kafka no existe.

•	**KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092: Le dice a la UI: "Para ver los tópicos, conéctate internamente al puerto interno de Kafka".

___________________________________________________________________________________________________________________________________________________________________________________________________________________________

4.	En VSCode creamos la carpeta hive-conf y, dentro de ella creamos el archivo hive-site.xml ingresándole el siguiente código.

 código:
 
	         <configuration>

          <property> <name>javax.jdo.option.ConnectionURL</name> <value>jdbc:postgresql://postgres-metastore:5432/metastore</value> </property> <property> <name>javax.jdo.option.ConnectionDriverName</name>           <value>org.postgresql.Driver</value> </property> <property> <name>javax.jdo.option.ConnectionUserName</name> <value>hive</value> </property> <property> <name>javax.jdo.option.ConnectionPassword</name> <value>hive</value> </property> <property> <name>datanucleus.autoCreateSchema</name> <value>false</value> </property> <property> <name>hive.metastore.schema.verification</name> <value>true</value> </property> <property> <name>hive.metastore.uris</name> <value>thrift://hive-metastore:9083</value> </property> <property> <name>hive.metastore.warehouse.dir</name> <value>/opt/hive/warehouse</value> </property> </configuration>


![Image](https://github.com/user-attachments/assets/ad751d33-b60f-41a4-8932-3b28389d6f23)

5.	Ahora levantamos el docker-compose. 

Código:

	    Docker compose up -d


![Image](https://github.com/user-attachments/assets/2537dd3d-8dbb-4a0d-92bf-50a8e8d55498)


Ahora verificamos que se haya creado y este activo los contenedores.

![Image](https://github.com/user-attachments/assets/4eae091a-7434-447e-9810-5442ca216263)

📌<mark>NOTA:</mark> en caso que pauses el proyecto y vuelvas en otro momento a levantar los contenedores, observaras el contenedor hive no arrancara. Para ello debemos eliminar todo el container y volver al VSC y, eliminar la carpeta hive-conf y volverla a crear, luego por ultimo volvemos a levantar el docker compose y veras que ya no hay fallas al enceder el container hive-metastore.

6.	Creamos un nuevo carpeta en VSC llamada “producer” y dentro de ella un archivo llamado traffic_dirty_producer.py con el siguiente código.

Código:

         from kafka import KafkaProducer
         from faker import Faker
         import json
         import random
         import time
         from datetime import datetime,timedelta 
         import pytz

         fake = Faker()

         producer = KafkaProducer(
            bootstrap_servers="localhost:29092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
          )

         roads = ["R100", "R200", "R300", "R400"]
         zones = ["CBD", "AIRPORT", "TECHPARK", "SUBURB", "TRAINSTATION"]
         weather = ["CLEAR", "RAIN", "FOG", "STORM"]

         vehicle_cache = []

         def generate_clean_event():

             vid = fake.uuid4()
             vehicle_cache.append(vid)

             return {
                 "vehicle_id": vid,
                 "road_id": random.choice(roads),
                 "city_zone": random.choice(zones),
                 "speed": random.randint(20, 100),
                 "congestion_level": random.randint(1, 5),
                 "weather": random.choice(weather),
                 "event_time": datetime.now(pytz.utc).isoformat()
             }


         def generate_dirty_event():

            dirty_type = random.choice([
                "null_speed",
                "negative_speed",
                "extreme_speed",
                "duplicate_vehicle",
                "late_event",
                "future_event",
                "wrong_datatype",
                "schema_drift",
                "corrupt_json"
            ])

            base = generate_clean_event()

            if dirty_type == "null_speed":
                base["speed"] = None

            elif dirty_type == "negative_speed":
                base["speed"] = -40

            elif dirty_type == "extreme_speed":
                base["speed"] = 420

            elif dirty_type == "duplicate_vehicle" and vehicle_cache:
                base["vehicle_id"] = random.choice(vehicle_cache)

            elif dirty_type == "late_event":
                base["event_time"] = (
                    datetime.now(pytz.utc) - timedelta(minutes=random.randint(10, 120))
                ).isoformat()

            elif dirty_type == "future_event":
                base["event_time"] = (
                    datetime.now(pytz.utc) + timedelta(minutes=random.randint(5, 60))
                ).isoformat()

            elif dirty_type == "wrong_datatype":
                base["speed"] = "FAST"

            elif dirty_type == "schema_drift":
                base["road_condition"] = random.choice(["GOOD", "BAD", "UNDER_CONSTRUCTION"])

            elif dirty_type == "corrupt_json":
                return "###CORRUPTED_EVENT###"

            return base


        while True:

            if random.random() < 0.7:
                event = generate_clean_event()
            else:
                event = generate_dirty_event()

            if isinstance(event, str):
                producer.send("traffic-topic", value={"raw": event})
                print("CORRUPT EVENT SENT")
            else:
                producer.send("traffic-topic", value=event)
                print(event)

            time.sleep(random.uniform(0.5, 1.5))

___________________________________________________________________________________________________________________________________________________________________________________________________________________________

### 🔥 Ahora paso a explicar el presente código.

**📦 Imports y Configuración Inicial**

![Image](https://github.com/user-attachments/assets/26e49254-2e08-4189-ab23-26eb21e79063) 

•	 Faker: Librería de Python que genera datos falsos realistas (nombres de ciudades, correos, IDs).

•	pytz: Librería de zonas horarias. Usar datetime.utcnow() está obsoleto y ya no se recomienda. La forma profesional y moderna es datetime.now(pytz.utc) para asegurar que estés manejando la misma zona horaria en todos tus sistemas.

**🚀 Configuración del Productor Kafka**

![Image](https://github.com/user-attachments/assets/0f45b4c2-4687-4d2f-9e95-b3a2a8b05b3a)

•	**producer = KafkaProducer(...):** Conecta a Kafka en tu Docker (localhost:29002) y usa un serializador lambda para convertir tu diccionario en texto JSON y luego en bytes (encode("utf-18"`) para que Kafka lo entienda.

**📋 Listas de Datos Predefinidos**

![Image](https://github.com/user-attachments/assets/a06927a4-fc05-4fae-969b-5b285739a798)

•	No es solo generar datos al azar; es generar datos coherentes. Usar "R100" y "AIRPORT" asegura que las reglas de negocio tengan sentido lógico.

**🧹 Generador de Eventos Limpios**

![Image](https://github.com/user-attachments/assets/dff0eddd-9abf-4a48-bd6e-1714ae576286)

•	Para simular un "Dato Duplicado", necesitas que el mismo vehicle_id exista previamente. Esta lista en memoria guarda los IDs válidos generados. Si se elige el error "duplicado", el código "robará" uno de estos IDs y lo sobrescribirá, simulando que el sensor envió el mismo evento dos veces.

•	 Cuando un semáforo se cae, a menudo envía el mismo evento múltiples veces antes de levantarse. Si el semáforo envía el "R100", lo más probable es que el registro esté duplicado en la base de datos. Aquí generamos un evento perfectamente limpio con datetime moderno y velocidades lógicas.

**💥 Generador de Eventos Sucios**

Esta función es clave: simula 9 tipos diferentes de errores que podrían ocurrir en datos reales:

![Image](https://github.com/user-attachments/assets/55726874-91a5-4484-a083-fccb5934158a)

![Image](https://github.com/user-attachments/assets/0902da99-786e-4f04-83c3-9a5533ded74f)

•	El propósito: Simular fallos de hardware en la fuente de datos cruda. Un motor de tráfico nunca medirá velocidades negativas. Si tu pipeline falla aquí, el Data Engineer pierde el dato, lo cual es peor que recibir un dato sucio pero estructural.

![Image](https://github.com/user-attachments/assets/719e9712-a0fc-4b27-b4cc-209f83a0cf64)

•	El propósito: Simula que el paquete de red del sensor falló en la red y el dato te llega 10 minutos tarde. En tiempo real, si tu pipeline tiene configurado Watermarks en Databricks, este evento debería ser descartado, o actualizado.

![Image](https://github.com/user-attachments/assets/0c9f6baf-b37d-4d14-9a7e-11e323172f8d)

•	El propósito: Simula un error del reloj del sensor. El reloj del dispositivo de tráfico está adelantado. Si tu pipeline no maneja "Tiempos Futuros", destruirá tu análisis temporal.

![Image](https://github.com/user-attachments/assets/9f8676e2-6e8b-476b-9911-19c271a11360)

____________________________________________________________________________________________________________________________________________________________________________________________________
**🔄 Bucle Principal de Producción**

![Image](https://github.com/user-attachments/assets/25162624-fa7a-4dfd-9012-69e6a360485d)

•	En la vida real, los datos buenos y los errores viajan por el mismo canal de Kafka. Un 30% de error es un porcentaje de ruido alto, pero realista.

•	Nota técnica: Al inyectar basura en la fuente (el productor), te ahorras el costo de procesamiento. El filtro de limpieza se hace en el consumidor. Si el 30% de los datos son basura, es mejor filtrarlos aquí en Python antes de enviarlos a Databricks para no desperdiciar recursos de red y cómputo en Databricks.

![Image](https://github.com/user-attachments/assets/732997c4-ad2c-459d-a532-8d95b6c124dc)

•	El control de velocidad aleatorio

![Image](https://github.com/user-attachments/assets/413966ae-c30e-44f9-b7bb-3f1824d0d181)

•	El problema del streaming a nivel de transporte: Si quitamos el time.sleep y dejas que Python envíe datos lo más rápido posible, puedes saturar el ancho de banda de red o sobrecargar el Kernel de la máquina de Kafka o Databricks. Poner una pausa aleatoria simula el comportamiento de una red de sensores que no envían datos a velocidad constante, dándole un "respiro" a la tubería de Streaming.

**🎯 Propósito del Código**

Este productor se usa típicamente para:

1.	Pruebas de sistemas de streaming (Apache Kafka, Spark Streaming, Flink)
	
2.	Validación de pipelines de datos - ¿Cómo maneja el sistema datos corruptos?
	
3.	Demostración de calidad de datos - Muestra problemas comunes en datos del mundo real
	
4.	Monitoreo y alertas - ¿Se detectan eventos anómalos?


**⚠️ Tipos de Suciedad Generados**

![image](https://github.com/user-attachments/assets/0c34327d-b0f4-4768-9e19-cda4a9dc8a93)
 
Este código es excelente para entrenar sistemas de detección de anomalías y probar cómo reaccionan las aplicaciones ante datos impuros.

______________________________________________________________________________________________________________________________________________________
🧠 ¿Qué pasa internamente en Kafka?

Tú estás enviando dos tipos de estructuras al tópico traffic-topic:

1.	Para el error de JSON Corrupto: Mandas un texto crudo: {"raw": "###CORRUPTO###"}. El consumidor en Databricks es lo suficientemente inteligente para ver el string y enviarlo a la "Papelera de basura" (Dead Letter Queue en Databricks) sin que el proceso se rompa.
   
2.	Para todos los demás errores (Null speed, Late Event, Duplicado, Futuro, Schema Shift): Mandas un JSON perfectamente estructuralmente, pero con los valores corruptos por dentro. El consumidor en Databricks leerá el JSON, detectará los valores nulos, negativos o erróneos y aplicará las reglas para enviarlos a la papelera, sin fallar el proceso completo de Databricks.

**<mark>Nota técnica:</mark>** A pesar de ser "Sucio" bajo el punto de vista de la regla de negocio, el JSON subyacente sigue siendo válido estructuralmente. El string "###CORRUPTO###" simula que el paquete de red se corrompió en la transmisión de red y no se pudo leer bien el archivo JSON antes de enviarlo). En la vida real, esto suele ser un "Mensaje no analizable: Cannot read field 'x'").


**💡 Nota sobre el error del código original (La trampa del KeyError)**

La línea del Duplicado está así: 

elif dirty_type == "duplicate_vehicle" and vehicle_cache: base["vehicle_id"] = random.choice(vehicle_cache)

**El problema de esta línea:** Si la lista <mark>vehicle_cache</mark> está vacía (porque no se han generado IDs todavía, o porque el caché se vació), <mark>random.choice([])</mark> lanzará un error <mark>IndexError: Cannot choose from an empty sequence</mark>, hundirá tu código.

**¿Por qué el autor lo dejó así? (El concepto "Data Filtering Pre-Shuffle")**

 Se llama así porque es una técnica para optimizar la red. Si tu pipeline tiene una alta cardinalidad (ej. 100,000 sensores), hacer un <mark>filter()</mark> en Databricks es costoso porque requiere mover millones de datos por la red.
La técnica de <mark>"Pre-Shuffle" (Pre-Filtrado en la fuente)</mark> consiste en simplemente descartar el evento sucio en Python antes de enviarlo a Kafka. De esta forma, Databricks recibirá un flujo mucho más limpio. Si el sensor reporta un error de duplicado pero la lista <mark>vehicle_cache</mark> está vacía, el código está programado para no fallar intencionalmente, simplemente ignora el error y deja pasar el dato "sucio" con la etiqueta de error inyectada <mark>({"raw": event})</mark>.

Código:

        def generate_dirty_event():
            # Elige aleatoriamente un tipo de "suciedad"
            dirty_type = random.choice([
                "null_speed",          # Velocidad nula
                "negative_speed",      # Velocidad negativa
                "extreme_speed",       # Velocidad extrema (420 km/h)
                "duplicate_vehicle",   # ID de vehículo duplicado
                "late_event",          # Evento del pasado (10-120 min atrás)
                "future_event",        # Evento del futuro (5-60 min adelante)
                "wrong_datatype",      # Tipo de dato incorrecto (string en lugar de número)
                "schema_drift",        # Campo nuevo no esperado
                "corrupt_json"         # JSON corrupto (string plano)
            ])
    
            base = generate_clean_event()  # Genera evento base limpio
    
            # Aplica la corrupción según el tipo elegido
            if dirty_type == "null_speed":
                base["speed"] = None       # El sensor falló y no capturó el dato.
        
            elif dirty_type == "negative_speed":
                base["speed"] = -40       # El sensor falló y leyó negativo.
        
            elif dirty_type == "extreme_speed":
                base["speed"] = 420
        
            elif dirty_type == "duplicate_vehicle" and vehicle_cache:
                base["vehicle_id"] = random.choice(vehicle_cache)  # Usa ID existente
        
            elif dirty_type == "late_event":
                base["event_time"] = (datetime.now(pytz.utc) - timedelta(minutes=random.randint(10, 120))).isoformat()
        
            elif dirty_type == "future_event":
                base["event_time"] = (datetime.now(pytz.utc) + timedelta(minutes=random.randint(5, 60))).isoformat()
        
            elif dirty_type == "wrong_datatype":
                base["speed"] = "FAST"  # String en lugar de número
        
            elif dirty_type == "schema_drift":
                base["road_condition"] = random.choice(["GOOD", "BAD", "UNDER_CONSTRUCTION"])  # Campo extra
        
            elif dirty_type == "corrupt_json":
                return "###CORRUPTED_EVENT###"  # No es un diccionario válido
    
            return base


ahora iremos a la interface de usuario de Kafka, a través de docker desktop, haciendo click en el puerto de Kafka-ui (localhost:8090).

![Image](https://github.com/user-attachments/assets/9599b6e7-fb3a-4a5a-a67c-f6270909182c)

Y se abrirá la siguiente ventana.

![Image](https://github.com/user-attachments/assets/2beace32-1b8d-46fe-8d04-2d6d95299eb6)

Lo siguiente será crear un tema (topic); asi es que, iremos a la terminal y pasamos el siguiente 

código:

        Docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic traffic-topic --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1


![Image](https://github.com/user-attachments/assets/2f079c78-71a2-48f9-baf5-eb5eaec864fc)

#### 🧠 Para que lo entiendan perfectamente, voy a desarmar el comando pieza por pieza:

**1. Docker exec -it kafka**

•	*docker exec:* Es una instrucción de Docker que sirve para ejecutar un comando dentro de un contenedor que ya está corriendo.

•	*-it*: Son dos banderas (flags). -i mantiene la entrada estándar abierta (interactivo) y -t asigna una terminal. En resumen, permite interactuar con el contenedor como si estuviéramos escribiendo directamente en su pantalla.

•	*kafka*: Es el nombre del contenedor de Docker donde está instalado Kafka. (Si tu contenedor se llamara diferente, aquí iría otro nombre).

**2. /opt/kafka/bin/kafka-topics.sh**

•	Esta es la ruta completa dentro del contenedor donde se encuentra el script de consola de Kafka encargado de administrar los temas (crear, borrar, listar, modificar). Es el ejecutable que hace el trabajo real.

**3. --create**

•	Es la orden específica que le da al script kafka-topics.sh. Le dice: "Quiero crear un topic nuevo". (Otros ejemplos podrían ser --list para listarlos o --describe para ver su configuración).

**4. --topic traffic-topic**

•	Define el nombre del tema que vas a crear. En este caso, el tema se llamará traffic-topic (probablemente se usará para enviar y recibir datos sobre tráfico).

•	<mark>Nota:</mark> Un topic en Kafka es como una carpeta o un canal de chat donde se guardan los mensajes de un tema específico.

5. <mark>--bootstrap-server kafka:9092</mark>

•	Le dice al script a qué servidor de Kafka debe conectarse para crear el tema.

•	*kafka*: Es el nombre del host (usualmente el nombre del contenedor en la red de Docker).

•	9092: Es el puerto por defecto en el que Kafka escucha las conexiones.

•	¿Por qué "bootstrap"?: Significa "servidor de arranque". El script se conecta a este servidor primero, y luego Kafka le dice: "Hola, soy yo, puedes hacer la creación aquí".

6. <mark>--partitions 3</mark>

•	Define el número de particiones que tendrá este tema (en este caso, 3).

•	**¿Qué es una partición?** Es la forma en que Kafka divide los datos. Imagina que el topic es una carretera. En lugar de tener un solo carril, Kafka crea 3 carriles. Esto permite que múltiples aplicaciones (consumers) lean los datos al mismo tiempo en paralelo, una por cada partición, lo que hace que Kafka sea súper rápido y escale bien.

7. <mark>--replication-factor 1</mark>

•	Define el factor de replicación (en este caso, 1).

•	¿Qué significa? Es la cantidad de copias (réplicas) de los datos que Kafka guardará por si algo falla.

•	Al ser 1, significa que solo hay una copia de los datos. Si el servidor de Kafka se cae, esos datos no estarán disponibles hasta que se levante de nuevo (no hay respaldo en otro servidor).

•	En un entorno de producción real (con varias máquinas), esto suele ser 2 o 3 para que, si un servidor se quema, los datos estén a salvo en otro. Como estás en Docker (probablemente local), 1 es lo correcto.

Siguiendo con la creación del tema, vamos a la interface de usuario de Kafka para verificar la creación. 


![Image](https://github.com/user-attachments/assets/85f90154-8381-44fe-b3c3-ede375c9829d)

![Image](https://github.com/user-attachments/assets/8ace3a43-8bcc-4c92-b93a-91fadcd5f8af)

Ahora creamos las dependencias para correr el código python, así es que, vamos a la terminal y pasamos los siguientes códigos.

Código:
               
		python -m pip install kafka-python faker pytz

luego entramos a la carpeta producer.

Código:
              
		cd producer

código:

        python traffic_dirty_producer.py


![Image](https://github.com/user-attachments/assets/f143a4c3-89f7-4d99-a877-c47138230853)

Luego de un corto tiempo (2 minutos aprox.)  para detener el proceso y no saturar la memoria local, detenemos el proceso con <mark>control C</mark>.

Ahora regresamos a la interface de usuario de Kafka para la verificación.


![Image](https://github.com/user-attachments/assets/2f01bfd1-dc61-4249-bacc-df020cc4ca15)

![Image](https://github.com/user-attachments/assets/dafe98d3-7431-4922-9796-0a6c7410a235)

![Image](https://github.com/user-attachments/assets/807572f8-e843-45ce-9920-d3e62818d546)


___________________________________________________________________________________________________________________________________________________________________________________________________________________________
## ![Image](https://github.com/user-attachments/assets/5771a008-2cfb-42b1-82b6-e40cd70bcddf) CAPA DE BRONCE ([traffic_bronze.py](https://github.com/AllGoHer/Trafico-en-Tiempo-Real/blob/main/apps/traffic_bronze.py))
___________________________________________________________________________________________________________________________________________________________________________________________________________________________


En esta parte, usaremos Spark para extraer los datos brutos de Kafka para crear la capa de bronce.

Primero creare un archivo llamado traffic_bronce.py dentro del carpeta app. en VSCode.

Código:

        from pyspark.sql import SparkSession
        from pyspark.sql.functions import *
        from pyspark.sql.types import *

        # Spark Session Config

        spark = (
            SparkSession.builder
            .appName("TrafficStreamingLakehouse")
            # cluster master
            .master("spark://spark-master:7077")
            # delta lake
            .config("spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .enableHiveSupport()
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")

        # Kafka Raw Stream

        raw_stream = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "kafka:9092")
            .option("subscribe", "traffic-topic")
            .option("startingOffsets", "latest")
            .load()
        )

        # Convert Binary to String

        json_stream = raw_stream.selectExpr(
            "CAST(value AS STRING) as raw_json",
            "timestamp as kafka_timestamp"
        )

        # Flexible Schema

        traffic_schema = StructType([
            StructField("vehicle_id", StringType()),
            StructField("road_id", StringType()),
            StructField("city_zone", StringType()),
            StructField("speed", StringType()),
            StructField("congestion_level", IntegerType()),
            StructField("weather", StringType()),
            StructField("event_time", StringType())
        ])

        parsed = json_stream.withColumn(
            "data",
            from_json(col("raw_json"), traffic_schema)
        )

        flattened = parsed.select(
            "raw_json",
            "kafka_timestamp",
            "data.*"
        )

        # Bronze Delta Write

        bronze_query = (
            flattened.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", "/opt/spark/warehouse/chk/traffic_bronze")
            .option("path", "/opt/spark/warehouse/traffic_bronze")
            .start()
        )

        spark.streams.awaitAnyTermination()



### 🧠Explicación del código paso a paso.

El presente código lee datos de tráfico en vivo desde **Apache Kafka**, convierte el formato binario a un formato estructurado y lo guarda de forma continua e inmutable en un almacenamiento **Delta Lake**.
A continuación, explico cada bloque del código al detalle:

**📦 IMPORTS - Librerías necesarias**

![Image](https://github.com/user-attachments/assets/4d5996e9-1197-4197-9248-4117204a8661)

•	SparkSession: Es el punto de entrada principal para programar en Spark SQL con DataFrame.

•	functions: Importa todas las funciones de columna de Spark (como col, from_json, etc.).

**Importa TODAS las funciones de Spark SQL (el * significa "todas")**. Estas incluyen:

•	col() - Para referenciar columnas.

•	from_json() - Para parsear JSON.

•	when(), lit(), avg(), sum(), etc.

•	types: Importa los tipos de datos para definir esquemas (como StringType, IntegerType, StructType).
              
**Importa TODOS los tipos de datos de Spark:**

•	StructType - Estructura compleja (como un objeto).

•	StringType, IntegerType, DoubleType, etc.

•	ArrayType, MapType.

__________________________________________________________________________________________________________________________________________________________________________________________________________________________
**🚀 CONFIGURACIÓN DE SPARK SESSION**

![Image](https://github.com/user-attachments/assets/81e126b4-be2b-45ee-bf81-6441409512af)

![Image](https://github.com/user-attachments/assets/7284c625-abbc-4396-b9f6-35a2fd013f5f)

Crea un builder de SparkSession con:

•	SparkSession.builder – Inicia la construcción.

•	.appName(“…”) – Nombre de la aplicación (aparece en la UI de Spark).

![Image](https://github.com/user-attachments/assets/b1c4fe99-eeee-49a2-9392-2902bb527f23)

Define el cluster master:

•	spark:// - Protocolo para el cluster manager de Spark standalone.

•	spark-master:7077 – Hostname y puerto del master (en el docker-compose).

•	Esto significa que Spark se conectará al cluster que tienes en Docker.

![Image](https://github.com/user-attachments/assets/ccd703a8-b436-44c3-8ada-f588ad7565e3)

Configura extensiones de Spark SQL para habilitar Delta Lake:

•	DeltaSparkSessionExtension – Permite usar comandos específicos de Delta (como MERGE, OPTIMIZE, etc.)


![Image](https://github.com/user-attachments/assets/27d1a088-038c-4f35-a6de-c2da3c71a0d5)

Configura el catálogo de Spark para usar el catálogo de Delta:

•	Reemplaza el catálogo predeterminado con el de Delta.

•	Permite que Spark trate las tablas Delta como tablas Spark normales.

![Image](https://github.com/user-attachments/assets/dea4cb9d-82a1-4b5c-93b6-13ddebba338e)

Habilita soporte para Hive Metastore:

•	Permite a Spark leer/escribir tablas gestionadas por Hive.

•	Usa el metastore de Hive (que configuraste en el docker-compose).

•	Permite usar SQL de Hive en Spark.

![Image](https://github.com/user-attachments/assets/7a0dde16-e12a-4292-8c6e-395a6faf904b)

Obtiene o crea la SparkSession:

•	Si ya existe una sesión activa, la reutiliza.

•	Si no existe, crea una nueva.

•	Esta es la forma recomendada en entornos de producción.

![Image](https://github.com/user-attachments/assets/9c5a4edb-21c1-434a-883a-b04056834d13)

Configura el nivel de logging:

•	"WARN" solo muestra advertencias y errores (menos ruido).

•	Otras opciones: "ERROR", "INFO", "DEBUG", "TRACE".

__________________________________________________________________________________________________________________________________________
**📡 LECTURA DEL STREAM DE KAFKA**

![Image](https://github.com/user-attachments/assets/9babb8b5-4804-473b-8ab2-aa4b6994095f)

Inicia la lectura de streaming:

•	spark.readStream - Indica que es un stream continuo (no batch).

•	.format("kafka") - Fuente de datos: Apache Kafka.

![Image](https://github.com/user-attachments/assets/5db0dba1-7a81-4be8-8125-d3d1249000bb)

Configura el servidor de Kafka:

•	kafka:9092 - Hostname y puerto.

•	Importante: Usa el nombre del servicio dentro de la red Docker, no localhost.

![Image](https://github.com/user-attachments/assets/6587bc5c-0475-467c-b104-900d16d4a1e7)

Especifica el topic a suscribir:

•	traffic-topic - El topic donde tu productor envía los datos

![Image](https://github.com/user-attachments/assets/daa44a94-b334-4873-ae5d-5488ebd8a017)

Define dónde empezar a leer:

•	"latest" - Solo mensajes nuevos (desde ahora).

•	Otras opciones: "earliest" (todos los mensajes desde el inicio).

•	También puedes especificar un timestamp: "startingOffsets": "{\"topic\":{\"0\":12345}}".

![Image](https://github.com/user-attachments/assets/712ddb57-3d49-4985-8453-05f844cffb6e)

Ejecuta la carga del DataFrame de streaming:

•	Este DataFrame es un "stream" que se actualiza continuamente.

•	Todavía NO está procesando datos, solo definió la fuente.

_______________________________________________________________________________________________________________________________________________________________________________________________
#### 🔄 TRANSFORMACIÓN DE DATOS

![Image](https://github.com/user-attachments/assets/0c6e3c59-9cfe-42ff-a26c-66591ca43c44)

Convierte y selecciona campos:

•	selectExpr: Permite usar expresiones SQL directamente sobre el stream.

•	CAST(value AS STRING): convierte el arreglo de bytes (el payload de Kafka) en una cadena de texto legible, renombrándola como raw_json.

•	timestamp as kafka_timestamp: Renombra el timestamp de Kafka (cuando llegó el mensaje al broker) para distinguirlo del timestamp del evento en sí (que vendrá dentro del JSON).

•	Resultado: DataFrame con dos columnas: raw_json (string) y kafka_timestamp.

_______________________________________________________________________________________________________________________________________________________________________________________________________________________
#### 📋 DEFINICIÓN DEL ESQUEMA

![Image](https://github.com/user-attachments/assets/c23078a8-d4aa-4242-a384-e75cadbb4e44)

En el procesamiento de streams, es una buena práctica definir el esquema manualmente en lugar de dejar que Spark lo infiera (ya que la inferencia requiere revisar todos los datos primero, lo cual es ineficiente en streaming). Se define la estructura exacta que tendrá el JSON: identificador del vehículo, carretera, zona, velocidad (como texto, probablemente para limpiarlo después), nivel de congestión (como entero), clima y la hora del evento.

Define el esquema para el JSON de tráfico:

•	StructType - Define un objeto complejo (como un JSON).

•	StructField - Cada campo del objeto.

•	Campos:

o	vehicle_id - String (ID del vehículo).
	  
o	road_id - String (ID de la carretera).
	  
o	city_zone - String (Zona de la ciudad).
	  
o	speed - String ⚠️ (Velocidad - definida como String para manejar datos sucios).
	  
o	congestion_level - Integer (Nivel de congestión 1-5).
	  
o	weather - String (Condición climática).
	  
o	event_time - String (Timestamp del evento).
	  
📌Nota importante: speed es StringType() para manejar valores como "FAST" o null que genera el productor.

_________________________________________________________________________________________________________________________________________________________________________________________________________________

#### 🔍 PARSEO DEL JSON

![Image](https://github.com/user-attachments/assets/d09fdee5-890b-4290-9cf2-6c618c45ab8f)

Parsea el JSON y lo convierte en columnas anidadas:

•	withColumn() - Añade una nueva columna.

•	"data" - Nombre de la nueva columna.

•	from_json() - Función que parsea JSON.

•	col("raw_json") - Columna que contiene el JSON.

•	traffic_schema - Esquema que define la estructura.

•	Resultado: Una columna data que contiene todas las propiedades del JSON como campos.

#### 🧹 APLANADO DE LA ESTRUCTURA

![Image](https://github.com/user-attachments/assets/763f7142-8a33-4d8d-b671-4ba33d1ee945)

**Aplana la estructura anidada:**

•	select() - Selecciona las columnas.

•	"raw_json" - Mantiene el JSON original.

•	"kafka_timestamp" - Mantiene el timestamp.

•	"data.*" - Aplana todos los campos dentro de data.

•	El operador * dentro de un struct "explota" o aplana las columnas anidadas. Es decir, convierte data.vehicle_id en simplemente vehicle_id a nivel de raíz, facilitando su consulta posterior. También conserva el JSON original y el timestamp de Kafka por si se necesitan para auditoría.

•	Resultado: DataFrame plano con todas las columnas del JSON como columnas de nivel superior.

Ejemplo:


Antes:
| raw_json | kafka_timestamp | data (struct) |
|----------|-----------------|---------------|
|          |                 | {vehicle_id: '123'} |
|          |                 | {road_id: ‘45’}… |

Después:

| raw_json | kafka_timestamp | vehicle_id | road_id | speed | ...
|----------|-----------------|------------|---------|-------|-

_______________________________________________________________________________________________________________________________________________________________________________________________________________________
#### 💾 ESCRITURA EN DELTA LAKE (BRONZE)

![Image](https://github.com/user-attachments/assets/cb5f29f8-b1d4-443d-a135-b97dfd4f2ac7)

Configura la escritura en streaming:

•	flattened.writeStream - Indica que es un stream de salida.

•	.format("delta") - Escribe los datos usando Delta Lake. Esto permite transacciones ACID, time travel (viaje en el tiempo) y que el directorio se comporte como una "tabla".

![Image](https://github.com/user-attachments/assets/3f7f5197-8a7f-42b4-9ebe-90ecccddbc2b)

Define el modo de salida:

•	"append" - Solo añade nuevos datos en filas a las tablas (no actualiza ni elimina). Tiene sentido porque en la capa Bronze solo queremos guardar el histórico crudo sin sobrescribir nada.

•	Para streaming, las opciones son: append, update, complete

![Image](https://github.com/user-attachments/assets/18cae80c-65e6-4ee2-a623-7d14a808e0f4)

•	.option("checkpointLocation", ...): La parte más crítica del streaming. Spark guarda aquí un registro de qué mensajes de Kafka ya ha leído y procesado. Si el programa falla y se reinicia, leerá este directorio y retomará exactamente desde donde se quedó, sin perder datos ni duplicarlos.

![Image](https://github.com/user-attachments/assets/d75d594f-e873-405a-926c-bc48ebc89355)

•	.option("path", ...): La ruta física en el disco donde se guardarán los archivos Parquet y el log de transacciones de Delta (_delta_log).

![Image](https://github.com/user-attachments/assets/5080b40a-0f62-46dc-bf5b-4359d88fca2b)

Inicia el streaming:

•	Comienza a procesar datos en background.

•	Devuelve un StreamingQuery que puedes monitorear.

•	No bloquea - continúa ejecutando en segundo plano.

•	Al guardarse en la variable bronze_query, podrías detenerlo después con bronze_query.stop().

_____________________________________________________________________________________________________________________________________________________________________________________________________________________
#### 🔁 ESPERA INDEFINIDA

![Image](https://github.com/user-attachments/assets/cfaff03c-97f1-43d5-a07e-af1a655372c4)

Espera hasta que el stream termine:

•	spark.streams - Obtiene todas las queries activas.

•	awaitAnyTermination() - Bloquea el programa.

•	Espera indefinidamente hasta que un stream falle o sea detenido manualmente.

•	Importante: Sin esto, el programa terminaría inmediatamente y el stream no procesaría nada.


__________________________________________________________________________________________________________________________________________________________________________________________________________________________
#### 🎯 ARQUITECTURA COMPLETA

![Image](https://github.com/user-attachments/assets/bab8628e-8507-4232-ac7d-0f0412078d8b)

* Volviendo al proceso del proyecto, abrimos el spark master en el navegador web y escribimos localhost:8080 


![Image](https://github.com/user-attachments/assets/3da30a1b-a632-490b-917f-91534d90d2c7)


Luego regresamos a la terminal y retrocedemos una carpeta.

Código:
          
		cd ..

![Image](https://github.com/user-attachments/assets/014ec2ab-68be-4412-97e9-c84d6cbe62ac)

Como las aplicaciones corren en docker pasaremos el siguiente comando para ejecutar el script de Spark.

Código:

        docker exec -it spark-worker /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /opt/spark-apps/traffic_bronze.py

_____________________________________________________________________________________
### 🧠 Explicación del código.

**🎯 VISIÓN GENERAL DEL COMANDO**

Este comando hace lo siguiente:

1.	Entra al contenedor spark-worker (que está corriendo en Docker).
   
2.	Ejecuta spark-submit (la herramienta para enviar trabajos a Spark).
   
3.	Configura dependencias (paquetes necesarios).
   
4.	Ejecuta tu script traffic_bronze.py

________________________________________________________________________________________
**📖 DESGLOSE LÍNEA POR LÍNEA**

1️⃣ docker exec -it spark-worker

docker exec - Comando de Docker para ejecutar algo dentro de un contenedor ya corriendo.

-i - Modo interactivo (mantiene STDIN abierto).

-t - Asigna un pseudo-TTY (terminal) para que la salida sea legible.

spark-worker - Nombre del contenedor donde quieres ejecutar el comando.

📌 Equivalente a: "Abre una terminal dentro del contenedor spark-worker que está corriendo".

2️⃣ /opt/spark/bin/spark-submit

Ruta completa al ejecutable spark-submit dentro del contenedor.

spark-submit es la herramienta principal para enviar trabajos a Spark. Es como el "lanzador" de aplicaciones Spark.

¿Qué hace?

•	Empaqueta tu aplicación

•	La envía al cluster Spark

•	Monitorea su ejecución


📌 Equivalente a: "Ejecuta el lanzador de aplicaciones Spark".


3️⃣ --conf spark.jars.ivy=/tmp/.ivy

Configuración de Spark para el directorio Ivy (sistema de gestión de dependencias).

--conf - Establece una configuración personalizada para esta ejecución.

spark.jars.ivy=/tmp/.ivy - Especifica dónde Spark debe guardar las dependencias descargadas.

📌 ¿Por qué es importante?

•	Sin esto, Spark descargaría paquetes en /root/.ivy2/ (dentro del contenedor)

•	Al usar /tmp/.ivy, puedes montar un volumen para compartir dependencias entre contenedores

•	Esto acelera ejecuciones futuras (no descarga siempre lo mismo)


4️⃣ --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1

Esta es la parte más importante. Descarga e incluye librerías externas necesarias. 

**Paquete 1: Delta Lake** 

io.delta:delta-spark_2.12:3.2.0                                                                                                                                                                                            


**Formato:** groupId:artifactId:version


| Componente | Significado |
|------------|-------------|
| io.delta | Grupo (organización) |
| delta-spark_2.12 | Nombre del artefacto (versión de Scala 2.12) |
| 3.2.0	| Versión del paquete |


**Paquete 2: Kafka Connector**

Código:

        org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1

| Componente |  Significado |
|------------|--------------|
| org.apache.spark | Grupo Apache Spark |
| spark-sql-kafka-0-10 | Conector Kafka para Spark SQL |
| _2.12 |	Compilado para Scala 2.12 |
| 3.5.1	| Versión de Spark |

¿Qué hace este paquete?

•	Permite leer/escribir desde/a Kafka

•	Soporte para versiones de Kafka 0.10+

•	Proporciona los formatos de lectura/escritura


5️⃣ /opt/spark-apps/traffic_bronze.py

/opt/spark-apps/traffic_bronze.py

Ruta completa a tu script Python dentro del contenedor.

/opt/spark-apps - Carpeta donde montaste tus aplicaciones (en el docker-compose).

traffic_bronze.py - Tu archivo Python con el pipeline de streaming.

📌 Equivalente a: "Ejecuta este script con el cluster Spark".

___________________________________________________________________________________________________________________________________________________________________________________________________________________________

**🔄 FLUJO COMPLETO DE EJECUCIÓN**

1. docker exec -it spark-worker
   ↓ (entra al contenedor)
   
2. /opt/spark/bin/spark-submit
   ↓ (ejecuta el lanzador)
   
3. --conf spark.jars.ivy=/tmp/.ivy
   ↓ (configura directorio de caché)
   
4. --packages io.delta:...,org.apache.spark:...
   ↓ (descarga dependencias)
   
5. /opt/spark-apps/traffic_bronze.py
   ↓ (ejecuta tu script)



![Image](https://github.com/user-attachments/assets/dc9377b7-0426-49f6-b124-a153f445d7e0)

Ahora abrimos otra terminal para volver a ejecutar el archivo traffic_dirty_producer. Así es que, primero ingresamos a nuestra carpeta principal.

Código:

        Cd Real_Time_Traffic

Segundo ingresamos a la carpeta producer

Código:

        Cd producer

Tercero, corremos la carpeta traffic_dirty_producer.

Código:

        Python traffic_dirty_producer.py
 

![Image](https://github.com/user-attachments/assets/c05cbd0d-7517-4387-8b59-a9ef78fa5077)

Ahora para verificar si se ejcuto bien, vamos a revisar que en la carpeta warehouse de VSCode se encuentre las otras 2 subcarpetas chk\traffic_bronze y traffic_bronze donde se este generando los archivos parquet de datos en DELTA_LOG

![Image](https://github.com/user-attachments/assets/2236b211-0ecd-4406-9054-c2fbe1032a52)

Y también verificaremos la transmisión en el Spark Master UI

![Image](https://github.com/user-attachments/assets/d47e1db9-1df9-40dd-957d-00d65677926e)


___________________________________________________________________________________________________________________________________________________________________________________________________________________________
## ![Image](https://github.com/user-attachments/assets/a2162002-b311-448a-a4f2-c9fb204fe1d8) CAPA DE PLATA
___________________________________________________________________________________________________________________________________________________________________________________________________________________________

Código:

        from pyspark.sql import SparkSession
        from pyspark.sql.functions import *
        from pyspark.sql.types import *

        # Spark Session Config

        spark = (
            SparkSession.builder
            .appName("TrafficSilverLayer")
            # cluster master
            .master("spark://spark-master:7077")
            # delta lake
            .config("spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .enableHiveSupport()
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")

        # Read Bronze Stream

        bronze_df = (
            spark.readStream
            .format("delta")
            .load("/opt/spark/warehouse/traffic_bronze")
        )

        # Data Quality Flag

        dq_df = bronze_df.withColumn(
            "dq_flag",
            when(col("vehicle_id").isNull(), "MISSING_VEHICLE")
            .when(col("event_time").isNull(), "MISSING_TIME")
            .when(col("raw_json").contains("CORRUPTED"), "CORRUPT_JSON")
            .otherwise("OK")
        )

        # Safe Type Casting

        typed = dq_df.withColumn(
            "speed_int",
            col("speed").cast("int")
        ).withColumn(
            "event_ts",
            to_timestamp("event_time")
        )

        # Business Validation Rules

        validated = typed.withColumn(
            "speed_valid",
            when((col("speed_int") >= 0) & (col("speed_int") <= 160), 1).otherwise(0)
        ).withColumn(
            "time_valid",
            when(col("event_ts") <= current_timestamp() + expr("INTERVAL 10 MINUTES"), 1).otherwise(0)
        )

        # Filter Good Records

        clean_stream = validated.filter(
            (col("dq_flag") == "OK") &
            (col("speed_valid") == 1) &
            (col("time_valid") == 1)
        )

        # Handle Late Data

        watermarked = clean_stream.withWatermark("event_ts", "15 minutes")

        # Deduplication

        deduped = watermarked.dropDuplicates(
            ["vehicle_id", "event_ts"]
        )

        # Feature Engineering

        silver_final = (
            deduped
            .withColumn("hour", hour("event_ts"))
            .withColumn("peak_flag",
                when((col("hour").between(8,11)) | 
                (col("hour").between(17,20)), 1).otherwise(0))
            .withColumn("speed_band",
                when(col("speed_int") <30, "LOW")
                .when(col("speed_int") < 70, "MEDIUM")
                .otherwise("High"))
        )
        # Write Silver Table

        silver_query = (
            silver_final.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", "/opt/spark/warehouse/chk/traffic_silver")
            .option("path", "/opt/spark/warehouse/traffic_silver")
            .start()
        )

        spark.streams.awaitAnyTermination()

___________________________________________________________________________________________________________________________________________________________________________________________________________________
### 🧠 EXPLICACIÓN DEL CÓDIGO
___________________________________________________________________________________________________________________________________________________________________________________________________________________

📦 IMPORTS - Librerías necesarias

![Image](https://github.com/user-attachments/assets/e0568aa2-18bc-4324-99e4-3714751e5e49)

Importa SparkSession y todas las funciones y tipos de Spark SQL:

•	SparkSession - Punto de entrada para Spark.

•	col() - Referencia a columnas.

•	when() - Condicional (como IF en SQL).

•	to_timestamp() - Conversión a timestamp.

•	hour() - Extrae la hora de un timestamp.

•	current_timestamp() - Timestamp actual.


#### 🚀 CONFIGURACIÓN DE SPARK SESSION

![Image](https://github.com/user-attachments/assets/93731c63-3404-4473-9d53-213a33f3e670)

Crea una SparkSession para la capa Silver:

•	appName("TrafficSilverLayer") - Identifica la aplicación en la UI de Spark.

![Image](https://github.com/user-attachments/assets/a43ce1f4-2970-4191-8f24-fae17b59020c)

Conecta al cluster Spark en el puerto 7077 del master.

![Image](https://github.com/user-attachments/assets/4b2afcf9-56ad-4d05-a812-3699eaf4c789)

•	.config(...): Las dos líneas configuran las extensiones de Delta Lake, igual que en el código anterior, imprescindibles para leer y escribir en formato Delta.

•	DeltaSparkSessionExtension - Funcionalidades extra de Delta (MERGE, OPTIMIZE, etc.)

•	DeltaCatalog - Reemplaza el catálogo de Spark para manejar tablas Delta.

![Image](https://github.com/user-attachments/assets/bb904e11-65ce-4088-98db-39d430dc6b0e)

* Habilita Hive Metastore para que las tablas sean accesibles desde Hive.

![Image](https://github.com/user-attachments/assets/80a168c4-e5d7-433e-afc4-9ad004100825)

•	.getOrCreate(): Crea la sesión (o la recupera si ya existe una con ese nombre).

![Image](https://github.com/user-attachments/assets/71cb5f9c-f38a-4cdc-9766-bfe01a90d21e)

•	Configura los logs para que solo muestre advertencias (WARN) y errores, ignorando la infinidad de logs de INFO que genera Spark en segundo plano.

____________________________________________________________________________________________________________________________________________________________________________________________
**📡 LECTURA DEL STREAM BRONZE**

Código:

        bronze_df = (
            spark.readStream
            .format("delta")
            .load("/opt/spark/warehouse/traffic_bronze")
        )

•	spark.readStream: Indica que vamos a hacer una lectura en tiempo real (streaming).

•	.format("delta"): La magia de Delta Lake. En lugar de leer de Kafka, leemos directamente de la tabla Bronze que creamos en el código anterior.

•	.load(...): Spark monitorizará ese directorio. Cada vez que el trabajo de Bronze escriba un nuevo archivo, este trabajo de Silver lo detectará y lo leerá al instante.

•	Resultado: Un DataFrame de streaming que se actualiza con cada nuevo archivo en Bronze

📌 Diferencia clave: El productor escribe en Bronze, y este script detecta automáticamente los nuevos archivos y los procesa.

_____________________________________________________________________________________________________________________________________________________________________________________________________
**🏷️ DATA QUALITY FLAG (Bandera de Calidad)**

Código:

        dq_df = bronze_df.withColumn(
            "dq_flag",
            when(col("vehicle_id").isNull(), "MISSING_VEHICLE")
            .when(col("event_time").isNull(), "MISSING_TIME")
            .when(col("raw_json").contains("CORRUPTED"), "CORRUPT_JSON")
            .otherwise("OK")
        )

•	withColumn("dq_flag", ...): Crea una nueva columna para etiquetar la calidad de cada fila.

•	when(col("vehicle_id").isNull(), "MISSING_VEHICLE"): Si no hay ID de vehículo, le asigna la etiqueta "MISSING_VEHICLE".

•	.when(col("event_time").isNull(), "MISSING_TIME"): Si no hay marca de tiempo, le asigna "MISSING_TIME".

•	.when(col("raw_json").contains("CORRUPTED"), "CORRUPT_JSON"): Si el texto original contiene la palabra "CORRUPTED" (asumiendo que el sistema emisor así lo marca), le asigna "CORRUPT_JSON".

•	.otherwise("OK"): Si no cumple ninguna de las malas condiciones, la fila está bien ("OK").

📌 Propósito: Marcar registros problemáticos para saber qué datos están sucios y por qué.


**🔢 CONVERSIÓN SEGURA DE TIPOS (SAFE CASTING)**

Código:

        typed = dq_df.withColumn(
            "speed_int",
            col("speed").cast("int")
        ).withColumn(
            "event_ts",
            to_timestamp("event_time")
        )

•	withColumn("speed_int", col("speed").cast("int")): En la capa Bronze, la velocidad era un String (texto). Aquí se convierte a entero (int). Es seguro: si el texto fuera "N/A", el .cast("int") no hará explotar el código, simplemente pondrá un null en esa fila.

•	.withColumn("event_ts", to_timestamp("event_time")): Convierte la cadena de texto del tiempo (ej. "2023-10-25 15:30:00") a un verdadero objeto tipo Timestamp de Spark, necesario para hacer cálculos de tiempo después.

**✅ REGLAS DE VALIDACIÓN DE NEGOCIO**

Código:

        validated = typed.withColumn(
            "speed_valid",
            when((col("speed_int") >= 0) & (col("speed_int") <= 160), 1).otherwise(0)
        )


Valida que la velocidad esté en un rango lógico:

•	Condición: speed_int entre 0 y 160 km/h.

•	Si se cumple → 1 (válido).

•	Si no se cumple → 0 (inválido).

•	when((col("speed_int") >= 0) & (col("speed_int") <= 160), 1).otherwise(0): Regla de negocio: un coche no puede ir a velocidad negativa, ni a más de 160 km/h. Si está fuera de ese rango, es 0.


Código:

        .withColumn(
            "time_valid",
            when(col("event_ts") <= current_timestamp() + expr("INTERVAL 10 MINUTES"), 1).otherwise(0)
        )

Valida que el timestamp no sea del futuro:

•	current_timestamp() - Fecha/hora actual.

•	+ expr("INTERVAL 10 MINUTES") - Permite hasta 10 minutos en el futuro.

•	Condición: event_ts <= ahora + 10 minutos.

•	Si se cumple → 1 (válido).

•	Si no se cumple → 0 (inválido).

•	when(col("event_ts") <= current_timestamp() + expr("INTERVAL 10 MINUTES"), 1).otherwise(0): Regla de negocio: el tiempo del evento no puede estar más de 10 minutos en el futuro (esto detecta errores de reloj en los sensores GPS de los coches). Si el evento dice que ocurre dentro de más de 10 minutos, se marca como inválido (0).

**📌 Propósito:** Filtrar datos anómalos (velocidades extremas o fechas futuras).


**🧹 FILTRADO DE REGISTROS BUENOS**

Código:

        clean_stream = validated.filter(
            (col("dq_flag") == "OK") &
            (col("speed_valid") == 1) &
            (col("time_valid") == 1)
        )


•	.filter(...): Aquí es donde se descarta la basura. Solo dejan pasar las filas que pasaron las tres pruebas:

_______________________________________________________________________________________________________________________________________________________________
| Condición | Significado |
|-----------|-------------|
| dq_flag == "OK" |	No hay problemas de calidad (ID válido, timestamp presente, no corrupto) |
| speed_valid == 1 | Velocidad en rango lógico (0-160 km/h) |
| time_valid == 1 | Fecha no es futura (dentro de margen de 10 min) |
| Resultado: | Solo datos limpios y válidos pasan a la siguiente etapa. |

___________________________________________________________________________________________________________________________________________________________________
**⏰ MANEJO DE DATOS TARDÍOS (Watermark)**

Código:

         watermarked = clean_stream.withWatermark("event_ts", "15 minutes")
		 

•	.withWatermark("event_ts", "15 minutes"): En streaming, los datos llegan desordenados (un evento de las 8:00 puede llegar a las 8:20 por problemas de red). Esto le dice a Spark: "Establece una marca de agua de 15 minutos sobre la columna event_ts". Esto permite que Spark limpie su memoria interna y sepa cuándo un dato ha llegado demasiado tarde para ser procesado en agregaciones temporales.

_______________________________________________________________________________________________________________________________________________________________________
**🗑️ DEDUPLICACIÓN**

Código:

        deduped = watermarked.dropDuplicates(
            ["vehicle_id", "event_ts"]
        )

•	.dropDuplicates(["vehicle_id", "event_ts"]): Si por algún motivo (reintentos de red, micro-batches superpuestos) el mismo coche (vehicle_id) envía dos veces el evento exactamente en el mismo segundo (event_ts), esta línea elimina el duplicado, dejando solo uno.

•	**Nota técnica:** En streaming, dropDuplicates requiere obligatoriamente que se haya definido un <mark>withWatermark</mark> antes, de lo contrario Spark lanzará un error, porque necesita saber cuándo puede olvidar los eventos antiguos en su memoria.

__________________________________________________________________________________________________________________________________________________________________________________________________________________________
**🎨 INGENIERÍA DE CARACTERÍSTICAS (Feature Engineering)**

Código:

        silver_final = (
            deduped
            .withColumn("hour", hour("event_ts"))
        )

Extrae la hora del timestamp:

•	hour("event_ts") - Obtiene la hora (0-23) del timestamp.

•	Útil para análisis por hora del día.


![Image](https://github.com/user-attachments/assets/f4824d98-ee1b-49a0-be31-58942e70e368)

Identifica horas pico:

•	Horas pico: 8-11 AM y 5-8 PM (hora laboral).

•	between(8,11) - Incluye 8, 9, 10, 11.

•	between(17,20) - Incluye 17, 18, 19, 20.

•	1 = Horario pico, 0 = Fuera de pico.

![Image](https://github.com/user-attachments/assets/281616be-97b6-4d47-99cc-0dd54c92c395)

Categoriza la velocidad en bandas:

•	< 30 km/h → "LOW" (tráfico lento/congestión).

•	30-70 km/h → "MEDIUM" (flujo normal).

•	>= 70 km/h → "High" (tráfico fluido).

___________________________________________________________________________________________________________________________________________________________________________________________________________________________

**💾 ESCRITURA EN SILVER (Capa Limpia)**

Código:

        silver_query = (
            silver_final.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", "/opt/spark/warehouse/chk/traffic_silver")
            .option("path", "/opt/spark/warehouse/traffic_silver")
            .start()
         )
		 
•	silver_final.writeStream: Inicia la escritura del stream resultante.

•	.format("delta"): Lo guarda en formato Delta Lake.

•	.outputMode("append"): Como estamos filtrando filas y no haciendo agregaciones (como sumas o promedios), usamos "append" (añadir las filas limpias nuevas).

•	.option("checkpointLocation", ...): Vital. Guarda el progreso en una ruta diferente a la del Bronze para que este trabajo sea independiente y tolerante a fallos.

•	.option("path", ...): Ruta física donde quedarán guardados los datos ya limpios y enriquecidos (la tabla Silver).

•	.start(): Dispara la ejecución en segundo plano.

__________________________________________________________________________________________________________________________________________________________________________________________________________________________
**🔥 MANTENER EL PROCESO VIVO**

Código:

        spark.streams.awaitAnyTermination()


Espera indefinidamente hasta que el stream termine o sea detenido.

___________________________________________________________________________________________________________________________________________________________________________________________________________________________
**📊 FLUJO DE TRANSFORMACIONES**

![Image](https://github.com/user-attachments/assets/75326cc0-9e04-4889-891b-4e7779b224c3)


* Ahora volviendo al proceso de ejecución,regresamos a la terminal y corremos el siguiente código.
  
Código:

         docker exec -it spark-worker /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /opt/spark-apps/traffic_silver.py

![Image](https://github.com/user-attachments/assets/97e6ea30-3916-433a-af68-38b686f20fae)

Luego verificamos que se esté ejecutando en VSC la carpeta traffic_silver/_delta_log.

![Image](https://github.com/user-attachments/assets/62b52dff-9183-4a69-9acc-053b724f6027)


___________________________________________________________________________________________________________________________________________________________________________________________________________________________
###  ![Imagen](https://github.com/user-attachments/assets/4d8e0754-1ce5-4252-9586-f1762341f26b) CAPA DE ORO / GOLDEN LAYER
___________________________________________________________________________________________________________________________________________________________________________________________________________________________

Código:

        from pyspark.sql import SparkSession
        from pyspark.sql.functions import *

        # Spark Session Config

        spark = (
            SparkSession.builder
            .appName("TrafficGoldLayer")
            # cluster master
            .master("spark://spark-master:7077")
            # delta lake
            .config("spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .enableHiveSupport()
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")

        # Read Silver Silver

        silver_stream = (
            spark.readStream
            .format("delta")
            .load("/opt/spark/warehouse/traffic_silver")
        )

        # Dimension Zone

        dim_zone = silver_stream.select(
            "city_zone"
        ).dropDuplicates() \
        .withColumn(
            "zone_type",
            when(col("city_zone") == "CBD", "Commercial")
            .when(col("city_zone") == "TECHPARK", "IT HUB")
            .when(col("city_zone").isin("AIRPORT", "TRAINSTATION"), "Transit Hub")
            .otherwise("Residential")
        ) \
        .withColumn(
            "traffic_risk",
            when(col("city_zone").isin("CBD", "AIRPORT", "TRAINSTATION"), "HIGH")
            .when(col("city_zone") == "TECHPARK", "MEDIUM")
            .otherwise("LOW")
        )

        zone_query = (
            dim_zone.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", "/opt/spark/warehouse/chk/dim_zone")
            .option("path", "/opt/spark/warehouse/dim_zone")
            .start()
        )

        # Dimesion Road

        dim_road = silver_stream.select(
            "road_id"
        ).dropDuplicates() \
        .withColumn(
            "road_type" ,
            when(col("road_id").isin("R100", "R200"), "Highway")
            .otherwise("City Road")
        ) \
        .withColumn(
             "speed_limit",
             when(col("road_id").isin("R100", "R200"), 100)
            .otherwise(60)
        )

        road_query = (
            dim_road.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", "/opt/spark/warehouse/chk/dim_road")
            .option("path", "/opt/spark/warehouse/dim_road")
            .start()
        )

        # Fact Table

        fact_stream = silver_stream.select(
            "vehicle_id",
            "road_id",
            "city_zone",
            "speed_int",
            "congestion_level",
            "event_ts",
            "peak_flag",
            "speed_band",
            "hour",
            "weather"
        )

        fact_enriched = fact_stream.withColumn("date", to_date("event_ts"))

        fact_query = (
            fact_enriched.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", "/opt/spark/warehouse/chk/fact_traffic")
            .option("path", "/opt/spark/warehouse/fact_traffic")
            .start()
        )

        spark.streams.awaitAnyTermination()

__________________________________________________________________________________________________________________________________________________________________________________________________________________________
### 🧠 Explicación del código.

Les explico este código línea por línea. Es un pipeline de streaming en Spark que construye una capa Gold siguiendo el modelo estrella (Star Schema) con dimensiones y una tabla de hechos para análisis de tráfico.


📦 IMPORTS - Librerías necesarias


Código:


        from pyspark.sql import SparkSession
        from pyspark.sql.functions import *

Importa SparkSession y todas las funciones de Spark SQL:

•	SparkSession - Punto de entrada para Spark.

•	col() - Referencia a columnas.

•	when() - Condicional (IF en SQL).

•	isin() - Verifica si un valor está en una lista.

•	to_date() - Convierte a fecha.

•	dropDuplicates() - Elimina duplicados.

Nota: No importa types porque no se definen esquemas explícitos.

___________________________________________________________________________________________________________________________________________________________________________________________________________________
**🚀 CONFIGURACIÓN DE SPARK SESSION**


Código:

        spark = (
            SparkSession.builder
            .appName("TrafficGoldLayer")
            .master("spark://spark-master:7077")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .enableHiveSupport()
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")


•	Es la misma configuración de clúster y Delta Lake que en las capas anteriores. El único cambio es el .appName("TrafficGoldLayer") para identificar este proceso específico en la interfaz de monitoreo.

_______________________________________________________________________________________________________________________________________________________________________________________________________________________
**📡 LECTURA DE LA CAPA SILVER**

Código:

        silver_stream = (
            spark.readStream
            .format("delta")
            .load("/opt/spark/warehouse/traffic_silver")
        )

•	Inicia la lectura en tiempo real desde la tabla Silver que creamos en el código anterior. Spark vigilará esta carpeta para procesar los datos limpios a medida que llegan.
Lee los datos limpios de la capa Silver como un stream continuo:

•	spark.readStream - Lectura en streaming.

•	.format("delta") - Fuente en formato Delta Lake.

•	.load("/opt/spark/warehouse/traffic_silver") - Ruta donde está la tabla Silver.

**📌 Importante:** Este script detecta automáticamente nuevos datos en Silver y los procesa en tiempo real para construir la capa Gold.

___________________________________________________________________________________________________________________________________________________________________________________________________________________________
**🏗️ ARQUITECTURA STAR SCHEMA (Modelo Estrella)**
___________________________________________________________________________________________________________________________________________________________________________________________________________________________

Este código construye un modelo estrella típico de data warehousing:

![Image](https://github.com/user-attachments/assets/c4fcc769-4b66-4c1b-ad16-f7e1a4504703)

Componentes:

1.	Tablas de Dimensión (dim_zone, dim_road) - Atributos descriptivos.
   
2.	Tabla de Hechos (fact_traffic) - Mediciones y métricas.
   
___________________________________________________________________________________________________________________________________________________________________________________________________________________________
**🏙️ DIMENSIÓN 1: ZONA (dim_zone)**
___________________________________________________________________________________________________________________________________________________________________________________________________________________________
Código:

        dim_zone = silver_stream.select(
            "city_zone"
        ).dropDuplicates()

•	.select("city_zone"): Selecciona únicamente la columna de la zona de la ciudad.

•	.dropDuplicates(): Como muchos vehículos estarán en la misma zona al mismo tiempo, esto asegura que nuestra tabla de dimensiones solo tenga una fila por zona única.

**📌 Resultado:** Lista única de zonas: ["CBD", "AIRPORT", "TECHPARK", "SUBURB", "TRAINSTATION"]


Código:

        .withColumn(
            "zone_type",
            when(col("city_zone") == "CBD", "Commercial")
            .when(col("city_zone") == "TECHPARK", "IT HUB")
            .when(col("city_zone").isin("AIRPORT", "TRAINSTATION"), "Transit Hub")
            .otherwise("Residential")
         )

* Crea una nueva columna categorizando el tipo de zona basándose en reglas de negocio.
  
Clasifica cada zona según su propósito:

| Zona | Tipo |
|------|------|
| CBD  | "Commercial" (Comercial) |
| TECHPARK | "IT HUB" (Centro tecnológico) |
| AIRPORT, TRAINSTATION | "Transit Hub" (Nodo de transporte) |
| Otras (ej: "SUBURB") | "Residential" (Residencial) |


Añadir columna traffic_risk (Riesgo de Tráfico)

Código:

        .withColumn(
             "traffic_risk",
             when(col("city_zone").isin("CBD", "AIRPORT", "TRAINSTATION"), "HIGH")
            .when(col("city_zone") == "TECHPARK", "MEDIUM")
            .otherwise("LOW")
        )

•	Crea otra columna de negocio que asigna un nivel de riesgo de tráfico a cada zona. 


Asigna nivel de riesgo de congestión:


| Zona | Riesgo |
|------|--------|
| CBD, AIRPORT, TRAINSTATION | "HIGH" (Alto - zonas concurridas) |
| TECHPARK | "MEDIUM" (Medio) |
| Otras (ej: "SUBURB") | "LOW" (Bajo) |


Escribir dimensión dim_zone

Código:

        zone_query = (
            dim_zone.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", "/opt/spark/warehouse/chk/dim_zone")
            .option("path", "/opt/spark/warehouse/dim_zone")
            .start()
        )

Guarda la dimensión zona en Delta Lake:

•	Formato: Delta Lake.

•	Modo: Append (añadir nuevas zonas).

•	Checkpoint: /opt/spark/warehouse/chk/dim_zone (para recuperación).

•	Ruta: /opt/spark/warehouse/dim_zone.


**📌 Resultado de dim_zone:**

| city_zone | zone_type | traffic_risk |
|-----------|-----------|--------------|
| CBD |	Commercial | HIGH |
| AIRPORT | Transit Hub | HIGH |
| TECHPARK | IT HUB | MEDIUM |
| SUBURB | Residential | LOW |
| TRAINSTATION | Transit Hub | HIGH |

•	.start() arranca este primer micro-proceso en segundo plano. Se guarda en la variable zone_query para que Spark lo mantenga vivo.

___________________________________________________________________________________________________________________________________________________________________________________________________________________________
**🛣️ DIMENSIÓN 2: CARRETERA (dim_road)**
___________________________________________________________________________________________________________________________________________________________________________________________________________________________
Código:

        dim_road = silver_stream.select(
            "road_id"
        ).dropDuplicates()

Selecciona y limpia los IDs de carreteras:

•	select("road_id") - Solo la columna road_id.

•	.dropDuplicates() - Lista única de carreteras.

**📌 Resultado: Lista única: ["R100", "R200", "R300", "R400"]**



Añadir columna road_type (Tipo de Carretera)


Código:

        .withColumn(
            "road_type",
            when(col("road_id").isin("R100", "R200"), "Highway")
            .otherwise("City Road")
        )


Clasifica carreteras según su tipo:

•	R100, R200 → "Highway" (Autopista).

•	R300, R400 → "City Road" (Vía urbana).


Añadir columna speed_limit (Límite de Velocidad).

Código:

        .withColumn(
            "speed_limit",
            when(col("road_id").isin("R100", "R200"), 100)
            .otherwise(60)
         )

Asigna límite de velocidad por tipo de vía:

•	R100, R200 → 100 km/h (autopista).

•	R300, R400 → 60 km/h (vía urbana).


Escribir dimensión dim_road

Código:

        road_query = (
            dim_road.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", "/opt/spark/warehouse/chk/dim_road")
            .option("path", "/opt/spark/warehouse/dim_road")
            .start()
        )


Guarda la dimensión carretera en Delta Lake.

**📌 Resultado de dim_road:**

| road_id | road_type | speed_limit |
|---------|-----------|-------------|
| R100 | Highway | 100 |
| R200 | Highway | 100 |
| R300 | City Road | 60 |
| R400 | City Road | 60 |

___________________________________________________________________________________________________________________________________________________________________________________________________________________________
**📊 TABLA DE HECHOS (fact_traffic)**
___________________________________________________________________________________________________________________________________________________________________________________________________________________________
Código:

        fact_stream = silver_stream.select(
            "vehicle_id",
            "road_id",
            "city_zone",
            "speed_int",
            "congestion_level",
            "event_ts",
            "peak_flag",
            "speed_band",
            "hour",
            "weather"
        )

Selecciona las columnas relevantes de Silver para la tabla de hechos:

| Columna | Tipo | Descripción |
|---------|------|-------------|
| vehicle_id | String | ID del vehículo |
| road_id | String | ID de la carretera (clave foránea) |
| city_zone | String | Zona (clave foránea) |
| speed_int | Integer | Velocidad en km/h |
| congestion_level | Integer | Nivel de congestión (1-5) |
| event_ts | Timestamp | Fecha y hora del evento |
| peak_flag | Integer | 1=Hora pico, 0=Fuera de pico |
| speed_band | String | LOW/MEDIUM/HIGH |
| hour | Integer | Hora del día (0-23) |
| weather |	String | Condición climática |


Añadir columna date (Fecha)

Código:

        fact_enriched = fact_stream.withColumn("date", to_date("event_ts"))


Extrae la fecha del timestamp:

•	to_date("event_ts") - Convierte timestamp a solo fecha.

•	Útil para agregaciones diarias (ej: tráfico por día).

📌 Ejemplo: event_ts = "2026-06-20 15:30:45" → date = "2026-06-20"

___________________________________________________________________________________________________________________________________________________________________________________________________________________________

Escribir tabla de hechos fact_traffic

Código:

        fact_query = (
            fact_enriched.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", "/opt/spark/warehouse/chk/fact_traffic")
            .option("path", "/opt/spark/warehouse/fact_traffic")
            .start()
        )

Guarda la tabla de hechos en Delta Lake.

**📌 Resultado de fact_traffic:**

![Image](https://github.com/user-attachments/assets/c33ce119-2985-4f1e-ad8b-03df803db1c8)

___________________________________________________________________________________________________________________________________________________________________________________________________________________________
**🔁 ESPERA INDEFINIDA**

Código:

        spark.streams.awaitAnyTermination()

•	Muy importante aquí: A diferencia de los códigos anteriores donde solo había UN stream corriendo, en este script hemos lanzado TRES streams en paralelo (zone_query, road_query, y fact_query).

•	Esta línea le dice a Python: "No termines el programa. Quédate dormido aquí indefinidamente hasta que alguno de los tres streams falle o sea detenido manualmente". Si omitieras esta línea, el script de Python terminaría al instante y los tres procesos de escritura morirían.

___________________________________________________________________________________________________________________________________________________________________________________________________________________________
**🏛️ ARQUITECTURA COMPLETA (Medallion + Star Schema)**

![Image](https://github.com/user-attachments/assets/b11b464f-929d-4038-bea9-2318775fdb64)

* Ahora volvemos al proceso de ejecución del proyecto y hacemos correr la transformación de oro.

Código:

         docker exec -it spark-worker /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /opt/spark-apps/traffic_gold.py 


![Image](https://github.com/user-attachments/assets/805c566f-2ef1-4bd4-8393-f9f03ae3159a)

Ahora verificamos en VSC que se hayan creado las carpetas de hechos y dimensiones (fact_traffic, dim_road y dim_zone).

![Image](https://github.com/user-attachments/assets/200cc4d8-854b-4227-9f17-1f1d99207541)

___________________________________________________________________________________________________________________________________________________________________________________________________________________________
### ![Image](https://github.com/user-attachments/assets/c6a6426a-947d-4418-ba0f-116e2d36675c) METADATA DATABASE 
___________________________________________________________________________________________________________________________________________________________________________________________________________________________

Luego volvemos al terminal y abrimos una terminal interactiva (consola de comandos) directamente dentro del contenedor Docker llamado spark-worker. 

Código:
         
		docker exec -it spark-worker bash

![Image](https://github.com/user-attachments/assets/b3d67c5e-8e48-4c0a-b122-47889ca82dff)

Código:
                
		mkdir -p /tmp/spark-warehouse

Código:
                
		chmod -R 777 /tmp/spark-warehouse


**🧰 Desglose Rápido del código:**

| Parte | Significado |
|-------|-------------|
| chmod | Change Mode - Cambia permisos de archivos/directorios |
| -R | Recursive - Aplica cambios a TODOS los archivos y subcarpetas dentro |
| 777 | Permisos: Todos pueden LEER, ESCRIBIR y EJECUTAR |
| /tmp/spark-warehouse | Directorio objetivo |


Ahora nos conectamos a base de datos de **metadatos hive metastore**.

Código:

        /opt/spark/bin/spark-sql \
        --packages io.delta:delta-spark_2.12:3.2.0 \
        --conf spark.jars.ivy=/tmp/.ivy \
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
        --conf spark.sql.catalogImplementation=hive \
        --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
        --conf spark.sql.warehouse.dir=/tmp/spark-warehouse


_________________________________________________________________________________________________________________________________________________________________________________________________________________________
**📖 Desglose Línea por Línea**

1. Comando Principal:
   
Código:

        /opt/spark/bin/spark-sql
		
¿Qué es? La herramienta de línea de comandos de Spark que permite ejecutar consultas SQL interactivas.

📌 Equivalente a: "Abre una consola SQL de Spark"

___________________________________________________________________________________________________________________________________________________________________________________________________________________________

2. Paquetes Requeridos:

Código:
             
		--packages io.delta:delta-spark_2.12:3.2.0
		
¿Qué hace? Descarga e incluye Delta Lake para trabajar con tablas Delta.

📌 Necesario porque: Spark no incluye Delta por defecto.
__________________________________________________________________________________________________________________________________________________________________________________________________________________________
3. Directorio de Dependencias:
   
Código:
   
		--conf spark.jars.ivy=/tmp/.ivy
		
¿Qué hace? Define dónde Spark guarda las dependencias descargadas (JARs).

📌 Beneficio: Acelera ejecuciones futuras (no descarga siempre lo mismo).
___________________________________________________________________________________________________________________________________________________________________________________________________________________________

4. Extensiones de SQL:

Código:

        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension

¿Qué hace? Habilita comandos específicos de Delta Lake:

•	MERGE (UPSERT).

•	OPTIMIZE (compactación de archivos).

•	VACUUM (limpieza de archivos viejos).

•	CONVERT TO DELTA (convertir formatos).

📌 Sin esto: No funcionarían los comandos de Delta.

___________________________________________________________________________________________________________________________________________________________________________________________________________________________

5. Catálogo de Spark:
   
Código:

        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
		
¿Qué hace? Configura el catálogo de Spark para usar Delta Catalog en lugar del catálogo predeterminado.

📌 Equivalente a: "Trata las tablas Delta como tablas Spark normales."
___________________________________________________________________________________________________________________________________________________________________________________________________________________________
6. Soporte para Hive:
   
Código:

        --conf spark.sql.catalogImplementation=hive
		
¿Qué hace? Usa el catálogo de Hive para gestionar metadatos (tablas, columnas, particiones, etc.).

📌 Importante: Permite usar la terminal de Hive y compartir tablas con otras herramientas.
___________________________________________________________________________________________________________________________________________________________________________________________________________________________
7. URI del Hive Metastore:

Código:

        --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083

¿Qué hace? Conecta al Hive Metastore (el servicio que guarda los metadatos de las tablas).

📌 Ubicación: hive-metastore:9083 es el servicio que corre en tu Docker.
___________________________________________________________________________________________________________________________________________________________________________________________________________________________
8. Directorio del Warehouse:

Código:

       --conf spark.sql.warehouse.dir=/tmp/spark-warehouse

¿Qué hace? Define dónde Spark almacena las tablas de Hive.

📌 Temporal: Al estar en /tmp/, los datos se borran al reiniciar.
___________________________________________________________________________________________________________________________________________________________________________________________________________________________
🏗️ Arquitectura Completa
___________________________________________________________________________________________________________________________________________________________________________________________________________________________

![Image](https://github.com/user-attachments/assets/09869556-8988-44d0-bdf3-f9e8b9505b7e)

Ahora creamos una base de datos llamada traffic.

Código: 
              
		 CREATE DATABASE traffic;

![Image](https://github.com/user-attachments/assets/90096d4b-9be5-4db9-88a2-788c0983e3c0)

Código:
              
		USE traffic;

![Image](https://github.com/user-attachments/assets/805c226a-7b19-4351-b99d-88b9e9c07ac9)

Código:
               
		CREATE TABLE fact_traffic

Código:
               
		USING DELTA

Codigo:
                
		LOCATION '/opt/spark/warehouse/fact_traffic';


![Image](https://github.com/user-attachments/assets/5f1a4a90-2aa6-4fc4-ab5a-77482ad5eb9f)

Código:
               
		SELECT COUNT(*) FROM fact_traffic;


![Image](https://github.com/user-attachments/assets/fd90269f-72f6-401e-8925-5568bf7d425d)

Código:
                   
		CREATE TABLE dim_zone
        USING DELTA
        LOCATION '/opt/spark/warehouse/dim_zone';

![Image](https://github.com/user-attachments/assets/6179b204-4415-4764-8e02-4b50df9f11eb)

Código:
               
		SELECT COUNT(*) FROM dim_zone;

![Image](https://github.com/user-attachments/assets/a48fab58-4b05-44d4-a235-dfbff7c71e27)

Código:
                
		> CREATE TABLE dim_road
        > USING DELTA
        > LOCATION '/opt/spark/warehouse/dim_road';

![Image](https://github.com/user-attachments/assets/a7ff7698-e0e5-4e0d-948f-43e4fa863832)

Código:
               
		SELECT COUNT(*) FROM dim_road;

![Image](https://github.com/user-attachments/assets/6ead3a77-1f5f-42c4-9fa1-d6ec7f21050f)

* Ahora nos toca crear vistas para Power BI 

Código:

        CREATE VIEW bi_fact_traffic AS
        > SELECT
        > CAST(vehicle_id AS STRING) AS vehicle_id,
        > CAST(road_id AS STRING) AS road_id,
        > CAST(city_zone AS STRING) AS city_zone,
        > CAST(speed_int AS DOUBLE) AS speed,
        > CAST(congestion_level AS INT) AS congestion_level,
        > CAST(event_ts AS TIMESTAMP) AS event_time,
        > CAST(peak_flag AS STRING) AS peak_flag,
        > CAST(speed_band AS STRING) AS speed_band,
        > CAST(weather AS STRING) AS weather,
        > CAST(date AS DATE) AS event_date,
        > CAST(hour AS INT) AS event_hour
        > FROM fact_traffic;

___________________________________________________________________________________________________________________________________________________________________________________________________________________________
#### 🧠 Explicación del código.
___________________________________________________________________________________________________________________________________________________________________________________________________________________________
🎯 ¿Qué hace este código?

Crea una VISTA (VIEW) llamada bi_fact_traffic basada en la tabla fact_traffic, pero con:

1.	Tipos de datos explícitos (usando CAST).

2.	Nombres de columnas más claros (usando AS).

3.	Preparación para herramientas BI (Business Intelligence).
   
___________________________________________________________________________________________________________________________________________________________________________________________________________________________
**📖 Desglose Línea por Línea**
___________________________________________________________________________________________________________________________________________________________________________________________________________________________

1. Creación de la Vista:

sql:

     CREATE VIEW bi_fact_traffic AS

¿Qué hace?

•	CREATE VIEW → Crea una vista (tabla virtual).

•	bi_fact_traffic → Nombre de la vista.

•	AS → Define la consulta que genera la vista.

📌 Características de una VIEW:

•	No almacena datos físicamente.

•	Es como una "ventana" a los datos.

•	Se actualiza automáticamente cuando cambian los datos originales.

•	Es segura (puedes ocultar columnas sensibles).

___________________________________________________________________________________________________________________________________________________________________________________________________________________________
2. Selección y Casting de Columnas:

Columna 1: Vehicle ID

sql:

      CAST(vehicle_id AS STRING) AS vehicle_id

¿Qué hace?

•	CAST(vehicle_id AS STRING) → Convierte a STRING (texto).

•	AS vehicle_id → Mantiene el mismo nombre.

•	Propósito: Asegurar que sea texto (aunque ya lo era).

___________________________________________________________________________________________________________________________________________________________________________________________________________________________
Columna 2: Road ID

sql:

      CAST(road_id AS STRING) AS road_id

Similar: Asegura que sea STRING.
_____________________________________________________________________________________________________________________________________________________________________________________________________
Columna 3: City Zone

sql:

      CAST(city_zone AS STRING) AS city_zone

Similar: Asegura que sea STRING.
________________________________________________________________________________________________________________________________________________________________________________________________________
Columna 4: Speed (Velocidad)

sql:

      CAST(speed_int AS DOUBLE) AS speed

¿Qué hace?

•	CAST(speed_int AS DOUBLE) → Convierte a número decimal (doble precisión).

•	AS speed → Renombra a speed (más claro que speed_int).

•	Propósito: Permite cálculos precisos de promedio, sumas, etc.
______________________________________________________________________________________________________________________________________________________________________________________________________________
Columna 5: Congestion Level

sql:

      CAST(congestion_level AS INT) AS congestion_level

¿Qué hace?

•	CAST(...AS INT) → Convierte a entero.

•	Propósito: Niveles de congestión son números enteros (1-5)
____________________________________________________________________________________________________________________________________________________________________________________________________________________
Columna 6: Event Time

sql:

      CAST(event_ts AS TIMESTAMP) AS event_time

¿Qué hace?

•	CAST(...AS TIMESTAMP) → Convierte a tipo fecha-hora.

•	AS event_time → Renombra a event_time (más claro que event_ts).

•	Propósito: Para análisis temporales y series de tiempo.
___________________________________________________________________________________________________________________________________________________________________________________________________________________________
Columna 7: Peak Flag

sql:

      CAST(peak_flag AS STRING) AS peak_flag

¿Qué hace?

•	Convierte a STRING.

•	Alternativa: Podría ser BOOLEAN (verdadero/falso).

•	Nota: En tu código original era INT (1/0), ahora es STRING.
__________________________________________________________________________________________________________________________________________________________________________________________________________________________
Columna 8: Speed Band

sql:

      CAST(speed_band AS STRING) AS speed_band

Similar: Asegura que sea STRING (LOW/MEDIUM/HIGH).
_________________________________________________________________________________________________________________________________________________________________________________________________________________________
Columna 9: Weather

sql:

      CAST(weather AS STRING) AS weather

Similar: Asegura que sea STRING.
________________________________________________________________________________________________________________________________________________________________________________________________________________________
Columna 10: Event Date

sql:

      CAST(date AS DATE) AS event_date

¿Qué hace?

•	CAST(...AS DATE) → Convierte a tipo fecha (sin hora).

•	AS event_date → Renombra a event_date (más descriptivo).

•	Propósito: Agregaciones diarias, particionamiento, etc.
___________________________________________________________________________________________________________________________________________________________________________________________________________________________
Columna 11: Event Hour

sql:

     CAST(hour AS INT) AS event_hour

¿Qué hace?

•	CAST(...AS INT) → Convierte a entero:

•	AS event_hour → Renombra a event_hour (más claro).

•	Propósito: Análisis por hora del día.

___________________________________________________________________________________________________________________________________________________________________________________________________________________________
**🏗️ Fuente vs Vista**


**Tabla Original (fact_traffic):**


| vehicle_id  | road_id | city_zone | speed_int | congestion_level | event_ts | peak_flag | speed_band | weather | date | hour |
|-------------|---------|-----------|-----------|------------------|----------|-----------|------------|---------|------|------|
| abc123 | R100 | CBD | 45 | 3 | 2026-06-20 08:30:00 | 1 | MEDIUM | CLEAR | 2026-06-20 | 8 |



**Vista (bi_fact_traffic):**


| vehicle_id  | road_id | city_zone | speed | congestion_level | event_time | peak_flag | speed_band | weather | event_date | event_hour |
|-------------|---------|-----------|-------|------------------|------------|-----------|------------|---------|------------|------------|
| abc123 | R100 | CBD | 45.0 | 3  | 2026-06-20 08:30:00 | 1 | MEDIUM | CLEAR | 2026-06-20 | 8 |

__________________________________________________________________________________________________________________________________________________________________________________________________________________________
**🎯 ¿Por qué usar CAST explícito?**

1. Consistencia de Tipos:

sql:

      -- Sin CAST: tipos podrían variar
      SELECT speed_int FROM fact_traffic  -- Podría ser String o Int

      -- Con CAST: siempre DOUBLE
      SELECT CAST(speed_int AS DOUBLE) FROM fact_traffic  -- Siempre número decimal


2. Compatibilidad con Herramientas BI:
   
•	Tableau, Power BI, Looker esperan tipos específicos.

•	Vista asegura que siempre reciben el tipo correcto.

3. Rendimiento:

•	Tipos explícitos ayudan al optimizador de consultas.

•	Mejores planes de ejecución.

4. Documentación:
   
•	Los nombres claros (event_time vs event_ts) documentan el propósito

___________________________________________________________________________________________________________________________________________________________________________________________________________________

**📊 Comparación: Tabla vs Vista**

| Característica | Tabla (fact_traffic) | Vista (bi_fact_traffic) |
|----------------|----------------------|-------------------------|
| Almacenamiento | Físico en disco | Virtual (solo definición) |
| Datos	| Datos reales | Datos de la tabla transformados |
| Tipos | Mixtos (pueden ser genéricos) | Explícitos y consistentes |
| Nombres | Técnicos (speed_int, event_ts) | Amigables (speed, event_time) |
| Actualización | Por streams | Automática (al consultar) |
| Rendimiento | Más rápido para lectura | Similar (consulta subyacente) |

_________________________________________________________________________________________________________________________________________________________________________________________________________________________
**🎯 Resumen Rápido**

![Image](https://github.com/user-attachments/assets/55fcc526-5770-4755-8bb5-0c4875c4075d)

•	Luego creamos las otras 2 vistas.

Código:

       CREATE OR REPLACE VIEW bi_dim_zone AS
       SELECT
       CAST(city_zone AS STRING) AS city_zone,
       CAST(zone_type AS STRING) AS zone_type,
       CAST(traffic_risk AS STRING) AS traffic_risk
       FROM dim_zone;

Código:

       CREATE OR REPLACE VIEW bi_dim_road AS
       SELECT
       CAST(road_id AS STRING) AS road_id,
       CAST(road_type AS STRING) AS road_type,
       CAST(speed_limit AS INT) AS speed_limit
       FROM dim_road;

![Image](https://github.com/user-attachments/assets/6344d35e-9d0d-4050-8dcd-6d382f963271)

Ahora salimos de la conexión con exit.

![Image](https://github.com/user-attachments/assets/b71f8a79-d9dc-40e0-ad62-82ccd717299d)

Luego cambiamos de directorio 

Código:
               
		cd /opt/spark/jars

![Image](https://github.com/user-attachments/assets/b23c50ea-afcb-423e-baa2-8ac7a19bdd6c)

¿Qué hace?

Cambia el directorio de trabajo actual a la carpeta /opt/spark/jars dentro del sistema.

📌 Equivalente a: "Muévete a la carpeta donde Spark guarda sus librerías JAR"

Código:

         wget https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar
         wget https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar


¿Qué hace?

Descarga los archivos JAR de Delta Lake desde el repositorio central de Maven y los guarda en el directorio actual.
__________________________________________________________________________________________________________________________________________________________________________________________________________________________
Desglose Rápido:

| Parte | Significado |
|-------|-------------|
| wget | Comando para descargar archivos desde internet |
| repo1.maven.org | Repositorio central de Maven (donde están las librerías) |
| delta-spark_2.12-3.2.0.jar | Librería principal de Delta Lake para Spark (versión 3.2.0, compilada para Scala 2.12) |
| delta-storage-3.2.0.jar | Librería de almacenamiento de Delta (manejo de archivos) |

__________________________________________________________________________________________________________________________________________________________________________________________________________________________

¿Para qué sirve?

1.	Instalación manual de Delta Lake en Spark.
2.	Evitar descargas repetidas con --packages.
3.	Funciona sin internet (una vez descargados).
4.	Mismo efecto que usar --packages io.delta:delta-spark_2.12:3.2.0


código:

        /opt/spark/sbin/start-thriftserver.sh \
        --master spark://spark-master:7077 \
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
        --conf spark.sql.catalogImplementation=hive \
        --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
        --conf spark.sql.warehouse.dir=/opt/spark/warehouse


![Image](https://github.com/user-attachments/assets/feb26273-c5e6-4ac6-a289-3b06edf8fa03)

**🎯 ¿Qué hace este comando?**

Inicia el Thrift Server de Spark, que es un servidor JDBC/ODBC que permite que herramientas externas se conecten a Spark y ejecuten consultas SQL.

📌 Equivalente a: "Convierte Spark en un servidor de base de datos al que me puedo conectar con cualquier herramienta BI."

**📖 Desglose de Cada Parte**

1. Script de inicio:

Código:
               
		/opt/spark/sbin/start-thriftserver.sh

Script que inicia el servidor Thrift de Spark.
_________________________________________________________________________________________________________________________________________________________________________________________________________________________
2. Master del Cluster:

Código:
              
		--master spark://spark-master:7077

Conecta al cluster Spark para ejecutar las consultas.
__________________________________________________________________________________________________________________________________________________________________________________________________________________________
3. Extensiones de Spark SQL:

Código:

        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension

Habilita comandos de Delta Lake (OPTIMIZE, MERGE, VACUUM, etc.)
__________________________________________________________________________________________________________________________________________________________________________________________________________________________
4. Catálogo de Delta:

Código:

        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog

Usa el catálogo de Delta Lake para gestionar tablas.
__________________________________________________________________________________________________________________________________________________________________________________________________________________________
5. Soporte Hive:

Código:

        --conf spark.sql.catalogImplementation=hive

Usa el catálogo de Hive para metadatos.
_________________________________________________________________________________________________________________________________________________________________________________________________________________________
6. Ubicación del Hive Metastore:

Código:

        --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083

Conecta al metastore de Hive para compartir metadatos entre herramientas.
___________________________________________________________________________________________________________________________________________________________________________________________________________________________
7. Directorio del Warehouse:

Código:

         --conf spark.sql.warehouse.dir=/opt/spark/warehouse

Dónde se almacenan las tablas físicamente.

___________________________________________________________________________________________________________________________________________________________________________________________________________________________
### 📊 DASHBOARD - Power BI
___________________________________________________________________________________________________________________________________________________________________________________________________________________________

Comenzamos por abrir Power BI y, hacemos click en obtener datos y, en el buscador escribimos spark y seleccionamos.

![Image](https://github.com/user-attachments/assets/571f8d23-137e-4536-8225-e587f9e868b9)

Ahora usamos el nombre de nuestro servidor (localhost:10000) que tenemos definido en docker compose para conectar y, ponemos protocolo standard con modo de conectividad de datos DirectQuery.

![Image](https://github.com/user-attachments/assets/cf880272-5524-40d7-a702-6a12c9b1e943)

Luego creamos un usuario y contraseña, y por ultimo conectar.

![Image](https://github.com/user-attachments/assets/eb375262-9c0d-4950-9832-2133f33a77b4)

Y aquí solo podremos acceder a tablas y no a mesas.

![Image](https://github.com/user-attachments/assets/69fcc073-176f-4bd5-93f9-132d50d5f0f2)

Solo seleccionaremos las tres tablas y damos a cargar.

![Image](https://github.com/user-attachments/assets/ba4646f5-f215-47c3-8322-9c63a012adb8)

![Image](https://github.com/user-attachments/assets/4e66a472-d87a-4dd8-98a0-1d7f128b244e)

Ahora vamos a la vista del modelo.

![Image](https://github.com/user-attachments/assets/b033b346-1bf4-4885-a72a-8ea0a93cfec6)

Ahora generaremos las relaciones entre tablas, para ello, seleccionamos city_zone y lo arrastramos hasta city_zone de la tabla de hechos y, los mismo con la otra tabla, seleccionamos road_id y lo arrastramos a road_id de la tabla hechos, esto generará una relación de uno a muchos.

![Image](https://github.com/user-attachments/assets/16e34d52-b06c-4631-976f-086195f8ebe8)

![Image](https://github.com/user-attachments/assets/baec29a7-ec4b-4879-9e61-a968a13068b1)

![Image](https://github.com/user-attachments/assets/12195545-62f9-4600-b920-9989a022396b)

![Image](https://github.com/user-attachments/assets/f346aee2-a039-43ca-b91d-5d5ce3381d59)

