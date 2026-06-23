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


____________________________________________________________________________________________________________________________________________________________________________________________________________________________
## 👨‍💻 Author
____________________________________________________________________________________________________________________________________________________________________________________________________________________________

**Allan Gonzales (All GoHer)**

Data Engineer | Streaming Data Engineer | Spark | Kafka | Delta Lake | Databricks | Airflow | Lakehouse Architecture

*"Creación de plataformas de datos en tiempo real escalables, evento a evento."*

📧 Contacto
Para preguntas, sugerencias o colaboraciones:

Email: [allgoher007@gmail.com]

LinkedIn: [  ]

GitHub: [https://github.com/AllGoHer]

____________________________________________________________________________________________________________________________________________________________________________________________________________________________
## 🧠 DESARROLLO DEL PROYECTO
____________________________________________________________________________________________________________________________________________________________________________________________________________________________

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

____________________________________________________________________________________________________________________________________________________________________________________________________________________________
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

   o	hive-site.xml: Sobreescribe el archivo de configuración nativo de Hive para que se conecte a tu PostgreSQL, o a un S3/MinIO si estás usando el optimizador de KRaft.

   o	warehouse: Es la carpeta donde se guardan los datos reales si usaras Hive nativo. Aquí es donde Spark escribe los datos procesados.

•	**El nombre de la variable SERVICE_NAME: metastore: Es un identificador interno. En sistemas Hadoop clásicos, puedes tener varios Metastores (uno para desarrollo, otro para producción). Aquí tienes uno solo.


**3. SPARK MASTER (El Gerente del clúster)**

•	La tecnología: Usas la imagen de Apache Spark 3.5.1.

•	El propósito: Es el "Director de Orquestación". Asigna las tareas a los Workers y mantiene el estado global de la aplicación Spark.

•	**El comando command: Le dice a la máquina: "Arranca el proceso Maestro y quédate mirando los logs".

•	Puerto 8080: Exposición del servidor web de la interfaz de usuario de Spark.

•	Puerto 7077: EL PUERTO CLAVE DE COMUNICACIÓN INTERNA. Los Workers usarán este puerto para reportarse con el Master y pedir trabajo.

•	Los Volumes:

   o	warehouse: Comparte la carpeta de datos con los Workers. Si el Master necesita procesar algo, lo hace directamente en este directorio sin pasar por la red. A esto se le llama Compute o Shuffle I/O Local. Es extremadamente rápido.

   o	apps: Donde pones tus scripts .py de Airflow o Scala.

   o	spark-ivy: Almacena las dependencias de Java (la librería de Spark) de forma cacheada para no descargarlas cada vez que mandas un trabajo.

•	hostname: spark-master: Fija el nombre de la máquina. Los Workers lo usarán para conectarse al puerto 7077.

**4. SPARK WORKER (Los Operarios)**

•	La tecnología: Misma versión de Spark (3.5.1).

•	El propósito: Son los que ejecutan el código real (Transformaciones, Joins, GroupBys). Son los que consumen la memoria y la CPU.

•	**El comando command: Le dice al Worker: "Conéctate al Master en el puerto 7077. Usa 2 núcleos (cores) y 2GB de RAM". "Quédate mirando los logs".

•	Puertos del Worker:

   o	4040: Puerto interno por si un trabajo se desborda.

   o	10000: Puerto para el backend del Spark UI (si lo usas).

   o	8081: Puerto por defecto para métricas internas.

•	depends_on - spark-master: Un Worker no puede trabajar sin un Master vivo. Tiene que esperar a que el Master esté 100% arriba.


**5. KAFKA (El Transportador)**

•	La tecnología: Usas apache/kafka:latest (Modo KRaft).

•	El propósito: Mover datos masivos a velocidad extrema entre sistemas diferentes, sin que nadie pierda datos. En este entorno, suele usarse para transportar los datos crudos antes de que Spark los procese.

•	KRaft (La innovación): Tradicionalmente, Kafka necesitaba un clúster de ZooKeeper aparte solo para saber quién es el "Jefe" (Controller). KRaft elimina ZooKeeper. Aquí ves cómo configurar el modo combinado: Este único contenedor actúa como nodo de red (broker) Y como "Jefe" (controller) al mismo tiempo.

•	Puertos:

   o	9092: Usado para la comunicación interna entre los contenedores Spark.

   o	29092: Expuesto a tu computadora local (host.docker.internal:29092) para que tus scripts de Python en Windows puedan leer o escribir en Kafka sin instalar Kafka en tu PC.

•	**CONTROLLER (Puerto 9093): Es un canal oculto que Kafka usa internamente para que los nodos se pongan de acuerdo en cuanto a quién es el "Jefe".

•	ADVERTISED_LISTENERS: Aquí está el truco de red. Si te conectas desde Windows, Kafka te dirá: "Yo soy 'host.docker.internal:29092, conéctate por ahí". Si te conectas internamente (Docker a Docker), te dirá: "Yo soy 'kafka:9092, conéctate aquí".

•	**KAFKA_LOG_DIRS: Donde Kafka guarda internamente los datos en el contenedor (en /tmp/...).


**6. KAFKA UI (El Visor)**

•	La tecnología: Una herramienta web de terceros (provectuslabs/kafka-ui:Puerto 8090 en tu código).

•	**depends_on - kafka: No tiene sentido arrancar la interfaz si Kafka no existe.

•	**KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092: Le dice a la UI: "Para ver los tópicos, conéctate internamente al puerto interno de Kafka".

____________________________________________________________________________________________________________________________________________________________________________________________________________________________

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

____________________________________________________________________________________________________________________________________________________________________________________________________________________________
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
## ![Image](https://github.com/user-attachments/assets/5771a008-2cfb-42b1-82b6-e40cd70bcddf) CAPA DE BRONCE ([traffic_bronze.py]())
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

#### 🔄 TRANSFORMACIÓN DE DATOS

![Image](https://github.com/user-attachments/assets/0c6e3c59-9cfe-42ff-a26c-66591ca43c44)

Convierte y selecciona campos:

•	selectExpr: Permite usar expresiones SQL directamente sobre el stream.

•	CAST(value AS STRING): convierte el arreglo de bytes (el payload de Kafka) en una cadena de texto legible, renombrándola como raw_json.

•	timestamp as kafka_timestamp: Renombra el timestamp de Kafka (cuando llegó el mensaje al broker) para distinguirlo del timestamp del evento en sí (que vendrá dentro del JSON).

•	Resultado: DataFrame con dos columnas: raw_json (string) y kafka_timestamp.


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

#### 🔁 ESPERA INDEFINIDA

![Image](https://github.com/user-attachments/assets/cfaff03c-97f1-43d5-a07e-af1a655372c4)

Espera hasta que el stream termine:

•	spark.streams - Obtiene todas las queries activas.

•	awaitAnyTermination() - Bloquea el programa.

•	Espera indefinidamente hasta que un stream falle o sea detenido manualmente.

•	Importante: Sin esto, el programa terminaría inmediatamente y el stream no procesaría nada.


#### 🎯 ARQUITECTURA COMPLETA


Kafka (traffic-topic) (Binario)
    ↓
Spark ReadStream 
    ↓
JSON Parse (con esquema flexible)
    ↓
Data Aplanamiento
    ↓
Delta Lake (Guardar crudo + metadata = Bronze layer)



* Volviendo al proceso del proyecto, abrimos el spark master en el navegador web y escribimos localhost:8080 


![Image](https://github.com/user-attachments/assets/3da30a1b-a632-490b-917f-91534d90d2c7)

![Image]()

![Image]()

![Image]()

![Image]()




