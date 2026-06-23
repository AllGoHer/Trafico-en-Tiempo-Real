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


### Ahora paso a explicar el código del docker-compose por cada container creado.

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



4.	En VSCode creamos la carpeta hive-conf y, dentro de ella creamos el archivo hive-site.xml ingresándole el siguiente código.

 código:
 
	         <configuration>

          <property> <name>javax.jdo.option.ConnectionURL</name> <value>jdbc:postgresql://postgres-metastore:5432/metastore</value> </property> <property> <name>javax.jdo.option.ConnectionDriverName</name>           <value>org.postgresql.Driver</value> </property> <property> <name>javax.jdo.option.ConnectionUserName</name> <value>hive</value> </property> <property> <name>javax.jdo.option.ConnectionPassword</name> <value>hive</value> </property> <property> <name>datanucleus.autoCreateSchema</name> <value>false</value> </property> <property> <name>hive.metastore.schema.verification</name> <value>true</value> </property> <property> <name>hive.metastore.uris</name> <value>thrift://hive-metastore:9083</value> </property> <property> <name>hive.metastore.warehouse.dir</name> <value>/opt/hive/warehouse</value> </property> </configuration>


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


