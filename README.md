# 🚦Trafico-en-Tiempo-Real

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

![Image]()
