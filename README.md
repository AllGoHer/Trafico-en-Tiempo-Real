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
 ## 🏗️ Arquitectura Técnica
________________________________________________________________________________________________________________________________________________________________________________________________________________
 
![Image](https://github.com/user-attachments/assets/0c621b12-79e1-4d56-95b0-bfbf6b87ff38)

┌─────────────────────────────────────────────────────────────────────────────────┐
│                           PIPELINE DE DATOS EN TIEMPO REAL                      │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌─────────────────────┐      ┌─────────────────────┐      ┌─────────────────┐  │
│  │   CAPA DE INGESTA   │      │   CAPA DE PROCESO   │      │  CAPA DE DATOS  │  │
│  ├─────────────────────┤      ├─────────────────────┤      ├─────────────────┤  │
│  │                     │      │                     │      │                 │  │
│  │  Producer (Python)  │      │   Spark Streaming   │      │   Delta Lake    │  │
│  │  ─────────────────  │      │   ────────────────  │      │   ────────────  │  │
│  │  • Faker + UUID     │      │   • Struct Streaming│      │   • ACID        │  │
│  │  • Datos sucios     │───▶ │   • Checkpointing   │───▶  │   • Time Travel │  │
│  │    (70% limpio,     │      │   • Watermarking    │      │   • Schema      │  │
│  │     30% corrupto)   │      │   • Deduplicación   │      │     Evolution   │  │
│  │                     │      │                     │      │                 │  │
│  └─────────────────────┘      └─────────────────────┘      └─────────────────┘  │
│           │                            │                            │           │
│           ▼                            ▼                            ▼           │
│  ┌─────────────────────────────────────────────────────────────────────────────┐│
│  │                      ARQUITECTURA MEDALLION                                 ││
│  │                                                                             ││
│  │  ┌──────────────────┐     ┌──────────────────┐     ┌─────────────────────┐  ││
│  │  │     BRONZE       │     │     SILVER       │     │       GOLD          │  ││
│  │  │   (Raw Data)     │───▶│   (Clean Data)   │──▶  │  (Star Schema)      │  ││
│  │  │                  │     │                  │     │                     │  ││
│  │  │ • Kafka Ingest   │     │ • Data Quality   │     │ ┌────────┐ ┌───────┐│  ││
│  │  │ • Raw JSON       │     │ • Type Casting   │     │ │dim_zone│ │dim_road│  ││ 
│  │  │ • Immutable      │     │ • Validation     │     │ └───┬────┘ └──┬────┘│  ││
│  │  │   Storage        │     │ • Deduplication  │     │     │         │     │  ││
│  │  └──────────────────┘     │ • Feature Eng.   │     │     ▼         ▼     │  ││
│  │                           └──────────────────┘     │   ┌───────────────┐ │  ││
│  │                                                    │   │ fact_traffic  │ │  ││
│  │                                                    │   └───────────────┘ │  ││
│  │                                                    └─────────────────────┘  ││
│  └─────────────────────────────────────────────────────────────────────────────┘│
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────────┐│
│  │                      CAPA DE CATALOGACIÓN Y VISUALIZACIÓN                   ││
│  │                                                                             ││
│  │  ┌──────────────────┐                    ┌──────────────────────────────┐   ││
│  │  │  Hive Metastore  │                    │         Power BI             │   ││
│  │  │  ──────────────  │                    │         ──────────           │   ││
│  │  │  • PostgreSQL    │                    │  • Conexión JDBC/ODBC        │   ││
│  │  │  • Thrift URI    │                    │  • Modelado Semántico        │   ││
│  │  │  • Schema        │                    │  • Visualización Avanzada    │   ││
│  │  │    Registry      │                    │  • DAX / Medidas             │   ││
│  │  └──────────────────┘                    └──────────────────────────────┘   ││
│  └─────────────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────────┘

![Image](https://github.com/user-attachments/assets/aa9ce6c2-4a88-4f1b-bd71-d73ccc5f32aa)

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
