# End-to-End Flight Analytics Pipeline on Databricks Lakehouse

[![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-orange?logo=databricks)](https://databricks.com)
[![PySpark](https://img.shields.io/badge/PySpark-Structured_Streaming-red?logo=apachespark)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-ACID_Transactions-blue?logo=delta)](https://delta.io/)
[![DLT](https://img.shields.io/badge/Lakeflow-Declarative_Pipelines_(DLT)-green)](https://docs.databricks.com/gcp/en/ldp/concepts/)
[![Architecture](https://img.shields.io/badge/Architecture-Medallion_(Bronze_Silver_Gold)-purple)](#medallion-architecture)

This end-to-end project ingests, cleanses, dynamically models, and transforms raw airline operations data (flights, passengers, bookings, airports) into a high-performance Star Schema in the Gold layer.

---

## Architecture Overview

The system strictly adheres to the **Medallion Architecture** (*Bronze → Silver → Gold*) to guarantee high data quality, transactional reliability, and scalable downstream query performance.

```text
+-----------------------------------------------------------------------------------+
|                                 SOURCE DATA                                       |
|                  (Flight Telemetry, Bookings CSV/JSON, Stream Logs)               |
+-----------------------------------------------------------------------------------+
                                          |
                                          v  [Auto Loader / cloudFiles]
+-----------------------------------------------------------------------------------+
| BRONZE LAYER: Incremental Streaming Raw Ingestion                                 |
| - Databricks Auto Loader + PySpark Streaming (Trigger.AvailableNow / Continuous)  |
| - Schema evolution, drift protection & rescued data column (`_rescued_data`)      |
+-----------------------------------------------------------------------------------+
                                          |
                                          v  [Lakeflow Declarative Pipelines / DLT]
+-----------------------------------------------------------------------------------+
| SILVER LAYER: Cleansed & Enriched Delta Tables                                    |
| - Automated Quality Enforcement via DLT Expectations (`@dlt.expect_all_or_drop`)  |
| - Deduplication, Timestamp normalization, CDC Event Ordering                      |
+-----------------------------------------------------------------------------------+
                                          |
                                          v  [Dynamic Builders & Databricks Jobs]
+-----------------------------------------------------------------------------------+
| GOLD LAYER: Analytical Star Schema (Dimensional Modeling)                         |
| - Dynamic Dimension Builder: Metadata-driven generation                           |
| - Dynamic Fact Builder: Automated SK resolution, dynamic join matrix              |
| - CDC Upsert & SCD Logic: SCD Type 1 & Type 2 via `APPLY CHANGES INTO` & MERGE    |
+-----------------------------------------------------------------------------------+

```
---

## Key Technical Innovations & Highlights

### 1. Incremental Data Ingestion via Databricks Auto Loader (`cloudFiles`)
* **Automated File Arrival Triggering:** Utilizes `format("cloudFiles")` with file event notification capabilities to automatically ingest micro-batches as soon as flight telemetry and transactional files hit object storage (AWS S3 / Azure ADLS Gen2 / DBFS / Unity Catalog Volumes).
* **Schema Evolution & Rescue:** Configured with `cloudFiles.schemaLocation` and dynamic schema inference. Unexpected or malformed fields are safely isolated into `_rescued_data` columns, eliminating ingestion pipeline crashes due to upstream schema drift.
* **Idempotent Ingestion:** Employs PySpark Structured Streaming with continuous checkpoint management (`checkpointLocation`) and `Trigger.AvailableNow` to ensure exactly-once processing guarantees and cost-optimized compute cycles.
<p align="center">
  <img width="1605" height="1008" alt="Screenshot 2026-10-14 174159" src="https://github.com/user-attachments/assets/60fec026-8097-49f1-befb-0205a782757a" />
</p>

### 2. Lakeflow Declarative Pipelines (Delta Live Tables - DLT)
* **Declarative Pipeline Execution:** Built using Lakeflow Declarative Pipelines (DLT) in PySpark/SQL, creating managed **Streaming Tables** and **Streaming Views**.
* **Automated Data Quality Expectations:** Enforces business constraints using DLT expectations (e.g., `@dlt.expect_or_drop("valid_flight_id", "flight_id IS NOT NULL")`). Metrics are automatically logged to the DLT Event Log for governance.
<p align="center">
  <img width="2273" height="1156" alt="Lakeflow Declarative Pipeline DAG" src="https://github.com/user-attachments/assets/196eb8de-5149-4f8b-a47c-dd3bce3a958f" />
</p>

### 3. Dynamic Dimension Builder Framework
* **Automated Surrogate Key Assignment:** Dynamically computes deterministic surrogate keys using `monotonically_increasing_id()`.
* **Metadata-Driven Execution:** Built a parameterized PySpark engine capable of dynamically instantiating dimension tables (`dim_passengers`, `dim_airports`, `dim_airlines`, `dim_aircraft`) without hardcoding redundant code pipelines.
<p align="center">
  <img width="2263" height="1151" alt="" src="https://github.com/user-attachments/assets/37954252-c50d-479c-a4f6-db684cc4a7fc" />
</p>

### 4. Dynamic Fact Table Builder & Workflows (Databricks Jobs)
* **Dynamic Join & Key Resolution:** Implemented a generic Fact Table Orchestrator within Databricks Jobs. The framework dynamically resolves natural keys against Gold dimension tables to fetch active surrogate keys, handling late-arriving dimensions gracefully.
* **Orchestration via Databricks Workflows:** Multi-task Databricks Workflows manage task dependencies, dynamic parameter passing via `dbutils.widgets`.
<p align="center">
  <img width="1736" height="1153" alt="image" src="https://github.com/user-attachments/assets/d84c90cb-1878-4aea-88c9-cb87d949b418" />
  <br><br>
  <img width="2272" height="1160" alt="image" src="https://github.com/user-attachments/assets/2f13569b-8b80-4ead-852c-dc6cd1d7032f" />
</p>

### 5. Change Data Capture (CDC), Upsert & SCD Logic
* **Three-Layer CDC Pipeline Chain:**  
**- Bronze Layer:** Ingests raw flight and booking payload files incrementally using Databricks Auto Loader (`cloudFiles`) with `availableNow=True` for checkpoint-driven, file-level CDC  
**- Silver Layer:** Utilizes `create_auto_cdc_flow` for row-level insert, update, and delete tracking on business keys, driven by the `modifiedDate` field  
**- Gold Layer:** Executes high-performance row-level upserts via Delta `MERGE` operations governed by `MAX(modifiedDate)` watermark state management.
* **Entity-Specific Mixed SCD Strategy:**  
**- SCD Type 2 (bookings):** Tracks complete historical changes using `dp.create_auto_cdc_flow`, automatically appending `__START_AT` and `__END_AT` timestamp columns to preserve auditable booking revisions, cancellations, and rebookings.  
**- SCD Type 1 (flights, passengers, airports):** Maintains current-state master data by overwriting records in place for efficient dimension lookup performance.  
* **Kimball Fact Integration on Natural Keys:** Executes fact table `MERGE` operations matching on the business/natural key `booking_id` rather than dimension surrogate keys.  
* **Backdated Refresh & Watermark Override:** Features a `backdated_refresh` execution parameter that bypasses incremental watermark states to reprocess historical data from a specified timestamp.  

---

## Repository Structure

```text
Databricks-Flights-E2E-Project/
│
├─ bronze/
│  ├─ Bronze_Layer.ipynb                  # Ingest raw source data into Bronze
│  ├─ Incremental_Ingestion.yml           # Configure/schedule incremental ingestion workflow
│  └─ Source_Parameters.ipynb             # Define source ingestion parameters and configuration
│
├─ experiments/
│  └─ Querying_Test.ipynb                 # Test SQL queries and explore data
│
├─ gold/
│  ├─ Dimension Table/
│  │  ├─ Create_Parameters.ipynb                    # Define parameters for dimension table creation
│  │  └─ Incremental_Dimension_Builder.ipynb        # Build/update dimensions incrementally
│  │                                       
│  └─ Fact Table/
│     ├─ fact_jobs_config.json            # Configure fact-building jobs and their parameters
│     └─ Fact_Table_Builder.ipynb         # Build/update fact tables and resolve dimension keys
│
├─ Jobs/
│  ├─ dynamic_fact_builder_job.yml                # Define Databricks job for dynamic & incremental fact building
│  └─ incremental_dimension_builder_job.yml       # Define job for dynamic & incremental dimension building
│
├─ silver/
│  └─ DLT_Silver/                         # Clean, transform data, and turn them into streaming view, streaming table 
│     └─ transformations/
│        ├─ airports.py                  
│        ├─ bookings.py                   
│        ├─ flights.py                   
│        └─ passengers.py                 
│
├─ README.md                              # Project overview and documentation
└─ Setup.ipynb                            # Initialize and configure project resources
```

## 👤 Author & Acknowledgment

* **GitHub:** [@Jung2304](https://github.com/Jung2304)
* **Project Repository:** [Databricks-Flights-E2E-Project](https://github.com/Jung2304/Databricks-Flights-E2E-Project)
