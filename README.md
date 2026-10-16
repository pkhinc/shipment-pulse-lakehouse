# Shipment Pulse Lakehouse

A logistics data engineering portfolio project that processes shipment data using **Apache Spark / PySpark**, organizes it into **Bronze, Silver and Gold** layers, and presents business metrics through a **Databricks AI/BI Dashboard**.

## Project overview

The goal is to demonstrate an end-to-end analytical data workflow: transforming logistics data in a local Spark environment and making the resulting Gold-layer data available for reporting.

The core pipeline runs **locally with Docker and Docker Compose**. The reporting step uses **Databricks Free Edition**, where selected Gold results were **manually imported from Parquet files** into managed Delta tables.

## Architecture

```mermaid
flowchart LR
    A[Sample logistics data] --> B[Local PySpark pipeline]
    B --> C[Bronze]
    C --> D[Silver]
    D --> E[Gold / Parquet]
    E -->|Manual file upload| F[Databricks / Unity Catalog / Delta]
    F --> G[BI Dashboard]
```

- **Bronze:** initial ingestion and preservation of source data.
- **Silver:** cleaned and transformed datasets prepared for analysis.
- **Gold:** analytical summaries of shipments and transport routes.
- **Databricks reporting:** a snapshot of Gold data imported into Unity Catalog and displayed in a published BI dashboard.

> **Integration note:** The Databricks dashboard is **not directly connected to the local pipeline**. Re-running local Spark transformations does **not** automatically refresh the Databricks tables. Updating the dashboard data requires another import or an integration step, which is not implemented here.

## Technology stack

| Area | Tools |
| --- | --- |
| Data engineering | Apache Spark 4.1.3, PySpark, Python, SQL |
| Local environment | Docker, Docker Compose |
| Local analytical data | Parquet, Spark SQL / Hive Metastore |
| Cloud reporting | Databricks Free Edition, Unity Catalog, managed Delta tables, BI Dashboards |
| Version control | Git, GitHub |

## Gold analytics dashboard

The published **Shipment Platform - Gold Analytics** dashboard contains exactly three key performance indicators (KPIs):

| KPI | Metric definition | Example result |
| --- | --- | ---: |
| Total Shipments | Count of distinct `shipment_id` values | **20** |
| Total Cargo Weight (kg) | Sum of `cargo_weight_kg`, rounded to 2 decimals | **806,678.67 kg** |
| Total Routes | Count of distinct `(origin_port, destination_port)` pairs | **16** |

![Databricks dashboard showing three logistics KPIs](docs/images/databricks-gold-dashboard.png)

**Databricks tables:**

- `shipment_platform.gold.shipment_summary`
- `shipment_platform.gold.route_summary`

The dashboard is built on Gold-layer data manually imported
from the local PySpark pipeline into Databricks.
The three SQL queries used to calculate the KPIs are available
in [`sql/databricks_dashboard_kpis.sql`](sql/databricks_dashboard_kpis.sql).

The data transfer is currently manual; automatic synchronization
between the local pipeline and Databricks is not implemented.


## Repository structure (selected files)

```text
.
├── Dockerfile                  # Local Spark image
├── docker-compose.yml          # Local runtime configuration
├── spark-pipeline.yml          # Gold pipeline configuration
├── run_pipeline.ps1            # End-to-end pipeline orchestration
├── src/                        # Data generator and PySpark pipeline code
├── data/sample/                # Sample source data
├── docs/                       # Project documentation
│   └── images/databricks-gold-dashboard.png
└── sql/databricks_dashboard_kpis.sql
```

## Running the local pipeline

The complete local data pipeline can be executed using a single PowerShell command.

### Prerequisites

- Docker Desktop with Docker Compose
- PowerShell
- Python environment with the project dependencies installed

Make sure Docker Desktop is running. From the project root directory, execute:

```powershell
.\run_pipeline.ps1
```

The script automatically:

1. Generates 20 sample shipments using a fixed random seed (`42`).
2. Builds the local Apache Spark Docker image.
3. Ingests source data into the Bronze layer.
4. Executes three Silver transformations for ports, shipments CDC and shipment events.
5. Runs the Gold layer using Spark Declarative Pipelines, creating two materialized views: `shipment_summary` and `route_summary`.
6. Validates the Gold results using Spark SQL assertions.

The expected results for the default sample dataset are:

| Gold dataset | Expected rows |
| --- | ---: |
| `shipment_summary` | 20 |
| `route_summary` | 16 |

The script stops if a processing step fails or the Gold validation detects an unexpected row count.

**Important:** Running the script regenerates source data and overwrites the corresponding local Bronze and Silver outputs. The Gold tables are refreshed. Use a separate project copy if existing generated data must be preserved.

This orchestration runs entirely in the local Docker/Spark environment. It does not automatically update the Databricks dashboard, which uses a manually imported snapshot.

## Scope and limitations

- Processing takes place in the local Docker/Spark environment.
- Gold data is stored locally, with selected Parquet outputs imported into Databricks as managed Delta tables for reporting.
- The dashboard uses a manually uploaded data snapshot; **automated synchronization has not been implemented**.
- Power BI integration can be added separately; it is not part of the current dashboard deliverable.
