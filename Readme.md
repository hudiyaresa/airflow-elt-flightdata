# Pac-Flights Data Pipeline

## Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Pipeline Flow](#pipeline-flow)
4. [Setup and Run](#setup-and-run)
5. [Screenshots](#screenshots)

## Overview
This project builds an end-to-end data pipeline for a flight booking system. It extracts data from a source PostgreSQL DB, stores it in MinIO, loads it into a warehouse, and transforms it into analytical tables — all orchestrated by Apache Airflow.

## Architecture
- **Source DB**: PostgreSQL (`bookings` schema)
- **Data Lake**: MinIO (`extracted-data` bucket)
- **Data Warehouse**: PostgreSQL (`warehouse` schema)
- **Orchestrator**: Apache Airflow
- **Docker**: All services are containerized using Docker Compose

## Pipeline Flow
1. **Extract**: Extract tables from PostgreSQL → MinIO (CSV)
2. **Load**: MinIO CSV → PostgreSQL staging schema
3. **Transform**: SQL-based transform into `dim_` and `fct_` tables in warehouse

## Setup and Run
```bash
git clone https://github.com/yourusername/flights-data-pipeline.git
cd flights-data-pipeline
docker-compose up
````

Access:

* Airflow: `http://localhost:8080`
* MinIO UI: `http://localhost:9001`
* PostgreSQL: `localhost:5432`

## Screenshots


```

---



# Data Pipeline Overview

| Step | Process                 | Description                                                |
|------|-------------------------|------------------------------------------------------------|
| 1    | Extract                 | From `pacflight_db` to MinIO (`/temp/*.csv`)               |
| 2    | Load to Staging         | From MinIO to `staging.*` using `pangres.upsert()`         |
| 3    | Transform to Warehouse  | Read SQL from file and execute into `warehouse.*` schema   |


