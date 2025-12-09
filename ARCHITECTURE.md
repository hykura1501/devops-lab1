# DBT Airflow Project Architecture

## System Overview

This project implements a data transformation pipeline using DBT (Data Build Tool) orchestrated by Apache Airflow. The system extracts data from SQL Server, transforms it using DBT models, and makes the transformed data available for analysis.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           DBT Airflow Project                            │
└─────────────────────────────────────────────────────────────────────────┘
```

## Component Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│             │     │             │     │             │     │             │
│  SQL Server │────▶│     DBT     │────▶│  Airflow    │────▶│  PostgreSQL │
│  (Source)   │     │ (Transform) │     │ (Orchestrate)│     │  (Metadata) │
│             │     │             │     │             │     │             │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
```

## Detailed Architecture

### 1. Data Sources
- **SQL Server**: Contains the source data (AdventureWorks database)
  - Tables: SalesOrderHeader, SalesOrderDetail, etc.
  - Port: 1433
  - Credentials: sa/YourStrong@Passw0rd

### 2. Data Transformation (DBT)
- **DBT Container**: Handles data transformation logic
  - Models:
    - Staging: Initial data cleaning and standardization
    - Marts: Business-level transformations and aggregations
  - Profiles: Connection configuration to SQL Server
  - Packages: External DBT packages (dbt_utils)

### 3. Orchestration (Airflow)
- **Airflow Webserver**: Web UI for monitoring and management
  - Port: 8080
  - Credentials: admin/admin
- **Airflow Scheduler**: Executes DAGs based on schedule
- **DAGs**: Define the workflow for DBT transformations
  - dbt_transform_dag.py: Orchestrates DBT model runs

### 4. Metadata Storage
- **PostgreSQL**: Stores Airflow metadata
  - Tables: dag, dag_run, task_instance, etc.
  - Port: 5432
  - Credentials: airflow/airflow

## Data Flow

1. **Extraction**:
   - SQL Server contains the source data
   - DBT connects to SQL Server using profiles.yml

2. **Transformation**:
   - DBT models transform the data in stages:
     - Staging models clean and standardize data
     - Mart models create business-ready datasets

3. **Orchestration**:
   - Airflow DAG triggers DBT transformations
   - Scheduler manages execution timing
   - Webserver provides monitoring and control

4. **Storage**:
   - Transformed data remains in SQL Server
   - Airflow metadata stored in PostgreSQL

## Container Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           Docker Compose Network                        │
└─────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐     │
│  │             │  │             │  │             │  │             │     │
│  │  SQL Server │  │  PostgreSQL │  │Airflow Web  │  │Airflow Sched│     │
│  │             │  │             │  │             │  │             │     │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘     │
│                                                                         │
│  ┌─────────────┐                                                        │
│  │             │                                                        │
│  │     DBT     │                                                        │
│  │             │                                                        │
│  └─────────────┘                                                        │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

## Volume Mounts

- **SQL Server**: 
  - `sqlserver_data:/var/opt/mssql` (persistent data)

- **PostgreSQL**:
  - `postgres_data:/var/lib/postgresql/data` (persistent data)

- **Airflow**:
  - `./airflow/dags:/opt/airflow/dags` (DAG definitions)
  - `./airflow/logs:/opt/airflow/logs` (execution logs)
  - `./airflow/plugins:/opt/airflow/plugins` (custom plugins)
  - `./dbt:/opt/airflow/dbt` (DBT project)
  - `/var/run/docker.sock:/var/run/docker.sock` (Docker API access)

- **DBT**:
  - `./dbt:/usr/app/dbt` (DBT project files)

## Network Configuration

- All services are connected via a Docker network named `dbt_airflow_network`
- Network type: bridge
- Services can communicate using their service names as hostnames

## Security Considerations

- SQL Server credentials are defined in docker-compose.yml
- Airflow credentials are set during initialization
- Docker socket is mounted to allow Airflow to create containers
- File permissions are set to ensure proper access

## Scaling and Maintenance

- The architecture is containerized for easy deployment
- Each component can be scaled independently
- Logs are persisted for troubleshooting
- Database backups should be implemented for production use

## Development Workflow

1. Develop DBT models locally
2. Test transformations using DBT commands
3. Create or modify Airflow DAGs to orchestrate the transformations
4. Deploy changes by rebuilding containers
5. Monitor execution through Airflow UI 