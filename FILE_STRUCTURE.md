# Project File Structure and Purpose

## 1. Root Directory Files

### docker-compose.yml
**Purpose**: Main container orchestration file
- Defines all services (Airflow, DBT, SQL Server, PostgreSQL)
- Sets up networking between containers
- Configures volumes and environment variables
- Manages container dependencies
```yaml
services:
  airflow-webserver: # Airflow web interface
  airflow-scheduler: # Airflow task scheduler
  postgres:         # Airflow metadata database
  sqlserver:        # Source database
  dbt:             # DBT transformation container
```

### README.md
**Purpose**: Project documentation
- Project overview and architecture
- Setup instructions
- Usage guidelines
- Best practices
- Troubleshooting tips

### SETUP.md
**Purpose**: Detailed installation guide
- Step-by-step setup instructions
- Prerequisites
- Common issues and solutions
- Verification steps

### docker_commands.md
**Purpose**: Docker command reference
- Container management commands
- DBT operations
- Database operations
- Airflow operations
- Maintenance commands

## 2. Airflow Directory (`airflow/`)

### dags/dbt_dag.py
**Purpose**: Airflow DAG definition
- Defines the DBT transformation workflow
- Schedules DBT model runs
- Manages task dependencies
```python
# Key components:
- DAG definition with scheduling
- dbt_run task
- dbt_test task
- Task dependencies
```

### logs/
**Purpose**: Airflow execution logs
- Contains task execution logs
- Error logs
- DAG run history
- Task status information

## 3. DBT Directory (`dbt/`)

### dbt_project.yml
**Purpose**: DBT project configuration
- Project name and version
- Model configurations
- Materialization settings
- Custom macro configurations
```yaml
name: 'dbt_sqlserver_project'
version: '1.0.0'
config-version: 2
profile: 'default'
```

### packages.yml
**Purpose**: DBT package dependencies
- Lists external DBT packages
- Specifies package versions
```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.1.1
```

### profiles.yml
**Purpose**: Database connection profiles
- Database credentials
- Connection settings
- Environment configurations
```yaml
default:
  target: dev
  outputs:
    dev:
      type: sqlserver
      server: sqlserver
      database: AdventureWorks
```

### models/staging/
**Purpose**: First layer of transformation
- Raw data cleaning
- Data standardization
- Initial transformations
- Example: `stg_sales_orders.sql`
  - Combines sales order tables
  - Standardizes column names
  - Applies initial transformations

### models/marts/
**Purpose**: Final transformation layer
- Business-level transformations
- Reporting-ready models
- Aggregated data
- Final output tables

### models/staging/schema.yml
**Purpose**: Model documentation and tests
- Model descriptions
- Column documentation
- Data tests
- Custom test definitions
```yaml
version: 2
models:
  - name: stg_sales_orders
    columns:
      - name: sales_order_id
        tests:
          - not_null
```

## 4. Environment Files

### .env
**Purpose**: Environment variables
- Database credentials
- API keys
- Configuration settings
- Environment-specific variables

### .env.example
**Purpose**: Environment template
- Example environment variables
- Required configuration
- Default values
- Setup guidance

## 5. Docker Configuration Files

### Dockerfile (if present)
**Purpose**: Container image definition
- Base image selection
- Package installation
- Environment setup
- Configuration copying

## File Relationships

1. **Data Flow**:
```
SQL Server (source) → DBT Models → Transformed Data
```

2. **Configuration Flow**:
```
docker-compose.yml → Container Setup → Service Configuration
```

3. **Transformation Flow**:
```
staging models → marts models → Final Output
```

4. **Orchestration Flow**:
```
dbt_dag.py → Airflow Scheduler → DBT Execution
```

## Best Practices for File Management

1. **Version Control**:
   - Keep all files in version control
   - Exclude sensitive files (.env)
   - Maintain clear commit messages

2. **File Organization**:
   - Follow the established directory structure
   - Keep related files together
   - Use consistent naming conventions

3. **Documentation**:
   - Keep README.md updated
   - Document all model changes
   - Maintain clear file purposes

4. **Security**:
   - Never commit sensitive credentials
   - Use environment variables
   - Follow security best practices 