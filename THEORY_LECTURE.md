# Modern Data Engineering: Theory & Concepts
## DBT, Airflow, and Data Warehousing Fundamentals

---

## Course Overview

### Module 1: Introduction to Modern Data Engineering
### Module 2: Data Warehousing Concepts
### Module 3: ETL vs ELT Paradigm
### Module 4: DBT (Data Build Tool) Deep Dive
### Module 5: Apache Airflow Architecture
### Module 6: Docker & Containerization
### Module 7: Data Modeling Best Practices
### Module 8: Data Quality & Testing
### Module 9: CI/CD for Data Pipelines
### Module 10: Real-World Architecture Patterns

---

## Module 1: Introduction to Modern Data Engineering

### What is Data Engineering?

**Definition:**
Data Engineering is the practice of designing, building, and maintaining systems that collect, store, and analyze data at scale.

**Key Responsibilities:**
- Building data pipelines
- Ensuring data quality
- Optimizing data storage
- Enabling analytics and ML
- Maintaining data infrastructure


### The Data Engineering Lifecycle

```
Data Sources → Ingestion → Storage → Transformation → Serving → Analytics/ML
```

**1. Data Sources:**
- Databases (OLTP)
- APIs
- Files (CSV, JSON, Parquet)
- Streaming data
- Third-party services

**2. Ingestion:**
- Batch processing
- Real-time streaming
- Change Data Capture (CDC)
- API polling

**3. Storage:**
- Data Lakes (S3, Azure Data Lake)
- Data Warehouses (Snowflake, BigQuery, Redshift)
- Databases (PostgreSQL, SQL Server)

**4. Transformation:**
- Data cleaning
- Business logic application
- Aggregations
- Joins and enrichment

**5. Serving:**
- Analytics dashboards
- Machine learning models
- Operational systems
- APIs


### Evolution of Data Engineering

**1990s - Traditional Data Warehousing:**
- ETL tools (Informatica, DataStage)
- On-premise data warehouses
- Batch processing only
- Expensive hardware

**2000s - Big Data Era:**
- Hadoop ecosystem
- MapReduce
- Distributed computing
- Open source tools

**2010s - Cloud Data Platforms:**
- Cloud data warehouses
- Separation of storage and compute
- Elastic scaling
- Pay-as-you-go pricing

**2020s - Modern Data Stack:**
- ELT over ETL
- SQL-based transformations
- Version control for data
- DataOps practices
- Self-service analytics


---

## Module 2: Data Warehousing Concepts

### OLTP vs OLAP

**OLTP (Online Transaction Processing):**
- Purpose: Day-to-day operations
- Characteristics:
  - High volume of short transactions
  - Normalized data structure
  - Focus on INSERT, UPDATE, DELETE
  - Row-oriented storage
  - Optimized for writes
- Examples: E-commerce orders, banking transactions

**OLAP (Online Analytical Processing):**
- Purpose: Business intelligence and analytics
- Characteristics:
  - Complex queries on large datasets
  - Denormalized data structure
  - Focus on SELECT queries
  - Column-oriented storage
  - Optimized for reads
- Examples: Sales reports, trend analysis


### Data Warehouse Architecture

**Traditional 3-Tier Architecture:**

```
┌─────────────────────────────────────┐
│     Presentation Layer              │
│  (BI Tools, Dashboards, Reports)    │
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│     Application Layer               │
│  (OLAP Server, Analytics Engine)    │
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│     Data Storage Layer              │
│  (Data Warehouse, Data Marts)       │
└─────────────────────────────────────┘
```

**Components:**
1. **Staging Area:** Raw data landing zone
2. **Integration Layer:** Cleaned and conformed data
3. **Data Marts:** Subject-specific subsets
4. **Metadata Repository:** Data about data


### Data Modeling Approaches

**1. Kimball Dimensional Modeling:**
- Star schema
- Fact and dimension tables
- Business process oriented
- Bottom-up approach
- Optimized for query performance

**2. Inmon Enterprise Data Warehouse:**
- Normalized 3NF structure
- Enterprise-wide integration
- Top-down approach
- Single source of truth
- Data marts derived from EDW

**3. Data Vault:**
- Hub, Link, and Satellite tables
- Highly scalable
- Audit trail built-in
- Flexible for changes
- Complex to query

**4. One Big Table (OBT):**
- Denormalized wide table
- Simple to query
- Modern cloud warehouses
- Trade storage for simplicity
- Good for specific use cases


### Star Schema Deep Dive

**Structure:**
```
        ┌──────────────┐
        │   Dim_Date   │
        └──────┬───────┘
               │
┌──────────┐  │  ┌──────────────┐
│Dim_Product├──┼──┤  Fact_Sales  │
└──────────┘  │  └──────┬───────┘
              │         │
        ┌─────┴────┐    │
        │Dim_Customer├───┘
        └──────────┘
```

**Fact Table:**
- Contains measurements/metrics
- Foreign keys to dimensions
- Additive, semi-additive, or non-additive facts
- Grain: Level of detail

**Dimension Tables:**
- Descriptive attributes
- Slowly Changing Dimensions (SCD)
- Hierarchies (Year → Quarter → Month → Day)
- Denormalized for query performance

**Benefits:**
- Simple to understand
- Fast query performance
- Flexible for BI tools
- Intuitive for business users


---

## Module 3: ETL vs ELT Paradigm

### Traditional ETL (Extract, Transform, Load)

**Process Flow:**
```
Source → Extract → Transform → Load → Warehouse
```

**Characteristics:**
- Transformation happens before loading
- Requires separate ETL server
- Pre-defined transformations
- Limited raw data access
- Slower to adapt to changes

**Tools:**
- Informatica PowerCenter
- IBM DataStage
- Microsoft SSIS
- Talend

**When to Use:**
- Legacy systems
- Limited warehouse capacity
- Strict data governance
- Complex transformations needed upfront


### Modern ELT (Extract, Load, Transform)

**Process Flow:**
```
Source → Extract → Load → Transform → Analytics
```

**Characteristics:**
- Load raw data first
- Transform using warehouse compute
- Leverage SQL for transformations
- Access to raw data
- Flexible and iterative

**Tools:**
- DBT (Data Build Tool)
- Dataform
- SQL-based transformations
- Cloud data warehouses

**Advantages:**
- Faster time to value
- Lower infrastructure costs
- Scalable transformations
- Version control friendly
- Self-service analytics

**Why ELT Works Now:**
1. Cloud warehouses are powerful and cheap
2. Separation of storage and compute
3. SQL is universal and well-understood
4. Git-based workflows
5. Faster iteration cycles


### ETL vs ELT Comparison

| Aspect | ETL | ELT |
|--------|-----|-----|
| **Transformation Location** | External server | Data warehouse |
| **Data Loading** | Transformed data only | Raw + transformed |
| **Flexibility** | Pre-defined | Iterative |
| **Time to Insights** | Slower | Faster |
| **Infrastructure** | Separate ETL tools | Warehouse compute |
| **Skill Required** | ETL tool expertise | SQL knowledge |
| **Version Control** | Difficult | Git-friendly |
| **Cost Model** | Fixed infrastructure | Pay-per-use |
| **Scalability** | Limited by ETL server | Elastic warehouse |
| **Data Lineage** | Tool-dependent | Code-based |

**Hybrid Approach:**
Many organizations use both:
- ELT for analytics workloads
- ETL for operational systems
- Choose based on use case


---

## Module 4: DBT (Data Build Tool) Deep Dive

### What is DBT?

**Definition:**
DBT is a transformation tool that enables data analysts and engineers to transform data in their warehouse using SQL SELECT statements.

**Core Philosophy:**
- Transformations as code
- Version control for data logic
- Testing built-in
- Documentation generated
- Modular and reusable

**What DBT Does:**
✅ Transforms data using SQL
✅ Manages dependencies
✅ Tests data quality
✅ Generates documentation
✅ Enables collaboration

**What DBT Doesn't Do:**
❌ Extract data from sources
❌ Load data into warehouse
❌ Orchestrate workflows (needs Airflow)
❌ Visualize data


### DBT Architecture

**Compilation Process:**
```
SQL Templates → Jinja Rendering → Compiled SQL → Execution → Database
```

**Key Components:**

1. **Models:**
   - SQL SELECT statements
   - Materialized as tables or views
   - Reference other models
   - Organized in folders

2. **Sources:**
   - Raw data tables
   - Defined in YAML
   - Freshness checks
   - Documentation

3. **Tests:**
   - Data quality assertions
   - Schema tests (unique, not_null)
   - Custom SQL tests
   - Relationship tests

4. **Macros:**
   - Reusable SQL snippets
   - Jinja functions
   - DRY principle
   - Package ecosystem

5. **Documentation:**
   - Auto-generated
   - Lineage graphs
   - Column descriptions
   - Test results


### DBT Project Structure

```
dbt_project/
├── dbt_project.yml          # Project configuration
├── profiles.yml             # Connection profiles
├── packages.yml             # Dependencies
├── models/
│   ├── staging/            # Layer 1: Raw data cleanup
│   │   ├── stg_customers.sql
│   │   ├── stg_orders.sql
│   │   └── schema.yml
│   ├── intermediate/       # Layer 2: Business logic
│   │   ├── int_customer_orders.sql
│   │   └── schema.yml
│   └── marts/              # Layer 3: Analytics ready
│       ├── customer_analytics.sql
│       └── schema.yml
├── tests/                  # Custom tests
├── macros/                 # Reusable functions
├── snapshots/              # SCD Type 2
├── seeds/                  # CSV reference data
└── analyses/               # Ad-hoc queries
```

**Folder Organization Best Practices:**
- Group by business domain
- Separate by transformation layer
- Clear naming conventions
- Logical dependencies


### DBT Materialization Strategies

**1. View (Default):**
```sql
{{ config(materialized='view') }}
SELECT * FROM source_table
```
- Virtual table
- No storage cost
- Always fresh data
- Slower query performance
- Use for: Lightweight transformations

**2. Table:**
```sql
{{ config(materialized='table') }}
SELECT * FROM source_table
```
- Physical table
- Storage cost
- Faster queries
- Stale until rebuilt
- Use for: Heavy transformations, final marts

**3. Incremental:**
```sql
{{ config(materialized='incremental') }}
SELECT * FROM source_table
{% if is_incremental() %}
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
```
- Append or update only new records
- Efficient for large tables
- Complex logic
- Use for: Event logs, time-series data

**4. Ephemeral:**
```sql
{{ config(materialized='ephemeral') }}
SELECT * FROM source_table
```
- CTE in dependent models
- No database object
- Reduces clutter
- Use for: Intermediate logic


### DBT Jinja Templating

**What is Jinja?**
- Python templating language
- Dynamic SQL generation
- Control structures (if, for)
- Variables and macros

**Common Jinja Patterns:**

**1. Referencing Models:**
```sql
SELECT * FROM {{ ref('stg_customers') }}
```
- Builds dependency graph
- Handles schema changes
- Environment-aware

**2. Referencing Sources:**
```sql
SELECT * FROM {{ source('salesforce', 'accounts') }}
```
- Documents data lineage
- Freshness checks
- Abstraction layer

**3. Variables:**
```sql
WHERE created_at >= '{{ var("start_date") }}'
```
- Runtime parameters
- Environment-specific values
- Reusable logic

**4. Macros:**
```sql
{{ cents_to_dollars('amount_cents') }}
```
- Reusable functions
- DRY principle
- Package ecosystem


### DBT Testing Framework

**Schema Tests (Built-in):**

```yaml
models:
  - name: customers
    columns:
      - name: customer_id
        tests:
          - unique
          - not_null
      - name: email
        tests:
          - unique
      - name: status
        tests:
          - accepted_values:
              values: ['active', 'inactive', 'pending']
      - name: country_id
        tests:
          - relationships:
              to: ref('countries')
              field: id
```

**Custom Data Tests:**

```sql
-- tests/assert_positive_revenue.sql
SELECT *
FROM {{ ref('orders') }}
WHERE revenue < 0
```

**Test Execution:**
- Runs as SELECT queries
- Fails if returns rows
- Part of CI/CD pipeline
- Documents data contracts


### DBT Layered Architecture

**Staging Layer (stg_):**
- Purpose: Clean and standardize raw data
- Transformations:
  - Rename columns to snake_case
  - Cast data types
  - Basic filtering
  - No business logic
- Materialization: Views
- One source table = One staging model

**Intermediate Layer (int_):**
- Purpose: Apply business logic
- Transformations:
  - Joins between staging models
  - Aggregations
  - Calculations
  - Reusable components
- Materialization: Views or ephemeral
- Not exposed to end users

**Marts Layer (no prefix):**
- Purpose: Analytics-ready tables
- Transformations:
  - Final business metrics
  - Denormalized for reporting
  - Optimized for queries
- Materialization: Tables
- Exposed to BI tools

**Benefits:**
- Clear separation of concerns
- Easier to maintain
- Reusable components
- Testable at each layer


---

## Module 5: Apache Airflow Architecture

### What is Apache Airflow?

**Definition:**
Apache Airflow is an open-source platform to programmatically author, schedule, and monitor workflows.

**Core Concepts:**
- Workflows as code (Python)
- DAG (Directed Acyclic Graph)
- Task orchestration
- Dependency management
- Monitoring and alerting

**Use Cases:**
- ETL/ELT pipelines
- Machine learning workflows
- Data quality checks
- Report generation
- System maintenance tasks

**Why Airflow?**
- Open source and extensible
- Rich UI for monitoring
- Large ecosystem of operators
- Active community
- Battle-tested at scale


### Airflow Architecture Components

```
┌─────────────────────────────────────────────────┐
│              Web Server (UI)                    │
│         (Flask, Monitoring, Logs)               │
└────────────────┬────────────────────────────────┘
                 │
┌────────────────▼────────────────────────────────┐
│              Scheduler                          │
│    (DAG Parsing, Task Scheduling)               │
└────────────────┬────────────────────────────────┘
                 │
┌────────────────▼────────────────────────────────┐
│            Metadata Database                    │
│      (PostgreSQL, MySQL, SQLite)                │
└────────────────┬────────────────────────────────┘
                 │
┌────────────────▼────────────────────────────────┐
│              Executor                           │
│  (Sequential, Local, Celery, Kubernetes)        │
└────────────────┬────────────────────────────────┘
                 │
┌────────────────▼────────────────────────────────┐
│              Workers                            │
│         (Task Execution)                        │
└─────────────────────────────────────────────────┘
```

**Component Details:**

1. **Web Server:**
   - User interface
   - DAG visualization
   - Log viewing
   - Task management

2. **Scheduler:**
   - Monitors DAG folder
   - Triggers task execution
   - Manages dependencies
   - Handles retries

3. **Metadata Database:**
   - Stores DAG definitions
   - Task states
   - Execution history
   - Configuration

4. **Executor:**
   - Determines how tasks run
   - Manages worker pool
   - Handles parallelism

5. **Workers:**
   - Execute tasks
   - Report status
   - Handle failures


### DAG (Directed Acyclic Graph)

**What is a DAG?**
- Workflow definition
- Collection of tasks
- Dependencies between tasks
- No cycles allowed

**DAG Structure:**
```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

dag = DAG(
    dag_id='example_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
)

task1 = BashOperator(
    task_id='task1',
    bash_command='echo "Task 1"',
    dag=dag
)

task2 = BashOperator(
    task_id='task2',
    bash_command='echo "Task 2"',
    dag=dag
)

task1 >> task2  # task1 runs before task2
```

**DAG Parameters:**
- `dag_id`: Unique identifier
- `start_date`: When DAG becomes active
- `schedule_interval`: How often to run
- `catchup`: Backfill historical runs
- `default_args`: Task defaults


### Task Dependencies

**Linear Dependencies:**
```python
task1 >> task2 >> task3
# task1 → task2 → task3
```

**Parallel Tasks:**
```python
task1 >> [task2, task3] >> task4
#        ┌─ task2 ─┐
# task1 ─┤         ├─ task4
#        └─ task3 ─┘
```

**Complex Dependencies:**
```python
task1 >> task2
task1 >> task3
[task2, task3] >> task4
task4 >> task5
#        ┌─ task2 ─┐
# task1 ─┤         ├─ task4 ─ task5
#        └─ task3 ─┘
```

**Dependency Methods:**
```python
# Downstream
task1.set_downstream(task2)
task1 >> task2

# Upstream
task2.set_upstream(task1)
task2 << task1

# Multiple
task1 >> [task2, task3]
[task2, task3] >> task4
```


### Airflow Operators

**Common Operators:**

**1. BashOperator:**
```python
from airflow.operators.bash import BashOperator

run_script = BashOperator(
    task_id='run_script',
    bash_command='python /path/to/script.py',
    dag=dag
)
```

**2. PythonOperator:**
```python
from airflow.operators.python import PythonOperator

def my_function():
    print("Hello from Python")

run_python = PythonOperator(
    task_id='run_python',
    python_callable=my_function,
    dag=dag
)
```

**3. SQLOperator:**
```python
from airflow.providers.postgres.operators.postgres import PostgresOperator

run_query = PostgresOperator(
    task_id='run_query',
    postgres_conn_id='my_postgres',
    sql='SELECT * FROM table',
    dag=dag
)
```

**4. Sensors:**
```python
from airflow.sensors.filesystem import FileSensor

wait_for_file = FileSensor(
    task_id='wait_for_file',
    filepath='/path/to/file.csv',
    poke_interval=60,
    dag=dag
)
```


### Airflow Scheduling

**Schedule Intervals:**

```python
# Cron expressions
schedule_interval='0 0 * * *'  # Daily at midnight
schedule_interval='0 */4 * * *'  # Every 4 hours
schedule_interval='0 0 * * 1'  # Every Monday

# Presets
schedule_interval='@daily'     # Once a day at midnight
schedule_interval='@hourly'    # Once an hour
schedule_interval='@weekly'    # Once a week
schedule_interval='@monthly'   # Once a month
schedule_interval='@once'      # Run once
schedule_interval=None         # Manual trigger only
```

**Execution Date vs Run Date:**
- `execution_date`: Logical date (data period)
- `run_date`: Actual execution time
- Example: Daily DAG for Jan 1 runs on Jan 2

**Catchup:**
```python
catchup=True   # Backfill historical runs
catchup=False  # Only run from now forward
```

**Best Practices:**
- Use UTC timezone
- Set appropriate start_date
- Consider data availability
- Avoid overlapping runs


### Airflow Executors

**1. Sequential Executor:**
- One task at a time
- SQLite backend
- Development only
- No parallelism

**2. Local Executor:**
- Multiple tasks in parallel
- PostgreSQL/MySQL backend
- Single machine
- Good for small deployments

**3. Celery Executor:**
- Distributed task execution
- Multiple worker machines
- Redis/RabbitMQ message broker
- Horizontal scaling

**4. Kubernetes Executor:**
- Each task in separate pod
- Dynamic resource allocation
- Cloud-native
- Auto-scaling

**Choosing an Executor:**
- Development: Sequential
- Small production: Local
- Large production: Celery or Kubernetes
- Cloud deployment: Kubernetes


---

## Module 6: Docker & Containerization

### What is Docker?

**Definition:**
Docker is a platform for developing, shipping, and running applications in containers.

**Container vs Virtual Machine:**

```
┌─────────────────────┐  ┌─────────────────────┐
│   Container         │  │   Virtual Machine   │
├─────────────────────┤  ├─────────────────────┤
│   App A   │  App B  │  │   App A   │  App B  │
│   Bins    │  Bins   │  │   Bins    │  Bins   │
│   Libs    │  Libs   │  │   Libs    │  Libs   │
├───────────┴─────────┤  ├───────────┴─────────┤
│   Docker Engine     │  │   Guest OS │ Guest OS│
├─────────────────────┤  ├─────────────────────┤
│   Host OS           │  │   Hypervisor        │
├─────────────────────┤  ├─────────────────────┤
│   Infrastructure    │  │   Host OS           │
└─────────────────────┘  ├─────────────────────┤
                         │   Infrastructure    │
                         └─────────────────────┘
```

**Benefits:**
- Lightweight
- Fast startup
- Consistent environments
- Easy to share
- Version control


### Docker Core Concepts

**1. Image:**
- Read-only template
- Contains application code
- Includes dependencies
- Built from Dockerfile
- Stored in registry

**2. Container:**
- Running instance of image
- Isolated process
- Has own filesystem
- Can be started/stopped
- Ephemeral by default

**3. Dockerfile:**
```dockerfile
FROM python:3.9
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["python", "app.py"]
```

**4. Volume:**
- Persistent data storage
- Survives container deletion
- Shared between containers
- Managed by Docker

**5. Network:**
- Container communication
- Isolated networks
- Port mapping
- DNS resolution


### Docker Compose

**What is Docker Compose?**
- Tool for multi-container applications
- YAML configuration file
- Single command to start all services
- Manages networks and volumes

**docker-compose.yml Structure:**
```yaml
version: '3.8'

services:
  web:
    image: nginx:latest
    ports:
      - "80:80"
    volumes:
      - ./html:/usr/share/nginx/html
    networks:
      - app-network
  
  database:
    image: postgres:13
    environment:
      POSTGRES_PASSWORD: secret
    volumes:
      - db-data:/var/lib/postgresql/data
    networks:
      - app-network

volumes:
  db-data:

networks:
  app-network:
    driver: bridge
```

**Common Commands:**
```bash
docker-compose up -d        # Start all services
docker-compose down         # Stop and remove
docker-compose ps           # List services
docker-compose logs         # View logs
docker-compose exec web sh  # Execute command
```


### Why Docker for Data Engineering?

**Benefits:**

1. **Reproducibility:**
   - Same environment everywhere
   - No "works on my machine"
   - Version-controlled infrastructure

2. **Isolation:**
   - Separate dependencies
   - No conflicts
   - Clean environments

3. **Portability:**
   - Run anywhere
   - Easy deployment
   - Cloud-ready

4. **Scalability:**
   - Easy to replicate
   - Horizontal scaling
   - Resource management

5. **Development Speed:**
   - Quick setup
   - Easy testing
   - Fast iteration

**Our Project Stack:**
```
┌─────────────────┐
│   SQL Server    │ ← Source database
└────────┬────────┘
         │
┌────────▼────────┐
│      DBT        │ ← Transformations
└────────┬────────┘
         │
┌────────▼────────┐
│    Airflow      │ ← Orchestration
│  (Web + Sched)  │
└────────┬────────┘
         │
┌────────▼────────┐
│   PostgreSQL    │ ← Airflow metadata
└─────────────────┘
```

All running in Docker containers!


---

## Module 7: Data Modeling Best Practices

### Naming Conventions

**Tables and Views:**
```
✅ Good:
- stg_customers
- int_customer_orders
- customer_analytics

❌ Bad:
- CustomerTable
- tbl_customers
- customers_final_v2
```

**Columns:**
```
✅ Good:
- customer_id
- order_date
- total_amount

❌ Bad:
- CustomerID
- orderDate
- TotalAmt
```

**Rules:**
- Use snake_case
- Be descriptive
- Avoid abbreviations
- Use prefixes for layers
- Consistent across project


### Data Modeling Principles

**1. Single Source of Truth:**
- One canonical version of each entity
- Staging layer for raw data
- Marts for analytics
- Clear lineage

**2. DRY (Don't Repeat Yourself):**
- Reusable intermediate models
- Macros for common logic
- Avoid duplicating transformations
- Modular design

**3. Separation of Concerns:**
- Staging: Data cleaning
- Intermediate: Business logic
- Marts: Analytics ready
- Each layer has clear purpose

**4. Incremental Development:**
- Start simple
- Add complexity gradually
- Test at each step
- Iterate based on feedback

**5. Documentation:**
- Describe models
- Explain business logic
- Document assumptions
- Keep updated


### Slowly Changing Dimensions (SCD)

**Type 0: Retain Original**
- Never changes
- Historical data only
- Example: Birth date

**Type 1: Overwrite**
```sql
UPDATE customers
SET email = 'new@email.com'
WHERE customer_id = 123
```
- No history kept
- Simple
- Example: Typo corrections

**Type 2: Add New Row**
```sql
customer_id | name  | email         | valid_from | valid_to   | is_current
1          | John  | old@email.com | 2020-01-01 | 2023-01-01 | false
1          | John  | new@email.com | 2023-01-01 | NULL       | true
```
- Full history
- Most common
- Example: Address changes

**Type 3: Add New Column**
```sql
customer_id | name  | current_email | previous_email
1          | John  | new@email.com | old@email.com
```
- Limited history
- Simple queries
- Example: Previous value only

**Type 4: Separate History Table**
- Current in main table
- History in separate table
- Complex but flexible


### Data Grain

**Definition:**
The level of detail in a fact table.

**Examples:**

**Transaction Grain:**
```sql
order_id | product_id | quantity | amount | order_date
1        | 101        | 2        | 50.00  | 2024-01-01
1        | 102        | 1        | 25.00  | 2024-01-01
```
- Most detailed
- One row per line item
- Flexible for analysis

**Daily Aggregate Grain:**
```sql
date       | product_id | total_quantity | total_amount
2024-01-01 | 101        | 10             | 250.00
2024-01-01 | 102        | 5              | 125.00
```
- Summarized by day
- Faster queries
- Less flexible

**Choosing Grain:**
- Start with lowest grain possible
- Aggregate up as needed
- Consider query patterns
- Balance detail vs performance
- Document clearly

**Grain Statement:**
"Each row represents one order line item"


---

## Module 8: Data Quality & Testing

### Data Quality Dimensions

**1. Accuracy:**
- Data reflects reality
- Correct values
- No errors

**2. Completeness:**
- All required data present
- No missing values
- Full coverage

**3. Consistency:**
- Same data across systems
- No contradictions
- Standardized format

**4. Timeliness:**
- Data is current
- Updated regularly
- Available when needed

**5. Validity:**
- Conforms to rules
- Correct format
- Within acceptable range

**6. Uniqueness:**
- No duplicates
- Unique identifiers
- One version of truth


### Testing Strategy

**Test Pyramid:**
```
        ┌─────────┐
        │  E2E    │  ← Few, expensive
        ├─────────┤
        │Integration│ ← Some
        ├─────────┤
        │  Unit   │  ← Many, cheap
        └─────────┘
```

**Unit Tests (DBT Schema Tests):**
- Test individual columns
- Fast execution
- Run frequently
- Examples: unique, not_null

**Integration Tests (Custom SQL Tests):**
- Test relationships
- Business logic validation
- Cross-table checks
- Examples: referential integrity

**End-to-End Tests:**
- Full pipeline execution
- Data quality metrics
- Performance checks
- Examples: row counts, aggregates

**When to Run:**
- Unit: Every commit
- Integration: Every merge
- E2E: Daily/weekly


### Data Contracts

**What is a Data Contract?**
- Agreement between data producer and consumer
- Defines schema, quality, and SLAs
- Enforced through tests
- Documented explicitly

**Example Contract:**
```yaml
model: customer_analytics
owner: analytics_team
description: Customer segmentation and metrics

schema:
  - name: customer_id
    type: integer
    required: true
    unique: true
  - name: lifetime_value
    type: decimal
    required: true
    min_value: 0
  - name: customer_segment
    type: string
    required: true
    allowed_values: ['High', 'Medium', 'Low']

quality:
  - no_null_customer_ids
  - positive_lifetime_value
  - valid_segments

sla:
  freshness: 24 hours
  completeness: 99%
  availability: 99.9%
```

**Benefits:**
- Clear expectations
- Early error detection
- Better collaboration
- Reduced incidents


---

## Module 9: CI/CD for Data Pipelines

### What is CI/CD?

**Continuous Integration (CI):**
- Automated testing
- Code quality checks
- Build validation
- Run on every commit

**Continuous Deployment (CD):**
- Automated deployment
- Environment promotion
- Rollback capability
- Production releases

**Benefits:**
- Faster delivery
- Fewer bugs
- Consistent process
- Reduced risk


### CI/CD Pipeline for DBT

**Typical Workflow:**

```
Developer → Git Push → CI Pipeline → Deploy
                          ↓
                    ┌─────────────┐
                    │ Lint Code   │
                    ├─────────────┤
                    │ Run Tests   │
                    ├─────────────┤
                    │ Build Models│
                    ├─────────────┤
                    │ Generate Docs│
                    └─────────────┘
```

**GitHub Actions Example:**
```yaml
name: DBT CI

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: 3.9
      
      - name: Install DBT
        run: pip install dbt-sqlserver
      
      - name: Run DBT Tests
        run: |
          cd dbt
          dbt deps
          dbt run --target dev
          dbt test
      
      - name: Generate Docs
        run: dbt docs generate
```

**Environments:**
- Development: Local/feature branches
- Staging: Integration testing
- Production: Live data


### Version Control Best Practices

**Git Workflow:**

```
main (production)
  ↑
  └── develop (staging)
        ↑
        ├── feature/customer-analytics
        ├── feature/product-metrics
        └── bugfix/fix-date-logic
```

**Branch Strategy:**
- `main`: Production code
- `develop`: Integration branch
- `feature/*`: New features
- `bugfix/*`: Bug fixes
- `hotfix/*`: Emergency fixes

**Commit Messages:**
```
✅ Good:
feat: add customer segmentation model
fix: correct date calculation in orders
docs: update customer_analytics description

❌ Bad:
updated stuff
fix
changes
```

**Pull Request Process:**
1. Create feature branch
2. Make changes
3. Run tests locally
4. Submit PR
5. Code review
6. CI passes
7. Merge to develop
8. Deploy to staging
9. Merge to main
10. Deploy to production


---

## Module 10: Real-World Architecture Patterns

### Modern Data Stack Architecture

```
┌─────────────────────────────────────────────────┐
│              Data Sources                       │
│  (Databases, APIs, SaaS, Files, Streams)        │
└────────────────┬────────────────────────────────┘
                 │
┌────────────────▼────────────────────────────────┐
│           Ingestion Layer                       │
│  (Fivetran, Airbyte, Stitch, Custom ETL)        │
└────────────────┬────────────────────────────────┘
                 │
┌────────────────▼────────────────────────────────┐
│         Data Warehouse/Lake                     │
│  (Snowflake, BigQuery, Redshift, Databricks)    │
└────────────────┬────────────────────────────────┘
                 │
┌────────────────▼────────────────────────────────┐
│        Transformation Layer                     │
│            (DBT, Dataform)                      │
└────────────────┬────────────────────────────────┘
                 │
┌────────────────▼────────────────────────────────┐
│         Orchestration                           │
│      (Airflow, Dagster, Prefect)                │
└────────────────┬────────────────────────────────┘
                 │
┌────────────────▼────────────────────────────────┐
│         Analytics & ML                          │
│  (Tableau, Looker, Mode, Python, R)             │
└─────────────────────────────────────────────────┘
```

**Key Characteristics:**
- Cloud-native
- Modular components
- SQL-centric
- Version controlled
- Self-service


### Lambda Architecture

**For Real-Time + Batch Processing:**

```
┌─────────────┐
│ Data Source │
└──────┬──────┘
       │
   ┌───┴────┐
   │        │
   ▼        ▼
┌─────┐  ┌──────┐
│Batch│  │Stream│
│Layer│  │Layer │
└──┬──┘  └───┬──┘
   │         │
   │    ┌────▼────┐
   │    │ Speed   │
   │    │ View    │
   │    └────┬────┘
   │         │
   └────┬────┘
        │
   ┌────▼────┐
   │ Serving │
   │  Layer  │
   └─────────┘
```

**Layers:**
1. **Batch Layer:**
   - Historical data
   - Complete accuracy
   - High latency
   - DBT transformations

2. **Speed Layer:**
   - Real-time data
   - Approximate results
   - Low latency
   - Stream processing

3. **Serving Layer:**
   - Merge batch + speed
   - Query interface
   - Unified view

**Use Cases:**
- Real-time dashboards
- Fraud detection
- Recommendation engines
- IoT analytics


### Medallion Architecture (Lakehouse)

**Bronze → Silver → Gold:**

```
┌─────────────────┐
│  Raw Data Lake  │  Bronze: Raw ingestion
│   (Bronze)      │  - Exact copy of source
└────────┬────────┘  - No transformations
         │           - Append-only
         │
┌────────▼────────┐
│  Cleaned Data   │  Silver: Cleaned & conformed
│   (Silver)      │  - Data quality applied
└────────┬────────┘  - Standardized format
         │           - Deduplicated
         │
┌────────▼────────┐
│ Analytics Ready │  Gold: Business-level aggregates
│    (Gold)       │  - Denormalized
└─────────────────┘  - Optimized for queries
                     - Business metrics
```

**Benefits:**
- Clear data lineage
- Incremental refinement
- Flexibility
- Cost-effective storage

**Implementation:**
- Bronze: Parquet files
- Silver: Delta Lake tables
- Gold: Aggregated tables
- DBT for Silver → Gold


### Data Mesh Architecture

**Decentralized Data Ownership:**

```
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│   Sales      │  │  Marketing   │  │   Product    │
│   Domain     │  │   Domain     │  │   Domain     │
├──────────────┤  ├──────────────┤  ├──────────────┤
│ Data Product │  │ Data Product │  │ Data Product │
│   Owner      │  │   Owner      │  │   Owner      │
└──────┬───────┘  └──────┬───────┘  └──────┬───────┘
       │                 │                 │
       └─────────────────┼─────────────────┘
                         │
                ┌────────▼────────┐
                │  Data Platform  │
                │  (Self-Service) │
                └─────────────────┘
```

**Principles:**

1. **Domain Ownership:**
   - Teams own their data
   - Domain experts manage quality
   - Decentralized responsibility

2. **Data as a Product:**
   - Discoverable
   - Addressable
   - Trustworthy
   - Self-describing
   - Secure

3. **Self-Service Platform:**
   - Common infrastructure
   - Standardized tools
   - Automated governance
   - Easy to use

4. **Federated Governance:**
   - Global standards
   - Local implementation
   - Automated compliance
   - Interoperability

**When to Use:**
- Large organizations
- Multiple domains
- Distributed teams
- Complex data landscape


### Our Project Architecture

**What We're Building:**

```
┌─────────────────────────────────────────────┐
│         SQL Server (AdventureWorks)         │
│              Source Database                │
└──────────────────┬──────────────────────────┘
                   │
                   │ Raw Tables
                   │
┌──────────────────▼──────────────────────────┐
│              DBT Project                    │
│  ┌─────────────────────────────────────┐   │
│  │  Staging Layer (Views)              │   │
│  │  - stg_customers                    │   │
│  │  - stg_orders                       │   │
│  │  - stg_products                     │   │
│  └──────────────┬──────────────────────┘   │
│                 │                           │
│  ┌──────────────▼──────────────────────┐   │
│  │  Intermediate Layer (Views)         │   │
│  │  - int_customer_metrics             │   │
│  │  - int_product_performance          │   │
│  └──────────────┬──────────────────────┘   │
│                 │                           │
│  ┌──────────────▼──────────────────────┐   │
│  │  Marts Layer (Tables)               │   │
│  │  - customer_analytics               │   │
│  │  - product_analytics                │   │
│  └─────────────────────────────────────┘   │
└──────────────────┬──────────────────────────┘
                   │
┌──────────────────▼──────────────────────────┐
│           Apache Airflow                    │
│  - Schedule DBT runs                        │
│  - Monitor execution                        │
│  - Handle failures                          │
└──────────────────┬──────────────────────────┘
                   │
┌──────────────────▼──────────────────────────┐
│         Analytics & BI Tools                │
│  - Azure Data Studio                        │
│  - Tableau / Power BI                       │
│  - Custom Applications                      │
└─────────────────────────────────────────────┘
```

**All running in Docker containers!**


---

## Summary & Key Takeaways

### Modern Data Engineering Principles

**1. ELT over ETL:**
- Load raw data first
- Transform in warehouse
- Leverage SQL
- Faster iteration

**2. Code-Based Workflows:**
- Version control everything
- Infrastructure as code
- Reproducible pipelines
- Collaborative development

**3. Modular Architecture:**
- Separate concerns
- Reusable components
- Clear dependencies
- Easy to maintain

**4. Testing & Quality:**
- Test early and often
- Automated validation
- Data contracts
- Continuous monitoring

**5. Documentation:**
- Self-documenting code
- Generated documentation
- Clear lineage
- Business context


### Technology Stack Comparison

| Component | Traditional | Modern |
|-----------|-------------|--------|
| **Transformation** | Informatica, SSIS | DBT, Dataform |
| **Orchestration** | Control-M, Autosys | Airflow, Dagster |
| **Storage** | On-prem warehouse | Cloud warehouse |
| **Version Control** | Limited | Git-based |
| **Testing** | Manual | Automated |
| **Documentation** | Separate docs | Code-generated |
| **Deployment** | Manual | CI/CD |
| **Scalability** | Vertical | Horizontal |
| **Cost Model** | Fixed | Pay-per-use |
| **Skill Required** | Tool-specific | SQL + Python |


### Career Paths in Data Engineering

**1. Data Engineer:**
- Build data pipelines
- Maintain infrastructure
- Optimize performance
- Tools: Python, SQL, Airflow, DBT

**2. Analytics Engineer:**
- DBT specialist
- Data modeling
- Business logic
- Tools: SQL, DBT, Git

**3. Data Architect:**
- Design data systems
- Choose technologies
- Set standards
- Strategic planning

**4. DataOps Engineer:**
- CI/CD pipelines
- Monitoring & alerting
- Infrastructure automation
- Tools: Docker, Kubernetes, Terraform

**5. ML Engineer:**
- Feature engineering
- Model deployment
- ML pipelines
- Tools: Python, Airflow, MLflow

**Skills to Develop:**
- SQL (essential)
- Python (important)
- Cloud platforms (AWS/Azure/GCP)
- Version control (Git)
- Data modeling
- Orchestration tools
- Communication skills


### Learning Resources

**Official Documentation:**
- DBT: https://docs.getdbt.com/
- Airflow: https://airflow.apache.org/docs/
- Docker: https://docs.docker.com/

**Online Courses:**
- DBT Fundamentals (free)
- Airflow Fundamentals (Astronomer)
- Data Engineering Zoomcamp (DataTalks.Club)
- Coursera Data Engineering courses

**Books:**
- "Fundamentals of Data Engineering" by Joe Reis
- "The Data Warehouse Toolkit" by Ralph Kimball
- "Designing Data-Intensive Applications" by Martin Kleppmann

**Communities:**
- DBT Slack
- Airflow Slack
- r/dataengineering
- Data Engineering Weekly newsletter

**Practice:**
- Build personal projects
- Contribute to open source
- Kaggle datasets
- Real-world case studies


---

## Conclusion

### What We Covered

**Module 1:** Introduction to Modern Data Engineering
- Data engineering lifecycle
- Evolution of the field
- Modern data stack

**Module 2:** Data Warehousing Concepts
- OLTP vs OLAP
- Architecture patterns
- Data modeling approaches

**Module 3:** ETL vs ELT Paradigm
- Traditional ETL
- Modern ELT
- When to use each

**Module 4:** DBT Deep Dive
- Core concepts
- Project structure
- Materialization strategies
- Testing framework

**Module 5:** Apache Airflow Architecture
- Components and workflow
- DAGs and operators
- Scheduling and execution

**Module 6:** Docker & Containerization
- Container concepts
- Docker Compose
- Benefits for data engineering

**Module 7:** Data Modeling Best Practices
- Naming conventions
- Slowly changing dimensions
- Data grain

**Module 8:** Data Quality & Testing
- Quality dimensions
- Testing strategies
- Data contracts

**Module 9:** CI/CD for Data Pipelines
- Continuous integration
- Deployment automation
- Version control

**Module 10:** Real-World Architecture Patterns
- Modern data stack
- Lambda architecture
- Medallion architecture
- Data mesh


### Next Steps

**Immediate Actions:**
1. Complete the hands-on labs
2. Build your own DBT models
3. Experiment with Airflow DAGs
4. Practice with Docker

**Short-term Goals (1-3 months):**
1. Master SQL and DBT
2. Build a portfolio project
3. Learn cloud platform basics
4. Join data engineering communities

**Long-term Goals (6-12 months):**
1. Contribute to open source
2. Get certified (AWS/Azure/GCP)
3. Build production pipelines
4. Mentor others

**Remember:**
- Start small, iterate
- Focus on fundamentals
- Practice consistently
- Learn from failures
- Stay curious

---

## Questions & Discussion

**Topics for Discussion:**
1. How does your organization handle data transformations?
2. What challenges do you face with current tools?
3. How can these concepts apply to your work?
4. What architecture patterns interest you most?

**Open Floor:**
- Ask questions
- Share experiences
- Discuss use cases
- Network with peers

---

## Thank You!

**Workshop Materials:**
- Theory Lecture (this document)
- Hands-On Lab Guide
- Workshop Slides
- Project Repository

**Stay Connected:**
- Join our community
- Follow for updates
- Share your projects
- Keep learning!

**Good luck with your data engineering journey!**
