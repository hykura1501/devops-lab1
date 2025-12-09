# DataOps Implementation Guide
## Applying DataOps Principles to DBT + Airflow + SQL Server Project

---

## Table of Contents

1. [What is DataOps?](#what-is-dataops)
2. [DataOps Principles](#dataops-principles)
3. [CI/CD Pipeline Setup](#cicd-pipeline-setup)
4. [Automated Testing Strategy](#automated-testing-strategy)
5. [Monitoring & Observability](#monitoring--observability)
6. [Data Quality Framework](#data-quality-framework)
7. [Version Control Strategy](#version-control-strategy)
8. [Environment Management](#environment-management)
9. [Deployment Automation](#deployment-automation)
10. [Incident Response](#incident-response)

---

## What is DataOps?

**DataOps** is a collaborative data management practice focused on improving communication, integration, and automation of data flows between data managers and consumers.

### Key Goals:
- **Speed:** Faster delivery of data products
- **Quality:** Higher data quality and reliability
- **Collaboration:** Better teamwork across data teams
- **Automation:** Reduce manual processes
- **Monitoring:** Proactive issue detection

### DataOps vs DevOps:
| Aspect | DevOps | DataOps |
|--------|--------|---------|
| **Focus** | Application code | Data pipelines |
| **Testing** | Unit/integration tests | Data quality tests |
| **Deployment** | Code releases | Data model changes |
| **Monitoring** | Application metrics | Data quality metrics |
| **Rollback** | Previous code version | Data versioning |


---

## DataOps Principles

### 1. Continually Satisfy Your Customer
- Deliver analytics quickly (minutes to weeks)
- Iterate based on feedback
- Self-service analytics

### 2. Value Working Analytics
- Focus on delivering insights
- Measure business impact
- Prioritize usability

### 3. Embrace Change
- Flexible data models
- Adapt to new requirements
- Iterative development

### 4. It's a Team Sport
- Collaboration across roles
- Shared responsibility
- Cross-functional teams

### 5. Daily Interactions
- Regular communication
- Stand-ups and reviews
- Continuous feedback

### 6. Self-Organize
- Empowered teams
- Autonomous decision-making
- Clear ownership

### 7. Reduce Heroism
- Automated processes
- Documentation
- Knowledge sharing

### 8. Reflect and Improve
- Retrospectives
- Continuous improvement
- Learn from failures

### 9. Analytics is Code
- Version control
- Code reviews
- Automated testing

### 10. Orchestrate
- End-to-end automation
- Dependency management
- Workflow orchestration

### 11. Make it Reproducible
- Consistent environments
- Documented processes
- Automated deployments

### 12. Disposable Environments
- Easy to create/destroy
- Infrastructure as code
- Containerization

### 13. Simplicity
- Keep it simple
- Avoid over-engineering
- Clear and maintainable

### 14. Analytics is Manufacturing
- Treat data as product
- Quality control
- Production pipelines

### 15. Quality is Paramount
- Data quality tests
- Monitoring and alerts
- Continuous validation

### 16. Monitor Quality and Performance
- Real-time monitoring
- Performance metrics
- Proactive alerts


---

## CI/CD Pipeline Setup

### GitHub Actions Workflow

Create `.github/workflows/dbt-ci.yml`:

```yaml
name: DBT CI/CD Pipeline

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main, develop]

env:
  DBT_PROFILES_DIR: ./dbt

jobs:
  lint:
    name: Lint SQL and Python
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'
      
      - name: Install SQLFluff
        run: pip install sqlfluff sqlfluff-templater-dbt
      
      - name: Lint SQL files
        run: |
          cd dbt
          sqlfluff lint models/ --dialect tsql
      
      - name: Lint Python files
        run: |
          pip install flake8
          flake8 airflow/dags/ --max-line-length=120

  test:
    name: Run DBT Tests
    runs-on: ubuntu-latest
    needs: lint
    
    services:
      sqlserver:
        image: mcr.microsoft.com/mssql/server:2019-latest
        env:
          ACCEPT_EULA: Y
          SA_PASSWORD: YourStrong@Passw0rd
          MSSQL_PID: Express
        ports:
          - 1433:1433
        options: >-
          --health-cmd "/opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P YourStrong@Passw0rd -Q 'SELECT 1'"
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
    
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'
      
      - name: Install DBT
        run: |
          pip install dbt-sqlserver
      
      - name: Setup test database
        run: |
          # Restore test database or create test data
          echo "Setting up test database..."
      
      - name: DBT Debug
        run: |
          cd dbt
          dbt debug
      
      - name: DBT Deps
        run: |
          cd dbt
          dbt deps
      
      - name: DBT Run
        run: |
          cd dbt
          dbt run --target ci
      
      - name: DBT Test
        run: |
          cd dbt
          dbt test --target ci
      
      - name: Generate DBT Docs
        run: |
          cd dbt
          dbt docs generate
      
      - name: Upload DBT Artifacts
        uses: actions/upload-artifact@v3
        with:
          name: dbt-artifacts
          path: dbt/target/

  deploy-dev:
    name: Deploy to Development
    runs-on: ubuntu-latest
    needs: test
    if: github.ref == 'refs/heads/develop'
    
    steps:
      - uses: actions/checkout@v3
      
      - name: Deploy to Dev Environment
        run: |
          echo "Deploying to development..."
          # Add deployment commands here

  deploy-prod:
    name: Deploy to Production
    runs-on: ubuntu-latest
    needs: test
    if: github.ref == 'refs/heads/main'
    
    steps:
      - uses: actions/checkout@v3
      
      - name: Deploy to Production
        run: |
          echo "Deploying to production..."
          # Add deployment commands here
      
      - name: Notify Team
        run: |
          echo "Sending deployment notification..."
          # Add Slack/email notification
```


### Pre-commit Hooks

Create `.pre-commit-config.yaml`:

```yaml
repos:
  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v4.4.0
    hooks:
      - id: trailing-whitespace
      - id: end-of-file-fixer
      - id: check-yaml
      - id: check-added-large-files
      - id: check-merge-conflict
  
  - repo: https://github.com/sqlfluff/sqlfluff
    rev: 2.3.0
    hooks:
      - id: sqlfluff-lint
        args: [--dialect, tsql]
        files: \.sql$
      - id: sqlfluff-fix
        args: [--dialect, tsql]
        files: \.sql$
  
  - repo: https://github.com/psf/black
    rev: 23.7.0
    hooks:
      - id: black
        language_version: python3.9
        files: \.py$
  
  - repo: https://github.com/pycqa/flake8
    rev: 6.1.0
    hooks:
      - id: flake8
        args: [--max-line-length=120]
        files: \.py$
```

**Installation:**
```bash
pip install pre-commit
pre-commit install
```

**Usage:**
```bash
# Run on all files
pre-commit run --all-files

# Runs automatically on git commit
git commit -m "Your message"
```


---

## Automated Testing Strategy

### 1. DBT Schema Tests

**File:** `dbt/models/schema.yml`

```yaml
version: 2

models:
  - name: stg_customers
    description: "Staging table for customer data"
    tests:
      - dbt_utils.recency:
          datepart: day
          field: modified_date
          interval: 7
    columns:
      - name: customer_id
        description: "Primary key"
        tests:
          - unique
          - not_null
          - dbt_expectations.expect_column_values_to_be_of_type:
              column_type: integer
      
      - name: email
        tests:
          - unique
          - not_null
          - dbt_expectations.expect_column_values_to_match_regex:
              regex: "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$"
      
      - name: created_date
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: "2000-01-01"
              max_value: "{{ modules.datetime.date.today() }}"

  - name: customer_analytics
    description: "Customer analytics mart"
    tests:
      - dbt_utils.equal_rowcount:
          compare_model: ref('stg_customers')
    columns:
      - name: customer_id
        tests:
          - unique
          - not_null
          - relationships:
              to: ref('stg_customers')
              field: customer_id
      
      - name: lifetime_value
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 1000000
      
      - name: customer_segment
        tests:
          - accepted_values:
              values: ['High Value', 'Medium Value', 'Low Value']
```


### 2. Custom Data Tests

**File:** `dbt/tests/assert_positive_revenue.sql`

```sql
-- Test that all revenue values are positive
SELECT
    order_id,
    total_amount
FROM {{ ref('stg_orders') }}
WHERE total_amount < 0
```

**File:** `dbt/tests/assert_no_future_dates.sql`

```sql
-- Test that no order dates are in the future
SELECT
    order_id,
    order_date
FROM {{ ref('stg_orders') }}
WHERE order_date > GETDATE()
```

**File:** `dbt/tests/assert_customer_order_consistency.sql`

```sql
-- Test that all orders have valid customers
SELECT
    o.order_id,
    o.customer_id
FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('stg_customers') }} c
    ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL
```

**File:** `dbt/tests/assert_no_duplicate_orders.sql`

```sql
-- Test for duplicate orders
SELECT
    order_id,
    COUNT(*) as duplicate_count
FROM {{ ref('stg_orders') }}
GROUP BY order_id
HAVING COUNT(*) > 1
```


### 3. Source Freshness Tests

**File:** `dbt/models/sources.yml`

```yaml
version: 2

sources:
  - name: adventureworks
    database: AdventureWorks2014
    schema: Sales
    
    # Freshness configuration
    freshness:
      warn_after: {count: 12, period: hour}
      error_after: {count: 24, period: hour}
    
    loaded_at_field: ModifiedDate
    
    tables:
      - name: Customer
        description: "Customer master data"
        freshness:
          warn_after: {count: 6, period: hour}
          error_after: {count: 12, period: hour}
        columns:
          - name: CustomerID
            tests:
              - unique
              - not_null
      
      - name: SalesOrderHeader
        description: "Sales order transactions"
        freshness:
          warn_after: {count: 1, period: hour}
          error_after: {count: 3, period: hour}
```

**Run freshness checks:**
```bash
dbt source freshness
```


### 4. Unit Testing with pytest

**File:** `tests/test_dbt_models.py`

```python
import pytest
import subprocess
import json

def run_dbt_command(command):
    """Helper function to run dbt commands"""
    result = subprocess.run(
        f"cd dbt && dbt {command}",
        shell=True,
        capture_output=True,
        text=True
    )
    return result

def test_dbt_run_succeeds():
    """Test that dbt run completes successfully"""
    result = run_dbt_command("run")
    assert result.returncode == 0, f"dbt run failed: {result.stderr}"

def test_dbt_test_succeeds():
    """Test that all dbt tests pass"""
    result = run_dbt_command("test")
    assert result.returncode == 0, f"dbt tests failed: {result.stderr}"

def test_all_models_have_tests():
    """Test that all models have at least one test"""
    result = run_dbt_command("ls --resource-type model --output json")
    models = json.loads(result.stdout)
    
    result = run_dbt_command("ls --resource-type test --output json")
    tests = json.loads(result.stdout)
    
    tested_models = set([t['depends_on']['nodes'][0] for t in tests])
    untested_models = [m for m in models if m['unique_id'] not in tested_models]
    
    assert len(untested_models) == 0, f"Models without tests: {untested_models}"

def test_no_orphaned_models():
    """Test that all models are used somewhere"""
    result = run_dbt_command("ls --resource-type model --output json")
    models = json.loads(result.stdout)
    
    # Check if models are referenced
    for model in models:
        if model['config']['materialized'] != 'ephemeral':
            # Check if model is referenced by other models or exposures
            pass  # Add logic to check references

def test_model_naming_conventions():
    """Test that models follow naming conventions"""
    result = run_dbt_command("ls --resource-type model --output json")
    models = json.loads(result.stdout)
    
    for model in models:
        name = model['name']
        path = model['original_file_path']
        
        if 'staging' in path:
            assert name.startswith('stg_'), f"Staging model {name} should start with 'stg_'"
        elif 'intermediate' in path:
            assert name.startswith('int_'), f"Intermediate model {name} should start with 'int_'"
```

**Run tests:**
```bash
pytest tests/
```


---

## Monitoring & Observability

### 1. DBT Artifacts Monitoring

**File:** `airflow/dags/dbt_monitoring_dag.py`

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import json
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['data-team@company.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dbt_monitoring',
    default_args=default_args,
    description='Monitor DBT pipeline health',
    schedule_interval='@hourly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

def check_dbt_run_results(**context):
    """Check DBT run results for failures"""
    results_path = '/opt/airflow/dbt/target/run_results.json'
    
    if not os.path.exists(results_path):
        raise FileNotFoundError("DBT run results not found")
    
    with open(results_path, 'r') as f:
        results = json.load(f)
    
    failed_models = [
        r['unique_id'] 
        for r in results['results'] 
        if r['status'] == 'error'
    ]
    
    if failed_models:
        raise Exception(f"DBT models failed: {failed_models}")
    
    # Log metrics
    total_models = len(results['results'])
    execution_time = results['elapsed_time']
    
    print(f"Total models: {total_models}")
    print(f"Execution time: {execution_time}s")
    
    return {
        'total_models': total_models,
        'execution_time': execution_time,
        'failed_models': len(failed_models)
    }

def check_data_freshness(**context):
    """Check source data freshness"""
    freshness_path = '/opt/airflow/dbt/target/sources.json'
    
    if not os.path.exists(freshness_path):
        print("No freshness data available")
        return
    
    with open(freshness_path, 'r') as f:
        sources = json.load(f)
    
    stale_sources = [
        s['unique_id']
        for s in sources['results']
        if s['status'] == 'error'
    ]
    
    if stale_sources:
        raise Exception(f"Stale data sources: {stale_sources}")

def check_test_results(**context):
    """Check DBT test results"""
    results_path = '/opt/airflow/dbt/target/run_results.json'
    
    with open(results_path, 'r') as f:
        results = json.load(f)
    
    failed_tests = [
        r['unique_id']
        for r in results['results']
        if r['status'] == 'fail' and r['resource_type'] == 'test'
    ]
    
    if failed_tests:
        raise Exception(f"DBT tests failed: {failed_tests}")

# Tasks
check_run = PythonOperator(
    task_id='check_dbt_run_results',
    python_callable=check_dbt_run_results,
    dag=dag,
)

check_freshness = PythonOperator(
    task_id='check_data_freshness',
    python_callable=check_data_freshness,
    dag=dag,
)

check_tests = PythonOperator(
    task_id='check_test_results',
    python_callable=check_test_results,
    dag=dag,
)

# Dependencies
[check_run, check_freshness, check_tests]
```


### 2. Prometheus Metrics

**File:** `monitoring/prometheus.yml`

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'airflow'
    static_configs:
      - targets: ['airflow-webserver:8080']
    metrics_path: '/admin/metrics'
  
  - job_name: 'sqlserver'
    static_configs:
      - targets: ['sqlserver:1433']
  
  - job_name: 'dbt_metrics'
    static_configs:
      - targets: ['dbt-exporter:9090']
```

**File:** `monitoring/dbt_exporter.py`

```python
from prometheus_client import start_http_server, Gauge, Counter
import json
import time
import os

# Define metrics
dbt_models_total = Gauge('dbt_models_total', 'Total number of DBT models')
dbt_execution_time = Gauge('dbt_execution_time_seconds', 'DBT execution time')
dbt_model_failures = Counter('dbt_model_failures_total', 'Total DBT model failures')
dbt_test_failures = Counter('dbt_test_failures_total', 'Total DBT test failures')

def collect_dbt_metrics():
    """Collect metrics from DBT artifacts"""
    results_path = '/opt/airflow/dbt/target/run_results.json'
    
    if os.path.exists(results_path):
        with open(results_path, 'r') as f:
            results = json.load(f)
        
        # Update metrics
        dbt_models_total.set(len(results['results']))
        dbt_execution_time.set(results['elapsed_time'])
        
        for result in results['results']:
            if result['status'] == 'error':
                if result['resource_type'] == 'model':
                    dbt_model_failures.inc()
                elif result['resource_type'] == 'test':
                    dbt_test_failures.inc()

if __name__ == '__main__':
    # Start metrics server
    start_http_server(9090)
    
    # Collect metrics every 60 seconds
    while True:
        collect_dbt_metrics()
        time.sleep(60)
```


### 3. Grafana Dashboards

**File:** `monitoring/grafana-dashboard.json`

```json
{
  "dashboard": {
    "title": "DBT Pipeline Monitoring",
    "panels": [
      {
        "title": "DBT Model Execution Time",
        "type": "graph",
        "targets": [
          {
            "expr": "dbt_execution_time_seconds"
          }
        ]
      },
      {
        "title": "Total Models",
        "type": "stat",
        "targets": [
          {
            "expr": "dbt_models_total"
          }
        ]
      },
      {
        "title": "Model Failures (24h)",
        "type": "stat",
        "targets": [
          {
            "expr": "increase(dbt_model_failures_total[24h])"
          }
        ]
      },
      {
        "title": "Test Failures (24h)",
        "type": "stat",
        "targets": [
          {
            "expr": "increase(dbt_test_failures_total[24h])"
          }
        ]
      },
      {
        "title": "Airflow DAG Success Rate",
        "type": "graph",
        "targets": [
          {
            "expr": "rate(airflow_dag_run_success[1h])"
          }
        ]
      }
    ]
  }
}
```


### 4. Alerting Configuration

**File:** `monitoring/alertmanager.yml`

```yaml
global:
  slack_api_url: 'YOUR_SLACK_WEBHOOK_URL'

route:
  group_by: ['alertname', 'severity']
  group_wait: 10s
  group_interval: 10s
  repeat_interval: 12h
  receiver: 'slack-notifications'

receivers:
  - name: 'slack-notifications'
    slack_configs:
      - channel: '#data-alerts'
        title: 'Data Pipeline Alert'
        text: '{{ range .Alerts }}{{ .Annotations.description }}{{ end }}'

inhibit_rules:
  - source_match:
      severity: 'critical'
    target_match:
      severity: 'warning'
    equal: ['alertname']
```

**File:** `monitoring/alert-rules.yml`

```yaml
groups:
  - name: dbt_alerts
    interval: 1m
    rules:
      - alert: DBTModelFailure
        expr: increase(dbt_model_failures_total[5m]) > 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "DBT model failed"
          description: "One or more DBT models have failed in the last 5 minutes"
      
      - alert: DBTTestFailure
        expr: increase(dbt_test_failures_total[5m]) > 0
        for: 1m
        labels:
          severity: warning
        annotations:
          summary: "DBT test failed"
          description: "One or more DBT tests have failed"
      
      - alert: DBTExecutionTimeSlow
        expr: dbt_execution_time_seconds > 3600
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "DBT execution is slow"
          description: "DBT execution time exceeded 1 hour"
      
      - alert: AirflowDAGFailure
        expr: airflow_dag_run_failed > 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Airflow DAG failed"
          description: "Airflow DAG {{ $labels.dag_id }} has failed"
      
      - alert: DataFreshness
        expr: dbt_source_freshness_error > 0
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Data source is stale"
          description: "Data source freshness check failed"
```


---

## Data Quality Framework

### 1. Great Expectations Integration

**File:** `great_expectations/great_expectations.yml`

```yaml
config_version: 3.0

datasources:
  sqlserver_datasource:
    class_name: Datasource
    execution_engine:
      class_name: SqlAlchemyExecutionEngine
      connection_string: mssql+pyodbc://sa:YourStrong@Passw0rd@sqlserver:1433/AdventureWorks2014?driver=ODBC+Driver+17+for+SQL+Server
    data_connectors:
      default_runtime_data_connector:
        class_name: RuntimeDataConnector
        batch_identifiers:
          - default_identifier_name

stores:
  expectations_store:
    class_name: ExpectationsStore
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: expectations/

  validations_store:
    class_name: ValidationsStore
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: uncommitted/validations/

  evaluation_parameter_store:
    class_name: EvaluationParameterStore

  checkpoint_store:
    class_name: CheckpointStore
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: checkpoints/
```


### 2. Data Quality Expectations

**File:** `great_expectations/expectations/customer_suite.json`

```json
{
  "expectation_suite_name": "customer_quality_suite",
  "expectations": [
    {
      "expectation_type": "expect_table_row_count_to_be_between",
      "kwargs": {
        "min_value": 1000,
        "max_value": 1000000
      }
    },
    {
      "expectation_type": "expect_column_values_to_be_unique",
      "kwargs": {
        "column": "customer_id"
      }
    },
    {
      "expectation_type": "expect_column_values_to_not_be_null",
      "kwargs": {
        "column": "customer_id"
      }
    },
    {
      "expectation_type": "expect_column_values_to_match_regex",
      "kwargs": {
        "column": "email",
        "regex": "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$"
      }
    },
    {
      "expectation_type": "expect_column_mean_to_be_between",
      "kwargs": {
        "column": "lifetime_value",
        "min_value": 100,
        "max_value": 10000
      }
    }
  ]
}
```

**File:** `airflow/dags/data_quality_dag.py`

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import great_expectations as ge

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 1,
}

dag = DAG(
    'data_quality_checks',
    default_args=default_args,
    description='Run data quality checks',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

def run_ge_checkpoint(checkpoint_name):
    """Run Great Expectations checkpoint"""
    context = ge.data_context.DataContext()
    result = context.run_checkpoint(checkpoint_name=checkpoint_name)
    
    if not result["success"]:
        raise Exception(f"Data quality check failed: {checkpoint_name}")
    
    return result

check_customers = PythonOperator(
    task_id='check_customer_quality',
    python_callable=run_ge_checkpoint,
    op_kwargs={'checkpoint_name': 'customer_checkpoint'},
    dag=dag,
)

check_orders = PythonOperator(
    task_id='check_order_quality',
    python_callable=run_ge_checkpoint,
    op_kwargs={'checkpoint_name': 'order_checkpoint'},
    dag=dag,
)

[check_customers, check_orders]
```


---

## Version Control Strategy

### Git Branching Strategy

```
main (production)
  ↑
  └── develop (staging)
        ↑
        ├── feature/add-customer-segmentation
        ├── feature/product-analytics
        ├── bugfix/fix-date-calculation
        └── hotfix/critical-data-issue
```

### Branch Naming Conventions

```
feature/   - New features or models
bugfix/    - Bug fixes
hotfix/    - Emergency production fixes
refactor/  - Code refactoring
docs/      - Documentation updates
test/      - Test additions or updates
```

### Commit Message Format

```
<type>(<scope>): <subject>

<body>

<footer>
```

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation
- `style`: Formatting
- `refactor`: Code restructuring
- `test`: Adding tests
- `chore`: Maintenance

**Examples:**
```
feat(models): add customer segmentation model

- Added int_customer_metrics intermediate model
- Created customer_analytics mart
- Added tests for new models

Closes #123

---

fix(staging): correct date calculation in stg_orders

The order_date was being cast incorrectly, causing timezone issues.

Fixes #456

---

docs(readme): update setup instructions

Added Docker installation steps for Windows users
```


### Pull Request Template

**File:** `.github/pull_request_template.md`

```markdown
## Description
<!-- Describe your changes in detail -->

## Type of Change
- [ ] New feature (non-breaking change which adds functionality)
- [ ] Bug fix (non-breaking change which fixes an issue)
- [ ] Breaking change (fix or feature that would cause existing functionality to not work as expected)
- [ ] Documentation update

## Changes Made
<!-- List the specific changes -->
- 
- 
- 

## Models Affected
<!-- List DBT models that are new or modified -->
- 
- 

## Testing
- [ ] All DBT tests pass locally
- [ ] Added new tests for new models
- [ ] Tested with sample data
- [ ] Verified data quality

## Checklist
- [ ] My code follows the style guidelines
- [ ] I have performed a self-review
- [ ] I have commented my code where necessary
- [ ] I have updated the documentation
- [ ] My changes generate no new warnings
- [ ] I have added tests that prove my fix/feature works
- [ ] New and existing tests pass locally
- [ ] Any dependent changes have been merged

## Screenshots (if applicable)
<!-- Add screenshots of DBT docs, lineage, or results -->

## Additional Notes
<!-- Any additional information -->
```


---

## Environment Management

### Multi-Environment Setup

**File:** `dbt/profiles.yml`

```yaml
adventureworks:
  target: dev
  outputs:
    dev:
      type: sqlserver
      driver: 'ODBC Driver 17 for SQL Server'
      server: localhost
      port: 1433
      database: AdventureWorks2014_Dev
      schema: dbo
      user: sa
      password: "{{ env_var('DBT_DEV_PASSWORD') }}"
      threads: 4
    
    ci:
      type: sqlserver
      driver: 'ODBC Driver 17 for SQL Server'
      server: localhost
      port: 1433
      database: AdventureWorks2014_CI
      schema: dbo
      user: sa
      password: "{{ env_var('DBT_CI_PASSWORD') }}"
      threads: 2
    
    staging:
      type: sqlserver
      driver: 'ODBC Driver 17 for SQL Server'
      server: staging-sqlserver.company.com
      port: 1433
      database: AdventureWorks2014_Staging
      schema: dbo
      user: "{{ env_var('DBT_STAGING_USER') }}"
      password: "{{ env_var('DBT_STAGING_PASSWORD') }}"
      threads: 8
    
    prod:
      type: sqlserver
      driver: 'ODBC Driver 17 for SQL Server'
      server: prod-sqlserver.company.com
      port: 1433
      database: AdventureWorks2014_Prod
      schema: dbo
      user: "{{ env_var('DBT_PROD_USER') }}"
      password: "{{ env_var('DBT_PROD_PASSWORD') }}"
      threads: 16
```

### Environment Variables

**File:** `.env.example`

```bash
# Development
DBT_DEV_PASSWORD=YourStrong@Passw0rd

# CI/CD
DBT_CI_PASSWORD=YourStrong@Passw0rd

# Staging
DBT_STAGING_USER=staging_user
DBT_STAGING_PASSWORD=staging_password

# Production
DBT_PROD_USER=prod_user
DBT_PROD_PASSWORD=prod_password

# Airflow
AIRFLOW__CORE__FERNET_KEY=your_fernet_key
AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow

# Monitoring
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
PROMETHEUS_URL=http://prometheus:9090
GRAFANA_URL=http://grafana:3000
```


### Docker Compose for Multiple Environments

**File:** `docker-compose.staging.yml`

```yaml
version: '3.8'

services:
  sqlserver:
    image: mcr.microsoft.com/mssql/server:2019-latest
    environment:
      - ACCEPT_EULA=Y
      - SA_PASSWORD=${STAGING_SQL_PASSWORD}
      - MSSQL_PID=Standard
    ports:
      - "1434:1433"
    volumes:
      - sqlserver_staging_data:/var/opt/mssql
    networks:
      - staging_network

  dbt:
    build:
      context: ./dbt
      dockerfile: Dockerfile
    environment:
      - DBT_TARGET=staging
      - DBT_STAGING_PASSWORD=${STAGING_SQL_PASSWORD}
    volumes:
      - ./dbt:/usr/app/dbt
    networks:
      - staging_network

  airflow-webserver:
    build:
      context: ./airflow
      dockerfile: Dockerfile
    environment:
      - AIRFLOW__CORE__EXECUTOR=CeleryExecutor
      - AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow_staging
    ports:
      - "8081:8080"
    networks:
      - staging_network

volumes:
  sqlserver_staging_data:

networks:
  staging_network:
    driver: bridge
```

**Usage:**
```bash
# Development
docker-compose up -d

# Staging
docker-compose -f docker-compose.yml -f docker-compose.staging.yml up -d

# Production
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```


---

## Deployment Automation

### Deployment Script

**File:** `scripts/deploy.sh`

```bash
#!/bin/bash

set -e

ENVIRONMENT=$1
TARGET=$2

if [ -z "$ENVIRONMENT" ] || [ -z "$TARGET" ]; then
    echo "Usage: ./deploy.sh <environment> <target>"
    echo "Example: ./deploy.sh staging staging"
    exit 1
fi

echo "🚀 Deploying to $ENVIRONMENT environment..."

# 1. Run pre-deployment checks
echo "📋 Running pre-deployment checks..."
cd dbt
dbt deps --target $TARGET
dbt compile --target $TARGET

# 2. Run tests on current state
echo "🧪 Running tests..."
dbt test --target $TARGET --select state:modified+

# 3. Backup current state
echo "💾 Creating backup..."
timestamp=$(date +%Y%m%d_%H%M%S)
backup_dir="backups/${ENVIRONMENT}_${timestamp}"
mkdir -p $backup_dir
cp -r target/ $backup_dir/

# 4. Deploy models
echo "🔨 Deploying models..."
dbt run --target $TARGET

# 5. Run post-deployment tests
echo "✅ Running post-deployment tests..."
dbt test --target $TARGET

# 6. Generate documentation
echo "📚 Generating documentation..."
dbt docs generate --target $TARGET

# 7. Update data catalog
echo "📖 Updating data catalog..."
# Add your data catalog update logic here

# 8. Send notification
echo "📢 Sending deployment notification..."
curl -X POST $SLACK_WEBHOOK_URL \
  -H 'Content-Type: application/json' \
  -d "{
    \"text\": \"✅ Deployment to $ENVIRONMENT completed successfully\",
    \"blocks\": [
      {
        \"type\": \"section\",
        \"text\": {
          \"type\": \"mrkdwn\",
          \"text\": \"*Deployment Summary*\n• Environment: $ENVIRONMENT\n• Target: $TARGET\n• Time: $(date)\n• Status: Success\"
        }
      }
    ]
  }"

echo "✨ Deployment completed successfully!"
```

**Make executable:**
```bash
chmod +x scripts/deploy.sh
```

**Usage:**
```bash
./scripts/deploy.sh staging staging
./scripts/deploy.sh production prod
```


### Blue-Green Deployment

**File:** `scripts/blue_green_deploy.sh`

```bash
#!/bin/bash

set -e

ENVIRONMENT=$1
NEW_SCHEMA="blue"
OLD_SCHEMA="green"

echo "🔵🟢 Starting blue-green deployment..."

# 1. Deploy to blue schema
echo "Deploying to blue schema..."
dbt run --target $ENVIRONMENT --vars "{'schema_suffix': '_blue'}"

# 2. Run tests on blue
echo "Testing blue deployment..."
dbt test --target $ENVIRONMENT --vars "{'schema_suffix': '_blue'}"

# 3. Run smoke tests
echo "Running smoke tests..."
python scripts/smoke_tests.py --schema blue

# 4. Switch traffic to blue
echo "Switching traffic to blue..."
sqlcmd -S $SQL_SERVER -U $SQL_USER -P $SQL_PASSWORD -Q "
    -- Update views to point to blue schema
    ALTER VIEW customer_analytics AS 
    SELECT * FROM marts_blue.customer_analytics;
"

# 5. Monitor for issues
echo "Monitoring for 5 minutes..."
sleep 300

# 6. Check for errors
if [ $? -eq 0 ]; then
    echo "✅ Deployment successful! Cleaning up green schema..."
    # Drop old green schema
    sqlcmd -S $SQL_SERVER -U $SQL_USER -P $SQL_PASSWORD -Q "
        DROP SCHEMA IF EXISTS marts_green;
    "
else
    echo "❌ Deployment failed! Rolling back to green..."
    sqlcmd -S $SQL_SERVER -U $SQL_USER -P $SQL_PASSWORD -Q "
        ALTER VIEW customer_analytics AS 
        SELECT * FROM marts_green.customer_analytics;
    "
    exit 1
fi

echo "✨ Blue-green deployment completed!"
```


---

## Incident Response

### Incident Response Playbook

#### 1. Data Quality Issue

**Symptoms:**
- DBT tests failing
- Unexpected data values
- Missing data

**Response Steps:**

```bash
# 1. Identify the issue
dbt test --select <model_name>

# 2. Check recent changes
git log --oneline -10

# 3. Review data lineage
dbt docs generate
# Open docs and check lineage

# 4. Check source data
sqlcmd -S localhost -U sa -P "YourStrong@Passw0rd" -Q "
    SELECT TOP 100 * FROM Sales.Customer 
    ORDER BY ModifiedDate DESC;
"

# 5. Rollback if needed
git revert <commit_hash>
dbt run --select <model_name>

# 6. Document incident
# Create incident report in incidents/ folder
```

#### 2. Pipeline Failure

**Symptoms:**
- Airflow DAG failing
- DBT run errors
- Timeout issues

**Response Steps:**

```bash
# 1. Check Airflow logs
docker logs dbt_airflow_project-airflow-scheduler-1

# 2. Check DBT logs
cat dbt/logs/dbt.log

# 3. Verify database connectivity
docker exec dbt_airflow_project-dbt-1 dbt debug

# 4. Check resource usage
docker stats

# 5. Restart services if needed
docker-compose restart airflow-scheduler
docker-compose restart dbt

# 6. Re-run failed tasks
# In Airflow UI: Clear failed tasks and re-run
```


### Incident Report Template

**File:** `incidents/YYYY-MM-DD-incident-name.md`

```markdown
# Incident Report: [Incident Name]

## Summary
**Date:** YYYY-MM-DD
**Duration:** X hours
**Severity:** Critical/High/Medium/Low
**Status:** Resolved/Investigating/Monitoring

## Impact
- **Users Affected:** 
- **Data Affected:** 
- **Business Impact:** 

## Timeline
- **HH:MM** - Issue detected
- **HH:MM** - Team notified
- **HH:MM** - Root cause identified
- **HH:MM** - Fix deployed
- **HH:MM** - Issue resolved

## Root Cause
[Detailed explanation of what caused the incident]

## Resolution
[Steps taken to resolve the incident]

## Action Items
- [ ] Fix implemented
- [ ] Tests added
- [ ] Documentation updated
- [ ] Monitoring improved
- [ ] Team notified

## Prevention
[How to prevent this from happening again]

## Lessons Learned
[What we learned from this incident]
```


---

## Implementation Roadmap

### Phase 1: Foundation (Week 1-2)

**Goals:**
- Set up version control
- Implement basic CI/CD
- Add essential tests

**Tasks:**
- [ ] Initialize Git repository
- [ ] Set up GitHub Actions
- [ ] Add pre-commit hooks
- [ ] Create basic DBT tests
- [ ] Document setup process

### Phase 2: Testing & Quality (Week 3-4)

**Goals:**
- Comprehensive test coverage
- Data quality framework
- Automated testing

**Tasks:**
- [ ] Add schema tests to all models
- [ ] Create custom data tests
- [ ] Set up Great Expectations
- [ ] Implement source freshness checks
- [ ] Add unit tests for Python code

### Phase 3: Monitoring (Week 5-6)

**Goals:**
- Real-time monitoring
- Alerting system
- Performance tracking

**Tasks:**
- [ ] Set up Prometheus
- [ ] Configure Grafana dashboards
- [ ] Implement alerting rules
- [ ] Add DBT metrics collection
- [ ] Set up Slack notifications

### Phase 4: Automation (Week 7-8)

**Goals:**
- Automated deployments
- Environment management
- Incident response

**Tasks:**
- [ ] Create deployment scripts
- [ ] Set up multiple environments
- [ ] Implement blue-green deployment
- [ ] Create incident playbooks
- [ ] Automate documentation generation

### Phase 5: Optimization (Week 9-10)

**Goals:**
- Performance optimization
- Cost reduction
- Process improvement

**Tasks:**
- [ ] Optimize DBT models
- [ ] Implement incremental models
- [ ] Add caching strategies
- [ ] Review and optimize costs
- [ ] Conduct retrospective


---

## Best Practices Checklist

### Code Quality
- [ ] All SQL follows style guide
- [ ] Models have clear descriptions
- [ ] Complex logic is commented
- [ ] Naming conventions followed
- [ ] No hardcoded values

### Testing
- [ ] All models have tests
- [ ] Primary keys tested for uniqueness
- [ ] Foreign keys tested for relationships
- [ ] Business logic validated
- [ ] Edge cases covered

### Documentation
- [ ] README is up to date
- [ ] Models documented in YAML
- [ ] DBT docs generated
- [ ] Architecture diagrams current
- [ ] Runbooks available

### Version Control
- [ ] Meaningful commit messages
- [ ] Small, focused commits
- [ ] Pull requests reviewed
- [ ] No secrets in code
- [ ] .gitignore configured

### CI/CD
- [ ] Tests run on every commit
- [ ] Automated deployments
- [ ] Environment parity
- [ ] Rollback capability
- [ ] Deployment notifications

### Monitoring
- [ ] Key metrics tracked
- [ ] Alerts configured
- [ ] Dashboards available
- [ ] Logs centralized
- [ ] Performance monitored

### Security
- [ ] Credentials in environment variables
- [ ] Access controls implemented
- [ ] Audit logging enabled
- [ ] Data encryption at rest
- [ ] Regular security reviews

### Operations
- [ ] Incident response plan
- [ ] On-call rotation
- [ ] Backup strategy
- [ ] Disaster recovery plan
- [ ] Regular maintenance


---

## Tools & Resources

### Essential Tools

**Version Control:**
- Git
- GitHub/GitLab/Bitbucket

**CI/CD:**
- GitHub Actions
- GitLab CI
- Jenkins
- CircleCI

**Testing:**
- DBT built-in tests
- Great Expectations
- pytest
- SQLFluff

**Monitoring:**
- Prometheus
- Grafana
- Datadog
- New Relic

**Orchestration:**
- Apache Airflow
- Dagster
- Prefect

**Data Quality:**
- Great Expectations
- Soda
- Monte Carlo
- Datafold

**Documentation:**
- DBT docs
- Confluence
- Notion
- GitBook

### Learning Resources

**Books:**
- "The DataOps Cookbook" by Christopher Bergh
- "Fundamentals of Data Engineering" by Joe Reis
- "Data Pipelines Pocket Reference" by James Densmore

**Courses:**
- DataOps Fundamentals (DataKitchen)
- DBT Fundamentals (dbt Labs)
- Airflow Fundamentals (Astronomer)

**Communities:**
- DBT Slack
- Airflow Slack
- DataOps Community
- r/dataengineering

**Blogs:**
- dbt Labs Blog
- Astronomer Blog
- DataKitchen Blog
- Locally Optimistic


---

## Conclusion

### Key Takeaways

1. **Automation is Essential**
   - Automate testing, deployment, and monitoring
   - Reduce manual processes
   - Increase reliability

2. **Quality First**
   - Test early and often
   - Monitor continuously
   - Fix issues proactively

3. **Collaboration Matters**
   - Clear communication
   - Shared responsibility
   - Cross-functional teams

4. **Iterate and Improve**
   - Start small
   - Learn from failures
   - Continuous improvement

5. **Documentation is Critical**
   - Keep it updated
   - Make it accessible
   - Automate when possible

### Next Steps

**Immediate (This Week):**
1. Set up Git repository
2. Add basic tests
3. Create CI/CD pipeline
4. Document current state

**Short-term (This Month):**
1. Implement monitoring
2. Add comprehensive tests
3. Automate deployments
4. Create runbooks

**Long-term (This Quarter):**
1. Optimize performance
2. Implement advanced monitoring
3. Build data quality framework
4. Establish DataOps culture

### Success Metrics

Track these metrics to measure DataOps success:

**Speed:**
- Time from commit to production
- Mean time to recovery (MTTR)
- Deployment frequency

**Quality:**
- Test coverage percentage
- Data quality score
- Incident frequency

**Efficiency:**
- Pipeline execution time
- Resource utilization
- Cost per pipeline run

**Collaboration:**
- Pull request review time
- Documentation coverage
- Team satisfaction

---

## Questions?

For questions or support:
- Create an issue in the repository
- Contact the data engineering team
- Join our Slack channel: #dataops

**Happy DataOps-ing! 🚀**
