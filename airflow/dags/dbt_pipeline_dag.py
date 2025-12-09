"""
DBT Pipeline DAG for Airflow Orchestration

This DAG orchestrates the complete dbt data transformation pipeline following
a medallion architecture pattern (Bronze -> Silver -> Gold).

Pipeline Flow:
1. Staging Layer: Extract and clean raw data from source systems
2. Intermediate Layer: Apply business logic and transformations
3. Marts Layer: Create business-ready analytical models
4. Data Quality Tests: Validate data quality and integrity
5. Notifications: Send success/failure notifications

Dependencies:
- Staging models must complete before intermediate models
- Intermediate models must complete before mart models
- Mart models must complete before running tests
- Any failure triggers failure notification

Retry Strategy:
- All tasks retry up to 3 times with 5-minute delays
- Global failure callback ensures failure notifications are sent
- Prevents cascading failures with proper error handling

Scheduling:
- Runs daily at 2:00 AM UTC (0 2 * * *)
- catchup=False prevents backfilling
- max_active_runs=1 ensures only one instance runs at a time
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
import logging

from utils.logging_utils import DataOpsLogger, log_task_execution
from utils.alerting import AlertManager

# Configure logging
logger = logging.getLogger(__name__)

# Default arguments for all tasks
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# DAG definition
dag = DAG(
    'dbt_pipeline_dag',
    default_args=default_args,
    description='Orchestrates dbt data transformation pipeline (staging -> intermediate -> marts -> tests)',
    schedule_interval='0 2 * * *',  # Daily at 2:00 AM UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['dbt', 'data-pipeline', 'etl', 'data-quality'],
    doc_md=__doc__,
)

# ============================================================================
# HELPER FUNCTIONS FOR NOTIFICATIONS AND LOGGING
# ============================================================================

def log_pipeline_start(**context):
    """
    Log the start of the pipeline and store start time in XCom.
    
    Args:
        **context: Airflow context dictionary containing task instance info
    """
    dag_run = context.get('dag_run')
    execution_date = context.get('execution_date')
    
    # Initialize logger
    pipeline_logger = DataOpsLogger("dbt_pipeline", "pipeline_start")
    
    # Log pipeline start event
    start_time = datetime.now()
    pipeline_logger.log_event(
        "pipeline_start",
        f"Starting DBT pipeline execution for DAG: {dag_run.dag_id}",
        level="info"
    )
    
    # Store start time in XCom for later use
    context['ti'].xcom_push(key='start_time', value=start_time.isoformat())
    
    logger.info(f"Pipeline started at: {start_time.isoformat()}")
    return {"start_time": start_time.isoformat()}

def notify_success(**context):
    """
    Send success notification when pipeline completes successfully.
    
    This function logs success message and sends Slack notifications
    using the AlertManager utility.
    
    Args:
        **context: Airflow context dictionary containing task instance info
    """
    dag_run = context.get('dag_run')
    execution_date = context.get('execution_date')
    ti = context.get('ti')
    
    # Initialize logger and alert manager
    pipeline_logger = DataOpsLogger("dbt_pipeline", "success_notification")
    alert_manager = AlertManager()
    
    # Calculate execution time if start time is available
    start_time = None
    execution_time = None
    try:
        # Try to get start time from XCom (from log_pipeline_start task)
        start_data = ti.xcom_pull(task_ids='log_pipeline_start', key='start_time')
        if not start_data:
            # Fallback: try to get from return value
            start_data_dict = ti.xcom_pull(task_ids='log_pipeline_start')
            if start_data_dict and isinstance(start_data_dict, dict):
                start_data = start_data_dict.get('start_time')
        
        if start_data:
            start_time = datetime.fromisoformat(start_data)
            execution_time = (datetime.now() - start_time).total_seconds()
    except Exception as e:
        logger.warning(f"Could not calculate execution time: {e}")
        pass
    
    # Build success message
    success_message = f"""DBT Pipeline Successfully Completed!

DAG: {dag_run.dag_id}
Execution Date: {execution_date}
Run ID: {dag_run.run_id}

All stages completed:
• Staging models
• Intermediate models
• Mart models
• Data quality tests"""
    
    if execution_time:
        success_message += f"\n\nExecution Time: {execution_time:.2f} seconds"
        pipeline_logger.log_metric("pipeline_execution_time", execution_time, "seconds")
    
    success_message += f"\n\nPipeline completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}"
    
    # Log success event
    pipeline_logger.log_event("pipeline_success", "All pipeline stages completed successfully", level="info")
    logger.info(success_message)
    print(success_message)
    
    # Send Slack notification
    alert_manager.send_slack_alert(
        title=f"Pipeline Success: {dag_run.dag_id}",
        message=success_message,
        severity="info"
    )

def notify_failure(context):
    """
    Send failure notification when any task fails.
    
    This function is called as a failure callback and logs detailed
    error information. Sends Slack alerts using AlertManager.
    
    Args:
        context: Airflow context dictionary containing failure details
    """
    dag_run = context.get('dag_run')
    task_instance = context.get('task_instance')
    execution_date = context.get('execution_date')
    exception = context.get('exception')
    ti = context.get('ti')
    
    # Initialize logger and alert manager
    pipeline_logger = DataOpsLogger("dbt_pipeline", "failure_notification")
    alert_manager = AlertManager()
    
    # Get error message
    error_message = str(exception) if exception else 'Unknown error'
    
    # Try to get task logs for more context
    try:
        task_logs = ti.get_logs()
        if task_logs:
            # Get last 500 characters of logs for context
            log_snippet = task_logs[-500:] if len(task_logs) > 500 else task_logs
            error_message += f"\n\nLast log entries:\n{log_snippet}"
    except Exception:
        pass
    
    # Build failure message
    failure_message = f"""DBT Pipeline Failed!

DAG: {dag_run.dag_id}
Execution Date: {execution_date}
Run ID: {dag_run.run_id}
Failed Task: {task_instance.task_id}
Exception: {error_message}

Please check the Airflow logs for detailed error information.
Failure occurred at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}"""
    
    # Log failure event
    pipeline_logger.log_event(
        "pipeline_failure",
        f"Task {task_instance.task_id} failed: {error_message}",
        level="error"
    )
    
    # Log task execution failure
    log_task_execution(
        task_id=task_instance.task_id,
        status="failed",
        error=error_message
    )
    
    logger.error(failure_message)
    print(failure_message)
    
    # Send Slack alert using AlertManager
    alert_manager.alert_pipeline_failure(
        dag_id=dag_run.dag_id,
        task_id=task_instance.task_id,
        error_message=error_message
    )

# ============================================================================
# DBT TASKS
# ============================================================================

# Task 0: Log pipeline start
log_pipeline_start_task = PythonOperator(
    task_id='log_pipeline_start',
    python_callable=log_pipeline_start,
    dag=dag,
    doc_md="""
    **Log Pipeline Start**
    
    Logs the start of the pipeline execution and stores the start time
    in XCom for execution time calculation in the success notification.
    """,
)

# Task 1: Run staging models
# This extracts and cleans raw data from source systems
load_staging = BashOperator(
    task_id='load_staging',
    bash_command='docker exec dbt_airflow_project-dbt-1 dbt run --select staging',
    dag=dag,
    doc_md="""
    **Load Staging Models**
    
    Runs all dbt models in the staging layer.
    This includes:
    - stg_customers
    - stg_products
    - stg_sales_orders
    
    These models extract and clean raw data from AdventureWorks sources.
    """,
)

# Task 2: Run intermediate models
# This applies business logic and transformations
run_intermediate = BashOperator(
    task_id='run_intermediate',
    bash_command='docker exec dbt_airflow_project-dbt-1 dbt run --select intermediate',
    dag=dag,
    doc_md="""
    **Run Intermediate Models**
    
    Runs all dbt models in the intermediate layer.
    This includes:
    - int_customer_orders
    - int_product_sales
    
    These models apply business logic, create surrogate keys, and
    normalize enums. They depend on staging models.
    """,
)

# Task 3: Run mart models
# This creates business-ready analytical models
run_marts = BashOperator(
    task_id='run_marts',
    bash_command='docker exec dbt_airflow_project-dbt-1 dbt run --select marts',
    dag=dag,
    doc_md="""
    **Run Mart Models**
    
    Runs all dbt models in the marts layer.
    This includes:
    - fct_sales_summary
    - dim_customer_metrics
    - fct_product_performance
    
    These models create business-ready analytical tables with
    aggregations and KPIs. They depend on intermediate models.
    """,
)

# Task 4: Run data quality tests
# This validates data quality and integrity
run_tests = BashOperator(
    task_id='run_tests',
    bash_command='docker exec dbt_airflow_project-dbt-1 dbt test',
    dag=dag,
    doc_md="""
    **Run Data Quality Tests**
    
    Executes all dbt tests including:
    - Schema tests (not_null, unique, relationships, accepted_values)
    - Custom generic tests (is_positive, valid_date_range, no_future_dates)
    - Source freshness checks
    
    Tests validate data quality across all layers of the pipeline.
    """,
)

# Success notification task
notify_success_task = PythonOperator(
    task_id='notify_success',
    python_callable=notify_success,
    dag=dag,
    doc_md="""
    **Success Notification**
    
    Sends success notification when the entire pipeline completes successfully.
    Logs success message and can be extended to send Slack/email notifications.
    """,
)

# Failure notification task
notify_failure_task = PythonOperator(
    task_id='notify_failure',
    python_callable=notify_failure,
    trigger_rule='one_failed',  # Trigger if any upstream task fails
    dag=dag,
    doc_md="""
    **Failure Notification**
    
    Sends failure notification when any task in the pipeline fails.
    Triggered by the global failure callback or task failures.
    Logs error details and can be extended to send Slack/email alerts.
    """,
)

# Success path: Sequential execution of pipeline stages
# log_pipeline_start -> load_staging -> run_intermediate -> run_marts -> run_tests -> notify_success
log_pipeline_start_task >> load_staging >> run_intermediate >> run_marts >> run_tests >> notify_success_task

# Failure path: Any task failure triggers failure notification
# All tasks can trigger notify_failure on failure
log_pipeline_start_task >> notify_failure_task
load_staging >> notify_failure_task
run_intermediate >> notify_failure_task
run_marts >> notify_failure_task
run_tests >> notify_failure_task

# Set global failure callback for the DAG
dag.on_failure_callback = notify_failure

