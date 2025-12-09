"""
Test Failure Notification DAG

This DAG is designed specifically to test the failure notification system.
It intentionally fails tasks to verify that:
1. Failure callbacks are triggered correctly
2. AlertManager sends failure notifications
3. Logging captures failure details properly
4. Error messages are properly formatted and sent

Pipeline Flow:
1. Log test start - Records the start of the test
2. Simulate failure - Intentionally fails to test notification system
3. Failure notification - Should be triggered automatically via callback

This DAG should be run manually for testing purposes only.
It is not scheduled and should not run in production.

Dependencies:
- Uses the same AlertManager and DataOpsLogger utilities as the main pipeline
- Tests the global failure callback mechanism
- Validates Slack notification integration (if configured)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
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
    'retries': 0,  # No retries for test DAG - we want immediate failure
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=10),
}

# DAG definition
dag = DAG(
    'test_failure_notification',
    default_args=default_args,
    description='Test DAG for validating failure notification system',
    schedule_interval=None,  # Manual trigger only - no scheduling
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['test', 'notification', 'failure-testing'],
    doc_md=__doc__,
)

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def log_test_start(**context):
    """
    Log the start of the failure notification test.
    
    Args:
        **context: Airflow context dictionary containing task instance info
    """
    dag_run = context.get('dag_run')
    execution_date = context.get('execution_date')
    
    # Initialize logger
    test_logger = DataOpsLogger("test_failure_notification", "test_start")
    
    # Log test start event
    start_time = datetime.now()
    test_logger.log_event(
        "test_start",
        f"Starting failure notification test for DAG: {dag_run.dag_id}",
        level="info"
    )
    
    # Store start time in XCom
    context['ti'].xcom_push(key='start_time', value=start_time.isoformat())
    
    logger.info(f"Failure notification test started at: {start_time.isoformat()}")
    print("=" * 60)
    print("FAILURE NOTIFICATION TEST - STARTED")
    print("=" * 60)
    print(f"DAG: {dag_run.dag_id}")
    print(f"Run ID: {dag_run.run_id}")
    print(f"Start Time: {start_time.isoformat()}")
    print("=" * 60)
    
    return {"start_time": start_time.isoformat()}

def simulate_python_failure(**context):
    """
    Intentionally raise an exception to test failure notifications.
    
    This function simulates a Python task failure to verify that:
    - The failure callback is triggered
    - Error messages are captured correctly
    - Notifications are sent via AlertManager
    
    Args:
        **context: Airflow context dictionary containing task instance info
    """
    dag_run = context.get('dag_run')
    task_instance = context.get('ti')
    
    # Initialize logger
    test_logger = DataOpsLogger("test_failure_notification", "simulate_failure")
    
    # Log that we're about to fail
    test_logger.log_event(
        "simulating_failure",
        "Intentionally raising exception to test failure notification system",
        level="warning"
    )
    
    logger.warning("About to intentionally fail this task to test notifications...")
    print("=" * 60)
    print("SIMULATING TASK FAILURE")
    print("=" * 60)
    print("This is an intentional failure to test the notification system.")
    print("The failure callback should be triggered after this exception.")
    print("=" * 60)
    
    # Intentionally raise an exception
    raise Exception(
        "INTENTIONAL TEST FAILURE: This exception was raised to test the "
        "failure notification system. The AlertManager should send a "
        "notification about this failure."
    )

def simulate_bash_failure(**context):
    """
    Intentionally fail a bash command to test failure notifications.
    
    This simulates a bash command failure (e.g., dbt command failure).
    
    Args:
        **context: Airflow context dictionary containing task instance info
    """
    # This will be handled by BashOperator with exit code 1
    pass

def notify_failure(context):
    """
    Send failure notification when any task fails.
    
    This function is called as a failure callback and logs detailed
    error information. Sends Slack alerts using AlertManager.
    
    This is the same function as in the main pipeline to ensure
    consistent failure handling.
    
    Args:
        context: Airflow context dictionary containing failure details
    """
    try:
        dag_run = context.get('dag_run')
        task_instance = context.get('task_instance')
        execution_date = context.get('execution_date')
        exception = context.get('exception')
        ti = context.get('ti')
        
        # Initialize logger and alert manager
        test_logger = DataOpsLogger("test_failure_notification", "failure_notification")
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
        except Exception as e:
            logger.warning(f"Could not retrieve task logs: {e}")
        
        # Build failure message
        failure_message = f"""TEST: Failure Notification Test - FAILED

DAG: {dag_run.dag_id if dag_run else 'Unknown'}
Execution Date: {execution_date}
Run ID: {dag_run.run_id if dag_run else 'Unknown'}
Failed Task: {task_instance.task_id if task_instance else 'Unknown'}
Exception: {error_message}

This is a TEST failure to validate the notification system.
Failure occurred at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}

If you see this notification, the failure notification system is working correctly!"""
        
        # Log failure event
        test_logger.log_event(
            "test_failure",
            f"Task {task_instance.task_id if task_instance else 'Unknown'} failed (intentional test): {error_message}",
            level="error"
        )
        
        # Log task execution failure
        log_task_execution(
            task_id=task_instance.task_id if task_instance else 'Unknown',
            status="failed",
            error=error_message
        )
        
        logger.error(failure_message)
        print("=" * 60)
        print("FAILURE NOTIFICATION TRIGGERED")
        print("=" * 60)
        print(failure_message)
        print("=" * 60)
        
        # Send Slack alert using AlertManager
        alert_manager.alert_pipeline_failure(
            dag_id=dag_run.dag_id if dag_run else 'test_failure_notification',
            task_id=task_instance.task_id if task_instance else 'Unknown',
            error_message=error_message
        )
        
        print("\n✓ Failure notification sent successfully!")
        print("Check your Slack channel (if configured) or logs above for the alert details.")
        
    except Exception as callback_error:
        # If the callback itself fails, log it but don't raise
        logger.error(f"Error in failure callback: {callback_error}", exc_info=True)
        print(f"ERROR: Failure callback encountered an error: {callback_error}")

# ============================================================================
# TASKS
# ============================================================================

# Task 1: Log test start
log_test_start_task = PythonOperator(
    task_id='log_test_start',
    python_callable=log_test_start,
    dag=dag,
    doc_md="""
    **Log Test Start**
    
    Logs the start of the failure notification test and stores
    the start time in XCom.
    """,
)

# Task 2: Simulate Python failure
simulate_python_failure_task = PythonOperator(
    task_id='simulate_python_failure',
    python_callable=simulate_python_failure,
    on_failure_callback=notify_failure,  # Task-level failure callback
    dag=dag,
    doc_md="""
    **Simulate Python Failure**
    
    Intentionally raises an exception to test the failure notification system.
    This task will fail and trigger the failure callback.
    """,
)

# Task 3: Simulate Bash failure (alternative failure scenario)
simulate_bash_failure_task = BashOperator(
    task_id='simulate_bash_failure',
    bash_command='echo "About to fail..." && exit 1',  # Intentionally fails
    on_failure_callback=notify_failure,  # Task-level failure callback
    dag=dag,
    doc_md="""
    **Simulate Bash Failure**
    
    Intentionally fails a bash command to test failure notifications
    for bash-based tasks (like dbt commands).
    This task is commented out by default - uncomment to test bash failures.
    """,
)

# ============================================================================
# TASK DEPENDENCIES
# ============================================================================

# Main test flow: Start -> Python Failure
# The failure will trigger the global failure callback
log_test_start_task >> simulate_python_failure_task

# Uncomment the line below to test bash failure instead:
# log_test_start_task >> simulate_bash_failure_task

# Set global failure callback for the DAG
dag.on_failure_callback = notify_failure

