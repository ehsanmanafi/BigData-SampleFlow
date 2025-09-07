from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

# --- Paths to Spark job scripts ---
JOB1_SCRIPT = "/mnt/data/spark_jobs/job1_kafka_to_delta.py"
JOB2_SCRIPT = "/mnt/data/spark_jobs/job2_realtime_scoring.py"
JOB3_SCRIPT = "/mnt/data/spark_jobs/job3_batch_training.py"

# --- Default arguments for DAG ---
default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# --- DAG definition ---
dag = DAG(
    "spark_data_pipeline",
    default_args=default_args,
    description="Manage Spark jobs for Kafka → Delta → ClickHouse",
    # Run every 5 minutes to monitor streaming jobs
    schedule_interval="*/5 * * * *",
    start_date=datetime(2025, 9, 1),
    catchup=False,
)

# --- Helper function to check and restart streaming jobs ---
def check_and_restart(job_name, script_path):
    """
    Check if a Spark job (Job1 or Job2) is running.
    If not running, restart it using spark-submit.

    Parameters:
        job_name (str): Human-readable name of the job for logging.
        script_path (str): Full path to the Spark job Python script.
    """
    try:
        # Check if job process exists
        result = subprocess.run(
            ["pgrep", "-f", script_path],
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            print(f"{job_name} is already running.")
        else:
            print(f"{job_name} not running. Restarting...")
            subprocess.Popen(["spark-submit", script_path])
    except Exception as e:
        print(f"Error checking {job_name}: {e}")
        # Attempt restart even if check fails
        subprocess.Popen(["spark-submit", script_path])

# --- Tasks ---
# Health check for Job1 (Streaming ingestion: Kafka → Delta)
job1_health = PythonOperator(
    task_id="check_job1_kafka_to_delta",
    python_callable=check_and_restart,
    op_args=["Job1-KafkaToDelta", JOB1_SCRIPT],
    dag=dag,
)

# Health check for Job2 (Streaming realtime scoring)
job2_health = PythonOperator(
    task_id="check_job2_realtime_scoring",
    python_callable=check_and_restart,
    op_args=["Job2-RealtimeScoring", JOB2_SCRIPT],
    dag=dag,
)

# Batch training task (Job3) - runs after Job1 health check
job3_batch = BashOperator(
    task_id="run_job3_batch_training",
    bash_command=f"spark-submit {JOB3_SCRIPT}",
    dag=dag,
)

# --- Dependencies ---
# Job3 depends only on Job1 (needs cleaned data in Delta)
job1_health >> job3_batch
# Job2 health check runs independently; it is not a dependency for Job3
