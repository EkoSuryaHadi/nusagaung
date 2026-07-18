from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import os
import subprocess
import requests

def run_gaung_pipeline(**context):
    dag_run = context['dag_run']
    conf = dag_run.conf or {}
    
    run_id = conf.get("runId")
    pipeline_id = conf.get("pipelineId")
    
    if not run_id:
        raise ValueError("No runId provided in DAG run configuration")
        
    print(f"Starting Gaung pipeline run: {run_id} for pipeline: {pipeline_id}")
    
    # 1. Write the pipeline config JSON to a temp file
    config_path = f"/tmp/gaung_pipeline_{run_id}.json"
    with open(config_path, "w") as f:
        json.dump(conf, f)
        
    # 2. Execute etl_runner.py as a subprocess
    runner_path = "/opt/airflow/worker/etl_runner.py"
    
    # Pass current environment variables so etl_runner can access DB & WS Bridge
    env = os.environ.copy()
    
    # Run the ETL runner
    process = subprocess.Popen(
        ["python3", runner_path, config_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env
    )
    
    stdout, stderr = process.communicate()
    exit_code = process.returncode
    
    print(f"ETL Runner stdout:\n{stdout}")
    if stderr:
        print(f"ETL Runner stderr:\n{stderr}")
        
    print(f"ETL Runner exited with code {exit_code}")
    
    # Clean up temp config file
    if os.path.exists(config_path):
        try:
            os.remove(config_path)
        except Exception as e:
            print(f"Warning: Failed to clean up temp file {config_path}: {e}")
        
    # 3. Call Gaung's callback API to report status
    callback_url = os.environ.get("GAUNG_CALLBACK_URL", "http://host.docker.internal:3000/api/webhook/airflow")
    
    payload = {
        "runId": run_id,
        "pipelineId": pipeline_id,
        "exitCode": exit_code,
        "stdout": stdout,
        "stderr": stderr
    }
    
    try:
        response = requests.post(callback_url, json=payload, timeout=10)
        print(f"Callback response: {response.status_code} - {response.text}")
        response.raise_for_status()
    except Exception as e:
        print(f"Failed to send callback to Gaung: {e}")
        raise e
        
    if exit_code != 0:
        raise RuntimeError(f"ETL runner failed with exit code {exit_code}")

with DAG(
    dag_id="gaung_etl_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,  # Only triggered via API
    catchup=False,
    max_active_runs=4
) as dag:

    run_pipeline_task = PythonOperator(
        task_id="run_pipeline_task",
        python_callable=run_gaung_pipeline,
    )
