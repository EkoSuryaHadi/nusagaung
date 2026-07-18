from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timezone
import json
import os
import psycopg2
from croniter import croniter
from airflow.models import DagBag

def check_and_trigger_scheduled_pipelines(**context):
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("[Gaung Scheduler] DATABASE_URL not set in environment variables.")
        return
        
    # We use UTC time to match standard cron schedule checks
    now = datetime.now(timezone.utc)
    # Strip seconds and microseconds to match precise minutes
    now_minute = now.replace(second=0, microsecond=0)
    print(f"[Gaung Scheduler] Running schedule check at: {now_minute} UTC")
    
    dagbag = DagBag()
    target_dag = dagbag.get_dag("gaung_etl_pipeline")
    if not target_dag:
        print("[Gaung Scheduler] Target DAG 'gaung_etl_pipeline' not found in DagBag.")
        return

    conn = None
    try:
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        
        # 1. Fetch active pipelines with non-empty schedules
        cur.execute("""
            SELECT id, name, schedule, "userId", "tenantId" 
            FROM "Pipeline" 
            WHERE status = 'ACTIVE' AND schedule IS NOT NULL AND schedule != ''
        """)
        pipelines = cur.fetchall()
        
        for p_id, p_name, cron_expr, user_id, tenant_id in pipelines:
            cron_expr = cron_expr.strip()
            try:
                # Validate and check if cron matches the current minute
                # croniter expects a timezone-naive datetime if checking matches, 
                # so we convert now_minute to naive UTC
                now_naive = now_minute.replace(tzinfo=None)
                if not croniter.is_valid(cron_expr):
                    print(f"[Gaung Scheduler] Invalid cron expression for pipeline '{p_name}' (ID: {p_id}): '{cron_expr}'")
                    continue
                    
                if croniter.match(cron_expr, now_naive):
                    print(f"[Gaung Scheduler] Pipeline '{p_name}' (ID: {p_id}) matches cron '{cron_expr}'. Triggering run...")
                    
                    # 2. Create a PENDING PipelineRun record in the database
                    cur.execute("""
                        INSERT INTO "PipelineRun" ("pipelineId", status, "createdAt")
                        VALUES (%s, 'PENDING', %s)
                        RETURNING id
                    """, (p_id, now))
                    run_id = cur.fetchone()[0]
                    conn.commit()
                    
                    # 3. Retrieve all steps for this pipeline
                    cur.execute("""
                        SELECT id, "order", type, config, "inputLayer", "outputLayer", "outputTable"
                        FROM "PipelineStep"
                        WHERE "pipelineId" = %s
                        ORDER BY "order" ASC
                    """, (p_id,))
                    steps = []
                    for s_id, s_order, s_type, s_config, s_input, s_output, s_out_table in cur.fetchall():
                        steps.append({
                            "id": s_id,
                            "pipelineId": p_id,
                            "order": s_order,
                            "type": s_type,
                            "config": json.loads(s_config) if s_config else {},
                            "inputLayer": s_input,
                            "outputLayer": s_output,
                            "outputTable": s_out_table
                        })
                        
                    # 4. Retrieve pipeline source and check for Bronze table metadata
                    cur.execute("""
                        SELECT s.id, s.name, s.type, s.config, s."fileName", s."fileSize", s."filePath"
                        FROM "Pipeline" p
                        LEFT JOIN "DataSource" s ON p."sourceId" = s.id
                        WHERE p.id = %s
                    """, (p_id,))
                    source_row = cur.fetchone()
                    source_info = {}
                    
                    if source_row and source_row[0]:
                        s_id, s_name, s_type, s_config, s_file_name, s_file_size, s_file_path = source_row
                        
                        # Check if a BRONZE lakehouse table exists for this source
                        cur.execute("""
                            SELECT "tableName" FROM "LakehouseTable"
                            WHERE "sourceId" = %s AND layer = 'BRONZE'
                            ORDER BY "updatedAt" DESC LIMIT 1
                        """, (s_id,))
                        bronze_row = cur.fetchone()
                        
                        if bronze_row:
                            source_info = {
                                "sourceTable": bronze_row[0],
                                "sourceLayer": "BRONZE",
                                "fromLakehouse": True
                            }
                        else:
                            source_info = {
                                "filePath": s_file_path,
                                "fileSize": s_file_size,
                                "fileName": s_file_name
                            }
                            
                            try:
                                source_config_parsed = json.loads(s_config or "{}")
                                source_info["category"] = source_config_parsed.get("category")
                            except:
                                source_info["category"] = None
                                
                    # 5. Build conf payload and trigger the main gaung_etl_pipeline DAG
                    conf_payload = {
                        "pipelineId": p_id,
                        "runId": run_id,
                        "source": source_info,
                        "steps": steps
                    }
                    
                    # Generate a unique run ID for this DAG run
                    dag_run_id = f"scheduled__{p_id}_{run_id}_{int(now.timestamp())}"
                    
                    target_dag.create_dagrun(
                        run_id=dag_run_id,
                        conf=conf_payload,
                        logical_date=now
                    )
                    print(f"[Gaung Scheduler] Successfully triggered DAG run '{dag_run_id}' for pipeline ID: {p_id}")
            except Exception as e:
                print(f"[Gaung Scheduler] Error triggering pipeline ID: {p_id}: {e}")
                
        cur.close()
    except Exception as e:
        print(f"[Gaung Scheduler] Database error: {e}")
    finally:
        if conn:
            conn.close()

with DAG(
    dag_id="gaung_scheduler_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule_interval="* * * * *",  # Runs every minute
    catchup=False,
    max_active_runs=1
) as dag:

    scheduler_task = PythonOperator(
        task_id="scheduler_task",
        python_callable=check_and_trigger_scheduled_pipelines,
    )
