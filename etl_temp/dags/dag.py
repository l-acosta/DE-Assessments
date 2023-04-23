import os
import sys

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

sys.path.append(os.environ.get("PROJECT_ABS_PATH"))
from etl.etl_temperature import main

args = {"owner": "airflow", "start_date": datetime(2023, 3, 20)}

# parameters
ETL_SCRIPT = "etl_temperature.py"
INPUT_PATH = "data/input/sample_city_temperature.csv"
OUTPUT_FOLDER_PATH = "data/output/output_etl.csv"

# set up DAG
dag = DAG(dag_id="ETL_temperature_dag", default_args=args, schedule_interval=None)

# tasks
start_task = EmptyOperator(task_id="start_task", dag=dag)

run_etl = PythonOperator(
    task_id="run_etl",
    python_callable=main,
    op_kwargs={"input_data_path": INPUT_PATH, "output_data_path": OUTPUT_FOLDER_PATH},
    dag=dag,
)

end_task = EmptyOperator(task_id="end_task", dag=dag)

start_task >> run_etl >> end_task
