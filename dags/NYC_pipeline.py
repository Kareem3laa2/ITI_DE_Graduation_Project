from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime


with DAG(
    dag_id= "NYC_pipeline",
    start_date = datetime(2025, 1, 1),
    catchup= False,
) as dag:
    store_hdfs_bronze = BashOperator(
        task_id= "store_hdfs_bronze",
        bash_command= 'docker exec nyc-namenode \
                  hdfs dfs -mkdir -p /data/bronze \
                  -put -f /data/bronze /data/'
    )