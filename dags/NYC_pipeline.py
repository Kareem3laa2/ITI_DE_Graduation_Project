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

    clean_to_silver = BashOperator(
       task_id = "silver_layer_hdfs",
       bash_command = 'docker exec nyc-spark-master \
                       /opt/bitnami/spark/bin/spark-submit \
                       --master spark://spark-master:7077 /opt/spark-apps/scripts/clean_to_silver.py'
   )

    stage_to_snowflake =  BashOperator(
       task_id = "silver_layer_snowflake",
       bash_command = 'docker exec nyc-spark-master \
                       /opt/bitnami/spark/bin/spark-submit \
                       --master spark://spark-master:7077 /opt/spark-apps/scripts/stage_to_snowflake.py'
   )


    store_hdfs_bronze >> clean_to_silver >> stage_to_snowflake