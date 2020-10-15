import CONFIG
import DAG
import BashOperator
from datetime import datetime

with DAG(dag_id='bash_dag', schedule_interval="@once", start_date=datetime(2020, 1, 1), catchup=False) as dag:
    # Command template
    tasks = []
    for i in range(0, 640000, 10000):
        spark_command = f"spark-submit --packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.7 --master spark://{CONFIG.master} --driver-memory 6G --executor-memory 34G --num-executors 8 --executor-cores 5 ~/bitinsight/spark_processing.py s3a://{CONFIG.S3blocks} s3a://{CONFIG.S3csv} s3://{CONFIG.S3flagged} s3a://{CONFIG.S3vout} {i}"
        task_name = 'bash_task' + str(i)
        t = BashOperator(task_id=task_name, bash_command=spark_command)
        tasks.append(t)
        if i != 0:
            tasks[i] << tasks[i-1]