from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator

from load_data import load_data
from split_data import split_data
from put_to_minio import put_to_minio

dag = DAG(
    dag_id='movielens',
    start_date=datetime(2024, 12, 11),
    schedule_interval=None,
    catchup=False,
)

download_task = PythonOperator(
    task_id='download_data',
    python_callable=load_data,
    dag=dag,
)

split_task = PythonOperator(
    task_id='split',
    python_callable=split_data,
    dag=dag,
)

upload_task = PythonOperator(
    task_id='put_to_minio',
    python_callable=put_to_minio,
    dag=dag,
)

train_and_predict_task = BashOperator(task_id='train_and_predict',
                          bash_command="spark-submit --jars /opt/applications/aws-java-sdk-bundle-1.12.540.jar,/opt/applications/hadoop-aws-3.3.4.jar /opt/applications/app.py",
                          dag=dag,)


download_task >> split_task >> upload_task >> train_and_predict_task