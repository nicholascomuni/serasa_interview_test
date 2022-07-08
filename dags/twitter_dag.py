import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from transformations import *

from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.microsoft.azure.sensors.wasb import WasbPrefixSensor

default_args = {
'owner':'Nicholas',
'start_date':datetime(2022,7,1),
'email':['nicholas.comuni@outlook.com'],
'email_on_failure' : True,
'email_on_retry': False,
'retries':3,
'depends_on_past' : False,
'retry_delay':timedelta(minutes=30)
}

with DAG('twitter_dag',
default_args=default_args,
schedule_interval= "0 0 * * *") as dag:

    get_data_task = PythonOperator(
    task_id='request_twitter_data',python_callable = request_twitter_data,depends_on_past=False
    )

    wait_data_task = WasbPrefixSensor(task_id="wait_twitter_data",
    wasb_conn_id="wasbconn",
    container_name="landingzone",
    prefix="/hashtags",
    timeout=18*60*60,
    poke_interval=1800)

    json_to_parquet_task = DatabricksRunNowOperator(task_id="json_to_parquet_task",
                                                databricks_conn_id="databricks_conn",
                                                json = {"job_id":711735064242409})

    process_data_task = DatabricksRunNowOperator(task_id="process_twitter_data",
                                                databricks_conn_id="databricks_conn",
                                                json = {"job_id":531841237317382})

    load_cosmos = DatabricksRunNowOperator(task_id="load_data_to_cosmosDB",
                                                databricks_conn_id="databricks_conn",
                                                json = {"job_id":854986352167304})

    request_OurWorld_Covid_Data = PythonOperator(task_id='request_OurWorld_Covid_data',python_callable = transform_function)

    wait_OurWorld_Covid_Data = PythonOperator(task_id='wait_OurWorld_Covid_data',python_callable = transform_function)

    #request_OurWorld_Covid_Data = PythonOperator(task_id='request_OurWorld_Covid_data',python_callable = transform_function)

    request_OurWorld_Geo_Data = PythonOperator(task_id='request_OurWorld_Geo_data',python_callable = transform_function)

    wait_OurWorld_Geo_Data = PythonOperator(task_id='wait_OurWorld_Geo_data',python_callable = transform_function)


[get_data_task >> wait_data_task, request_OurWorld_Covid_Data >> wait_OurWorld_Covid_Data, request_OurWorld_Geo_Data >> wait_OurWorld_Geo_Data] >> json_to_parquet_task >> process_data_task >> load_cosmos



