from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import logging
import os 
import sys 

## Import tasks folder
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
sys.path.append('/opt/airflow/dags')

from tasks import extract

dag_id = 'CMC-extract-data-from-portals'

default_args = {
    'owner': 'CMC @felipegaleao',
    'start_date': datetime(2021, 10, 24),
    "email": ["maycon.mota@ufms.br"],
    'description': 'Dag to extract info from Shopcar and Webmotors',
    "email_on_failure": True,
    "email_on_retry": False,
    'retries': 3,
    'catchup': False
}


dag = DAG(
    dag_id = dag_id,
    default_args = default_args,
    max_active_runs=16,
    concurrency = 64,
    schedule_interval="0 2 * * *"
)

for i in range(1, 593):
    # scrapping_webmotors = PythonOperator(
    #             task_id="EXTRACT_DATA_WEBMOTORS_PAGE_{}".format(i),
    #             python_callable = extract_webmotors_page,
    #             op_kwargs = {
    #                 "index" : i,
    #             },
    #             dag=dag)
    
    scrapping_shopcar_page = PythonOperator(
                task_id="EXTRACT_DATA_SHOPCAR_PAGE_{}".format(i),
                python_callable = extract.extract_shopcar_page,
                op_kwargs = {
                    "pagina" : i,
                },
                dag=dag)