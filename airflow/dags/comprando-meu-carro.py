from tasks import extract, transform
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import logging
import os
import sys

# Import tasks folder
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
sys.path.append('/opt/airflow/dags')


dag_id = 'CMC-extract-data-from-portals'

default_args = {
    'owner': 'CMC @felipegaleao',
    'start_date': datetime(2021, 12, 2),
    "email": ["maycon.mota@ufms.br"],
    'description': 'Dag to extract info from Shopcar and Webmotors',
    "email_on_failure": True,
    "email_on_retry": False,
    'retries': 4,
    'catchup': False
}


dag = DAG(
    start_date=None,
    dag_id=dag_id,
    default_args=default_args,
    max_active_runs=128,
    concurrency=128,
    schedule_interval="0 2 * * *"
)

begin_extract = DummyOperator(
    task_id="begin_extract",
    dag=dag
)
begin_olx = DummyOperator(
    task_id="begin_olx",
    dag=dag
)
end_olx = DummyOperator(
    task_id="end_olx",
    dag=dag
)
begin_shopcar = DummyOperator(
    task_id="begin_shopcar",
    dag=dag
)
end_shopcar = DummyOperator(
    task_id="end_shopcar",
    dag=dag
)

end_extract = DummyOperator(
    task_id="end_extract",
    dag=dag
)
for i in range(1, 101):
    extract_olx_veiculos = PythonOperator(
        task_id="EXTRACT_DATA_OLX_PAGE_{}".format(i),
        python_callable=extract.extract_olx_veiculos,
        op_kwargs={
            "pagina": i,
        },
        dag=dag)
    begin_extract >> begin_olx >> [extract_olx_veiculos] >> end_olx >> end_extract

for i in range(1, 592):
    # scrapping_webmotors = PythonOperator(
    #             task_id="EXTRACT_DATA_WEBMOTORS_PAGE_{}".format(i),
    #             python_callable = extract_webmotors_page,
    #             op_kwargs = {
    #                 "index" : i,
    #             },
    #             dag=dag)

    extract_shopcar_page = PythonOperator(
        task_id="EXTRACT_DATA_SHOPCAR_PAGE_{}".format(i),
        python_callable=extract.extract_shopcar_page,
        op_kwargs={
            "pagina": i,
        },
        dag=dag)

    transform_shopcar_dataset = PythonOperator(
        task_id="TRANSFORM_SHOPCAR_DATASET",
        python_callable=transform.transform_shopcar_dataset,
        dag=dag)

    begin_extract >> begin_shopcar >> [extract_shopcar_page] >> end_shopcar >> end_extract >> [transform_shopcar_dataset]
