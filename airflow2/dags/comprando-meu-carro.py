from tasks import extract, transform, load
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
    "email": ["maycon.mota@ufms.br"],
    'description': 'Dag to extract info from Shopcar and Webmotors',
    "email_on_failure": True,
    "email_on_retry": False,
    'retries': 4,
    'catchup': False
}


dag = DAG(
    start_date=datetime(2021, 12, 18),
    dag_id=dag_id,
    default_args=default_args,
    max_active_runs=1,
    concurrency=32,
    schedule_interval="0 0/6 * * *",
    catchup=False
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
begin_fipe = DummyOperator(
    task_id="begin_fipe",
    dag=dag
)
end_fipe = DummyOperator(
    task_id="end_fipe",
    dag=dag
)
begin_shopcar = DummyOperator(
    task_id="begin_shopcar",
    dag=dag
)
end_shopcar = DummyOperator(
    task_id="end_shopcar",
    trigger_rule="one_failed",
    dag=dag
)
begin_mastertable = DummyOperator(
    task_id="begin_mastertable",
    dag=dag
)
end_mastertable = DummyOperator(
    task_id="end_mastertable",
    dag=dag
)
begin_transform = DummyOperator(
    task_id="begin_transform",
    dag=dag
)
end_transform = DummyOperator(
    task_id="end_transform",
    dag=dag
)

end_extract = DummyOperator(
    task_id="end_extract",
    dag=dag
)

begin_load = DummyOperator(
    task_id="begin_load",
    dag=dag
)
end_load = DummyOperator(
    task_id="end_load",
    dag=dag
)

extract_fipe_marca_modelos = PythonOperator(
    task_id="EXTRACT_FIPE_DATASET",
    python_callable=extract.extract_marcas_modelo_fipe,
    dag=dag
)

begin_extract >> begin_fipe >> extract_fipe_marca_modelos >> end_fipe >> end_extract

for i in range(1, 101): # 1, 101
    extract_olx_veiculos = PythonOperator(
        task_id="EXTRACT_DATA_OLX_PAGE_{}".format(i),
        python_callable=extract.extract_olx_veiculos,
        op_kwargs={
            "pagina": i,
        },
        dag=dag)
    begin_extract >> begin_olx >> [
        extract_olx_veiculos] >> end_olx >> end_extract


for i in range(1, 592): # 1, 592
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
    
    begin_extract >> begin_shopcar >> [extract_shopcar_page] >> end_shopcar >> end_extract 

transform_shopcar_dataset = PythonOperator(
    task_id="TRANSFORM_SHOPCAR_DATASET_{}".format(i),
    python_callable=transform.transform_shopcar_dataset,
    dag=dag)

begin_transform >> [transform_shopcar_dataset] >> end_transform

transform_olx_dataset = PythonOperator(
    task_id="TRANSFORM_OLX_DATASET",
    python_callable=transform.transform_olx_dataset,
    dag=dag)

mastertable_olx_shopcar = PythonOperator(
    task_id="MASTERTABLE_OLX_SHOPCAR",
    python_callable=transform.transform_mastertable,
    dag=dag)

load_to_sql = PythonOperator(
task_id="LOAD_TO_SQL",
python_callable=load.load_to_sql,
dag=dag)

end_extract >> begin_transform
begin_transform >> transform_olx_dataset >> end_transform >> begin_mastertable >> mastertable_olx_shopcar >> end_mastertable >> begin_load >> load_to_sql >> end_load