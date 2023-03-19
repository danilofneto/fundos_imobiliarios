from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator
# from airflow.operators.bash_operator import BashOperator
from notebooks.get_fiis import get_fiis, oportunidades, write_csv

# Definindo alguns argumentos básicos
default_args = {
   'owner': 'danilo_neto',
   'depends_on_past': False,
   'start_date': datetime(2022, 1, 1),
   'retries': 0,
   }

with DAG(
    'my_first_dag',
    schedule_interval=timedelta(minutes=1),
    catchup=False,
    default_args=default_args
) as dag:

    get_fiis = PythonOperator(
        task_id = 'get_data',
        python_callable = get_fiis
    )

    oportunidades = PythonOperator(
        task_id = 'oportunidades',
        python_callable = oportunidades
    )

    write_csv = PythonOperator(
        task_id = 'to_csv',
        python_callable = write_csv
    )

get_fiis >> oportunidades >> write_csv