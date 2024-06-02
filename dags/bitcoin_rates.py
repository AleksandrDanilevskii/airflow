from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from requests import get
from datetime import datetime

URL_API = "https://api.coincap.io/v2/rates/bitcoin"
TABLE_NAME = "bitcoin_rates"
PSQL_CONN = "psql_otus"
CRON = "*/5 * * * *"


def get_create_table(table: str) -> str:
    return f'''
    create table if not exists "{table}"(
        id serial primary key,
        rate float4 not null,
        tm timestamp not null,
        updated_at timestamp default now()
    )
    '''


def get_data() -> dict:
    return get(URL_API).json()


def extract_data(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='get_data')
    tm = datetime.fromtimestamp(data.get('timestamp') / 1000)
    rate = float(data.get('data').get('rateUsd'))
    return {'tm': tm, 'rate': rate}


default_args = {
    'depends_on_past': False,
    'owner': 'adanilevsky'
}

dag = DAG(
    dag_id="bitcoin_rates",
    description='Сбор курса биткоина',
    default_args=default_args,
    start_date=datetime(2024, 6, 1),
    schedule_interval=CRON
)

create_table = PostgresOperator(
    dag=dag,
    task_id="create_table",
    postgres_conn_id=PSQL_CONN,
    sql=get_create_table(TABLE_NAME)
)

get_data = PythonOperator(
    dag=dag,
    task_id="get_data",
    python_callable=get_data,
    provide_context=True
)

extract_data = PythonOperator(
    dag=dag,
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True
)

insert_data = PostgresOperator(
    dag=dag,
    task_id="insert_data",
    postgres_conn_id=PSQL_CONN,
    sql=f'''insert into "{TABLE_NAME}"(tm, rate) values (%(tm)s, %(rate)s);''',
    parameters={
        'tm': "{{ ti.xcom_pull(task_ids='extract_data')['tm'] }}",
        'rate': "{{ ti.xcom_pull(task_ids='extract_data')['rate'] }}"
    }
)

create_table >> insert_data
get_data >> extract_data >> insert_data

