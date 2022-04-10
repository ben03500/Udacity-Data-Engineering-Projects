from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

from udac_example_dag import default_args

dag = DAG('create_table_dag',
          default_args=default_args,
          schedule_interval=None
          )

create_table_task = PostgresOperator(
    task_id="create_table_task",
    dag=dag,
    postgres_conn_id="redshift",
    sql='create_tables.sql'
)
