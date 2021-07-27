
from datetime import timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from random import randint


S3_BUCKET = getenv("S3_BUCKET", "test-bucket")
S3_KEY = getenv("S3_KEY", "key")
REDSHIFT_TABLE = getenv("REDSHIFT_TABLE", "test_table")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'tutorial',
    default_args=default_args,
    schedule_interval=timedelta(days=1)
) as dag:

    t1 = BashOperator(
        task_id='print_date',
        bash_command='date'
    )

    t2 = BashOperator(
        task_id='sleep',
        bash_command='sleep 5',
        retries=3
    )
   
    t3 = BashOperator(
        task_id='print_hello',
        bash_command='echo hello'
    )

    t1 >> [t2, t3]


    task_transfer_s3_to_redshift = S3ToRedshiftOperator(
        s3_bucket=S3_BUCKET,
        s3_key=S3_KEY,
        schema="PUBLIC",
        table=REDSHIFT_TABLE,
        copy_options=['csv'],
        task_id='transfer_s3_to_redshift'
    )


    def is_greater_than_five():
        var = randint(0, 10) 
        if var > 5:
            return 'greater_than_five'
        else:
            return 'not_greater_than_five'


    branching = BranchPythonOperator(
        task_id = 'branching',
        python_callable = is_greater_than_five
    )


    create_pet_table = PostgresOperator(
        task_id="create_pet_table",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS pet (
            pet_id SERIAL PRIMARY KEY,
            name VARCHAR NOT NULL,
            pet_type VARCHAR NOT NULL,
            birth_date DATE NOT NULL,
            OWNER VARCHAR NOT NULL);
            """
    )

    def print_context(ds, **kwargs):        
        print("This is printed in the logs")

    run_this = PythonOperator(
        task_id='print_task',
        python_callable=print_context
    )