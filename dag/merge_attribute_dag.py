from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.variable import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook

from datetime import datetime
import logging
import pendulum
import pandas as pd
from io import StringIO
import boto3

kst = pendulum.timezone("Asia/Seoul")

dag = DAG(
    dag_id='merge_attribute_dag',
    start_date=datetime(2024, 7, 16, tzinfo=kst),
    schedule=None,
    catchup=False
)

CREATE_QUERY = '''
CREATE TABLE IF NOT EXISTS public.food_attribute (
    name VARCHAR(255) NOT NULL, 
    status VARCHAR(50),
    address_zip_code VARCHAR(50),
    road_zip_code VARCHAR(25),
    road_full_address VARCHAR(500),
    road_address VARCHAR(500),
    x DOUBLE PRECISION,
    y DOUBLE PRECISION,
    phone_number VARCHAR(50),
    category_1 VARCHAR(50),
    category_2 VARCHAR(50),
    district_name VARCHAR(50),
    attribute_1 VARCHAR(5),
    attribute_2 VARCHAR(5),
    attribute_3 VARCHAR(5),
    attribute_4 VARCHAR(5)
);
'''

create_food_attribute_table_task = PostgresOperator(
    task_id = "create_food_attribute_table",
    postgres_conn_id ='wouldUlike-redshift',
    sql = CREATE_QUERY,
    dag = dag
)