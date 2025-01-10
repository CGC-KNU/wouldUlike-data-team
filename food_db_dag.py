from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    dag_id='food_db_copy',
    default_args=default_args,
    description='daegu food db copy dag',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['copy'],
) as dag:
    
    create_table = PostgresOperator(
        task_id='create_redshift_table',
        postgres_conn_id='redshift_default', 
        sql="""
        CREATE TABLE IF NOT EXISTS raw.daegu_food(
            name VARCHAR(255),
            status VARCHAR(25),
            address_zip_code VARCHAR(10),
            road_zip_code VARCHAR(10),
            road_full_address VARCHAR(255),
            road_address VARCHAR(255),
            x POINT,
            y POINT,
            phone_number VARCHAR(25),
            category_1 VARCHAR(50) NOT NULL,
            category_2 VARCHAR(50) NOT NULL,
            district_name VARCHAR(10)
        );
        """,
    )

    copy_to_redshift = S3ToRedshiftOperator(
        task_id='copy_s3_to_redshift',
        redshift_conn_id='wouldUlike-redshift', 
        s3_bucket='wouldulike-data',
        s3_key='wouldulike-data/data/category/202411.csv', 
        schema='raw',  
        table='daegu_food',  
        copy_options=["CSV", "IGNOREHEADER 1"], 
        aws_conn_id='aws_default', 
    )

    create_table >> copy_to_redshift
