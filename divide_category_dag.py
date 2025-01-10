import pendulum
import pandas as pd
import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def fetch_csv_from_s3():
    date_str = datetime.today().strftime("%Y%m")
    bucket_name = "wouldulike-data"
    key = f"data/category/{date_str}.csv"
    download_path = f"./s3_download/{date_str}_category.csv"

    s3 = S3Hook(aws_conn_id="wouldUlike-S3")
    s3.get_key(key, bucket_name).download_file(download_path)
    logging.info(f"S3에서 다운로드 : {download_path}")

    return download_path

def split_category(df, type):
    category_df= df[df['카테고리'].str.contains(f'음식점 > {type}', na=False)][['사업장명', '카테고리']]
    category_list = category_df.values.tolist()

    category_2 = []
    for c in category_list:
        arr = c[1].split(' > ')
        if len(arr) >= 3:
            category_2.append(arr[2])
        else:
            category_2.append(type)

    category_df['카테고리_1'] = type
    category_df['카테고리_2'] = category_2

    return category_df

def divide_category(download_path, **kwargs):
    df = pd.read_csv(download_path, index_col=False)

    food_type = ['한식', '일식', '양식', '중식', '분식', '아시아음식', '퓨전요리', '간식', '술집', '패스트푸드']

    for type in food_type:
        category_df = split_category(df, type)
        category_json = category_df.to_json(orient='records')
        kwargs['ti'].xcom_push(key=type, value=category_json)

def merge_data(path, **kwargs):
    df = pd.read_csv(path, index_col=False)

    dfs = []
    food_type = ['한식', '일식', '양식', '중식', '분식', '아시아음식', '퓨전요리', '간식', '술집', '패스트푸드']
    for type in food_type:
        dfs.append(pd.read_json(kwargs['ti'].xcom_pull(task_ids='divide_category', key=type)))

    merged_df = pd.concat(dfs, ignore_index=True)
    joined_df = pd.merge(df, merged_df, on='사업장명', how='inner')
    joined_df = joined_df.drop(['카테고리_x', '카테고리_y'], axis=1)
    unique_df = joined_df.drop_duplicates()    
    unique_df.to_csv(path, index=False)

    return path

def upload_to_s3(file_path):
    date_str = datetime.today().strftime("%Y%m")
    file_name = f"{date_str}.csv"

    bucket_name = 'wouldulike-data'
    hook = S3Hook(aws_conn_id='wouldUlike-S3')
    hook.load_file(
        filename=file_path,
        key=f'data/category/{file_name}',
        bucket_name=bucket_name,
        replace=True
    )
    logging.info(f"uploadToS3 : {file_path} 업로드 완료")


with DAG(
    dag_id='divide_category',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['divide']
) as dag:

    fetch_csv_from_s3_task = PythonOperator(
        task_id="fetch_csv_from_s3",
        python_callable=fetch_csv_from_s3,
        provide_context=True, 
    )

    divide_category_task = PythonOperator(
        task_id="divide_category",
        python_callable=divide_category,
        op_args=['{{ ti.xcom_pull(task_ids="fetch_csv_from_s3") }}'],
        provide_context=True, 
    )

    merge_data_task = PythonOperator(
        task_id="merge_data",
        python_callable=merge_data,
        op_args=['{{ ti.xcom_pull(task_ids="fetch_csv_from_s3") }}'],
        provide_context=True, 
    )

    upload_to_s3_task = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_args=['{{ ti.xcom_pull(task_ids="merge_data") }}'],
        provide_context=True
    )

fetch_csv_from_s3_task >> divide_category_task >> merge_data_task >> upload_to_s3_task
    
