import pendulum
import pandas as pd
import logging
from datetime import datetime
import requests
import time

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def fetch_csv_from_s3():
    date_str = datetime.today().strftime("%Y%m")
    bucket_name = "wouldulike-data"
    key = f"data/coord/{date_str}.csv"
    download_path = f"./s3_download/{date_str}_coord.csv"

    s3 = S3Hook(aws_conn_id="wouldUlike-S3")
    s3.get_key(key, bucket_name).download_file(download_path)
    logging.info(f"S3에서 다운로드 : {download_path}")

    return download_path

def divide_data(download_path, **kwargs):
    df = pd.read_csv(download_path)

    data_list = df[['사업장명', 'X', 'Y']].values.tolist()

    chunk_size = len(data_list) // 3
    data_chunks = [
        data_list[:chunk_size], 
        data_list[chunk_size:2*chunk_size],
        data_list[2*chunk_size:] 
    ]

    for i, chunk in enumerate(data_chunks, start=1):
        kwargs['ti'].xcom_push(key=f"{i}", value=chunk)

def add_category_1(**kwargs):
    url = "https://dapi.kakao.com/v2/local/search/keyword.json"

    session = requests.Session()

    retries = Retry(
        total=5,  
        backoff_factor=1, 
        status_forcelist=[500, 502, 503, 504], 
        method_whitelist=["GET", "POST"] 
    )
    
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)  
    session.mount("http://", adapter) 

    category_data = []
    phone_data = []

    data_list = kwargs['ti'].xcom_pull(key='1', task_ids='divide_data')

    for d in data_list:
        headers = {
            'Authorization' : 'KakaoAK 8933bbdc3991477a4276c5d060cf5929'
        }
        params = {
            'query' : f'{d[0]}',
            'category_group_code' : 'FD6',
            'x' : f'{d[1]}',
            'y' : f'{d[2]}',
            'radius' : '1000',
            'size': '5',
            'sort' : 'distance'
        }

        try:
            response = session.get(url, headers=headers, params=params)
            response.raise_for_status()

            if response.status_code == 200:
                json_data = response.json()
                if len(json_data['documents']) == 0:
                    category_data.append('')
                    phone_data.append('')
                else:
                    try:
                        category_data.append(json_data['documents'][0]['category_name'])
                        phone_data.append(json_data['documents'][0]['phone'])
                    except KeyError:
                        category_data.append('')
                        phone_data.append('')
            else:
                category_data.append('')
                phone_data.append('')
        
        except requests.exceptions.RequestException as e:
            print(f"Request failed for address {d}: {e}")
            category_data.append('')
            phone_data.append('')
        
        time.sleep(0.3)
    
    return [category_data, phone_data]

def add_category_2(**kwargs):
    url = "https://dapi.kakao.com/v2/local/search/keyword.json"

    session = requests.Session()

    retries = Retry(
        total=5,  
        backoff_factor=1, 
        status_forcelist=[500, 502, 503, 504], 
        method_whitelist=["GET", "POST"] 
    )
    
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)  
    session.mount("http://", adapter) 

    category_data = []
    phone_data = []

    data_list = kwargs['ti'].xcom_pull(key='2', task_ids='divide_data')

    for d in data_list:
        headers = {
            'Authorization' : 'KakaoAK 4f517a75b577fdd55970fbf2348ecd3e'
        }
        params = {
            'query' : f'{d[0]}',
            'category_group_code' : 'FD6',
            'x' : f'{d[1]}',
            'y' : f'{d[2]}',
            'radius' : '1000',
            'size': '5',
            'sort' : 'distance'
        }

        try:
            response = session.get(url, headers=headers, params=params)
            response.raise_for_status()  

            if response.status_code == 200:
                print(f"음식점 이름 : {d[0]}")
                json_data = response.json()
                
                if len(json_data['documents']) == 0:
                    category_data.append('')
                    phone_data.append('')
                else:
                    try:
                        category_data.append(json_data['documents'][0]['category_name'])
                        phone_data.append(json_data['documents'][0]['phone'])
                    except KeyError:
                        category_data.append('')
                        phone_data.append('')
            else:
                category_data.append('')
                phone_data.append('')
        
        except requests.exceptions.RequestException as e:
            print(f"Request failed for address {d}: {e}")
            category_data.append('')
            phone_data.append('')
        
        time.sleep(0.3)
    
    return [category_data, phone_data]

def add_category_3(**kwargs):

    url = "https://dapi.kakao.com/v2/local/search/keyword.json"

    session = requests.Session()

    retries = Retry(
        total=5,  
        backoff_factor=1, 
        status_forcelist=[500, 502, 503, 504], 
        method_whitelist=["GET", "POST"] 
    )
    
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)  
    session.mount("http://", adapter) 

    category_data = []
    phone_data = []

    data_list = kwargs['ti'].xcom_pull(key='3', task_ids='divide_data')

    for d in data_list:
        headers = {
            'Authorization' : 'KakaoAK 4f517a75b577fdd55970fbf2348ecd3e'
        }
        params = {
            'query' : f'{d[0]}',
            'category_group_code' : 'FD6',
            'x' : f'{d[1]}',
            'y' : f'{d[2]}',
            'radius' : '1000',
            'size': '5',
            'sort' : 'distance'
        }

        try:
            response = session.get(url, headers=headers, params=params)
            response.raise_for_status()  

            if response.status_code == 200:
                print(f"음식점 이름 : {d[0]}")
                json_data = response.json()
                if len(json_data['documents']) == 0:
                    category_data.append('')
                    phone_data.append('')
                else:
                    try:
                        category_data.append(json_data['documents'][0]['category_name'])
                        phone_data.append(json_data['documents'][0]['phone'])
                    except KeyError:
                        category_data.append('')
                        phone_data.append('')
            else:
                category_data.append('')
                phone_data.append('')
        
        except requests.exceptions.RequestException as e:
            print(f"Request failed for address {d}: {e}")
            category_data.append('')
            phone_data.append('')
        
        time.sleep(0.3)
    
    return [category_data, phone_data]

def combine_data(path, **kwargs):
    list_1 = kwargs['ti'].xcom_pull(task_ids='add_category_1')
    list_2 = kwargs['ti'].xcom_pull(task_ids='add_category_2')
    list_3 = kwargs['ti'].xcom_pull(task_ids='add_category_3')
    category = list_1[0] + list_2[0] + list_3[0]
    phone = list_1[1] + list_2[1] + list_3[1]
    logging.info(category)

    df = pd.read_csv(path, index_col=False)
    df['카테고리'] = category
    df['전화번호'] = phone
    df.to_csv(path, index=False)
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
    dag_id='add_category_to_data',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['category']
) as dag:

    fetch_csv_from_s3_task = PythonOperator(
        task_id="fetch_csv_from_s3",
        python_callable=fetch_csv_from_s3,
        provide_context=True, 
    )

    divide_data_task = PythonOperator(
        task_id="divide_data",
        python_callable=divide_data,
        op_args=['{{ ti.xcom_pull(task_ids="fetch_csv_from_s3") }}'],
        provide_context=True, 
    )

    add_category_task_1 = PythonOperator(
        task_id="add_category_1",
        python_callable=add_category_1,
        provide_context=True, 
    )

    add_category_task_2 = PythonOperator(
        task_id="add_category_2",
        python_callable=add_category_2,
        provide_context=True, 
    )

    add_category_task_3 = PythonOperator(
        task_id="add_category_3",
        python_callable=add_category_3,
        provide_context=True, 
    )

    combine_data_task = PythonOperator(
        task_id="combine_data",
        python_callable=combine_data,
        op_args=['{{ ti.xcom_pull(task_ids="fetch_csv_from_s3") }}'],
        provide_context=True, 
    )

    upload_to_s3_task = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_args=['{{ ti.xcom_pull(task_ids="combine_data") }}'],
        provide_context=True
    )

fetch_csv_from_s3_task >>  divide_data_task
divide_data_task >> [add_category_task_1, add_category_task_2, add_category_task_3] >> combine_data_task >> upload_to_s3_task
