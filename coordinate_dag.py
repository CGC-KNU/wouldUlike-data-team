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
    key = f"data/original/{date_str}.csv"
    download_path = f"./s3_download/{date_str}.csv"

    s3 = S3Hook(aws_conn_id="wouldUlike-S3")
    s3.get_key(key, bucket_name).download_file(download_path)
    logging.info(f"S3에서 다운로드 : {download_path}")

    return download_path

def extract_road_address(download_path):
    df = pd.read_csv(download_path)
    df['도로명주소'] = df['도로명전체주소'].str.split(',').str[0]
    df.to_csv(download_path)
    return download_path

def divide_data(download_path, **kwargs):
    df = pd.read_csv(download_path)

    addresses = df['도로명주소'].dropna().tolist()

    chunk_size = len(addresses) // 3
    data_chunks = [
        addresses[:chunk_size], 
        addresses[chunk_size:2*chunk_size],
        addresses[2*chunk_size:] 
    ]

    for i, chunk in enumerate(data_chunks, start=1):
        kwargs['ti'].xcom_push(key=f"{i}", value=chunk)

def add_coordinate_1(**kwargs):

    url = "https://api.vworld.kr/req/address?"

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

    x_point = []
    y_point = []

    data_list = kwargs['ti'].xcom_pull(key='1', task_ids='divide_data')

    for d in data_list:
        params = {
            "service": "address",
            "request": "getcoord",
            "crs": "epsg:4326",
            "address": f"{d}",
            "format": "json",
            "type": "road",
            "key": "B76890D0-944A-3D4E-AEB8-AAA9FB1871D1"
        }

        try:
            # API 요청
            response = session.get(url, params=params)
            response.raise_for_status()  # 상태 코드가 4xx, 5xx일 경우 예외 발생

            # 응답 처리
            if response.status_code == 200:
                json_data = response.json()
                
                try:
                    x_point.append(json_data['response']['result']['point']['x'])
                    y_point.append(json_data['response']['result']['point']['y'])
                except KeyError:
                    x_point.append('')
                    y_point.append('')
            else:
                x_point.append('')
                y_point.append('')
        
        except requests.exceptions.RequestException as e:
            print(f"Request failed for address {d}: {e}")
            x_point.append('')
            y_point.append('')
        
        time.sleep(0.3)
    
    return [x_point, y_point]

def add_coordinate_2(**kwargs):

    url = "https://api.vworld.kr/req/address?"

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

    x_point = []
    y_point = []

    data_list = kwargs['ti'].xcom_pull(key='2', task_ids='divide_data')

    for d in data_list:
        params = {
            "service": "address",
            "request": "getcoord",
            "crs": "epsg:4326",
            "address": f"{d}",
            "format": "json",
            "type": "road",
            "key": "65EAF9ED-2425-34F9-A994-B2EFACEFCA8B"
        }

        try:
            # API 요청
            response = session.get(url, params=params)
            response.raise_for_status()  # 상태 코드가 4xx, 5xx일 경우 예외 발생

            # 응답 처리
            if response.status_code == 200:
                json_data = response.json()
                
                try:
                    x_point.append(json_data['response']['result']['point']['x'])
                    y_point.append(json_data['response']['result']['point']['y'])
                except KeyError:
                    x_point.append('')
                    y_point.append('')
            else:
                x_point.append('')
                y_point.append('')
        
        except requests.exceptions.RequestException as e:
            print(f"Request failed for address {d}: {e}")
            x_point.append('')
            y_point.append('')
        
        time.sleep(0.3)
    
    return [x_point, y_point]

def add_coordinate_3(**kwargs):

    url = "https://api.vworld.kr/req/address?"

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

    x_point = []
    y_point = []

    data_list = kwargs['ti'].xcom_pull(key='3', task_ids='divide_data')

    for d in data_list:
        params = {
            "service": "address",
            "request": "getcoord",
            "crs": "epsg:4326",
            "address": f"{d}",
            "format": "json",
            "type": "road",
            "key": "CF681D9F-89AB-3EC5-AF94-60CDCFC88B5E"
        }

        try:
            # API 요청
            response = session.get(url, params=params)
            response.raise_for_status()  # 상태 코드가 4xx, 5xx일 경우 예외 발생

            # 응답 처리
            if response.status_code == 200:
                json_data = response.json()
                
                try:
                    x_point.append(json_data['response']['result']['point']['x'])
                    y_point.append(json_data['response']['result']['point']['y'])
                except KeyError:
                    x_point.append('')
                    y_point.append('')
            else:
                x_point.append('')
                y_point.append('')
        
        except requests.exceptions.RequestException as e:
            print(f"Request failed for address {d}: {e}")
            x_point.append('')
            y_point.append('')
        
        time.sleep(0.3)
    
    return [x_point, y_point]

def combine_data(path, **kwargs):
    point_1 = kwargs['ti'].xcom_pull(task_ids='add_coordinate_1')
    point_2 = kwargs['ti'].xcom_pull(task_ids='add_coordinate_2')
    point_3 = kwargs['ti'].xcom_pull(task_ids='add_coordinate_3')
    x_list = point_1[0] + point_2[0] + point_3[0]
    y_list = point_1[1] + point_2[1] + point_3[1]
    logging.info(x_list)

    df = pd.read_csv(path, index_col=False)
    df['X'] = x_list
    df['Y'] = y_list
    df.to_csv(path, index=False)
    return path

def upload_to_s3(file_path):
    date_str = datetime.today().strftime("%Y%m")
    file_name = f"{date_str}.csv"

    bucket_name = 'wouldulike-data'
    hook = S3Hook(aws_conn_id='wouldUlike-S3')
    hook.load_file(
        filename=file_path,
        key=f'data/coord/{file_name}',
        bucket_name=bucket_name,
        replace=True
    )
    logging.info(f"uploadToS3 : {file_path} 업로드 완료")


with DAG(
    dag_id='add_coordinate_to_data',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['coordinate']
) as dag:

    fetch_csv_from_s3_task = PythonOperator(
        task_id="fetch_csv_from_s3",
        python_callable=fetch_csv_from_s3,
        provide_context=True, 
    )

    extract_road_address_task = PythonOperator(
        task_id="extract_road_address",
        python_callable=extract_road_address,
        op_args=['{{ ti.xcom_pull(task_ids="fetch_csv_from_s3") }}'],
        provide_context=True, 
    )

    divide_data_task = PythonOperator(
        task_id="divide_data",
        python_callable=divide_data,
        op_args=['{{ ti.xcom_pull(task_ids="extract_road_address") }}'],
        provide_context=True, 
    )

    add_coordinate_task_1 = PythonOperator(
        task_id="add_coordinate_1",
        python_callable=add_coordinate_1,
        provide_context=True, 
    )

    add_coordinate_task_2 = PythonOperator(
        task_id="add_coordinate_2",
        python_callable=add_coordinate_2,
        provide_context=True, 
    )

    add_coordinate_task_3 = PythonOperator(
        task_id="add_coordinate_3",
        python_callable=add_coordinate_3,
        provide_context=True, 
    )

    combine_data_task = PythonOperator(
        task_id="combine_data",
        python_callable=combine_data,
        op_args=['{{ ti.xcom_pull(task_ids="extract_road_address") }}'],
        provide_context=True, 
    )

    upload_to_s3_task = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_args=['{{ ti.xcom_pull(task_ids="combine_data") }}'],
        provide_context=True
    )

fetch_csv_from_s3_task >> extract_road_address_task >> divide_data_task
divide_data_task >> [add_coordinate_task_1, add_coordinate_task_2, add_coordinate_task_3] >> combine_data_task >> upload_to_s3_task

    
