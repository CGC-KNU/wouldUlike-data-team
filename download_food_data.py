import pendulum
import pandas as pd
import os
import logging
import zipfile
import shutil
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager


def download_csv():
    logging.info("download_csv : 음식점 DAG의 download_csv 함수 시작")

    chrome_options = Options()
    chrome_options.add_experimental_option("detach", True)
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": "/opt/airflow/downloads",
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })

    if not os.path.isdir("/opt/airflow/downloads"):
        os.mkdir("/opt/airflow/downloads")
        logging.info(f"/opt/airflow/downloads 디렉토리 생성")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.get("https://www.data.go.kr/data/15045016/fileData.do#")

    wait = WebDriverWait(driver, 20)
    button_1 = wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[2]/div/div[2]/div/div[2]/div[6]/table/tbody/tr[19]/td/a')))
    button_1.click()
    logging.info("Button clicked successfully!")

    download_file_name = "07_24_04_P_CSV.zip"
    file_exist = os.path.isfile(f"./downloads/{download_file_name}")
    logging.info(f"/opt/airflow/downloads/07_24_04_P_CSV.zip 다운받은 파일 존재 여부 : {file_exist}")

    return download_file_name


def zip_to_csv(file_name, **kwargs):
    output_dir = "./downloads/original"
    file_path = f"./downloads/{file_name}"
    os.makedirs(output_dir, exist_ok=True)

    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        zip_ref.extractall(output_dir)

    logging.info(f"zip_to_csv : {file_name} 압축 풀기 성공")
    return output_dir


def rename_file(dir_path):
    date_str = datetime.today().strftime("%Y%m")
    csv_file_name = f"food_{date_str}.csv"

    for file in os.listdir(dir_path):
        if file.endswith(".csv"):
            os.rename(f"{dir_path}/{file}", f"{dir_path}/{csv_file_name}")
            logging.info(f"rename_file : {file}에서 {csv_file_name}으로 파일 이름 변경")

    return csv_file_name


def move_file(dir_path, csv_file_name):
    before_path = f"{dir_path}/{csv_file_name}"
    after_dir = "./food"

    if not os.path.isdir(after_dir):
        os.mkdir(after_dir)

    shutil.copy2(before_path, after_dir)
    logging.info(f"move_file : {before_path} 파일을 {after_dir}로 이동함")
    return f"./food/{csv_file_name}"


def transform(file_path):
    df = pd.read_csv(file_path, index_col=False, encoding='cp949')
    df = df.drop(df.columns[0], axis=1)

    selected_columns = [
        '사업장명',
        '영업상태명',
        '소재지우편번호',
        '도로명우편번호',
        '도로명전체주소'
    ]

    df = df[selected_columns]
    df = df.reset_index(drop=True)
    df = df[df['영업상태명'] != '폐업']
    df = df[df['도로명전체주소'].str.contains('대구광역시', na=False)]
    if '카테고리' not in df.columns:
        df['카테고리'] = ''

    date_str = datetime.today().strftime("%Y%m")
    csv_file_name = f"daegu_food_{date_str}.csv"
    csv_file_path = f"./food/{csv_file_name}"
    df.to_csv(csv_file_path, index=False)

    return csv_file_path


def upload_to_s3(file_path):
    date_str = datetime.today().strftime("%Y%m")
    file_name = f"{date_str}.csv"

    bucket_name = 'wouldulike-data'
    hook = S3Hook(aws_conn_id='wouldUlike-S3')
    hook.load_file(
        filename=file_path,
        key=f'data/original/{file_name}',
        bucket_name=bucket_name,
        replace=True
    )
    logging.info(f"uploadToS3 : {file_path} 업로드 완료")


with DAG(
    dag_id='download_food_data',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['download']
) as dag:

    download_csv_task = PythonOperator(
        task_id='download_csv',
        python_callable=download_csv,
        provide_context=True
    )

    zip_to_csv_task = PythonOperator(
        task_id='zip_to_csv',
        python_callable=zip_to_csv,
        op_args=['{{ ti.xcom_pull(task_ids="download_csv") }}'],
        provide_context=True
    )

    rename_file_task = PythonOperator(
        task_id='rename_file',
        python_callable=rename_file,
        op_args=['{{ ti.xcom_pull(task_ids="zip_to_csv") }}'],
        provide_context=True
    )

    move_file_task = PythonOperator(
        task_id='move_file',
        python_callable=move_file,
        op_args=['{{ ti.xcom_pull(task_ids="zip_to_csv") }}', '{{ ti.xcom_pull(task_ids="rename_file") }}'],
        provide_context=True
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
        op_args=['{{ ti.xcom_pull(task_ids="move_file") }}'],
        provide_context=True
    )

    upload_to_s3_task = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_args=['{{ ti.xcom_pull(task_ids="transform") }}'],
        provide_context=True
    )

download_csv_task >> zip_to_csv_task >> rename_file_task >> move_file_task >> transform_task >> upload_to_s3_task
