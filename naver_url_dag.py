from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time
import pandas as pd
from selenium.webdriver.chrome.service import Service
from datetime import datetime
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import time
from datetime import datetime, timedelta
import os
import subprocess

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 9),
    'retries': 1,
}

dag = DAG(
    'naver_map_search',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
)

# def install_chromedriver():
#     """Chrome 및 ChromeDriver 자동 설치"""
#     # ChromeDriver 설치
#     chromedriver_autoinstaller.install()
    
#     # ChromeDriver 버전 확인
#     chromedriver_path = subprocess.check_output(["which", "chromedriver"]).decode("utf-8").strip()
#     version = subprocess.check_output([chromedriver_path, "--version"]).decode("utf-8").strip()
    
#     print(f"✅ ChromeDriver 설치 완료: {version}")
#     print(f"✅ ChromeDriver 경로: {chromedriver_path}")

# install_chromedriver_task = PythonOperator(
#     task_id='install_chromedriver',
#     python_callable=install_chromedriver,
#     dag=dag,
# )

# 1. restaurant_new 테이블에서 데이터 추출
def extract_data(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='wouldUlike-redshift')
    sql = "SELECT name, district_name FROM restaurant_new;"
    records = pg_hook.get_records(sql)
    
    # 데이터 리스트로 변환
    data = [{"name": row[0], "district_name": row[1]} for row in records]
    
    # XCom에 저장
    kwargs['ti'].xcom_push(key='restaurant_data', value=data)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

# 2. Selenium을 사용하여 네이버 지도에서 검색
def search_naver_maps(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_data', key='restaurant_data')

    
    if not data:
        raise ValueError("No data received from XCom")
    
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")  # GUI 없이 실행
    chrome_options.add_argument("--no-sandbox")  # 리눅스 환경에서 필수
    chrome_options.add_argument("--disable-dev-shm-usage")  # 공유 메모리 문제 해결
    chrome_options.add_argument("--remote-debugging-port=9222")  # 디버깅 포트 추가
    chrome_options.add_argument("--disable-gpu")  # GPU 가속 비활성화
    chrome_options.add_argument("--disable-software-rasterizer")

    chrome_bin_path = os.getenv("CHROME_BIN", "/usr/bin/google-chrome")
    chromedriver_bin_path = os.getenv("CHROMEDRIVER_BIN", "/usr/local/bin/chromedriver")

    service = Service(chromedriver_bin_path)

    # chrome_version = subprocess.run(["google-chrome", "--version"], capture_output=True, text=True).stdout.split()[2]
    # chrome_version = ".".join(chrome_version.split(".")[:3])

    # chromedriver_path = chromedriver_autoinstaller.install(version=chrome_version, cwd=True)
    # print("크롬 드라이버 경로" + chromedriver_path)
    # service = Service(chromedriver_path)

    # WebDriver 실행
    driver = webdriver.Chrome(service=service, options=chrome_options)

    # driver = webdriver.Chrome(service=Service(ChromeDriverManager('/home/***/.local/lib/python3.7/site-packages/chromedriver_autoinstaller/127/chromedriver/chromedriver.exe')), options=chrome_options)
    
    results = []
    for item in data:
        search_query = f"{item['district_name']} {item['name']}"

        driver.get("https://map.naver.com/")
        time.sleep(5)

        
        # 검색창에 입력 및 검색
        driver.switch_to.default_content()
        search_box = driver.find_element(By.CLASS_NAME, 'input_search')
        search_box.clear()
        search_box.send_keys(search_query)
        search_box.send_keys(Keys.RETURN)
        time.sleep(3)
        print("검색 완료")

        
        driver.switch_to.default_content()
        iframe = driver.find_element(By.CSS_SELECTOR, "iframe#searchIframe")
        driver.switch_to.frame(iframe)

        print("searchIframe 전환")

        ul_element = WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.TAG_NAME, "ul"))
        )
        
        # li 태그 목록 찾기
        li_elements = ul_element.find_elements(By.TAG_NAME, "li")
        
        if len(li_elements) > 1:
            # 첫 번째 li 태그 클릭
            print("개수" + len(li_elements))

            div_elements = li_elements[0].find_elements(By.TAG_NAME, "div")
            div_elements_2 = div_elements[0].find_elements(By.TAG_NAME, "div")
            div_elements_2[0].click()

            time.sleep(2)

            driver.switch_to.default_content()
            iframe = driver.find_element(By.ID, "entryIframe")
            driver.switch_to.frame(iframe)

            driver.execute_script("window.scrollTo(0, 0);")

            share_element = WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.ID, "_btp.share"))
            )
            share_element.click()

            time.sleep(1)

            div_element = WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.CLASS_NAME, "spi_copyurl"))
            )

            a_element = div_element.find_elements(By.TAG_NAME, 'a')
            url = a_element[1].get_attribute("href")
            print("공유 url : " + url)
        
        else:
            driver.switch_to.default_content()
            iframe = driver.find_element(By.ID, "entryIframe")
            driver.switch_to.frame(iframe)

            driver.execute_script("window.scrollTo(0, 0);")

            share_element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "_btp.share"))
            )
            share_element.click()
            
            print("공유 버튼 누름")

            time.sleep(1)

            div_element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "spi_copyurl"))
            )

            a_element = div_element.find_elements(By.TAG_NAME, 'a')
            url = a_element[1].get_attribute("href")
            print("공유 url : " + url)
        
        results.append({
            "name": item['name'],
            "district_name": item['district_name'],
            "url": url
        })


    driver.quit()
    time.sleep(1)

    # XCom에 검색 결과 저장
    ti.xcom_push(key='search_results', value=results)

search_task = PythonOperator(
    task_id='search_naver_maps',
    python_callable=search_naver_maps,
    provide_context=True,
    dag=dag,
)

# 3. 데이터베이스에 URL 저장
def save_to_db(**kwargs):
    ti = kwargs['ti']
    search_results = ti.xcom_pull(task_ids='search_naver_maps', key='search_results')

    if not search_results:
        raise ValueError("No search results received from XCom")

    pg_hook = PostgresHook(postgres_conn_id='wouldUlike-redshift')
    
    # 테이블 업데이트
    for result in search_results:
        sql = f"""
        UPDATE restaurant_new
        SET url = %s
        WHERE name = %s AND district_name = %s;
        """
        pg_hook.run(sql, parameters=(result['url'], result['name'], result['district_name']))

save_task = PythonOperator(
    task_id='save_to_db',
    python_callable=save_to_db,
    provide_context=True,
    dag=dag,
)

# Task 순서 지정
extract_task >> search_task >> save_task
