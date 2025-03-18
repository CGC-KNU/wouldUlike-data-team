# WouldULike : 취향에 맞는 음식 추천 앱

![image](https://github.com/user-attachments/assets/14808005-b65e-49c7-a095-3c4daf9e18a9)


<br>

## 프로젝트 개요
**메뉴를 고민하느라, 뭐하고 놀지 고민하느라 낭비되는 시간** 너무 아깝지 않나요? **매번 똑같은 선택**도 질릴테구요. **WouldULike**는 그런 사용자를 위해 탄생한 어플입니다.   

**WouldULike**는 테스트를 통해 **사용자에게 맞춤형 음식을 추천**해줍니다.

<br>

## 사용 기술 (Data Engineer)
- Python, SQL
- Pandas, Selenium
- Docker, Airflow
- AWS S3, Redshift, IAM

<br>

## 데이터 팀 구성 및 역할

|이름|역할|
|---------|--------|
|문소정| 음식점에 대한 다양한 데이터에 대한 ETL 파이프라인 구축, AWS 환경 구축 및 리소스 관리|

<br>

## DAG 종류

1. 음식점에 대한 데이터를 수집하고 가공해 S3에 적재하는 Dag
2. 음식점 위경도 값을 수집하고 가공해 S3에 적재하는 Dag
3. 카카오 API를 통해 음식점 카테고리, 전화번호 데이터 수집, 가공해 S3에 적재하는 Dag
4. 수집한 카테고리 2개의 분류로 나누는 Dag
5. Redshift DB 생성 후 데이터를 적재하는 Dag
6. 음식점에 대한 네이버 지도 URL 수집 및 가공, 적재하는 Dag




