FROM apache/airflow:2.7.2
USER airflow
RUN pip install --upgrade pip

COPY requirements.txt .

RUN pip install -r requirements.txt