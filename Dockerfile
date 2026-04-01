FROM apache/airflow:2.9.2-python3.11

USER airflow
RUN pip install --no-cache-dir \
    requests>=2.31 \
    python-dotenv>=1.0 \
    beautifulsoup4>=4.12 \
    lxml>=5.0 \
    pandas>=2.2 \
    psycopg2-binary>=2.9
