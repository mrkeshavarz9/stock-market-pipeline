import requests
import pandas as pd
import sqlite3
from io import StringIO
import boto3
from datetime import date, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import os



API_KEY = "MZEUEXSGODOVRZ9T"

def extract_data(symbol, api_key):
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "apikey": api_key
    }
    response = requests.get(url, params=params)
    data = response.json()
    return data


def transform_data(ti, symbol):
    raw_data = ti.xcom_pull(task_ids='extract_data_task')
    time_series = raw_data['Time Series (Daily)']
    df = pd.DataFrame.from_dict(time_series, orient='index')
    new_list = []
    for col in df.columns:
        clean = col.split(". ")[1]
        new_list.append(clean)
    df.columns = new_list
    for col in df.columns:
        df[col] = pd.to_numeric(df[col])
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()
    df["symbol"] = symbol
    df = df.reset_index()
    df = df.rename(columns={"index": "date"})
    return df

def load_to_sqlite(ti, db_name):
    df = ti.xcom_pull(task_ids='transform_data_task')
    conn = sqlite3.connect(db_name)
    df.to_sql('daily_prices', conn, if_exists='append', index=False)
    query = """
    DELETE FROM daily_prices
    WHERE rowid NOT IN (
        SELECT MIN(rowid)
        FROM daily_prices
        GROUP BY date, symbol
    )
    """
    conn.execute(query)
    conn.commit()
    conn.close()

def load_to_s3(ti, bucket_name):
    df = ti.xcom_pull(task_ids='transform_data_task')
    symbol = df['symbol'].iloc[0]
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3 = boto3.client('s3', region_name='ap-southeast-2')
    today = date.today()
    file_key = f"stock-data/{symbol}_{today}.csv"
    
    s3.put_object(
        Bucket=bucket_name,
        Key=file_key,
        Body=csv_buffer.getvalue()
    )

default_args = {
    'owner': 'jenesis',
    'start_date': datetime(2026, 8, 13),
}

dag = DAG(
    'stock_market_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

extract_task = PythonOperator(
    task_id='extract_data_task',
    python_callable=extract_data,
    op_kwargs={'symbol': 'AAPL', 'api_key': API_KEY},
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data_task',
    python_callable=transform_data,
    op_kwargs={'symbol': 'AAPL'},
    dag=dag
)

load_sqlite_task = PythonOperator(
    task_id='load_to_sqlite_task',
    python_callable=load_to_sqlite,
    op_kwargs={'db_name': 'stock_data.db'},
    dag=dag
)

load_s3_task = PythonOperator(
    task_id='load_to_s3_task',
    python_callable=load_to_s3,
    op_kwargs={'bucket_name': 'my-etl-data-jenesis-2026'},
    dag=dag
)

extract_task >> transform_task >> load_sqlite_task >> load_s3_task

