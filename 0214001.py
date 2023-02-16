# -*- coding: utf-8 -*-

import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta

# -------------------------------------------------------------------------------
# these args will get passed on to each operator
# you can override them on a per-task basis during operator initialization

default_args = {
    'owner': 'jifeng.si',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['1203745031@qq.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# -------------------------------------------------------------------------------
# dag

dag = DAG(
    'example_hello_world_dag',
    default_args=default_args,
    description='my first DAG',
    schedule_interval=timedelta(days=1))

# -------------------------------------------------------------------------------
# first operator 打印日期

date_operator = BashOperator(
    task_id='date_task',
    bash_command='date',
    dag=dag)

# -------------------------------------------------------------------------------
# second operator 休眠5秒

sleep_operator = BashOperator(
    task_id='sleep_task',
    depends_on_past=False,
    bash_command='sleep 5',
    dag=dag)


# -------------------------------------------------------------------------------
# third operator 在执行任务时调用print_hello函数

def print_hello():
    return 'Hello world!'


hello_operator = PythonOperator(
    task_id='hello_task',
    python_callable=print_hello,
    dag=dag)

# -------------------------------------------------------------------------------
# dependencies

sleep_operator.set_upstream(date_operator)
hello_operator.set_upstream(date_operator)