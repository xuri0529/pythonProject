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
    'owner': 'owen123',   # dag属于哪个用户
    'depends_on_past': False,   # True时，表示只有当上一个task成功时，当前task才能启动
    'start_date': airflow.utils.dates.days_ago(2),  # dag开始的时间
    'email': ['1203745031@qq.com'], # 可以配置一个邮件列表，触发邮件发送时将向列表中的邮箱发送对应邮件
    'email_on_failure': False,  # task失败时，是否触发邮件发送
    'email_on_retry': False,    # 重试时是否触发邮件发送
    'retries': 1,   # 失败重试次数
    'retry_delay': timedelta(minutes=5) # 失败重试时的时间间隔
}

# -------------------------------------------------------------------------------
# dag   实例化DAG对象来进行任务

dag = DAG(
    dag_id='example_hello_world_dag',   # 唯一标识， 必须完全由字母、数字、下划线组成
    default_args=default_args,  # 外部定义的 dic 格式的参数
    description='my first DAG', # 描述
    schedule_interval=timedelta(days=1) # 调度时间
    catchup=False  # 执行DAG时，将开始时间到目前所有该执行的任务都执行，默认为True
    )

# -------------------------------------------------------------------------------
# first operator 打印日期，BashOperator用来执行Bash脚本

date_operator = BashOperator(
    task_id='date_task',
    bash_command='date',
    dag=dag)

# -------------------------------------------------------------------------------
# second operator 休眠5秒

sleep_operator = BashOperator(
    task_id='sleep_task',
    depends_on_past=False,  # True时，表示只有当上一个task成功时，当前task才能启动
    bash_command='sleep 5',
    dag=dag)


# -------------------------------------------------------------------------------
# third operator 在执行任务时调用print_hello函数， PythonOperator用来调用Python函数

def print_hello():
    return 'Hello world!'


hello_operator = PythonOperator(
    task_id='hello_task',
    python_callable=print_hello,
    dag=dag)

# -------------------------------------------------------------------------------
# dependencies

sleep_operator.set_upstream(date_operator)  # sleep_operator会在date_operator执行完成之后执行
hello_operator.set_upstream(date_operator)


"""
catchup可以在airflow配置文件airflow.cfg的scheduler下，设置catchup_by_default=True（默认）或False，这个设置是全局性的设置；
将以上python配置文件上传到airflowhome/dags目录下，默认AIRFLOW_HOME为安装节点的/root/airflow目录，
该文件每修改一次，需要重新上传一次，并重启airflow，DAG执行调度
"""