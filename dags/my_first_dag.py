from airflow import DAG
from datetime import datetime, timedelta
# from airflow.decorators import dag 
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator 


# constants
MY_NAME = "Elizabeth Bishop"
MY_NUMBER = 19

def multiply_by_23(number):
    """Multiplies a number by 23 and prints the results to Airflow logs."""
    result = number * 23
    print(result)


# IF you want to use as context manager

with DAG(
	dag_id='my_first_dag', 
	start_date=datetime(2023,1,1),
	description='multiplies a number by 23',
	tags=['tutorial'],
	schedule=timedelta(minutes=30),
	catchup=False, # do you want to run dags since the last trigger?
	default_args={
		"owner": MY_NAME,
		"retries": 2,
		"retry_delay": timedelta(minutes=5)
		}
	) as dag:

	t1 = BashOperator(
	    task_id="say_my_name",
	    bash_command=f"echo {MY_NAME}"
	)

	t2 = PythonOperator(
	    task_id="multiply_my_number_by_23",
	    python_callable=multiply_by_23,
	    op_kwargs={"number": MY_NUMBER}
	)

	t1 >> t2

# With the Taskflow API, instead of using the context manager with, you use the dag decorator.

# When using the DAG decorator, The "dag_id" value defaults to the name of the function
# it is decorating if not explicitly set.

# @dag(start_date=datetime(2023,1,1),
# 	description='first data pipeline',
# 	tags=['define_your_dag'],
# 	schedule='@daily',
# 	catchup=False # do you want to run dags since the last trigger?
# 	) 

# # You will define/call tasks within this function
# def my_first_dag():
# 	None


# def print_a():
# 	print("hi from task a")

# task_a = PythonOperator(
# 	task_id='task_a',
# 	python_callable=print_a
# 	)



