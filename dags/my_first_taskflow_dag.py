# from airflow import DAG
from datetime import datetime, timedelta
from airflow.decorators import dag, task 
# from airflow.operators.python import PythonOperator
# from airflow.operators.bash import BashOperator 

# constants
MY_NAME = "Elizabeth Bishop"
MY_NUMBER = 19

# With the Taskflow API, instead of using the context manager with, you use the dag decorator.

# When using the DAG decorator, The "dag_id" value defaults to the name of the function
# it is decorating if not explicitly set.
@dag(
	dag_id='my_first_dag_taskflow',
	start_date=datetime(2023,1,1),
	description='multiplies a number by 23',
	tags=['tutorial'],
	schedule='@daily',
	default_args={
		"owner": "Jing C",
		"retries": 2,
		"retry_delay": timedelta(minutes=5)
		},
	catchup=False, # do you want to run dags since the last trigger?
) 

# # You will define/call tasks within this function
def my_first_dag():

	@task()
	def print_a_letter():
		print("~~~~~~hi from task a~~~~~~~~")

	# @task()
	# echo_name = BashOperator(
	#     task_id="say_my_name",
	#     bash_command=f"echo {MY_NAME}"
	# )

	@task()
	def multiply_by_23(number):
	    """Multiplies a number by 23 and prints the results to Airflow logs."""
	    result = number * 23
	    print(result)

	print_a_letter()
	multiply_by_23(MY_NUMBER)

# t2 = PythonOperator(
#     task_id="multiply_my_number_by_23",
#     python_callable=multiply_by_23,
#     op_kwargs={"number": MY_NUMBER}
# )

my_first_dag()



