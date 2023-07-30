# import os
from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd

default_args = {
    'start_date': datetime(2023, 1, 1),
    'retries': 3
}

@dag(
    dag_id='read_file_dag', 
    schedule_interval=None, 
    default_args=default_args,
    catchup=False
    )
def read_file_dag():

    @task()
    def create_data():
        data = {'name': ['Ann']}
        df = pd.DataFrame(data)
        return df

    @task
    def write_to_csv(processed_df):
        file_path = '/test_df_ann.csv'  # Replace with the desired path for the new CSV file
        processed_df.to_csv(file_path, index=False)
        return file_path
            
    # @task        
    # def read_csv():
    #     file_path = '/../AL_2020_01_01.csv'  # Replace with the actual path to your CSV file
    #     df = pd.read_csv(file_path)
    #     print("~~~~~~~TASK NUMBER ONE~~~~~~~~~~~~~~\n") 
    #     # print(df.head())
    #     return df

    # @task
    # def process_data(df):
    #     # Process the DataFrame as needed
    #     print(df.head())

    write_to_csv(create_data())
    # process_data(read_csv())

read_file_dag()
