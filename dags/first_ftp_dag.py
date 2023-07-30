from airflow.decorators import dag, task
from airflow.providers.ftp.hooks.ftp import FTPHook
from datetime import datetime
import pandas as pd

default_args = {
    'start_date': datetime(2023, 6, 9),
    'retries': 3
}

@dag('ftp_csv_dag', schedule_interval=None, default_args=default_args)
def ftp_csv_dag():

    @task
    def download_csv():
        ftp_hook = FTPHook(ftp_conn_id='your_ftp_conn_id')
        remote_file_path = '/path/to/your/file.csv'  # Replace with the path to the CSV file on the FTP server
        local_file_path = '/path/to/local/directory/file.csv'  # Replace with the desired local file path

        ftp_hook.retrieve_file(remote_file_path, local_file_path)
        return local_file_path

    @task
    def process_csv(local_file_path):
        df = pd.read_csv(local_file_path)
        # Process the DataFrame as needed
        print(df.head())

    downloaded_file = download_csv()
    process_csv(downloaded_file)

ftp_csv_dag = ftp_csv_dag()