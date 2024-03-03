import os
import subprocess
from dotenv import load_dotenv


load_dotenv()
SALES_DIR = os.environ['SALES_DIR']

def run_bash_command(command):
    try:
        result = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        print("Output:", result.stdout)
    except subprocess.CalledProcessError as e:
        print("Error:", e.stderr)

def load_files_to_gcp_storage(path_to_csv_file, csv_file_name) -> None:

    bash_command = f'gcloud storage cp {path_to_csv_file} gs://final_bucket-1/sales_data/{csv_file_name}'
    run_bash_command(bash_command)
    #print(bash_command)

for i in os.listdir(SALES_DIR):
    for j in os.listdir(os.path.join(SALES_DIR, i)):
        load_files_to_gcp_storage(path_to_csv_file=os.path.join(SALES_DIR, i, j), csv_file_name=j)
        #print(j, os.path.join(SALES_DIR, i, j))