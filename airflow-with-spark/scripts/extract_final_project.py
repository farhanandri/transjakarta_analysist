import kagglehub
import os
import pandas as pd
DATASET_ROOT_DIR = "/opt/airflow/data/tj"


def main_extract_callable():
    path = kagglehub.dataset_download("dikisahkan/transjakarta-transportation-transaction")
    print("Path to dataset files:", path)
    
    if not os.path.exists(DATASET_ROOT_DIR):
        os.makedirs(DATASET_ROOT_DIR)
        
    
    os.system("cp -r %s/* %s" % (path, DATASET_ROOT_DIR))
    print("Path to dataset files:", path)


if __name__ == "__main__":
    main_extract_callable()