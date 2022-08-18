
import pandas as pd
import os
import datetime as dt


def get_path(file_name):
    return os.path.join(os.path.expanduser('~'), file_name)

def download_titanic_dataset(ti):
    
    url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
    df = pd.read_csv(url)      
    out = df.to_json(orient='records')
  
    ti.xcom_push(key='key', value=out)   