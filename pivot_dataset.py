
import pandas as pd
import os
import datetime as dt

def pivot_dataset():
    titanic_data = ti.xcom_pull(key='key')
    titanic_df = pd.read_json(titanic_data)
    df = titanic_df.pivot_table(index=['Sex'],
                                columns=['Pclass'],
                                values='Name',
                                aggfunc='count').reset_index()
    engine = create_engine('postgresql+psycopg2://airflow:airflow@localhost:5432/airflow')
    return df.to_sql(f'{Variable.get("var")}', engine)