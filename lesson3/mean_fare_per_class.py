
import pandas as pd
import os
import datetime as dt

def mean_fare_per_class():
    titanic_data = ti.xcom_pull(key='key')
    titanic_mean_df = pd.read_json(titanic_data)
    df2 = titanic_mean_df.set_index('Pclass')[['Fare']].stack().mean(level=0)
    engine = create_engine('postgresql+psycopg2://airflow:airflow@localhost:5432/airflow')
    return df2.to_sql(f'{Variable.get("var2")}', engine)