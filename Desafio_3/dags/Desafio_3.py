from datetime import timedelta,datetime
from pathlib import Path
import json
import requests
import psycopg2
from airflow import DAG
from sqlalchemy import create_engine
# Operadores
from airflow.operators.python_operator import PythonOperator
#from airflow.utils.dates import days_ago
import pandas as pd
import os

dag_path = os.getcwd()     #path original.. home en Docker

url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws"
with open(dag_path+'/keys/'+"db.txt",'r') as f:
    data_base= f.read()
with open(dag_path+'/keys/'+"user.txt",'r') as f:
    user= f.read()
with open(dag_path+'/keys/'+"pwd.txt",'r') as f:
    pwd= f.read()

redshift_conn = {
    'host': url,
    'username': user,
    'database': data_base,
    'port': '5439',
    'pwd': pwd
}

# argumentos por defecto para el DAG
default_args = {
    'owner': 'GuidoG',
    'start_date': datetime(2023,7,10),
    'retries':2,
    'retry_delay': timedelta(minutes=15)
}

BC_dag = DAG(
    dag_id='Desafio_3',
    default_args=default_args,
    description='Extraccion de datos y storage en amazon redshift',
    schedule_interval="@daily",
    catchup=False
)

dag_path = os.getcwd()     #path original.. home en Docker

# funcion de extraccion de datos
def extraer_data(exec_date):
    try:
         print(f"Adquiriendo data para la fecha: {exec_date}")
         date = datetime.strptime(exec_date, '%Y-%m-%d %H')
         url = "https://data.cityofnewyork.us/resource/h9gi-nx95.json"
         response = requests.get(url)
         if response:
              print('Success!')
              data = response.json()
              with open(dag_path+'/raw_data/'+"data_"+str(date.year)+'-'+str(date.month)+'-'+str(date.day)+'-'+str(date.hour)+".json", "w") as json_file:
                   json.dump(data, json_file)
         else:
              print('An error has occurred.') 
    except ValueError as e:
        print("Formato datetime deberia ser %Y-%m-%d %H", e)
        raise e       

# Funcion de transformacion en tabla
def transformar_data(exec_date):       
    print(f"Transformando la data para la fecha: {exec_date}") 
    date = datetime.strptime(exec_date, '%Y-%m-%d %H')
    with open(dag_path+'/raw_data/'+"data_"+str(date.year)+'-'+str(date.month)+'-'+str(date.day)+'-'+str(date.hour)+".json", "r") as json_file:
        loaded_data=json.load(json_file)
    # Extraer la data en tabla
    datax = loaded_data['data']
    df = pd.json_normalize(datax)
    # Filtros, formatos y transformaciones

    df = df.iloc[:,[0,4,5]]
    df['crash_date'] = pd.to_datetime(df['crash_date'])
    df['number_of_persons_injured'] =  pd.to_numeric(df['number_of_persons_injured'], errors="coerce").fillna(0).astype('int64')
    df['number_of_persons_killed'] =  pd.to_numeric(df['number_of_persons_killed'], errors="coerce").fillna(0).astype('int64')
    #Group data by day
    df = df.groupby(by='crash_date').sum()
    df= df.reset_index()
    df['crash_date'] = df['crash_date'].astype(str)

    df.to_csv(dag_path+'/processed_data/'+"data_"+str(date.year)+'-'+str(date.month)+'-'+str(date.day)+'-'+str(date.hour)+".csv", index=False, mode='a')

# Funcion conexion a redshift
def conexion_redshift(exec_date):
    print(f"Conectandose a la BD en la fecha: {exec_date}") 
    url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
    try:
        conn = psycopg2.connect(
            host=url,
            dbname=redshift_conn["database"],
            user=redshift_conn["username"],
            password=redshift_conn["pwd"],
            port='5439')
        print(conn)
        print("Connected to Redshift successfully!")
    except Exception as e:
        print("Unable to connect to Redshift.")
        print(e)
    #engine = create_engine(f'redshift+psycopg2://{redshift_conn["username"]}:{redshift_conn["pwd"]}@{redshift_conn["host"]}:{redshift_conn["port"]}/{redshift_conn["database"]}')
    #print(engine)

from psycopg2.extras import execute_values
# Funcion de envio de data
def cargar_data(exec_date):
    print(f"Cargando la data para la fecha: {exec_date}")
    date = datetime.strptime(exec_date, '%Y-%m-%d %H')
    #date = datetime.strptime(exec_date, '%Y-%m-%d %H')
    records=pd.read_csv(dag_path+'/processed_data/'+"data_"+str(date.year)+'-'+str(date.month)+'-'+str(date.day)+'-'+str(date.hour)+".csv")
    print(records.shape)
    print(records.head())
    # conexion a database
    url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
    conn = psycopg2.connect(
        host=url,
        dbname=redshift_conn["database"],
        user=redshift_conn["username"],
        password=redshift_conn["pwd"],
        port='5439')
    # Definir columnas
    columns= ['crash_date','number_of_persons_injured','number_of_persons_killed']
    from psycopg2.extras import execute_values
    cur = conn.cursor()
    # Define the table name
    table_name = 'car_collitions'
    # Define the columns you want to insert data into
    columns = columns
    # Generate 
    values = [tuple(x) for x in records.to_numpy()]
    insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s"
    # Execute the INSERT statement using execute_values
    cur.execute("BEGIN")
    execute_values(cur, insert_sql, values)
    cur.execute("COMMIT")    
    #records.to_sql('mining_data', engine, index=False, if_exists='append')
    

# Tareas
##1. Extraccion
task_1 = PythonOperator(
    task_id='extraer_data',
    python_callable=extraer_data,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=BC_dag,
)

#2. Transformacion
task_2 = PythonOperator(
    task_id='transformar_data',
    python_callable=transformar_data,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=BC_dag,
)

# 3. Envio de data 
# 3.1 Conexion a base de datos
task_31= PythonOperator(
    task_id="conexion_BD",
    python_callable=conexion_redshift,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=BC_dag
)

# 3.2 Envio final
task_32 = PythonOperator(
    task_id='cargar_data',
    python_callable=cargar_data,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=BC_dag,
)

# Definicion orden de tareas
task_1 >> task_2 >> task_31 >> task_32