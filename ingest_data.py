import os
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine
from prefect import flow,task

#Función de ingesta
@task(log_prints=True,retries=3)
def ingest_data(user,password, host,db,tablename,url):
	#Para que pandas pueda abrir el archivo
	if url.endswith('.csv.gz'):
		csv_name="yellow_tripdata_2021-01.csv.gz"
	else:
		csv_name="output.csv"
		
os.system(f"wget{url} -0 {csv_name}")
postgres_url=f"postgresql://{user}:{password}@{host}:{port}/{db]"
engine=create_engine(postgres_url)

df_iter=pd.read_csv(csv_name,iterator=True,chunksize=100000)
df=next(df_iter)

df.tpep_pickup_datetime=pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime=pd.to_datetime(df.tpep_dropoff_datetime)

df.to_sql(name=table_name,con=engine,if_exist='append')
def main():
	user="postgres"
	password="admin"
	host="localhost"
	port="5433"
	db="ny_taxi"
	table_name="yellow_taxi_trips"
	csv_url=""
	ingest_data(user,password,host,port,db,table_name,csv_url)

@flow(name="Ingest Flow")
if __name__ == '__main__':
	main()
