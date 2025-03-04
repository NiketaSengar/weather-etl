from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
import requests
import json

# Constants
Latitude = '51.5074'
Longitude = '-0.1278'
postgres_conn_id = 'postgres_default'
api_conn_id = 'open_meteo_api'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

with DAG(dag_id='weather_etl_pipeline',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    @task()
    def extract_weather_data():
        """Extract weather data from API."""
        http_hook = HttpHook(http_conn_id=api_conn_id, method='GET')
        endpoint = f'/v1/forecast?latitude={Latitude}&longitude={Longitude}&current_weather=true'

        response = http_hook.run(endpoint)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch weather data: {
                            response.status_code}"
                            )

    @task()
    def transform_weather_data(weather_data):
        """Transform extracted weather data."""
        current_weather = weather_data['current_weather']
        transformed_data = {
            'latitude': Latitude,
            'longitude': Longitude,
            'temperature': current_weather['temperature'],
            'windspeed': current_weather['windspeed'],
            'winddirection': current_weather['winddirection'],
            'weathercode': current_weather['weathercode']
        }
        return transformed_data

    @task()
    def load_weather_data(transformed_data):
        """Load transformed data into postgres."""
        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        conn=pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
    CREATE TABLE IF NOT EXISTS weather_data (
        id SERIAL PRIMARY KEY,
        latitude FLOAT,
        longitude FLOAT,
        temperature FLOAT,
        windspeed FLOAT,
        winddirection FLOAT,
        weathercode INT,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
""")

        cursor.execute("""
        INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode)
        VALUES (%s, %s, %s, %s, %s, %s)""",(
            transformed_data['latitude'],
            transformed_data['longitude'],
            transformed_data['temperature'],
            transformed_data['windspeed'],
            transformed_data['winddirection'],
            transformed_data['weathercode']
            ))

        conn.commit()
        cursor.close()

    

    # DAG WORKFLOW - ETL PIPELINE
    weather_data = extract_weather_data()
    transformed_data = transform_weather_data(weather_data)
    load_weather_data(transformed_data)
