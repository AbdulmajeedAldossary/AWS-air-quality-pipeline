import boto3
import json
import requests

# AWS and Kinesis configuration
kinesis_client = boto3.client('kinesis', region_name='eu-north-1')
stream_name = 'AirQualityStream'

# OpenWeatherMap API configuration
api_key = ''
city = 'Riyadh'
url = f'http://api.openweathermap.org/data/2.5/air_pollution?lat=24.7136&lon=46.6753&appid={api_key}'

def lambda_handler(event, context):
    """Fetch air quality data and send it to Kinesis."""
    try:
        # Fetch air quality data
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            # Send data to Kinesis
            kinesis_client.put_record(
                StreamName=stream_name,
                Data=json.dumps(data),
                PartitionKey="partitionKey"
            )
            print("Data sent to Kinesis successfully.")
        else:
            print(f"Error: {response.status_code}, {response.text}")
    except Exception as e:
        print(f"Error: {e}")

