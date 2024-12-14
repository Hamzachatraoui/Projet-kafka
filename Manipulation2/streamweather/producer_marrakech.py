import requests
import json
from confluent_kafka import Producer
from datetime import datetime
import time

# Kafka Configuration
KAFKA_BROKER = 'localhost:9092'
NEW_TOPIC = 'morocco_weather'  # Topic name
PARTITION = 1  # Explicitly set to partition 1
producer = Producer({'bootstrap.servers': KAFKA_BROKER})

# Function to convert temperature from Kelvin to Celsius
def kelvin_to_celsius(kelvin):
    try:
        return kelvin - 273.15
    except TypeError:
        print("Invalid temperature data.")
        return None

# Fetch weather data by city name from API
def fetch_weather_data(api_url, city_name):
    try:
        response = requests.get(api_url.format(city_name=city_name, api_key="cb48670d0cdde1f786a9c92e3d7b7eeb"))  # Replace with your API key
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred for {city_name}: {err}")
    except Exception as err:
        print(f"An error occurred while fetching weather data for {city_name}: {err}")
    return None

# Send data to Kafka
def send_to_kafka(city_name, data):
    try:
        producer.produce(NEW_TOPIC, key=city_name, value=json.dumps(data), partition=PARTITION)
        producer.flush()  # Ensure data is sent
        print(f"Sent data for {city_name} to topic {NEW_TOPIC}, Partition {PARTITION}")
    except Exception as e:
        print(f"Error sending data to Kafka for {city_name}: {e}")

# Main logic to fetch and produce weather data
if __name__ == '__main__':
    api_url = "http://api.openweathermap.org/data/2.5/weather?q={city_name}&appid={api_key}"  # OpenWeatherMap API URL
    city = 'Marrakech'

    while True:  # Continuous process to fetch and produce weather data
        weather_data = fetch_weather_data(api_url, city)

        if weather_data and 'main' in weather_data and 'weather' in weather_data and len(weather_data['weather']) > 0:
            try:
                # Extract relevant weather data
                relevant_data = {
                    'city': city,
                    'temperature': str(round(kelvin_to_celsius(weather_data['main'].get('temp', 0)))),
                    'humidity': str(weather_data['main'].get('humidity', 0)),
                    'pressure': str(weather_data['main'].get('pressure', 0)),
                    'weather_description': str(weather_data['weather'][0].get('description', 'Unknown')),
                    'timestamp': datetime.now().isoformat()  # Add timestamp
                }
                # Send data to Kafka
                send_to_kafka(city, relevant_data)
            except KeyError as e:
                print(f"KeyError in weather data structure: {e}")
            except Exception as e:
                print(f"Error processing weather data: {e}")
        else:
            print(f"Incomplete or invalid weather data for {city}: {weather_data}")

        time.sleep(10)  # Wait for 10 seconds before fetching the next data point
