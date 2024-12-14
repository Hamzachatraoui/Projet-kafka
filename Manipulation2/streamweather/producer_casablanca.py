# import requests
# import json
# from confluent_kafka import Producer
# from datetime import datetime
# import time

# # Kafka Configuration
# KAFKA_BROKER = 'localhost:9092'
# NEW_TOPIC = 'morocco_weather'  # New topic name
# producer = Producer({'bootstrap.servers': KAFKA_BROKER})

# # Fetch weather data by city name from API
# def fetch_weather_data(api_url, city_name):
#     try:
#         response = requests.get(api_url.format(city_name=city_name, api_key="cb48670d0cdde1f786a9c92e3d7b7eeb"))  # Replace with your API key
#         response.raise_for_status()
#         return response.json()
#     except requests.exceptions.HTTPError as err:
#         print(f"HTTP error occurred for {city_name}: {err}")
#     except Exception as err:
#         print(f"An error occurred for {city_name}: {err}")
#     return None

# # Send data to the new Kafka topic, explicitly specifying partition 0 for Casablanca
# def send_to_kafka(city_name, data):
#     try:
#         # Send data to partition 0
#         producer.produce(NEW_TOPIC, key=city_name, value=json.dumps(data), partition=0)
#         producer.flush()  # Ensure the data is sent
#         print(f"Sent data for {city_name} to topic {NEW_TOPIC} (Partition 0)")
#     except Exception as e:
#         print(f"Error sending data to Kafka: {e}")

# # Main Producer Logic for Casablanca
# if __name__ == '__main__':
#     api_url = "http://api.openweathermap.org/data/2.5/weather?q={city_name}&appid={api_key}"  # API URL template
#     city = 'Casablanca'

#     while True:  # Continuous process
#         weather_data = fetch_weather_data(api_url, city)
#         if weather_data:
#             relevant_data = {
#                 'city': city,
#                 'temperature': weather_data['main'].get('temp'),
#                 'humidity': weather_data['main'].get('humidity'),
#                 'pressure': weather_data['main'].get('pressure'),
#                 'weather_description': weather_data['weather'][0].get('description'),
#                 'timestamp': datetime.now().isoformat()  # Add current timestamp
#             }
#             send_to_kafka(city, relevant_data)
#         else:
#             print(f"Skipping {city} due to error.")
        
#         time.sleep(3)  # Wait for 1 minute before fetching the next data point to avoid hitting rate limits



import requests
import json
from confluent_kafka import Producer
from datetime import datetime
import time

# Kafka Configuration
KAFKA_BROKER = 'localhost:9092'
NEW_TOPIC = 'morocco_weather'  # New topic name

# Initialize Kafka producer
producer = Producer({'bootstrap.servers': KAFKA_BROKER})


# Function to convert temperature from Kelvin to Celsius
def kelvin_to_celsius(kelvin):
    return kelvin - 273.15


# Fetch weather data by city name from OpenWeather API
def fetch_weather_data(api_url, city_name):
    try:
        response = requests.get(api_url.format(city_name=city_name, api_key="cb48670d0cdde1f786a9c92e3d7b7eeb"))
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred for {city_name}: {err}")
    except requests.exceptions.RequestException as err:
        print(f"Request error for {city_name}: {err}")
    except Exception as err:
        print(f"An unexpected error occurred for {city_name}: {err}")
    return None


# Send data to Kafka topic
def send_to_kafka(city_name, data):
    try:
        # Send data to partition 0
        producer.produce(NEW_TOPIC, key=city_name, value=json.dumps(data), partition=0)
        producer.flush()  # Ensure the data is actually sent
        print(f"Sent data for {city_name} to topic {NEW_TOPIC} (Partition 0)")
    except Exception as e:
        print(f"Error sending data to Kafka: {e}")


# Main Producer Logic
if __name__ == '__main__':
    # API URL template to get weather data
    api_url = "http://api.openweathermap.org/data/2.5/weather?q={city_name}&appid={api_key}"
    city = 'Casablanca'

    while True:
        weather_data = fetch_weather_data(api_url, city)

        if weather_data:
            try:
                # Extract and convert relevant weather data
                relevant_data = {
                    'city': city,
                    'temperature_C': round(kelvin_to_celsius(weather_data['main'].get('temp')),2),  # Convert to Celsius
                    'humidity': weather_data['main'].get('humidity'),
                    'pressure': weather_data['main'].get('pressure'),
                    'weather_description': weather_data['weather'][0].get('description'),
                    'timestamp': datetime.now().isoformat()  # Current timestamp
                }
                send_to_kafka(city, relevant_data)

            except KeyError as ke:
                print(f"Missing key in the weather data response for {city}: {ke}")
            except Exception as e:
                print(f"Error processing weather data for {city}: {e}")
        else:
            print(f"Skipping {city} due to an error.")

        time.sleep(10)  # Wait for 3 seconds before fetching the next data point
