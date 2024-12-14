# import json
# from confluent_kafka import Consumer, KafkaException
# import matplotlib.pyplot as plt
# import matplotlib.animation as animation
# import matplotlib.dates as mdates
# from datetime import datetime

# KAFKA_BROKER = 'localhost:9092'
# NEW_TOPIC = 'morocco_weather'

# # Initialize Kafka Consumer
# consumer = Consumer({
#     'bootstrap.servers': KAFKA_BROKER,
#     'group.id': 'weather_plot_consumer',
#     'auto.offset.reset': 'latest'
# })

# consumer.subscribe([NEW_TOPIC])

# # Data storage
# timestamps = []
# temperatures = []

# def update_plot(frame):
#     try:
#         msg = consumer.poll(1.0)

#         if msg is None:
#             return

#         if msg.error():
#             print(f"Consumer error: {msg.error()}")
#             return

#         # Decode Kafka message to JSON
#         data = json.loads(msg.value().decode('utf-8'))

#         city = data.get('city', 'Unknown')
#         temperature = data.get('temperature_C')
#         timestamp = data.get('timestamp')

#         if temperature is not None and timestamp:
#             # Correctly parse the timestamp with microseconds
#             try:
#                 timestamp = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f")
#             except ValueError:
#                 print(f"Invalid timestamp format: {timestamp}")
#                 return

#             # Append new data to lists
#             timestamps.append(timestamp)
#             temperatures.append(float(temperature))

#             # Maintain only the last 7 data points
#             if len(timestamps) > 7:
#                 timestamps.pop(0)
#                 temperatures.pop(0)

#             # Clear and redraw the plot
#             plt.cla()
#             plt.plot(timestamps, temperatures, label=f"{city}'s Temperature", color='b')

#             plt.title(f'Real-time Temperature for {city}')
#             plt.xlabel('Timestamp')
#             plt.ylabel('Temperature (°C)')

#             # Use date formatting for better readability on the x-axis
#             plt.gca().xaxis.set_major_locator(mdates.MinuteLocator(interval=1))
#             plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))


#             plt.xticks(rotation=45)
#             plt.legend()

#     except KafkaException as e:
#         print(f"Kafka error: {e}")
#     except json.JSONDecodeError:
#         print("Received an invalid JSON message.")

# # Plot configuration
# fig = plt.figure(figsize=(10, 6))

# # Use matplotlib's animation function to update the plot every 2 seconds
# ani = animation.FuncAnimation(fig, update_plot, interval=2000)

# # Show the plot window
# plt.show()













import json
from datetime import datetime
from confluent_kafka import Consumer, KafkaException
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import matplotlib.dates as mdates

KAFKA_BROKER = 'localhost:9092'
NEW_TOPIC = 'morocco_weather'

consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'weather_plot_consumer',
    'auto.offset.reset': 'latest'
})

consumer.subscribe([NEW_TOPIC])

# Listes pour stocker les données
timestamps = []
temperatures = []
humidities = []
pressures = []
weather_descriptions = []

# Création de la figure et des sous-graphiques une seule fois
fig, axs = plt.subplots(3, 1, figsize=(10, 8))

def update_plot(frame):
    try:
        msg = consumer.poll(1.0)

        if msg is None:
            return

        if msg.error():
            print(f"Consumer error: {msg.error()}")
            return

        # Décodage du message Kafka en JSON
        data = json.loads(msg.value().decode('utf-8'))

        city = data.get('city', 'Unknown')
        temperature = data.get('temperature_C')
        humidity = data.get('humidity')
        pressure = data.get('pressure')
        timestamp = data.get('timestamp')

        if temperature and humidity and pressure and timestamp:
            try:
                # Correction de la mise en forme des timestamps avec microsecondes
                timestamp = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f")
            except ValueError:
                print(f"Invalid timestamp format: {timestamp}")
                return

            # Mise à jour des listes
            timestamps.append(timestamp)
            temperatures.append(float(temperature))
            humidities.append(float(humidity))
            pressures.append(int(pressure))
            weather_descriptions.append(data.get('weather_description'))

            # Conserver uniquement les dernières 7 valeurs
            if len(timestamps) > 7:
                timestamps.pop(0)
                temperatures.pop(0)
                humidities.pop(0)
                pressures.pop(0)
                weather_descriptions.pop(0)

            # Mise à jour des graphiques existants sans recréer la figure
            axs[0].cla()
            axs[0].plot(timestamps, temperatures, label=f"{city}'s Temperature", color='b')
            axs[0].set_title(f'Temperature {city}')
            axs[0].set_ylabel('°C')
            axs[0].xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
            axs[0].legend()

            axs[1].cla()
            axs[1].plot(timestamps, humidities, label=f"{city}'s Humidity", color='g')
            axs[1].set_title(f'Humidity {city}')
            axs[1].set_ylabel('%')
            axs[1].xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
            axs[1].legend()

            axs[2].cla()
            axs[2].plot(timestamps, pressures, label=f"{city}'s Pressure", color='r')
            axs[2].set_title(f'Pressure {city}')
            axs[2].set_ylabel('hPa')
            axs[2].xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
            axs[2].legend()

            plt.tight_layout()

    except KafkaException as e:
        print(f"Kafka error: {e}")
    except json.JSONDecodeError:
        print("Received an invalid JSON message.")


# Animation en appelant la fonction `update_plot` toutes les 2000 ms
ani = animation.FuncAnimation(fig, update_plot, interval=2000)

plt.show()
