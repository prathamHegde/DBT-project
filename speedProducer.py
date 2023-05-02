import json
import requests
from kafka import KafkaProducer 
from time import sleep 
import math
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Initialize variables to store previous latitude, longitude, and timestamp
prev_lat, prev_lon, prev_time = None, None, None

for i in range(50):
    # Retrieve ISS location data from API
    res = requests.get('http://api.open-notify.org/iss-now.json')
    data = json.loads(res.content.decode('utf-8'))

    # Parse the latitude, longitude, and timestamp fields
    curr_lat = float(data['iss_position']['latitude'])
    curr_lon = float(data['iss_position']['longitude'])
    curr_time = data['timestamp']

    # Calculate the distance between the previous and current latitude-longitude coordinates
    if prev_lat is not None and prev_lon is not None:
        prev_lat_radians = math.radians(prev_lat)
        prev_lon_radians = math.radians(prev_lon)
        curr_lat_radians = math.radians(curr_lat)
        curr_lon_radians = math.radians(curr_lon)

        # Calculate the distance using the Haversine formula
        earth_radius = 6371.01  # kilometers
        delta_lon = curr_lon_radians - prev_lon_radians
        delta_lat = curr_lat_radians - prev_lat_radians
        a = math.sin(delta_lat / 2) ** 2 + \
            math.cos(prev_lat_radians) * math.cos(curr_lat_radians) * \
            math.sin(delta_lon / 2) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        distance = earth_radius * c  # kilometers

        # Calculate the time elapsed between the previous and current timestamps
        time_elapsed = curr_time - prev_time  # seconds

        # Calculate the speed of the ISS
        speed = distance / time_elapsed * 3600  # kilometers per hour

        # Add the speed field to the ISS location data
        data['iss_speed'] = speed
        data['iss_distance'] = distance
        data['iss_time_elapsed'] = time_elapsed
        print(data);

    # Send the ISS location data to the Kafka topic
    producer.send('speed', value=data)

    # Update the previous latitude, longitude, and timestamp variables
    prev_lat, prev_lon, prev_time = curr_lat, curr_lon, curr_time

    sleep(5)
    producer.flush()

