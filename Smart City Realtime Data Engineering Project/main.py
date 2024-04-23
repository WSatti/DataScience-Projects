import os
import random
import uuid

from confluent_kafka import SerializingProducer
import simplejson as json
from datetime import datetime, timedelta

LONDON_COORDINATES = {"latitude": 51.5074, "longitude": -0.1278}
BIRMINGHAM_COORDINATES = {"latitude": 52.4862, "longitude": -1.8904}

LATITUDE_INCREMENT = (BIRMINGHAM_COORDINATES["latitude"] - LONDON_COORDINATES["latitude"])
LONGITUDE_INCREMENT = (BIRMINGHAM_COORDINATES["longitude"] - LONDON_COORDINATES["longitude"])

# Envoirnment variable for configuration

KAFKA_BOOTSTRAP_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVER', 'localhost:9092')
VEHiCLE_TOPIC = os.getenv('VEHiCLE_TOPIC', 'vehicle_data')
GPS_TOPIC = os.getenv('GPS_TOPIC', 'gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'traffic_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'emer_data')

random.seed(42)
start_time = datetime.now()
start_location = LONDON_COORDINATES.copy()


def get_next_time():
    global start_time
    start_time += timedelta(seconds=random.randint(30, 60))
    return start_time


def simulate_vehicle_movement():
    global start_location
    start_location["latitude"] = start_location["latitude"] + LATITUDE_INCREMENT
    start_location["longitude"] = start_location["longitude"] + LONGITUDE_INCREMENT

    start_location["latitude"] += random.uniform(-0.0005, 0.0005)
    start_location["longitude"] += random.uniform(-0.0005, 0.0005)
    return start_location


def generate_vehicle_data(device_id):
    location = simulate_vehicle_movement()
    return {
        'id': uuid.uuid4(),
        'device_id': device_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location["latitude"], location["longitude"]),
        'speed': random.randint(10, 40),
        'direction': 'North-East',
        'make': 'BMW',
        'model': 'C500',
        'year': 2024,
        'fuel_type': 'hybrid'
    }


def generate_gps_data(device_id, timestamp, vehcile_type='private'):
    return {
        'id': uuid.uuid4(),
        'device_Id': device_id,
        'timestamp': timestamp,
        'vehicle_type': vehcile_type,
        'speed': random.randint(10, 40),
        'direction': 'North-East'
    }


def generate_traffic_camera_ata(device_id, timestamp, location, camera_id):
    return {
        'id': uuid.uuid4(),
        'device_Id': device_id,
        'timestamp': timestamp,
        'cameraId': camera_id,
        'snapshot': 'Base64EncodedString',
        'location': location
    }


def generate_weather_data(device_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'device_Id': device_id,
        'timestamp': timestamp,
        'location': location,
        'temperature': random.uniform(-5, 25),
        'weatherCondition': random.choice(['rainy', 'snow', 'thunderstorm']),
        'precipitation': random.uniform(0, 25),
        'windSpeed': random.uniform(0, 100)

    }


def generate_emergency_incident_data(device_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'device_Id': device_id,
        'timestamp': timestamp,
        'location': location,
        'incidentId': uuid.uuid4(),
        'type': random.choice(['accident', 'fire', 'medical']),
        'status': random.choice(['open', 'closed']),
        'description': 'description of the incident'
    }


def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    else:
        raise TypeError(f'Type {type(obj.__class__.__name__)} not serializable')


def delivery_report(err, msg):
    if err is not None:
        print(f'Failed to deliver message: {err}')
    else:
        print(f'Successfully delivered message: {msg}')


def produce_data_to_kafka(producer, topic, data):
    producer.produce(
        topic,
        key=str(data['id']),
        value=json.dumps(data, default=json_serializer).encode('utf-8'),
        on_delivery=delivery_report
    )
    producer.flush()


def simulate_journey(producer, device_id):
    while True:
        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id, vehicle_data['timestamp'])
        traffic_camera_data = generate_traffic_camera_ata(device_id, vehicle_data['timestamp'],
                                                          vehicle_data['location'], 'Camera123')
        weather_data = generate_weather_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])
        emergency_incident_data = generate_emergency_incident_data(device_id, vehicle_data['timestamp'],
                                                                   vehicle_data['location'])

        produce_data_to_kafka(producer, VEHiCLE_TOPIC, vehicle_data)
        produce_data_to_kafka(producer, GPS_TOPIC, gps_data)
        produce_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_camera_data)
        produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)
        produce_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_incident_data)

        #time.sleep(5)
        import time

        try:
            time.sleep(5)
            print("Sleep successful!")
        except Exception as e:
            print(f"Exception: {e}")


if __name__ == "__main__":

    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVER,
        # 'err_cb': lambda err: print(f'Kafka error:{err}')  # Remove this line
        'error_cb': lambda err: print(f'Kafka error:{err}')  # Use error_cb instead
    }

    producer = SerializingProducer(producer_config)

    try:
        simulate_journey(producer, 'vehicle-CodeWithYu-123')
    except KeyboardInterrupt:
        print('Simulation stopped by user.')
    except Exception as e:
        print(f'Exception: {e}')



