import paho.mqtt.client as mqtt
import time
import random

class MQTTSensorPublisher:
    """
    A class to represent an MQTT Sensor Publisher that simulates sensor data and sends it to an MQTT broker.

    Attributes:
        broker (str): The MQTT broker to connect to.
        port (int): The port of the MQTT broker.
        topic (str): The topic to which sensor data is published.
        log_file (str): The path to the log file where messages will be saved.
        client (mqtt.Client): The MQTT client used for communication.
    """

    def __init__(self, broker="test.mosquitto.org", port=1883, topic="building/office/sensors", log_file="../out/publisher_logs.txt"):
        """
        Initializes the MQTT Sensor Publisher.

        Args:
            broker (str): The MQTT broker to connect to (default is "test.mosquitto.org").
            port (int): The port of the MQTT broker (default is 1883).
            topic (str): The topic to publish sensor data to (default is "building/office/sensors").
            log_file (str): The path to the log file where messages will be saved (default is "publisher_logs.txt").
        """
        self.broker = broker
        self.port = port
        self.topic = topic
        self.log_file = log_file
        # Specify the MQTT version explicitly to avoid the unsupported callback API error
        self.client = mqtt.Client("SensorPublisher", protocol=mqtt.MQTTv311)

    def connect(self):
        """
        Connects to the MQTT broker.

        Raises:
            ConnectionError: If unable to connect to the broker.
        """
        self.client.connect(self.broker, self.port)
        print(f"Connected to MQTT Broker at {self.broker}:{self.port}")

    def get_sensor_data(self):
        """
        Simulates sensor data for temperature, humidity, and CO2 concentration.

        Returns:
            dict: A dictionary containing simulated values for temperature, humidity, and CO2 concentration.
        """
        temperature = round(random.uniform(20.0, 25.0), 2)
        humidity = round(random.uniform(30.0, 50.0), 2)
        co2 = round(random.uniform(400, 600), 2)
        return {"temperature": temperature, "humidity": humidity, "co2": co2}

    def save_message_to_log(self, message):
        """
        Saves the published message to a log file.

        Args:
            message (str): The message to be saved.
        """
        with open(self.log_file, "a") as log_file:
            log_file.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {message}\n")

    def publish_data(self, sampling_rate=60, duration=120):
        """
        Publishes sensor data at regular intervals for a given duration.

        Args:
            sampling_rate (int): The interval between each data publication in seconds (default is 60 seconds).
            duration (int): The total number of times to publish data (default is 120 times).

        Raises:
            RuntimeError: If an error occurs during data publication.
        """
        for _ in range(duration):
            data = self.get_sensor_data()
            payload = f"Temperature: {data['temperature']}°C, Humidity: {data['humidity']}%, CO2: {data['co2']}ppm"
            self.client.publish(self.topic, payload)
            print(f"Published: {payload}")
            self.save_message_to_log(payload)  # Save the published message to the log file
            time.sleep(sampling_rate)  # Sleep for the sampling rate (1 minute = 60 seconds)

    def start(self):
        """
        Connects to the MQTT broker and starts publishing sensor data.

        Connects to the broker, publishes sensor data at regular intervals, and then disconnects from the broker.
        
        Raises:
            RuntimeError: If unable to publish or disconnect from the broker.
        """
        self.connect()
        self.publish_data()
        self.client.disconnect()
        print("Disconnected from MQTT Broker.")

if __name__ == "__main__":
    publisher = MQTTSensorPublisher()
    publisher.start()