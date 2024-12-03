import paho.mqtt.client as mqtt
import time

class MQTTMonitoringService:
    """
    A class to represent an MQTT Monitoring Service that subscribes to a specific topic and processes incoming messages.

    Attributes:
        broker (str): The MQTT broker to connect to.
        port (int): The port number for the MQTT broker.
        topic (str): The topic to which the monitoring service will subscribe.
        log_file (str): The path to the log file where messages will be saved.
    """

    def __init__(self, broker="test.mosquitto.org", port=1883, topic="building/office/sensors", log_file="../out/subscriber_logs.txt"):
        """
        Initializes the MQTT Monitoring Service.

        Args:
            broker (str): The MQTT broker to connect to (default is "test.mosquitto.org").
            port (int): The port of the MQTT broker (default is 1883).
            topic (str): The topic to subscribe to (default is "building/office/sensors").
            log_file (str): The path to the log file where messages will be saved (default is "../out/subscriber_logs.txt").
        """
        self.broker = broker
        self.port = port
        self.topic = topic
        self.log_file = log_file
        # Specify the MQTT protocol version to avoid the unsupported callback API error
        self.client = mqtt.Client("MonitoringService", protocol=mqtt.MQTTv311)

    def connect(self):
        """
        Connects to the MQTT broker.

        Raises:
            ConnectionError: If unable to connect to the broker.
        """
        self.client.connect(self.broker, self.port)
        print(f"Connected to MQTT Broker at {self.broker}:{self.port}")

    def on_message(self, client, userdata, msg):
        """
        Callback function to handle incoming messages and save them to a log file.

        Args:
            client (paho.mqtt.client.Client): The MQTT client instance.
            userdata (any): User-defined data (not used here).
            msg (paho.mqtt.client.MQTTMessage): The MQTT message received.
        """
        message = f"Received message: {msg.payload.decode()} from topic: {msg.topic}"
        print(message)
        self.save_message_to_log(message)

    def save_message_to_log(self, message):
        """
        Saves the received message to a log file.

        Args:
            message (str): The message to be saved.
        """
        with open(self.log_file, "a") as log_file:
            log_file.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {message}\n")

    def start(self):
        """
        Connects to the broker and starts monitoring sensor data.

        Subscribes to the topic and listens indefinitely for incoming messages.
        This method will block indefinitely as it calls `loop_forever()`.
        """
        self.client.on_message = self.on_message
        self.connect()
        self.client.subscribe(self.topic)
        print(f"Subscribed to topic: {self.topic}")
        print("Monitoring service is listening for sensor data...")
        self.client.loop_forever()

if __name__ == "__main__":
    monitoring_service = MQTTMonitoringService()
    monitoring_service.start()