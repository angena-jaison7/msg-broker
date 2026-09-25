import pika
import logging
import json
from typing import Optional, Dict, Any

<<<<<<< HEAD

=======
>>>>>>> a36e0d681bcad20dca7451369e91d49f2787fef9
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class RabbitMQClient:
<<<<<<< HEAD
    """client to manage RabbitMQ connections and message publishing."""
=======
    """Client to manage RabbitMQ connections and message publishing."""
>>>>>>> a36e0d681bcad20dca7451369e91d49f2787fef9
    
    def __init__(self, host: str = 'localhost', port: int = 5672):
        self.host = host
        self.port = port
        self.connection: Optional[pika.BlockingConnection] = None
        self.channel = None

    def connect(self) -> None:
        """Establishes a connection to the RabbitMQ broker."""
        try:
<<<<<<< HEAD
            # Set up default guest credentials
=======
>>>>>>> a36e0d681bcad20dca7451369e91d49f2787fef9
            credentials = pika.PlainCredentials('admin', 'historian')
            parameters = pika.ConnectionParameters(
                host=self.host, 
                port=self.port, 
                credentials=credentials
            )
            
<<<<<<< HEAD
            # to Establish the connection and open a communication channel
=======
>>>>>>> a36e0d681bcad20dca7451369e91d49f2787fef9
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            
            logger.info(f"Successfully connected to RabbitMQ at {self.host}:{self.port}")
            
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            raise

    def publish(self, queue_name: str, payload: Dict[str, Any]) -> bool:
        """Publishes a JSON message to a specific queue."""
        if not self.channel:
            logger.warning("Channel is not open. Call connect() first.")
            return False
            
        try:
<<<<<<< HEAD
            # to ensure the queue exists before sending a message (durable=True means that it survives server restarts)
            self.channel.queue_declare(queue=queue_name, durable=True)
            
            # to Convert the Python dictionary to a standard JSON string
            message = json.dumps(payload)
=======
            # Ensures the queue exists before sending a message
            self.channel.queue_declare(queue=queue_name, durable=True)
            
            message = json.dumps(payload,default=str)
>>>>>>> a36e0d681bcad20dca7451369e91d49f2787fef9
            
            self.channel.basic_publish(
                exchange='',
                routing_key=queue_name,
                body=message,
                properties=pika.BasicProperties(
<<<<<<< HEAD
                    delivery_mode=2,  # 2 makes the message persistent on the disk
=======
                    delivery_mode=2,  # Persistent
>>>>>>> a36e0d681bcad20dca7451369e91d49f2787fef9
                )
            )
            logger.info(f"Message successfully published to '{queue_name}'")
            return True
            
        except Exception as e:
            logger.error(f"Failed to publish message: {e}")
            return False

    def close(self) -> None:
        """Safely closes the connection to prevent memory leaks."""
        if self.connection and self.connection.is_open:
            self.connection.close()
            logger.info("RabbitMQ connection closed safely.")

# --- Test Block ---
if __name__ == "__main__":
<<<<<<< HEAD
    # This block only runs if you execute this file directly in the terminal
=======
>>>>>>> a36e0d681bcad20dca7451369e91d49f2787fef9
    broker = RabbitMQClient()
    try:
        broker.connect()
        
<<<<<<< HEAD
        # Simulating sending a piece of sensor telemetry data
        test_data = {
            "sensor_id": "freezer_temp_01",
            "status": "warning",
            "reading": -78.5
=======
        # Simulating the exact SCADA payload specification
        test_data = {
            "recordId": "SCADA-EVT-2026-09-01-000001",
            "sourceEventId": "SCADA-EVT-2026-09-01-000001",
            "sourceSystem": "SCADA",
            "tenantId": "TNT-001",       
            "locationId": "LOC-001",
            "organizationId": "ORG-001",
            "instrumentId": "INS-001",
            "tagId": "TAG-001",
            "parameter": "Temperature",
            "value": 37.2,
            "unit": "C",
            "sourceTimestamp": "2026-09-01T09:30:00Z",
            "receivedTimestamp": "2026-09-01T09:30:02Z",
            "quality": "Good",
            "batchId": "BATCH-2026-001",
            "sampleId": None,
            "runId": "RUN-001",
            "metadata": {
                "dataType": "Numeric",
                "sourceQuality": "Good"
            }
>>>>>>> a36e0d681bcad20dca7451369e91d49f2787fef9
        }
        broker.publish(queue_name="telemetry_queue", payload=test_data)
        
    finally:
<<<<<<< HEAD
        # The 'finally' block ensures the connection closes even if an error crashes the script
=======
>>>>>>> a36e0d681bcad20dca7451369e91d49f2787fef9
        broker.close()