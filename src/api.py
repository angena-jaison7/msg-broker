from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import json

from src.main import RabbitMQClient

app = FastAPI(
    title="Admin Dashboard API",
    description="Backend API for the RabbitMQ telemetry broker.",
    version="1.1.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class TelemetryData(BaseModel):
    sensor_id: str
    status: str
    reading: float

# --- Dependency Injection part ---
# FastAPI's Dependency Injection 
# (Depends) to manage the RabbitMQ connection lifecycle.
# hence This opens a 
# dedicated broker connection for each incoming HTTP request
def get_broker():
    """This creates a fresh, temporary connection for EVERY single web request."""
    temp_broker = RabbitMQClient()
    temp_broker.connect()
    try:
        # Hand the temporary broker to the endpoint
        yield temp_broker
    finally:
        # Always safely close it when the request is finished!
        temp_broker.close()

# -------------------------------------

@app.get("/health")
def health_check():
    return {"status": "online", "system": "Telemetry API"}

# Notice we added `broker: RabbitMQClient = Depends(get_broker)` to the arguments
@app.post("/api/publish/{queue_name}")
def publish_to_queue(queue_name: str, payload: TelemetryData, broker: RabbitMQClient = Depends(get_broker)):
    data_dict = payload.model_dump()
    success = broker.publish(queue_name=queue_name, payload=data_dict)
    #if a sensor sends data to /api/publish/freezer_alerts, 
    # the queue_name variable becomes "freezer_alerts"
    
    if not success:
        raise HTTPException(status_code=500, detail="Failed to publish message to broker.")
        
    return {"status": "success", "message": f"Data sent to {queue_name}"}

@app.get("/api/messages/{queue_name}")
def get_latest_message(queue_name: str, broker: RabbitMQClient = Depends(get_broker)):
    if not broker.channel:
        raise HTTPException(status_code=500, detail="Broker connection is not active.")
        
    method_frame, header_frame, body = broker.channel.basic_get(queue=queue_name, auto_ack=True)
    
    if method_frame:
        message_data = json.loads(body)
        return {"messages_remaining": method_frame.message_count, "data": message_data}
    else:
        return {"messages_remaining": 0, "data": None, "message": "Queue is empty."}