from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Dict
import json
from datetime import datetime

from src.main import RabbitMQClient

app = FastAPI(
    title="Admin Dashboard API",
    description="Backend API for the RabbitMQ telemetry broker.",
    version="1.2.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Nested model for the metadata block
class ScadaMetadata(BaseModel):
    dataType: str
    sourceQuality: str

# Main schema for the new SCADA payload
class ScadaTelemetryEvent(BaseModel):
    recordId: str
    sourceEventId: str
    sourceSystem: str    
    locationId: str
    organizationId: str
    instrumentId: str
    tagId: str
    parameter: str
    value: float
    unit: str
    sourceTimestamp: datetime
    receivedTimestamp: datetime
    quality: str
    batchId: str
    sampleId: Optional[str] = None  # Optional allows this to accept null values
    runId: str
    metadata: ScadaMetadata

def get_broker():
    """This creates a fresh, temporary connection for EVERY single web request."""
    temp_broker = RabbitMQClient()
    temp_broker.connect()
    try:
        yield temp_broker
    finally:
        temp_broker.close()

@app.get("/health")
def health_check():
    return {"status": "online", "system": "Telemetry API"}

@app.post("/api/publish/{queue_name}")
def publish_to_queue(queue_name: str, payload: ScadaTelemetryEvent, broker: RabbitMQClient = Depends(get_broker)):
    # Convert the validated Pydantic object back into a dictionary for RabbitMQ
    data_dict = payload.model_dump(mode='json')
    success = broker.publish(queue_name=queue_name, payload=data_dict)
    
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