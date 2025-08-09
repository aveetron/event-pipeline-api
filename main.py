import asyncio
import json
from datetime import datetime
from typing import Dict

from fastapi import FastAPI, HTTPException, status, BackgroundTasks
from pydantic import BaseModel

from config.clickhouse import clickhouse_client
from config.rabbitmq import rabbitmq_client
from model.topic import TopicRequest, TopicResponse, ConsumerStatus

app = FastAPI()

# Global counters and state tracking
message_count = 0
processed_integrations = set()
service_status = {
    "consumer": "stopped",
    "clickhouse": "disconnected",
    "rabbitmq": "disconnected"
}

@app.get("/health", status_code=status.HTTP_200_OK)
async def health_check():
    """Health check endpoint for the API"""
    return {
        "name": "Data Pipeline Consumer API",
        "status": "healthy",
        "services": service_status
    }


"""
    whenever you application is starting
"""
@app.on_event("startup")
async def startup_event():
    """Initialize connections and start consumer on startup"""
    print("🔄 Starting up services...")
    
    # Initialize ClickHouse connection
    try:
        await clickhouse_client.connect()
        service_status["clickhouse"] = "connected"
        print("✅ Connected to ClickHouse")
    except Exception as e:
        service_status["clickhouse"] = "error"
        print(f"❌ Failed to connect to ClickHouse: {e}")
    
    # Start consumer in background
    asyncio.create_task(start_consumer())


@app.post("/publish-topic", response_model=TopicResponse)
async def publish_topic(request: TopicRequest):
    """
    Publish a topic JSON to RabbitMQ queue
    Consumer will process this JSON based on service_type
    """
    try:
        # Create message to publish using request data
        message_data = {
            "integration_id": request.integration_id,
            "service_type": request.service_type,
        }
        
        # Publish to RabbitMQ
        await rabbitmq_client.publish_message(message_data)
        
        return TopicResponse(
            message=f"Topic published successfully for integration {request.integration_id}",
            status="published"
        )
    except Exception as e:
        print(f"❌ Failed to publish message: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to publish message: {str(e)}"
        )


async def handle_qi_service(integration_id: str):
    """
    Handle QI service request:
    1. Fetch log data from ClickHouse based on integration_id
    2. Process and return the data
    """
    try:
        # Fetch log data from ClickHouse
        print(f"📊 Fetching QI data for integration: {integration_id}")
        log_data = await clickhouse_client.fetch_log_data_by_integration_id(integration_id)
        
        # Process the data
        result = {
            "integration_id": integration_id,
            "service": "qi",
            "data_count": len(log_data),
            "last_id": log_data[-1][0] if log_data else None,
            "timestamp": str(datetime.now())
        }
        
        print(f"✅ QI service processed {len(log_data)} records for integration: {integration_id}")
        return result
    except Exception as e:
        print(f"❌ Error in QI service for integration {integration_id}: {e}")
        return {
            "integration_id": integration_id,
            "service": "qi",
            "error": str(e),
            "timestamp": str(datetime.now())
        }

async def process_message(message):
    """
    Process message from RabbitMQ:
    1. Log the received message
    2. Route to appropriate service handler based on service_type
    3. Track processed messages and integrations
    """
    global message_count, processed_integrations
    
    try:
        # Decode and parse message
        message_body = json.loads(message.body.decode())
        message_count += 1
        
        # Print the received message
        print("=" * 80)
        print(f"📩 MESSAGE #{message_count} RECEIVED:")
        print("=" * 80)
        print(json.dumps(message_body, indent=2))
        print("=" * 80)
        
        # Extract message details
        integration_id = message_body.get("integration_id")
        service_type = message_body.get("service_type")
        
        # Log the service type
        print(f"📋 Processing message for service type: {service_type}")
        
        # Route to appropriate service handler
        if service_type == "qi":
            await handle_qi_service(integration_id)
        else:
            print(f"⚠️ Unsupported service type: {service_type}")
        
        # Track processed integration
        if integration_id:
            processed_integrations.add(integration_id)
        
        # Acknowledge message
        await message.ack()
        
    except Exception as e:
        print(f"❌ Error processing message: {e}")
        # Reject message on error
        await message.nack()

async def start_consumer():
    """Start consuming messages from RabbitMQ"""
    global service_status
    
    try:
        print("🚀 Starting RabbitMQ consumer...")
        await rabbitmq_client.connect()
        service_status["rabbitmq"] = "connected"
        print("✅ Connected to RabbitMQ")
        
        service_status["consumer"] = "running"
        await rabbitmq_client.consume_messages(process_message)
    except Exception as e:
        service_status["rabbitmq"] = "error"
        service_status["consumer"] = "error"
        print(f"❌ Failed to start consumer: {e}")


# End of consumer functionality
@app.get("/consumer-status", response_model=ConsumerStatus)
async def get_consumer_status():
    """Get detailed consumer status"""
    return ConsumerStatus(
        service="Data Pipeline Consumer",
        status=service_status,
        messages_processed=message_count
    )


@app.post("/service/qi")
async def trigger_qi_service(request: TopicRequest):
    """Manually trigger the QI service"""
    try:
        # Call the QI service handler
        result = await handle_qi_service(request.integration_id)
        
        return result
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process QI service request: {str(e)}"
        )

@app.post("/restart-consumer")
async def restart_consumer(background_tasks: BackgroundTasks):
    """Restart the consumer service"""
    global service_status
    
    try:
        # Disconnect existing connections
        service_status["consumer"] = "restarting"
        await rabbitmq_client.disconnect()
        
        # Start consumer in background
        background_tasks.add_task(start_consumer)
        
        return {"message": "Consumer restart initiated", "status": "restarting"}
    except Exception as e:
        service_status["consumer"] = "error"
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to restart consumer: {str(e)}"
        )

@app.on_event("shutdown")
async def shutdown_event():
    """Close connections on shutdown"""
    print("🔄 Shutting down services...")
    
    # Disconnect from RabbitMQ
    try:
        await rabbitmq_client.disconnect()
        print("✅ Disconnected from RabbitMQ")
    except Exception as e:
        print(f"❌ Error disconnecting from RabbitMQ: {e}")
    
    # Disconnect from ClickHouse
    try:
        await clickhouse_client.disconnect()
        print("✅ Disconnected from ClickHouse")
    except Exception as e:
        print(f"❌ Error disconnecting from ClickHouse: {e}")
    
    print("✅ Shutdown complete")
