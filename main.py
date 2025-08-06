import asyncio
import datetime
import json
import uuid
from fastapi import FastAPI, HTTPException, status
from config.rabbitmq import rabbitmq_client
from model.topic import TopicRequest, TopicResponse

app = FastAPI()

# Global message counter
message_count = 0

@app.get("/health", status_code=status.HTTP_200_OK)
async def health_check():
    return {
        "name": "Data Pipeline Consumer API",
        "status": "healthy"
    }

@app.post("/publish-topic", response_model=TopicResponse)
async def publish_topic(request: TopicRequest):
    """
    Publish a topic JSON to RabbitMQ queue
    Consumer will just print this JSON
    """
    try:
        # Generate unique topic ID
        topic_id = str(uuid.uuid4())
        current_time = datetime.datetime.now()
        
        # Create message to publish
        message_data = {
            "topic_id": topic_id,
            "service": request.service,
            "date_from": request.date_from,
            "date_to": request.date_to,
            "parameters": request.parameters or {},
            "published_at": current_time.isoformat()
        }
        
        # Publish to RabbitMQ
        await rabbitmq_client.publish_message(message_data)
        
        return TopicResponse(
            message="Topic published successfully",
            topic_id=topic_id,
            status="published",
            submitted_at=current_time
        )
        
    except Exception as e:
        print(f"❌ Failed to publish message: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def process_message(message):
    """
    Process message from RabbitMQ - just print the JSON
    """
    global message_count
    
    try:
        # Decode and parse message
        message_body = json.loads(message.body.decode())
        message_count += 1
        
        # Print the received JSON
        print("=" * 60)
        print(f"📩 MESSAGE #{message_count} RECEIVED:")
        print("=" * 60)
        print(json.dumps(message_body, indent=2))
        print("=" * 60)
        
        # Acknowledge message
        await message.ack()
        
    except json.JSONDecodeError as e:
        print(f"❌ Failed to decode JSON: {e}")
        await message.nack()
    except Exception as e:
        print(f"❌ Error processing message: {e}")
        await message.nack()

async def start_consumer():
    """Start consuming messages from RabbitMQ"""
    try:
        print("🚀 Starting RabbitMQ consumer...")
        await rabbitmq_client.connect()
        print("✅ Connected to RabbitMQ")
        await rabbitmq_client.consume_messages(process_message)
    except Exception as e:
        print(f"❌ Failed to start consumer: {e}")

@app.on_event("startup")
async def startup_event():
    """Initialize RabbitMQ connection and start consumer on startup"""
    print("🔄 Starting up consumer service...")
    asyncio.create_task(start_consumer())

@app.on_event("shutdown")
async def shutdown_event():
    """Close RabbitMQ connection on shutdown"""
    print("🔄 Shutting down consumer service...")
    await rabbitmq_client.disconnect()
    print("✅ Disconnected from RabbitMQ")

@app.get("/message-count")
async def get_message_count():
    """Get count of messages processed"""
    return {
        "messages_processed": message_count,
        "status": "active"
    }

@app.get("/consumer-status")
async def get_consumer_status():
    """Get consumer status"""
    return {
        "service": "Data Pipeline Consumer",
        "status": "running",
        "messages_processed": message_count
    }