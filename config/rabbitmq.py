import asyncio
import json
from typing import Callable, Optional

import aio_pika

from core.config import settings


class RabbitMQClient:
    def __init__(self):
        self.connection: Optional[aio_pika.Connection] = None
        self.channel: Optional[aio_pika.Channel] = None
        self.exchange: Optional[aio_pika.Exchange] = None
        self.queue: Optional[aio_pika.Queue] = None

    async def connect(self):
        """Establish connection to RabbitMQ"""
        try:
            self.connection = await aio_pika.connect_robust(settings.rabbitmq_url)
            self.channel = await self.connection.channel()
            
            # Set QoS to process one message at a time for better resource management
            await self.channel.set_qos(prefetch_count=1)
            
            # Declare exchange (use default exchange if name is empty)
            if settings.exchange_name:
                self.exchange = await self.channel.declare_exchange(
                    settings.exchange_name,
                    aio_pika.ExchangeType.DIRECT,
                    durable=True
                )
            else:
                self.exchange = self.channel.default_exchange
            
            # Declare queue with TTL for failed messages
            self.queue = await self.channel.declare_queue(
                settings.queue_name,
                durable=True,
                arguments={
                    "x-message-ttl": 86400000,  # 24 hours TTL
                    "x-max-retries": 3
                }
            )
            
            # Bind queue to exchange (only if using named exchange)
            if settings.exchange_name:
                await self.queue.bind(self.exchange, settings.routing_key)
                
        except Exception as e:
            print(f"Failed to connect to RabbitMQ: {e}")
            raise

    async def disconnect(self):
        """Close RabbitMQ connection"""
        if self.connection:
            await self.connection.close()

    async def publish_message(self, topic_data: dict, priority: int = 5):
        """Publish message to queue"""
        try:
            message = aio_pika.Message(
                json.dumps(topic_data).encode(),
                priority=priority,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                headers={
                    "content_type": "application/json",
                    "topic_id": topic_data.get("topic_id"),
                    "service": topic_data.get("service")
                }
            )
            
            # Use routing_key as queue name for default exchange
            routing_key = settings.queue_name if not settings.exchange_name else settings.routing_key
            
            await self.exchange.publish(
                message,
                routing_key=routing_key
            )
        except Exception as e:
            print(f"Failed to publish message: {e}")
            raise

    async def consume_messages(self, callback: Callable):
        """Start consuming messages from queue"""
        try:
            await self.queue.consume(callback)
            print(f"🎯 Started consuming messages from queue: {settings.queue_name}")
            
            # Keep the consumer running
            try:
                await asyncio.Future()  # Run forever
            except asyncio.CancelledError:
                print("Consumer cancelled")
                
        except Exception as e:
            print(f"Failed to consume messages: {e}")
            raise

    # Keep the old method name for backwards compatibility
    async def consume_topics(self, callback: Callable):
        """Legacy method - redirects to consume_messages"""
        await self.consume_messages(callback)


# Create global RabbitMQ client instance
rabbitmq_client = RabbitMQClient()