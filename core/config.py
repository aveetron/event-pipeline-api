from pydantic import BaseModel, Field
import os
from typing import Optional


class Settings(BaseModel):
    # RabbitMQ settings
    rabbitmq_url: str = Field(default="amqp://pipeline:pipeline@localhost:5672/")
    exchange_name: str = Field(default="")
    queue_name: str = Field(default="pipeline_queue")
    routing_key: str = Field(default="pipeline")

    # ClickHouse Cloud settings
    clickhouse_host: str = Field(default="0.0.0.0")
    clickhouse_port: int = Field(default=8443)  # HTTPS port for ClickHouse Cloud
    clickhouse_database: str = Field(default="pipeline")
    clickhouse_username: str = Field(default="default")
    clickhouse_password: str = Field(default="default")
    clickhouse_secure: bool = Field(default=True)  # Always True for ClickHouse Cloud
    
    # Connection settings optimized for cloud
    clickhouse_timeout: int = Field(default=60)
    clickhouse_connect_timeout: int = Field(default=30)
    clickhouse_send_receive_timeout: int = Field(default=300)

    # SSL/TLS settings for ClickHouse Cloud
    clickhouse_verify_ssl: bool = Field(default=False)  # Set to False to skip SSL verification
    clickhouse_ca_cert: Optional[str] = Field(default="")  # Path to CA certificate if needed

    class Config:
        env_file = ".env"
        env_prefix = ""  # No prefix for environment variables


settings = Settings()