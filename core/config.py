from pydantic import BaseModel


class Settings(BaseModel):
    rabbitmq_url: str = "amqp://pipeline:pipeline@localhost:5672/"
    exchange_name: str = ""
    queue_name: str = "pipeline_queue"
    routing_key: str = "pipeline"

    class Config:
        env_file = ".env"


settings = Settings()